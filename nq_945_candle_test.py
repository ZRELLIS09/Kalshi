"""
9:45 5m Candle Tests
====================

Three things the prior studies did NOT measure about the 9:45 candle:

  TEST A: 9:45 high / low / CE as reversal LEVELS on subsequent days
          (parallel to the 9AM FVG first-touch test)

  TEST B: 9:45 candle as a same-day pivot / breakout
          - did price break above 9:45_high or below 9:45_low post-10:00?
          - which break came first, and did the breakout direction predict
            the post-10:00 move?

  TEST C: 9:45 'manipulation/expansion' pattern (per @LimitlessgoatMi)
          A 9:45 candle with a dominant wick OPPOSITE to its body color is
          a sweep-then-reverse signature. We classify and test whether the
          BODY direction (not the wick direction) predicts post-10:00 move.

Direction expectations:
  - Touch of 9:45 HIGH from BELOW: expected = reverse DOWN (resistance)
  - Touch of 9:45 HIGH from ABOVE: expected = reverse UP   (support after
    failure, level flip)
  - Same for 9:45 LOW (mirrored)
  - Reversal threshold = 10 pts in expected direction within 60 min.

Usage:
  export DATABENTO_API_KEY=db-...
  python3 nq_945_candle_test.py --cache mnq_1m_clean.csv --outdir out_945
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd

from nq_range_alignment_backtest import (
    load_databento_mnq_1m, build_daily_table, NY_TZ
)
from nq_9am_fvg_backtest import _two_prop_z, _one_prop_z

REACTION_MIN = 60
REACTION_PTS = 10.0
LEVEL_TOL_PTS = 2.0  # tolerance window for level touch
LOOKAHEAD_DAYS = 20


def extract_945_candles(df_1m: pd.DataFrame, daily: pd.DataFrame) -> pd.DataFrame:
    """Daily 9:45-9:49 5m candle: O, H, L, C, body_dir, wick analysis."""
    rows = []
    for d in daily.index:
        start = pd.Timestamp(d.date(), tz=NY_TZ).replace(hour=9, minute=45)
        end = start + pd.Timedelta(minutes=5)
        sl = df_1m.loc[start:end - pd.Timedelta(seconds=1)]
        if len(sl) < 3:
            continue
        o = float(sl["open"].iloc[0]); c = float(sl["close"].iloc[-1])
        h = float(sl["high"].max()); l = float(sl["low"].min())
        body_dir = 1 if c > o else (-1 if c < o else 0)
        body_size = abs(c - o)
        upper_wick = h - max(o, c)
        lower_wick = min(o, c) - l
        # manipulation pattern: dominant wick opposite to body color
        is_manip = False
        manip_dir = 0
        if body_dir > 0 and lower_wick > 1.5 * max(upper_wick, 0.1) and lower_wick > body_size:
            is_manip = True; manip_dir = 1   # swept down, closed up
        elif body_dir < 0 and upper_wick > 1.5 * max(lower_wick, 0.1) and upper_wick > body_size:
            is_manip = True; manip_dir = -1  # swept up, closed down
        rows.append({
            "date": d, "o": o, "h": h, "l": l, "c": c, "ce": (h + l) / 2,
            "body_dir": body_dir, "body_size": body_size,
            "upper_wick": upper_wick, "lower_wick": lower_wick,
            "range_pts": h - l, "is_manip": is_manip, "manip_dir": manip_dir,
        })
    out = pd.DataFrame(rows).set_index("date")
    return out


# ---------------------------------------------------------------------------
# TEST A: 9:45 high/low/CE first-touch on subsequent days
# ---------------------------------------------------------------------------

def find_level_first_touches(df_1m: pd.DataFrame, level_price: float,
                             form_ts: pd.Timestamp, search_end: pd.Timestamp,
                             tol: float = LEVEL_TOL_PTS,
                             lookahead_days: int = LOOKAHEAD_DAYS,
                             max_touches: int = 6) -> list[dict]:
    """Find up to max_touches discrete touch events of [price-tol, price+tol]
    on days AFTER form_ts. Returns list of touch dicts with approach dir.
    """
    next_day = pd.Timestamp(form_ts.date(), tz=NY_TZ) + pd.Timedelta(days=1)
    end = min(search_end, next_day + pd.Timedelta(days=lookahead_days))
    sl = df_1m.loc[next_day:end]
    if sl.empty:
        return []
    in_zone = ((sl["high"] >= level_price - tol) & (sl["low"] <= level_price + tol)).to_numpy()
    if not in_zone.any():
        return []
    pad_prev = np.concatenate([[False], in_zone[:-1]])
    enters = in_zone & ~pad_prev
    enter_idx = np.where(enters)[0]
    out = []
    for ei in enter_idx[:max_touches]:
        ts = sl.index[ei]
        # Approach direction: was the prior bar above or below the level?
        prior_idx = ei - 1
        if prior_idx < 0 or pd.isna(sl["close"].iloc[prior_idx]):
            approach = "unknown"
        else:
            prior_close = float(sl["close"].iloc[prior_idx])
            approach = "from_above" if prior_close > level_price else "from_below"
        out.append({"touch_ts": ts, "approach": approach})
    return out


def measure_level_reaction(df_1m: pd.DataFrame, ts: pd.Timestamp,
                           level_price: float, approach: str,
                           react_min: int = REACTION_MIN,
                           react_pts: float = REACTION_PTS) -> dict:
    """Reversal direction depends on approach: from_below = expect reject DOWN,
    from_above = expect bounce UP."""
    react_end = ts + pd.Timedelta(minutes=react_min)
    react = df_1m.loc[ts:react_end]
    if react.empty:
        return {"reversed": False, "mfe": np.nan, "mae": np.nan}
    px = float(df_1m.loc[ts, "close"])
    mfe_up = float(react["high"].max() - px)
    mfe_dn = float(px - react["low"].min())
    if approach == "from_below":
        reversed_ = mfe_dn >= react_pts
        mfe, mae = mfe_dn, -mfe_up
    elif approach == "from_above":
        reversed_ = mfe_up >= react_pts
        mfe, mae = mfe_up, -mfe_dn
    else:
        reversed_ = False; mfe = np.nan; mae = np.nan
    return {"reversed": bool(reversed_), "mfe": mfe, "mae": mae}


def run_test_a(df_1m: pd.DataFrame, candles: pd.DataFrame) -> pd.DataFrame:
    """Build long-format touch table for 9:45 high, low, CE across all days."""
    rows = []
    search_end = df_1m.index.max()
    for date, c in candles.iterrows():
        form_ts = pd.Timestamp(date.date(), tz=NY_TZ).replace(hour=9, minute=49, second=59)
        for label, lvl in [("h", c["h"]), ("l", c["l"]), ("ce", c["ce"])]:
            touches = find_level_first_touches(df_1m, lvl, form_ts, search_end)
            for ord_, t in enumerate(touches, start=1):
                r = measure_level_reaction(df_1m, t["touch_ts"], lvl, t["approach"])
                rows.append({
                    "fvg_date": date.date(),
                    "level": label, "level_price": lvl,
                    "touch_ordinal": ord_,
                    "touch_ts": t["touch_ts"],
                    "touch_date": t["touch_ts"].date(),
                    "touch_hour": t["touch_ts"].hour,
                    "days_since": (t["touch_ts"].date() - date.date()).days,
                    "approach": t["approach"],
                    "candle_dir": int(c["body_dir"]),
                    "is_manip": bool(c["is_manip"]),
                    **r,
                })
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# TEST B: same-day pivot / first breakout
# ---------------------------------------------------------------------------

def run_test_b(df_1m: pd.DataFrame, candles: pd.DataFrame,
               daily: pd.DataFrame) -> pd.DataFrame:
    """For each session, after the 9:45 candle closes, scan post-9:50:
    which side breaks first (above 9:45_high or below 9:45_low)?
    Then measure post-break move to 13:00.
    """
    rows = []
    for date, c in candles.iterrows():
        if date not in daily.index:
            continue
        start = pd.Timestamp(date.date(), tz=NY_TZ).replace(hour=9, minute=50)
        end = pd.Timestamp(date.date(), tz=NY_TZ).replace(hour=13, minute=0)
        sl = df_1m.loc[start:end - pd.Timedelta(seconds=1)]
        if sl.empty:
            continue
        up_mask = sl["high"] > c["h"]
        dn_mask = sl["low"] < c["l"]
        first_up = sl.index[up_mask][0] if up_mask.any() else None
        first_dn = sl.index[dn_mask][0] if dn_mask.any() else None
        if first_up is None and first_dn is None:
            continue  # never broke either side by 13:00
        if first_up is not None and (first_dn is None or first_up < first_dn):
            break_dir = 1; break_ts = first_up; break_px = c["h"]
        else:
            break_dir = -1; break_ts = first_dn; break_px = c["l"]
        # post-break move to 13:00
        post = df_1m.loc[break_ts:end - pd.Timedelta(seconds=1)]
        if post.empty:
            continue
        post_close = float(post["close"].iloc[-1])
        post_mfe_up = float(post["high"].max() - break_px)
        post_mfe_dn = float(break_px - post["low"].min())
        # did price come back through the OTHER side after first break?
        if break_dir > 0:
            other_break = (post["low"] < c["l"]).any()
            cont_pts = post_close - break_px
        else:
            other_break = (post["high"] > c["h"]).any()
            cont_pts = break_px - post_close
        rows.append({
            "date": date.date(),
            "break_dir": break_dir,
            "break_ts": break_ts,
            "break_minutes_after_950": int((break_ts - start).total_seconds() // 60),
            "post_close_to_13": float(post_close),
            "continuation_pts": float(cont_pts),
            "post_mfe_in_dir": float(post_mfe_up if break_dir > 0 else post_mfe_dn),
            "other_side_broken": bool(other_break),
            "candle_body_dir": int(c["body_dir"]),
            "agrees_with_candle": int(c["body_dir"]) == break_dir,
            "is_manip": bool(c["is_manip"]),
            "manip_dir": int(c["manip_dir"]) if not pd.isna(c["manip_dir"]) else 0,
            "agrees_with_manip": (int(c["manip_dir"]) == break_dir) if c["is_manip"] else None,
            "nine_bias": int(daily.loc[date, "bias_dir"]),
            "agrees_with_9am_bias": int(daily.loc[date, "bias_dir"]) == break_dir,
        })
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# TEST C: 9:45 candle direction as bias signal vs 9AM 1H
# ---------------------------------------------------------------------------

def run_test_c(daily: pd.DataFrame, candles: pd.DataFrame) -> dict:
    """Compare predictive power of 9:45 body direction vs 9AM 1H direction
    on post-10:00 move (close_10_13)."""
    j = daily.join(candles[["body_dir", "is_manip", "manip_dir"]],
                   how="inner", rsuffix="_945")
    j = j.dropna(subset=["close_10_13"])
    out = {}

    # 9AM 1H bias hit rate
    j_9am = j[j["bias_dir"] != 0]
    p_9am = float((np.sign(j_9am["close_10_13"]) == j_9am["bias_dir"]).mean())
    n_9am = len(j_9am)

    # 9:45 body direction hit rate
    j_945 = j[j["body_dir"] != 0]
    p_945 = float((np.sign(j_945["close_10_13"]) == j_945["body_dir"]).mean())
    n_945 = len(j_945)

    # Agreement subset (both same direction)
    agree = j[(j["bias_dir"] != 0) & (j["body_dir"] != 0) &
              (j["bias_dir"] == j["body_dir"])]
    p_agree = float((np.sign(agree["close_10_13"]) == agree["bias_dir"]).mean()) if len(agree) else float("nan")

    # Disagreement subset (opposite)
    disagree = j[(j["bias_dir"] != 0) & (j["body_dir"] != 0) &
                 (j["bias_dir"] == -j["body_dir"])]
    p_dis_9am = float((np.sign(disagree["close_10_13"]) == disagree["bias_dir"]).mean()) if len(disagree) else float("nan")
    p_dis_945 = float((np.sign(disagree["close_10_13"]) == disagree["body_dir"]).mean()) if len(disagree) else float("nan")

    # Manipulation: when body_dir is set AND is_manip True, body should be
    # the direction (sweep-then-close). Test post-10:00 move.
    manip = j[j["is_manip"] == True]
    p_manip = float((np.sign(manip["close_10_13"]) == manip["body_dir"]).mean()) if len(manip) else float("nan")

    out["bias_9am_only"] = {"hit_rate": p_9am, "n": n_9am}
    out["body_945_only"] = {"hit_rate": p_945, "n": n_945}
    out["both_agree"] = {"hit_rate": p_agree, "n": len(agree)}
    out["disagree_9am_correct"] = {"hit_rate": p_dis_9am, "n": len(disagree)}
    out["disagree_945_correct"] = {"hit_rate": p_dis_945, "n": len(disagree)}
    out["manip_body_dir_correct"] = {"hit_rate": p_manip, "n": len(manip)}
    return out


# ---------------------------------------------------------------------------
# Summaries
# ---------------------------------------------------------------------------

def summarize_test_a(touches: pd.DataFrame) -> pd.DataFrame:
    rows = []
    first = touches[touches["touch_ordinal"] == 1]
    for level in ["h", "l", "ce"]:
        for approach in ["from_above", "from_below"]:
            g = first[(first["level"] == level) & (first["approach"] == approach)]
            if g.empty:
                continue
            rev = g["reversed"].mean()
            z, p = _one_prop_z(rev, len(g))
            rows.append({
                "level": level, "approach": approach,
                "n": len(g),
                "reversal_rate": rev,
                "avg_mfe": g["mfe"].mean(),
                "avg_mae": g["mae"].mean(),
                "z_vs_50": z, "p_vs_50": p,
            })
    return pd.DataFrame(rows)


def hour_breakdown_a(touches: pd.DataFrame, level: str) -> pd.DataFrame:
    g = touches[(touches["touch_ordinal"] == 1) & (touches["level"] == level)]
    out = g.groupby("touch_hour").agg(
        n=("reversed", "size"),
        rev=("reversed", "mean"),
        avg_mfe=("mfe", "mean"),
    ).reset_index()
    return out


def days_breakdown_a(touches: pd.DataFrame) -> pd.DataFrame:
    g = touches[touches["touch_ordinal"] == 1].copy()
    g["bucket"] = pd.cut(g["days_since"], bins=[-0.5, 0.5, 1.5, 3.5, 7.5, 30.5],
                         labels=["same_day", "next_day", "2-3d", "4-7d", "8-30d"])
    return g.groupby(["level", "bucket"], observed=True).agg(
        n=("reversed", "size"), rev=("reversed", "mean"),
    ).reset_index()


def ordinal_breakdown_a(touches: pd.DataFrame) -> pd.DataFrame:
    return touches.groupby(["level", "touch_ordinal"]).agg(
        n=("reversed", "size"), rev=("reversed", "mean"),
    ).reset_index()


# ---------------------------------------------------------------------------

def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", default=None)
    ap.add_argument("--end", default=None)
    ap.add_argument("--cache", default="mnq_1m_clean.csv")
    ap.add_argument("--outdir", default="out_945")
    args = ap.parse_args()

    end = pd.Timestamp(args.end) if args.end else pd.Timestamp.utcnow().normalize()
    start = pd.Timestamp(args.start) if args.start else (end - pd.Timedelta(days=380))
    cache_path = Path(args.cache)
    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    df_1m = load_databento_mnq_1m(start.strftime("%Y-%m-%d"),
                                   end.strftime("%Y-%m-%d"), cache_path)
    daily = build_daily_table(df_1m)
    print(f"[data] {len(df_1m):,} 1m bars, {len(daily)} sessions")

    candles = extract_945_candles(df_1m, daily)
    candles.to_csv(outdir / "945_candles.csv")
    print(f"[945] extracted {len(candles)} 9:45 candles")
    print(f"  bullish: {(candles['body_dir']==1).sum()}  "
          f"bearish: {(candles['body_dir']==-1).sum()}  "
          f"doji: {(candles['body_dir']==0).sum()}")
    print(f"  manipulation pattern: {candles['is_manip'].sum()}")

    # ---- TEST A ----
    print("\n[A] scanning subsequent-day touches of 9:45 h/l/ce ...")
    a = run_test_a(df_1m, candles)
    a.to_csv(outdir / "945_level_touches.csv", index=False)
    print(f"[A] {len(a):,} touch events")

    sum_a = summarize_test_a(a)
    sum_a.to_csv(outdir / "test_a_summary.csv", index=False)
    print("\n=== TEST A: 9:45 level first-touch reversal ===")
    print(sum_a.to_string(index=False))

    print("\n  TEST A: ordinal breakdown")
    ord_a = ordinal_breakdown_a(a)
    ord_a.to_csv(outdir / "test_a_ordinal.csv", index=False)
    print(ord_a.to_string(index=False))

    print("\n  TEST A: days-since breakdown")
    d_a = days_breakdown_a(a)
    d_a.to_csv(outdir / "test_a_days.csv", index=False)
    print(d_a.to_string(index=False))

    print("\n  TEST A: hour-of-touch (9:45 HIGH only, first touch)")
    ha = hour_breakdown_a(a, "h")
    ha.to_csv(outdir / "test_a_hour_high.csv", index=False)
    print(ha.to_string(index=False))

    # ---- TEST B ----
    print("\n[B] same-day first breakout of 9:45 high or low ...")
    b = run_test_b(df_1m, candles, daily)
    b.to_csv(outdir / "945_breakouts.csv", index=False)
    print(f"[B] {len(b)} sessions had a break by 13:00")

    if not b.empty:
        # break-direction agreement with 9AM bias
        agree_9am = (b["agrees_with_9am_bias"]).mean()
        # post-break continuation by direction
        cont_up = b[b["break_dir"] == 1]["continuation_pts"]
        cont_dn = b[b["break_dir"] == -1]["continuation_pts"]
        # how often does the break direction equal the eventual close-13 direction
        b["close_dir"] = np.sign(b["continuation_pts"])
        cont_rate = float((b["close_dir"] == b["break_dir"]).mean())
        no_other_rate = float((~b["other_side_broken"]).mean())
        print(f"\n=== TEST B summary ===")
        print(f"  break direction == close direction at 13:00: {cont_rate*100:.1f}% (n={len(b)})")
        print(f"  no other-side break by 13:00 (clean break):  {no_other_rate*100:.1f}%")
        print(f"  break agrees with 9AM 1H bias:               {agree_9am*100:.1f}%")
        print(f"  avg continuation if up break:   {cont_up.mean():+.1f} pts (n={len(cont_up)})")
        print(f"  avg continuation if down break: {cont_dn.mean():+.1f} pts (n={len(cont_dn)})")

        # split by manip
        manip_b = b[b["is_manip"] == True]
        if len(manip_b):
            cont_manip = float((np.sign(manip_b["continuation_pts"]) ==
                                manip_b["manip_dir"]).mean())
            print(f"  on manipulation days, body direction predicts close: "
                  f"{cont_manip*100:.1f}% (n={len(manip_b)})")

        # split by agreement
        for grp_name, grp in [("agree_9am", b[b["agrees_with_9am_bias"]]),
                              ("disagree_9am", b[~b["agrees_with_9am_bias"]])]:
            if len(grp):
                cr = float((grp["close_dir"] == grp["break_dir"]).mean())
                print(f"  {grp_name}: cont_rate={cr*100:.1f}%, n={len(grp)}, "
                      f"avg_cont={grp['continuation_pts'].mean():+.1f}")

        b_sum = pd.DataFrame([{
            "n_break_sessions": len(b),
            "cont_rate": cont_rate, "no_otherside_rate": no_other_rate,
            "agree_9am_rate": agree_9am,
            "avg_cont_up": cont_up.mean(),
            "avg_cont_dn": cont_dn.mean(),
        }])
        b_sum.to_csv(outdir / "test_b_summary.csv", index=False)

    # ---- TEST C ----
    print("\n[C] 9:45 body direction vs 9AM 1H bias as predictor ...")
    c_results = run_test_c(daily, candles)
    c_df = pd.DataFrame([{"comparison": k, **v} for k, v in c_results.items()])
    c_df.to_csv(outdir / "test_c_summary.csv", index=False)
    print("\n=== TEST C: predictive power for post-10:00 close direction ===")
    print(c_df.to_string(index=False))

    # ---- Plain English ----
    print("\n" + "=" * 70)
    print("INTERPRETATION (9:45 candle)")
    print("=" * 70)
    if not sum_a.empty:
        # best level on average
        best = sum_a.sort_values("reversal_rate", ascending=False).iloc[0]
        print(f"  Best 9:45 level reversal: {best['level']} "
              f"approach={best['approach']} -> {best['reversal_rate']*100:.1f}% "
              f"(n={int(best['n'])}, p={best['p_vs_50']:.3f})")
    if not b.empty:
        print(f"  Same-day 9:45 break direction predicts close at 13:00: "
              f"{cont_rate*100:.1f}%")
    bias_p = c_results["bias_9am_only"]
    body_p = c_results["body_945_only"]
    agree_p = c_results["both_agree"]
    print(f"  9AM 1H bias hits at 13:00:  {bias_p['hit_rate']*100:.1f}% (n={bias_p['n']})")
    print(f"  9:45 body direction hits:    {body_p['hit_rate']*100:.1f}% (n={body_p['n']})")
    print(f"  Both agree -> hit rate:      {agree_p['hit_rate']*100:.1f}% (n={agree_p['n']})")

    print(f"\nAll outputs in {outdir.resolve()}")
    print("Done.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
