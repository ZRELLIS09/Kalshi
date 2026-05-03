"""
Intervening-Level Test: does a 10AM (or other) same-direction FVG cause a
reversal BEFORE price retraces back to the 9AM candle?

Setup
-----
On a session with bullish 9AM bias (close > open):
  - Find 10AM FVGs that are bullish AND positioned ABOVE the 9AM candle
    high (i.e. fvg.low > nine_high). These sit in the "shadow" between
    a possible later peak and the 9AM zone.
  - After the FVG completes (ts >= middle + 2*tf), watch post-FVG price
    until 16:00 NY. If price ever extends ABOVE the FVG (peaks higher),
    then retraces DOWN, where does it stop?

Outcome buckets (per qualifying FVG-day):
  NO_EXTENSION   : price never got above FVG.high after FVG formed -> no test
  NO_RETRACE     : price extended past FVG and never came back (kept going)
  HELD_AT_FVG    : retraced into FVG, did not break below FVG.low
                   -> FVG caught reversal BEFORE 9AM (the hypothesis)
  BROKE_FVG_HELD_9AM : retraced through FVG.low but did not breach 9AM high
                   -> FVG failed, 9AM zone caught it before tag
  HIT_9AM        : price entered the 9AM candle range [nine_low, nine_high]
  PIERCED_9AM    : price went below nine_low (full breach)

Mirror logic for bearish bias.

Headline: P(HELD_AT_FVG | NO_EXTENSION-excluded) == FVG-as-reversal-zone rate
Compared to: P(price would have reached 9AM zone if FVG weren't there)
which we approximate by reverse-direction days (no qualifying FVG above) where
price reached the 9AM range on retrace.

Also tests:
  - any prior-DAY same-direction FVG that sits in the shadow (formed 0-5
    days before today, in 9AM hour, same direction, above/below 9AM zone)
  - reaction size at FVG (60-min MFE/MAE in expected direction)

Usage:
  export DATABENTO_API_KEY=db-...
  python3 nq_intervening_level_test.py --cache mnq_1m_clean.csv \\
      --outdir out_intervening
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
from nq_9am_fvg_backtest import (
    aggregate_to_tf, find_9am_fvgs, FVG,
    _two_prop_z, _one_prop_z,
)

TF_LIST = [5, 15]


def find_hour_fvgs(df_tf: pd.DataFrame, tf_min: int,
                   hour_start: int, hour_end: int) -> list[FVG]:
    """Generalize 9AM FVG detector to any hour window [hour_start, hour_end)."""
    fvgs: list[FVG] = []
    days = sorted({ts.date() for ts in df_tf.index})
    for d in days:
        win_start = pd.Timestamp(d, tz=NY_TZ).replace(hour=hour_start, minute=0)
        win_end = pd.Timestamp(d, tz=NY_TZ).replace(hour=hour_end, minute=0)
        ctx_start = win_start - pd.Timedelta(minutes=tf_min)
        ctx_end = win_end + pd.Timedelta(minutes=tf_min)
        ctx = df_tf.loc[ctx_start:ctx_end]
        if len(ctx) < 3:
            continue
        for i in range(1, len(ctx) - 1):
            b2_ts = ctx.index[i]
            if not (win_start <= b2_ts < win_end):
                continue
            b1, b3 = ctx.iloc[i - 1], ctx.iloc[i + 1]
            if b1["high"] < b3["low"]:
                lo, hi = float(b1["high"]), float(b3["low"])
                fvgs.append(FVG(pd.Timestamp(d, tz=NY_TZ), b2_ts, tf_min,
                                "bullish", lo, hi, (lo + hi) / 2))
            elif b1["low"] > b3["high"]:
                lo, hi = float(b3["high"]), float(b1["low"])
                fvgs.append(FVG(pd.Timestamp(d, tz=NY_TZ), b2_ts, tf_min,
                                "bearish", lo, hi, (lo + hi) / 2))
    return fvgs


def classify_intervention(df_1m: pd.DataFrame, fvg: FVG,
                          nine_low: float, nine_high: float,
                          bias: int,
                          end_hour: int = 16,
                          bounce_pts: float = 30.0,
                          react_min: int = 60) -> dict:
    """Run the geometry test for one qualifying FVG on its session day.
    bias: +1 bullish 9AM, -1 bearish 9AM.
    Returns dict with outcome bucket, FVG-touch reaction, and 9AM-touch
    reaction (when 9AM zone was reached).

    A "bounce" is reaction MFE >= bounce_pts in the bias direction within
    react_min minutes after the touch.
    """
    start = fvg.middle_ts + pd.Timedelta(minutes=fvg.timeframe_min * 2)
    end = pd.Timestamp(fvg.date.date(), tz=NY_TZ).replace(hour=end_hour, minute=0)
    if start >= end:
        return {"outcome": "NO_DATA"}
    sl = df_1m.loc[start:end - pd.Timedelta(seconds=1)]
    if sl.empty:
        return {"outcome": "NO_DATA"}

    if bias > 0:  # bullish: FVG above 9AM, look for retrace down
        ext_mask = sl["high"] >= fvg.high
        if not ext_mask.any():
            return {"outcome": "NO_EXTENSION"}
        ext_ts = sl.index[ext_mask][0]
        after = sl.loc[ext_ts:]
        peak_high = float(after["high"].max())
        in_fvg = (after["low"] <= fvg.high) & (after["high"] >= fvg.low)
        if not in_fvg.any():
            return {"outcome": "NO_RETRACE", "peak": peak_high}
        ret_ts = after.index[in_fvg][0]
        post_ret = after.loc[ret_ts:]
        post_low = float(post_ret["low"].min())
        ref_px_fvg = float(df_1m.loc[ret_ts, "close"])
        react_end = ret_ts + pd.Timedelta(minutes=react_min)
        react = df_1m.loc[ret_ts:react_end]
        fvg_react_mfe_up = float(react["high"].max() - ref_px_fvg) if len(react) else float("nan")
        fvg_react_mae_dn = float(ref_px_fvg - react["low"].min()) if len(react) else float("nan")

        # 9AM zone interaction (only meaningful if price entered nine_low..nine_high)
        nine_touch_mask = (post_ret["low"] <= nine_high) & (post_ret["high"] >= nine_low)
        nine_touched = bool(nine_touch_mask.any())
        nine_react_mfe_up = float("nan")
        nine_react_mae_dn = float("nan")
        nine_touch_ts = pd.NaT
        nine_penetration = float("nan")
        nine_bounced = False
        if nine_touched:
            nine_touch_ts = post_ret.index[nine_touch_mask][0]
            ref_px_9am = float(df_1m.loc[nine_touch_ts, "close"])
            r9 = df_1m.loc[nine_touch_ts:nine_touch_ts + pd.Timedelta(minutes=react_min)]
            if len(r9):
                nine_react_mfe_up = float(r9["high"].max() - ref_px_9am)
                nine_react_mae_dn = float(ref_px_9am - r9["low"].min())
                nine_bounced = nine_react_mfe_up >= bounce_pts
            # how deep into 9AM zone? 0 = just clipped top (nine_high), 1 = at nine_low
            min_after_9am = float(post_ret.loc[nine_touch_ts:]["low"].min())
            rng9 = nine_high - nine_low
            nine_penetration = (nine_high - min_after_9am) / rng9 if rng9 > 0 else float("nan")
            nine_penetration = max(0.0, min(1.5, nine_penetration))  # >1 = pierced

        if post_low > fvg.low:
            oc = "HELD_AT_FVG"
        elif post_low > nine_high:
            oc = "BROKE_FVG_HELD_9AM"
        elif post_low > nine_low:
            oc = "HIT_9AM"
        else:
            oc = "PIERCED_9AM"

        return {"outcome": oc,
                "peak": peak_high, "min_after_ret": post_low,
                "fvg_react_mfe": fvg_react_mfe_up,
                "fvg_react_mae": fvg_react_mae_dn,
                "fvg_bounced": fvg_react_mfe_up >= bounce_pts if not np.isnan(fvg_react_mfe_up) else False,
                "ret_ts": ret_ts,
                "nine_touched": nine_touched,
                "nine_touch_ts": nine_touch_ts,
                "nine_react_mfe": nine_react_mfe_up,
                "nine_react_mae": nine_react_mae_dn,
                "nine_penetration": nine_penetration,
                "nine_bounced": bool(nine_bounced),
                }
    else:  # bearish: FVG below 9AM, retrace up
        ext_mask = sl["low"] <= fvg.low
        if not ext_mask.any():
            return {"outcome": "NO_EXTENSION"}
        ext_ts = sl.index[ext_mask][0]
        after = sl.loc[ext_ts:]
        trough_low = float(after["low"].min())
        in_fvg = (after["low"] <= fvg.high) & (after["high"] >= fvg.low)
        if not in_fvg.any():
            return {"outcome": "NO_RETRACE", "trough": trough_low}
        ret_ts = after.index[in_fvg][0]
        post_ret = after.loc[ret_ts:]
        post_high = float(post_ret["high"].max())
        ref_px_fvg = float(df_1m.loc[ret_ts, "close"])
        react = df_1m.loc[ret_ts:ret_ts + pd.Timedelta(minutes=react_min)]
        fvg_react_mfe_dn = float(ref_px_fvg - react["low"].min()) if len(react) else float("nan")
        fvg_react_mae_up = float(react["high"].max() - ref_px_fvg) if len(react) else float("nan")

        nine_touch_mask = (post_ret["low"] <= nine_high) & (post_ret["high"] >= nine_low)
        nine_touched = bool(nine_touch_mask.any())
        nine_react_mfe_dn = float("nan")
        nine_react_mae_up = float("nan")
        nine_touch_ts = pd.NaT
        nine_penetration = float("nan")
        nine_bounced = False
        if nine_touched:
            nine_touch_ts = post_ret.index[nine_touch_mask][0]
            ref_px_9am = float(df_1m.loc[nine_touch_ts, "close"])
            r9 = df_1m.loc[nine_touch_ts:nine_touch_ts + pd.Timedelta(minutes=react_min)]
            if len(r9):
                nine_react_mfe_dn = float(ref_px_9am - r9["low"].min())
                nine_react_mae_up = float(r9["high"].max() - ref_px_9am)
                nine_bounced = nine_react_mfe_dn >= bounce_pts
            max_after_9am = float(post_ret.loc[nine_touch_ts:]["high"].max())
            rng9 = nine_high - nine_low
            nine_penetration = (max_after_9am - nine_low) / rng9 if rng9 > 0 else float("nan")
            nine_penetration = max(0.0, min(1.5, nine_penetration))

        if post_high < fvg.high:
            oc = "HELD_AT_FVG"
        elif post_high < nine_low:
            oc = "BROKE_FVG_HELD_9AM"
        elif post_high < nine_high:
            oc = "HIT_9AM"
        else:
            oc = "PIERCED_9AM"

        return {"outcome": oc,
                "trough": trough_low, "max_after_ret": post_high,
                "fvg_react_mfe": fvg_react_mfe_dn,
                "fvg_react_mae": fvg_react_mae_up,
                "fvg_bounced": fvg_react_mfe_dn >= bounce_pts if not np.isnan(fvg_react_mfe_dn) else False,
                "ret_ts": ret_ts,
                "nine_touched": nine_touched,
                "nine_touch_ts": nine_touch_ts,
                "nine_react_mfe": nine_react_mfe_dn,
                "nine_react_mae": nine_react_mae_up,
                "nine_penetration": nine_penetration,
                "nine_bounced": bool(nine_bounced),
                }


def qualifies(fvg: FVG, daily_row: pd.Series) -> bool:
    """Is this FVG (a) same direction as 9AM bias, and (b) positioned
    in the shadow above (bullish) or below (bearish) the 9AM zone?"""
    if daily_row["bias_dir"] == 0:
        return False
    nine_low = float(daily_row["nine_low"])
    nine_high = float(daily_row["nine_high"])
    if daily_row["bias_dir"] > 0 and fvg.direction == "bullish":
        return fvg.low > nine_high
    if daily_row["bias_dir"] < 0 and fvg.direction == "bearish":
        return fvg.high < nine_low
    return False


def run_test(df_1m: pd.DataFrame, daily: pd.DataFrame,
             fvgs: list[FVG], label: str) -> pd.DataFrame:
    """For each FVG, lookup the SAME-DAY 9AM bias and run the test.
    Returns one row per (fvg, day) qualifying instance."""
    rows = []
    for fvg in fvgs:
        day_idx = pd.Timestamp(fvg.date.date())
        if day_idx not in daily.index:
            continue
        row = daily.loc[day_idx]
        if not qualifies(fvg, row):
            continue
        result = classify_intervention(df_1m, fvg,
                                       float(row["nine_low"]),
                                       float(row["nine_high"]),
                                       int(row["bias_dir"]))
        rows.append({
            "label": label,
            "date": day_idx.date(),
            "fvg_tf": fvg.timeframe_min,
            "fvg_dir": fvg.direction,
            "fvg_low": fvg.low, "fvg_high": fvg.high,
            "fvg_size": fvg.high - fvg.low,
            "nine_low": float(row["nine_low"]),
            "nine_high": float(row["nine_high"]),
            "bias": int(row["bias_dir"]),
            "fvg_distance_from_9am": (
                fvg.low - float(row["nine_high"])
                if row["bias_dir"] > 0 else
                float(row["nine_low"]) - fvg.high),
            **result,
        })
    return pd.DataFrame(rows)


def summarize(df: pd.DataFrame) -> pd.DataFrame:
    """Outcome distribution + held rate (excluding NO_EXTENSION/NO_RETRACE)."""
    if df.empty:
        return pd.DataFrame()
    total = len(df)
    counts = df["outcome"].value_counts().to_dict()
    tested = df[~df["outcome"].isin(["NO_EXTENSION", "NO_RETRACE", "NO_DATA"])]
    n_tested = len(tested)
    held = (tested["outcome"] == "HELD_AT_FVG").sum()
    n_failed = n_tested - held
    z, p = _one_prop_z(held / n_tested if n_tested else float("nan"), n_tested)
    rows = [{
        "metric": "n_qualifying_fvgs", "value": total,
    }, {
        "metric": "n_with_extension_and_retrace", "value": n_tested,
    }, {
        "metric": "n_HELD_AT_FVG", "value": int(held),
    }, {
        "metric": "held_rate", "value": (held / n_tested) if n_tested else float("nan"),
    }, {
        "metric": "z_held_vs_50", "value": z,
    }, {
        "metric": "p_held_vs_50", "value": p,
    }]
    for oc, c in counts.items():
        rows.append({"metric": f"count_{oc}", "value": int(c)})
    return pd.DataFrame(rows)


def by_tf(df: pd.DataFrame) -> pd.DataFrame:
    """Outcome split by FVG timeframe."""
    if df.empty:
        return pd.DataFrame()
    tested = df[~df["outcome"].isin(["NO_EXTENSION", "NO_RETRACE", "NO_DATA"])]
    out = tested.groupby("fvg_tf").apply(
        lambda g: pd.Series({
            "n_tested": len(g),
            "n_HELD": (g["outcome"] == "HELD_AT_FVG").sum(),
            "held_rate": (g["outcome"] == "HELD_AT_FVG").mean(),
            "n_HIT_9AM": (g["outcome"].isin(["HIT_9AM", "PIERCED_9AM"])).sum(),
            "hit_9am_rate": (g["outcome"].isin(["HIT_9AM", "PIERCED_9AM"])).mean(),
        })
    ).reset_index()
    return out


def by_bias(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()
    tested = df[~df["outcome"].isin(["NO_EXTENSION", "NO_RETRACE", "NO_DATA"])]
    return tested.groupby("bias").apply(
        lambda g: pd.Series({
            "n_tested": len(g),
            "held_rate": (g["outcome"] == "HELD_AT_FVG").mean(),
            "hit_9am_rate": g["outcome"].isin(["HIT_9AM", "PIERCED_9AM"]).mean(),
        })
    ).reset_index()


def baseline_no_fvg(daily: pd.DataFrame, df_1m: pd.DataFrame,
                    days_with_qualifying_fvg: set,
                    min_extension_pts: float = 30.0) -> dict:
    """Baseline: on days with bias but NO qualifying same-dir intervening
    FVG, did price extend at least min_extension_pts beyond the 9AM zone
    AND THEN retrace back to it?

    Uses peak/trough as the reference, not the first cross bar (a single
    bar that just clips the 9AM line typically has low<=line, which would
    give a spurious instant retrace).
    """
    rows = []
    for d, row in daily.iterrows():
        if int(row["bias_dir"]) == 0:
            continue
        if d.date() in days_with_qualifying_fvg:
            continue
        ten = pd.Timestamp(d.date(), tz=NY_TZ).replace(hour=10, minute=0)
        end = ten.replace(hour=16, minute=0)
        sl = df_1m.loc[ten:end - pd.Timedelta(seconds=1)]
        if sl.empty:
            continue
        nine_low = float(row["nine_low"]); nine_high = float(row["nine_high"])
        if int(row["bias_dir"]) > 0:
            peak_high = float(sl["high"].max())
            extension = peak_high - nine_high
            if extension < min_extension_pts:
                continue
            peak_ts = sl["high"].idxmax()
            after_peak = sl.loc[peak_ts:]
            if len(after_peak) < 2:
                continue
            after_peak = after_peak.iloc[1:]  # skip the peak bar itself
            min_after = float(after_peak["low"].min())
            hit_9am = min_after <= nine_high
            pierced = min_after <= nine_low
            rows.append({"date": d.date(), "bias": 1,
                         "extension_pts": extension,
                         "min_low_after_peak": min_after,
                         "hit_9am": bool(hit_9am), "pierced": bool(pierced)})
        else:
            trough_low = float(sl["low"].min())
            extension = nine_low - trough_low
            if extension < min_extension_pts:
                continue
            trough_ts = sl["low"].idxmin()
            after_trough = sl.loc[trough_ts:]
            if len(after_trough) < 2:
                continue
            after_trough = after_trough.iloc[1:]
            max_after = float(after_trough["high"].max())
            hit_9am = max_after >= nine_low
            pierced = max_after >= nine_high
            rows.append({"date": d.date(), "bias": -1,
                         "extension_pts": extension,
                         "max_high_after_trough": max_after,
                         "hit_9am": bool(hit_9am), "pierced": bool(pierced)})
    base = pd.DataFrame(rows)
    if base.empty:
        return {"n": 0, "hit_rate": float("nan"), "pierce_rate": float("nan")}
    return {
        "n": len(base),
        "hit_rate": float(base["hit_9am"].mean()),
        "pierce_rate": float(base["pierced"].mean()),
        "median_extension": float(base["extension_pts"].median()),
        "table": base,
    }


# ---------------------------------------------------------------------------

def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", default=None)
    ap.add_argument("--end", default=None)
    ap.add_argument("--cache", default="mnq_1m_clean.csv")
    ap.add_argument("--outdir", default="out_intervening")
    args = ap.parse_args()

    end = pd.Timestamp(args.end) if args.end else pd.Timestamp.utcnow().normalize()
    start = pd.Timestamp(args.start) if args.start else (end - pd.Timedelta(days=380))
    cache_path = Path(args.cache)
    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    df_1m = load_databento_mnq_1m(start.strftime("%Y-%m-%d"),
                                   end.strftime("%Y-%m-%d"), cache_path)
    print(f"[data] {len(df_1m):,} 1m bars")

    daily = build_daily_table(df_1m)
    daily.to_csv(outdir / "daily_levels.csv")
    print(f"[daily] {len(daily)} sessions")

    # Detect FVGs at 5m and 15m TFs in different hour windows
    print("\n[fvg] detecting 10AM FVGs ...")
    fvg_10am: list[FVG] = []
    for tf in TF_LIST:
        df_tf = aggregate_to_tf(df_1m, tf)
        f = find_hour_fvgs(df_tf, tf, 10, 11)
        print(f"  tf={tf}m: {len(f)} 10AM FVGs")
        fvg_10am.extend(f)

    # Filter, classify, summarize for 10AM
    df10 = run_test(df_1m, daily, fvg_10am, "10AM_FVG")
    df10.to_csv(outdir / "10am_fvg_results.csv", index=False)
    print(f"[10AM] qualifying instances: {len(df10)}")

    summary10 = summarize(df10)
    summary10.to_csv(outdir / "10am_summary.csv", index=False)
    print("\n=== 10AM FVG intervention test ===")
    print(summary10.to_string(index=False))

    if not df10.empty:
        print("\n  by timeframe:")
        bytf = by_tf(df10)
        bytf.to_csv(outdir / "10am_by_tf.csv", index=False)
        print(bytf.to_string(index=False))
        print("\n  by 9AM bias direction:")
        bb = by_bias(df10)
        bb.to_csv(outdir / "10am_by_bias.csv", index=False)
        print(bb.to_string(index=False))

    # ---- baseline: days WITHOUT a qualifying 10AM FVG ----
    # Match the median extension distance from the FVG cohort
    if not df10.empty:
        med_ext = float(df10["fvg_distance_from_9am"].median())
    else:
        med_ext = 30.0
    days_with = set(df10["date"].unique()) if not df10.empty else set()
    baseline = baseline_no_fvg(daily, df_1m, days_with,
                                min_extension_pts=max(20.0, med_ext * 0.5))
    print(f"\n=== Baseline (days with bias but NO qualifying 10AM FVG above) ===")
    print(f"  min_extension_pts = {max(20.0, med_ext * 0.5):.1f} "
          f"(median FVG distance from 9AM = {med_ext:.1f})")
    print(f"  n eligible = {baseline['n']}")
    print(f"  median extension = {baseline.get('median_extension', float('nan')):.1f} pts")
    print(f"  fraction that hit 9AM zone on retrace = {baseline['hit_rate']*100:.1f}%")
    print(f"  fraction that pierced 9AM zone        = {baseline['pierce_rate']*100:.1f}%")
    if "table" in baseline:
        baseline["table"].to_csv(outdir / "baseline_no_fvg.csv", index=False)

    # ---- compare: with-FVG hit rate vs baseline hit rate ----
    if not df10.empty:
        tested = df10[~df10["outcome"].isin(["NO_EXTENSION", "NO_RETRACE", "NO_DATA"])]
        if len(tested) and baseline["n"]:
            with_hit = tested["outcome"].isin(["HIT_9AM", "PIERCED_9AM"]).mean()
            base_hit = baseline["hit_rate"]
            z, p = _two_prop_z(with_hit, len(tested), base_hit, baseline["n"])
            print(f"\n=== HEADLINE COMPARISON ===")
            print(f"  with qualifying 10AM FVG: P(reach 9AM zone) = {with_hit*100:.1f}% (n={len(tested)})")
            print(f"  without qualifying FVG:   P(reach 9AM zone) = {base_hit*100:.1f}% (n={baseline['n']})")
            print(f"  diff (FVG protects 9AM by): {(base_hit - with_hit)*100:.1f}pp")
            print(f"  z = {z:+.2f}, p = {p:.4f}")

    # ---- NEW: when FVG didn't hold, did the 9AM range bounce price? ----
    print("\n=== 9AM-zone bounce when FVG did NOT hold ===")
    if not df10.empty:
        broke = df10[df10["outcome"].isin(["BROKE_FVG_HELD_9AM", "HIT_9AM",
                                           "PIERCED_9AM"])]
        n_broke = len(broke)
        n_no9am = (broke["outcome"] == "BROKE_FVG_HELD_9AM").sum()
        n_hit = (broke["outcome"] == "HIT_9AM").sum()
        n_pierce = (broke["outcome"] == "PIERCED_9AM").sum()
        print(f"  total 'FVG broken' instances: {n_broke}")
        print(f"    reversed BETWEEN FVG and 9AM (didn't reach 9AM): "
              f"{n_no9am} ({n_no9am/n_broke*100:.1f}%)")
        print(f"    reached 9AM zone, didn't pierce (HIT_9AM):       "
              f"{n_hit} ({n_hit/n_broke*100:.1f}%)")
        print(f"    pierced 9AM zone:                                "
              f"{n_pierce} ({n_pierce/n_broke*100:.1f}%)")

        # 9AM bounce quality: of cases that reached 9AM zone, did they
        # bounce >= 30 pts in bias direction within 60 min?
        reached_9am = df10[df10["nine_touched"] == True]
        if len(reached_9am):
            bounce_rate = float(reached_9am["nine_bounced"].mean())
            avg_mfe = float(reached_9am["nine_react_mfe"].mean())
            avg_mae = float(reached_9am["nine_react_mae"].mean())
            print(f"\n  Of {len(reached_9am)} cases that ENTERED 9AM zone:")
            print(f"    P(bounce >= 30pts in bias dir, 60min) = {bounce_rate*100:.1f}%")
            print(f"    avg 9AM-touch reaction MFE = {avg_mfe:.1f} pts")
            print(f"    avg 9AM-touch reaction MAE = {avg_mae:.1f} pts")

            # Split by penetration depth
            reached_9am_c = reached_9am.copy()
            reached_9am_c["pen_bucket"] = pd.cut(
                reached_9am_c["nine_penetration"],
                bins=[-0.01, 0.25, 0.5, 0.75, 1.0, 2.0],
                labels=["just_clipped", "to_25", "to_50_CE",
                        "to_75", "pierced"])
            pen_summary = reached_9am_c.groupby("pen_bucket", observed=True).agg(
                n=("nine_bounced", "size"),
                bounce_rate=("nine_bounced", "mean"),
                avg_mfe=("nine_react_mfe", "mean"),
                avg_mae=("nine_react_mae", "mean"),
            ).reset_index()
            print(f"\n  9AM bounce rate by penetration depth into 9AM zone:")
            print(pen_summary.to_string(index=False))
            pen_summary.to_csv(outdir / "9am_bounce_by_penetration.csv", index=False)

            # Split by FVG outcome (HIT_9AM vs PIERCED_9AM)
            by_oc = reached_9am.groupby("outcome").agg(
                n=("nine_bounced", "size"),
                bounce_rate=("nine_bounced", "mean"),
                avg_mfe=("nine_react_mfe", "mean"),
                avg_mae=("nine_react_mae", "mean"),
            ).reset_index()
            print(f"\n  9AM bounce rate by FVG outcome:")
            print(by_oc.to_string(index=False))
            by_oc.to_csv(outdir / "9am_bounce_by_outcome.csv", index=False)

            # Split by bias
            by_bias_9am = reached_9am.groupby("bias").agg(
                n=("nine_bounced", "size"),
                bounce_rate=("nine_bounced", "mean"),
                avg_mfe=("nine_react_mfe", "mean"),
            ).reset_index()
            print(f"\n  9AM bounce rate by bias direction:")
            print(by_bias_9am.to_string(index=False))
            by_bias_9am.to_csv(outdir / "9am_bounce_by_bias.csv", index=False)

            # Compare: P(bounce|9AM) vs P(bounce|FVG when FVG was tested)
            tested = df10[df10["fvg_bounced"].notna()]
            fvg_bounce_rate = float(tested["fvg_bounced"].mean()) if len(tested) else float("nan")
            z, p = _two_prop_z(bounce_rate, len(reached_9am),
                                fvg_bounce_rate, len(tested))
            print(f"\n  P(bounce 30+pts) at 9AM zone:  {bounce_rate*100:.1f}% (n={len(reached_9am)})")
            print(f"  P(bounce 30+pts) at FVG entry: {fvg_bounce_rate*100:.1f}% (n={len(tested)})")
            print(f"  z = {z:+.2f}, p = {p:.4f}")

    # ---- ALSO: any-prior-day same-dir 9AM FVG above current 9AM ----
    print("\n[fvg] also testing prior-day 9AM FVGs that sit above today's 9AM zone ...")
    fvg_9am_prior: list[FVG] = []
    for tf in TF_LIST:
        df_tf = aggregate_to_tf(df_1m, tf)
        f = find_hour_fvgs(df_tf, tf, 9, 10)
        fvg_9am_prior.extend(f)
    print(f"  {len(fvg_9am_prior)} 9AM-hour FVGs total across {len(TF_LIST)} TFs")

    rows_prior = []
    daily_dates = list(daily.index)
    for fvg in fvg_9am_prior:
        fvg_d = pd.Timestamp(fvg.date.date())
        # For each subsequent session within 5 days, check if today's bias
        # matches FVG direction AND FVG sits in shadow.
        try:
            i0 = daily.index.get_loc(fvg_d)
        except KeyError:
            continue
        for j in range(i0 + 1, min(i0 + 6, len(daily))):
            today = daily_dates[j]
            row = daily.loc[today]
            if not qualifies(fvg, row):
                continue
            res = classify_intervention(df_1m, fvg,
                                        float(row["nine_low"]),
                                        float(row["nine_high"]),
                                        int(row["bias_dir"]))
            rows_prior.append({
                "fvg_date": fvg_d.date(), "today": today.date(),
                "days_since_fvg": (today - fvg_d).days,
                "fvg_tf": fvg.timeframe_min, "fvg_dir": fvg.direction,
                "bias": int(row["bias_dir"]),
                "fvg_low": fvg.low, "fvg_high": fvg.high,
                **res,
            })
    df_prior = pd.DataFrame(rows_prior)
    df_prior.to_csv(outdir / "prior_9am_fvg_results.csv", index=False)
    print(f"  qualifying prior-FVG x today instances: {len(df_prior)}")
    if not df_prior.empty:
        s_prior = summarize(df_prior)
        s_prior.to_csv(outdir / "prior_9am_summary.csv", index=False)
        print("\n=== Prior-day 9AM FVG intervention test ===")
        print(s_prior.to_string(index=False))

    print(f"\nAll outputs in {outdir.resolve()}")
    print("Done.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
