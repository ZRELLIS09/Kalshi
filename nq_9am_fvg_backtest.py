"""
NQ/MNQ 9AM FVG Reversal Backtest
================================

Tests reversal rates of Fair Value Gaps (FVGs) formed during the 9:00 AM
NY hour, when touched on subsequent sessions. Targets the user's prior
findings:
  - 9AM FVGs reverse 78.8% on later-day touch (n=52, p<0.001)
  - 13:00 touches on 9AM FVGs reverse 90%
  - First touch strongest (73.2%), degrades on retests (Rule of Three is
    OPPOSITE of folklore)
  - CE (50%) as reversal target is BUSTED (6.4%)

FVG definition (3-bar imbalance):
  Bullish FVG: bar1.high < bar3.low  -> gap zone = [bar1.high, bar3.low]
  Bearish FVG: bar1.low  > bar3.high -> gap zone = [bar3.high, bar1.low]
  middle bar (bar2) must START inside the 9:00-9:59 NY window.

Tested timeframes: 5m, 15m, 1H. (1H FVG = 8AM/9AM/10AM 1H bars.)

Reaction definition:
  Bullish FVG touch: 'reversed' if price moves UP by >= REACTION_PTS
    within REACTION_MIN minutes after first touch.
  Bearish FVG touch: 'reversed' if price moves DOWN by >= REACTION_PTS.

Touch position categories:
  - 'edge_far'  : closer to far edge (full fill)
  - 'CE'        : within tolerance of midpoint
  - 'edge_near' : closer to near edge (just clipped)

Usage:
  export DATABENTO_API_KEY=db-...
  python3 nq_9am_fvg_backtest.py --cache mnq_1m_clean.csv --outdir out_fvg
"""

from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd

# Reuse data loaders from the alignment script
from nq_range_alignment_backtest import (
    load_databento_mnq_1m,
    load_csv_fallback,
    NY_TZ,
)

REACTION_MIN = 60        # minutes to measure reaction after first touch
REACTION_PTS = 10.0      # pts of move (in expected direction) = "reversed"
CE_TOLERANCE_PCT = 0.10  # within 10% of midpoint = "CE touch"
TIMEFRAMES = [5, 15, 60]  # minutes


# ---------------------------------------------------------------------------
# FVG detection
# ---------------------------------------------------------------------------

@dataclass
class FVG:
    date: pd.Timestamp     # session date the FVG formed
    middle_ts: pd.Timestamp
    timeframe_min: int
    direction: str         # "bullish" or "bearish"
    low: float
    high: float
    ce: float


def aggregate_to_tf(df_1m: pd.DataFrame, tf_minutes: int) -> pd.DataFrame:
    """Resample 1m to tf_minutes. Returns DataFrame indexed by bar OPEN ts."""
    rule = f"{tf_minutes}min"
    out = df_1m.resample(rule, label="left", closed="left").agg({
        "open": "first", "high": "max", "low": "min", "close": "last",
    }).dropna(subset=["open", "high", "low", "close"])
    return out


def find_9am_fvgs(df_tf: pd.DataFrame, tf_min: int) -> list[FVG]:
    """For each session day, find 3-bar FVGs where the middle bar's open
    timestamp is in [09:00, 10:00) NY."""
    fvgs: list[FVG] = []
    days = sorted({ts.date() for ts in df_tf.index})
    for d in days:
        win_start = pd.Timestamp(d, tz=NY_TZ).replace(hour=9, minute=0)
        win_end = pd.Timestamp(d, tz=NY_TZ).replace(hour=10, minute=0)
        # Need bar BEFORE 9:00 and bar AFTER 9:59 to form 3-bar pattern
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


# ---------------------------------------------------------------------------
# Touch detection & reaction
# ---------------------------------------------------------------------------

def find_touch_events(df_1m: pd.DataFrame, fvg: FVG,
                      search_end: pd.Timestamp,
                      lookahead_days: int = 20,
                      same_day: bool = False) -> list[dict]:
    """Find distinct touch events (contiguous runs of in-zone bars) AFTER
    the FVG day. Returns list of dicts with touch_ts (first in-zone bar),
    exit_ts, and max_penetration (0=just clipped near edge, 1=fully filled
    to far edge).

    same_day=False (default) skips same-day re-entry to mirror the user's
    'touched at 9AM on a LATER day' criterion.
    """
    if same_day:
        start = fvg.middle_ts + pd.Timedelta(minutes=fvg.timeframe_min * 2)
    else:
        # next NY calendar day at 00:00
        start = pd.Timestamp(fvg.date.date(), tz=NY_TZ) + pd.Timedelta(days=1)
    end = min(search_end, start + pd.Timedelta(days=lookahead_days))
    sl = df_1m.loc[start:end]
    if sl.empty:
        return []
    rng = fvg.high - fvg.low
    if rng <= 0:
        return []
    in_zone = ((sl["high"] >= fvg.low) & (sl["low"] <= fvg.high)).to_numpy()
    if not in_zone.any():
        return []
    # Run boundaries: True at first/last bar of each contiguous in-zone run
    pad_prev = np.concatenate([[False], in_zone[:-1]])
    pad_next = np.concatenate([in_zone[1:], [False]])
    enters = in_zone & ~pad_prev
    exits = in_zone & ~pad_next
    enter_idx = np.where(enters)[0]
    exit_idx = np.where(exits)[0]
    n = min(len(enter_idx), len(exit_idx))
    events = []
    for ei, xi in zip(enter_idx[:n], exit_idx[:n]):
        run = sl.iloc[ei:xi + 1]
        if fvg.direction == "bullish":
            extreme = float(run["low"].min())
            penetration = (fvg.high - extreme) / rng
        else:
            extreme = float(run["high"].max())
            penetration = (extreme - fvg.low) / rng
        penetration = max(0.0, min(1.0, penetration))
        events.append({
            "touch_ts": sl.index[ei],
            "exit_ts": sl.index[xi],
            "max_penetration": penetration,
            "n_bars_in_zone": int(xi - ei + 1),
        })
    return events


def position_bucket(penetration: float) -> str:
    if penetration < 0.25:
        return "edge_near"        # just clipped near edge, no CE reach
    if penetration < 0.50:
        return "approached_CE"    # past quartile, didn't reach CE
    if penetration < 0.75:
        return "through_CE"       # past CE, didn't fill
    return "filled"               # past 3/4, near full fill


def measure_reaction(df_1m: pd.DataFrame, fvg: FVG, touch_ts: pd.Timestamp,
                     react_min: int = REACTION_MIN,
                     react_pts: float = REACTION_PTS) -> dict:
    """Measure REACTION_MIN-minute reaction in the FVG's expected direction
    starting from the touch bar. Reversal = move >= react_pts in expected dir.
    Reference price = close of the bar that first entered the zone."""
    react_end = touch_ts + pd.Timedelta(minutes=react_min)
    react = df_1m.loc[touch_ts:react_end]
    if react.empty:
        return {"reversed": False, "mfe": np.nan, "mae": np.nan, "n_min": 0}
    px = float(df_1m.loc[touch_ts, "close"])
    mfe_up = float(react["high"].max() - px)
    mfe_dn = float(px - react["low"].min())
    if fvg.direction == "bullish":
        reversed_ = mfe_up >= react_pts
        mfe, mae = mfe_up, -mfe_dn
    else:
        reversed_ = mfe_dn >= react_pts
        mfe, mae = mfe_dn, -mfe_up
    return {"reversed": bool(reversed_), "mfe": mfe, "mae": mae,
            "n_min": int(len(react))}


def build_touch_table(df_1m: pd.DataFrame, fvgs: list[FVG],
                      search_end: pd.Timestamp,
                      max_touches_per_fvg: int = 6) -> pd.DataFrame:
    """For each FVG, record up to max_touches_per_fvg distinct touch events
    on days AFTER the FVG forms, with reaction metrics."""
    rows = []
    for fvg in fvgs:
        events = find_touch_events(df_1m, fvg, search_end,
                                   lookahead_days=20, same_day=False)
        for ord_, ev in enumerate(events[:max_touches_per_fvg], start=1):
            ts = ev["touch_ts"]
            px = float(df_1m.loc[ts, "close"])
            pos = position_bucket(ev["max_penetration"])
            r = measure_reaction(df_1m, fvg, ts)
            rows.append({
                "fvg_date": fvg.date.date(),
                "fvg_tf_min": fvg.timeframe_min,
                "fvg_direction": fvg.direction,
                "fvg_low": fvg.low, "fvg_high": fvg.high, "fvg_ce": fvg.ce,
                "fvg_size_pts": fvg.high - fvg.low,
                "touch_ordinal": ord_,
                "touch_ts": ts,
                "touch_date": ts.date(),
                "touch_hour": ts.hour,
                "days_since_fvg": (ts.date() - fvg.date.date()).days,
                "touch_px": px,
                "max_penetration": ev["max_penetration"],
                "touch_position": pos,
                "reversed": r["reversed"],
                "react_mfe": r["mfe"],
                "react_mae": r["mae"],
            })
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Stats
# ---------------------------------------------------------------------------

def _two_prop_z(p1, n1, p2, n2):
    if n1 == 0 or n2 == 0:
        return float("nan"), float("nan")
    p = (p1 * n1 + p2 * n2) / (n1 + n2)
    se = (p * (1 - p) * (1 / n1 + 1 / n2)) ** 0.5
    if se == 0:
        return float("nan"), float("nan")
    z = (p1 - p2) / se
    from math import erf, sqrt
    p_two = 2 * (1 - 0.5 * (1 + erf(abs(z) / sqrt(2))))
    return float(z), float(p_two)


def _one_prop_z(p, n, p0=0.5):
    if n == 0:
        return float("nan"), float("nan")
    se = (p0 * (1 - p0) / n) ** 0.5
    z = (p - p0) / se
    from math import erf, sqrt
    p_two = 2 * (1 - 0.5 * (1 + erf(abs(z) / sqrt(2))))
    return float(z), float(p_two)


def by_timeframe(df: pd.DataFrame, ord_filter: Optional[int] = 1) -> pd.DataFrame:
    """Reversal rate by timeframe, restricted to first touch by default."""
    g = df if ord_filter is None else df[df["touch_ordinal"] == ord_filter]
    out = g.groupby("fvg_tf_min").agg(
        n=("reversed", "size"),
        rev_rate=("reversed", "mean"),
        avg_mfe=("react_mfe", "mean"),
        avg_mae=("react_mae", "mean"),
    ).reset_index()
    out["z_vs_50"], out["p_vs_50"] = zip(*[
        _one_prop_z(r["rev_rate"], r["n"]) for _, r in out.iterrows()])
    return out


def by_touch_ordinal(df: pd.DataFrame, tf_min: int) -> pd.DataFrame:
    """Rule of Three: does first touch differ from retests?"""
    g = df[df["fvg_tf_min"] == tf_min]
    return g.groupby("touch_ordinal").agg(
        n=("reversed", "size"),
        rev_rate=("reversed", "mean"),
        avg_mfe=("react_mfe", "mean"),
    ).reset_index()


def by_hour_of_touch(df: pd.DataFrame, tf_min: int,
                     ord_filter: Optional[int] = 1) -> pd.DataFrame:
    g = df[df["fvg_tf_min"] == tf_min]
    if ord_filter is not None:
        g = g[g["touch_ordinal"] == ord_filter]
    out = g.groupby("touch_hour").agg(
        n=("reversed", "size"),
        rev_rate=("reversed", "mean"),
        avg_mfe=("react_mfe", "mean"),
    ).reset_index()
    out["z_vs_50"], out["p_vs_50"] = zip(*[
        _one_prop_z(r["rev_rate"], r["n"]) for _, r in out.iterrows()])
    return out


def by_position(df: pd.DataFrame, tf_min: int,
                ord_filter: Optional[int] = 1) -> pd.DataFrame:
    """CE vs edge_near vs edge_far."""
    g = df[df["fvg_tf_min"] == tf_min]
    if ord_filter is not None:
        g = g[g["touch_ordinal"] == ord_filter]
    return g.groupby("touch_position").agg(
        n=("reversed", "size"),
        rev_rate=("reversed", "mean"),
        avg_mfe=("react_mfe", "mean"),
        avg_mae=("react_mae", "mean"),
    ).reset_index()


def by_days_since(df: pd.DataFrame, tf_min: int,
                  ord_filter: Optional[int] = 1) -> pd.DataFrame:
    g = df[df["fvg_tf_min"] == tf_min]
    if ord_filter is not None:
        g = g[g["touch_ordinal"] == ord_filter]
    g = g.copy()
    g["days_bucket"] = pd.cut(g["days_since_fvg"],
                              bins=[-0.5, 0.5, 1.5, 3.5, 7.5, 30.5],
                              labels=["same_day", "next_day", "2-3d",
                                      "4-7d", "8-30d"])
    return g.groupby("days_bucket", observed=True).agg(
        n=("reversed", "size"),
        rev_rate=("reversed", "mean"),
        avg_mfe=("react_mfe", "mean"),
    ).reset_index()


def by_direction(df: pd.DataFrame, tf_min: int,
                 ord_filter: Optional[int] = 1) -> pd.DataFrame:
    g = df[df["fvg_tf_min"] == tf_min]
    if ord_filter is not None:
        g = g[g["touch_ordinal"] == ord_filter]
    return g.groupby("fvg_direction").agg(
        n=("reversed", "size"),
        rev_rate=("reversed", "mean"),
        avg_mfe=("react_mfe", "mean"),
    ).reset_index()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", default=None)
    ap.add_argument("--end", default=None)
    ap.add_argument("--cache", default="mnq_1m_clean.csv")
    ap.add_argument("--outdir", default="out_fvg")
    ap.add_argument("--lookahead-days", type=int, default=20,
                    help="how many days after FVG forms to scan for touches")
    args = ap.parse_args()

    end = pd.Timestamp(args.end) if args.end else pd.Timestamp.utcnow().normalize()
    start = pd.Timestamp(args.start) if args.start else (end - pd.Timedelta(days=380))
    cache_path = Path(args.cache)
    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    df_1m = load_databento_mnq_1m(start.strftime("%Y-%m-%d"),
                                   end.strftime("%Y-%m-%d"), cache_path)
    print(f"[data] {len(df_1m):,} 1m bars, {df_1m.index.min()} -> {df_1m.index.max()}")

    # Detect FVGs at each timeframe
    all_fvgs: list[FVG] = []
    for tf in TIMEFRAMES:
        df_tf = aggregate_to_tf(df_1m, tf)
        fvgs = find_9am_fvgs(df_tf, tf)
        print(f"[fvg] tf={tf}m: {len(fvgs)} FVGs in 9AM hour")
        all_fvgs.extend(fvgs)

    # Save FVG list
    pd.DataFrame([asdict(f) for f in all_fvgs]).to_csv(
        outdir / "fvgs_detected.csv", index=False)

    # Build touch table
    print("[touch] scanning post-formation bars for touches...")
    touches = build_touch_table(df_1m, all_fvgs,
                                search_end=df_1m.index.max(),
                                max_touches_per_fvg=6)
    touches.to_csv(outdir / "fvg_touches.csv", index=False)
    print(f"[touch] {len(touches):,} touch events recorded "
          f"({touches['fvg_date'].nunique()} unique FVGs touched)")

    # ---- Tables ----
    tf_summary = by_timeframe(touches)
    tf_summary.to_csv(outdir / "tf_summary.csv", index=False)
    print("\n=== Reversal rate by FVG timeframe (FIRST touch) ===")
    print(tf_summary.to_string(index=False))

    print("\n=== Rule of Three: by touch ordinal ===")
    for tf in TIMEFRAMES:
        g = by_touch_ordinal(touches, tf)
        if not g.empty:
            print(f"  -- tf={tf}m --")
            print(g.to_string(index=False))
            g.to_csv(outdir / f"ordinal_tf{tf}.csv", index=False)

    print("\n=== Hour of touch (FIRST touch only) ===")
    for tf in TIMEFRAMES:
        g = by_hour_of_touch(touches, tf)
        if not g.empty:
            print(f"  -- tf={tf}m --")
            print(g.to_string(index=False))
            g.to_csv(outdir / f"hour_tf{tf}.csv", index=False)

    print("\n=== Touch position (CE vs edges, FIRST touch) ===")
    for tf in TIMEFRAMES:
        g = by_position(touches, tf)
        if not g.empty:
            print(f"  -- tf={tf}m --")
            print(g.to_string(index=False))
            g.to_csv(outdir / f"position_tf{tf}.csv", index=False)

    print("\n=== Days since FVG (FIRST touch) ===")
    for tf in TIMEFRAMES:
        g = by_days_since(touches, tf)
        if not g.empty:
            print(f"  -- tf={tf}m --")
            print(g.to_string(index=False))
            g.to_csv(outdir / f"days_since_tf{tf}.csv", index=False)

    print("\n=== Direction (bullish vs bearish FVG, FIRST touch) ===")
    for tf in TIMEFRAMES:
        g = by_direction(touches, tf)
        if not g.empty:
            print(f"  -- tf={tf}m --")
            print(g.to_string(index=False))
            g.to_csv(outdir / f"direction_tf{tf}.csv", index=False)

    # ---- Plain-English interpretation ----
    print("\n" + "=" * 70)
    print("INTERPRETATION (9AM FVG REVERSAL TEST)")
    print("=" * 70)

    for tf in TIMEFRAMES:
        first = touches[(touches["fvg_tf_min"] == tf) & (touches["touch_ordinal"] == 1)]
        if first.empty:
            continue
        rev = first["reversed"].mean()
        z, p = _one_prop_z(rev, len(first))
        print(f"\n  TF {tf}m: first-touch reversal = {rev*100:.1f}% (n={len(first)}, "
              f"z={z:+.2f} vs 50%, p={p:.3f})")

        # 13:00 specific
        t13 = first[first["touch_hour"] == 13]
        if len(t13):
            r13 = t13["reversed"].mean()
            z13, p13 = _one_prop_z(r13, len(t13))
            print(f"    13:00 touches: {r13*100:.1f}% (n={len(t13)}, p={p13:.3f}) "
                  f"-- compare to your prior 90% claim")

        # CE bust check
        ce = first[first["touch_position"] == "CE"]
        if len(ce):
            ce_rev = ce["reversed"].mean()
            z_ce, p_ce = _one_prop_z(ce_rev, len(ce))
            print(f"    CE touches:    {ce_rev*100:.1f}% (n={len(ce)}, p={p_ce:.3f}) "
                  f"-- compare to your prior 6.4% bust claim")

        # First vs retest comparison
        all_t = touches[touches["fvg_tf_min"] == tf]
        rest = all_t[all_t["touch_ordinal"] > 1]
        if len(rest):
            r_first = first["reversed"].mean()
            r_rest = rest["reversed"].mean()
            z, p = _two_prop_z(r_first, len(first), r_rest, len(rest))
            print(f"    First vs retest: {r_first*100:.1f}% vs {r_rest*100:.1f}% "
                  f"(p={p:.3f})")

    print(f"\nAll outputs in {outdir.resolve()}")
    print("Done.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
