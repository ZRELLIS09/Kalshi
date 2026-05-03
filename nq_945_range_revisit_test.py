"""
9:45 5m Candle Range Revisit + Chain Test
=========================================

For each 9:45 candle:
  1. Project levels L0/L25/L50/L75/L100 (wick-to-wick range)
  2. After 10:00 NY, track every NEW touch of each level
     (a new touch = bar enters tolerance zone, prior bar was out)
  3. Detect "expansion away" = price moved >= range_size beyond hi/lo
     AND stayed outside the entire range for >= STAY_OUT_MIN minutes
  4. Count revisits per level BEFORE expansion
  5. After expansion, scan forward up to CHAIN_LOOKAHEAD_DAYS to see if
     price reaches a DIFFERENT day's 9:45 candle range (chain test)

Outputs:
  - per_candle.csv: one row per 9:45 candle with summary
  - touch_events.csv: long-format every revisit
  - by_hour_pre_expansion.csv: revisit count by hour-of-day
  - by_level.csv: revisit count by level, split by hour
  - chain_results.csv: chain to next 9:45 range (which day, which level, gap)
  - level_first_hit.csv: which level gets touched FIRST after 10:00

Usage:
  export DATABENTO_API_KEY=db-...
  python3 nq_945_range_revisit_test.py --cache mnq_1m_clean.csv \
      --outdir out_945_revisit
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

LEVEL_MULTS = {"L0": 0.0, "L25": 0.25, "L50": 0.5, "L75": 0.75, "L100": 1.0}
LEVEL_ORDER = ["L0", "L25", "L50", "L75", "L100"]
TOLERANCE_PTS = 2.0
EXPANSION_MULT = 1.0       # expansion = move >= 1.0 * range_size beyond hi/lo
STAY_OUT_MIN = 30          # minutes outside whole range to confirm expansion
CHAIN_LOOKAHEAD_DAYS = 10
SCAN_END_HOUR = 16         # NY session end


def compute_levels(low: float, high: float) -> dict[str, float]:
    rng = high - low
    return {name: low + LEVEL_MULTS[name] * rng for name in LEVEL_ORDER}


def extract_945_candles(df_1m: pd.DataFrame, daily: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for d in daily.index:
        start = pd.Timestamp(d.date(), tz=NY_TZ).replace(hour=9, minute=45)
        end = start + pd.Timedelta(minutes=5)
        sl = df_1m.loc[start:end - pd.Timedelta(seconds=1)]
        if len(sl) < 3:
            continue
        h = float(sl["high"].max()); l = float(sl["low"].min())
        rows.append({
            "date": d, "high": h, "low": l, "range_pts": h - l,
            "open": float(sl["open"].iloc[0]),
            "close": float(sl["close"].iloc[-1]),
        })
    return pd.DataFrame(rows).set_index("date")


def detect_expansion(df_post: pd.DataFrame, hi: float, lo: float,
                     range_size: float,
                     stay_out_min: int = STAY_OUT_MIN,
                     mult: float = EXPANSION_MULT) -> tuple[pd.Timestamp | None, int]:
    """Find first ts where price moved >= range_size*mult above hi or below lo
    AND stayed entirely outside [lo, hi] for stay_out_min consecutive minutes
    after that. Returns (expansion_ts, direction +1/-1) or (None, 0).
    """
    if df_post.empty or range_size <= 0:
        return None, 0
    threshold_up = hi + range_size * mult
    threshold_dn = lo - range_size * mult
    # Candidate moments where threshold is breached
    up_mask = df_post["high"] >= threshold_up
    dn_mask = df_post["low"] <= threshold_dn
    candidates_up = df_post.index[up_mask]
    candidates_dn = df_post.index[dn_mask]
    # Confirm "stays out" for stay_out_min after breach
    def confirm(ts: pd.Timestamp, direction: int) -> bool:
        end = ts + pd.Timedelta(minutes=stay_out_min)
        seg = df_post.loc[ts:end]
        if len(seg) < stay_out_min:
            return False
        if direction > 0:
            # never came back DOWN into [lo, hi] for stay_out_min
            in_range = (seg["low"] <= hi) & (seg["high"] >= lo)
            return not in_range.any()
        else:
            in_range = (seg["low"] <= hi) & (seg["high"] >= lo)
            return not in_range.any()
    first_up = next((ts for ts in candidates_up if confirm(ts, 1)), None)
    first_dn = next((ts for ts in candidates_dn if confirm(ts, -1)), None)
    if first_up is None and first_dn is None:
        return None, 0
    if first_up is not None and (first_dn is None or first_up < first_dn):
        return first_up, 1
    return first_dn, -1


def count_level_touches(df_post: pd.DataFrame, levels: dict[str, float],
                        tol: float = TOLERANCE_PTS,
                        end_ts: pd.Timestamp | None = None) -> dict:
    """Returns {level_name: [touch_ts1, touch_ts2, ...]} for new touches
    (bar enters tolerance zone, prior bar was out)."""
    if df_post.empty:
        return {ln: [] for ln in LEVEL_ORDER}
    sl = df_post if end_ts is None else df_post.loc[:end_ts]
    if sl.empty:
        return {ln: [] for ln in LEVEL_ORDER}
    out = {}
    for name, lvl in levels.items():
        in_zone = ((sl["high"] >= lvl - tol) & (sl["low"] <= lvl + tol)).to_numpy()
        if not in_zone.any():
            out[name] = []
            continue
        pad_prev = np.concatenate([[False], in_zone[:-1]])
        starts = in_zone & ~pad_prev
        idx = np.where(starts)[0]
        out[name] = [sl.index[i] for i in idx]
    return out


def first_level_hit(touches: dict) -> tuple[str | None, pd.Timestamp | None]:
    """Across all levels, which one was touched FIRST?"""
    earliest = []
    for name, ts_list in touches.items():
        if ts_list:
            earliest.append((ts_list[0], name))
    if not earliest:
        return None, None
    earliest.sort()
    return earliest[0][1], earliest[0][0]


def find_next_945_hit(df_1m: pd.DataFrame, candles: pd.DataFrame,
                      from_ts: pd.Timestamp, current_date: pd.Timestamp,
                      lookahead_days: int = CHAIN_LOOKAHEAD_DAYS) -> dict:
    """After from_ts, find first time price enters ANOTHER day's 9:45
    candle range (any level inside [low, high])."""
    end = from_ts + pd.Timedelta(days=lookahead_days)
    sl = df_1m.loc[from_ts:end]
    if sl.empty:
        return {}
    # Iterate over candles in chronological order; check each one (skip current)
    for cd, c in candles.iterrows():
        if cd.date() == current_date.date():
            continue
        # Only consider candles whose date is on/after current_date for forward
        # chain (or within lookback for backward chain — we test forward only).
        if cd.date() < current_date.date():
            continue
        in_zone = (sl["high"] >= c["low"]) & (sl["low"] <= c["high"])
        if not in_zone.any():
            continue
        first_hit = sl.index[in_zone][0]
        # which level was closest to first hit price?
        px = float(sl.loc[first_hit, "close"])
        levels_other = compute_levels(c["low"], c["high"])
        nearest = min(levels_other, key=lambda k: abs(levels_other[k] - px))
        return {
            "next_candle_date": cd.date(),
            "next_first_hit_ts": first_hit,
            "next_first_hit_level": nearest,
            "next_first_hit_px": px,
            "minutes_from_expansion": int((first_hit - from_ts).total_seconds() / 60),
        }
    return {}


# ---------------------------------------------------------------------------

def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", default=None)
    ap.add_argument("--end", default=None)
    ap.add_argument("--cache", default="mnq_1m_clean.csv")
    ap.add_argument("--outdir", default="out_945_revisit")
    ap.add_argument("--scan-days", type=int, default=5,
                    help="how many days after 9:45 candle to scan for revisits")
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
    print(f"[945] {len(candles)} candles, median range = {candles['range_pts'].median():.1f} pts")

    per_candle_rows = []
    touch_event_rows = []
    chain_rows = []
    first_hit_rows = []

    for date, c in candles.iterrows():
        levels = compute_levels(c["low"], c["high"])
        scan_start = pd.Timestamp(date.date(), tz=NY_TZ).replace(hour=9, minute=50)
        # scan up to scan_days * full sessions
        scan_end = pd.Timestamp(date.date(), tz=NY_TZ) + pd.Timedelta(days=args.scan_days)
        scan_end = scan_end.replace(hour=SCAN_END_HOUR)
        sl = df_1m.loc[scan_start:scan_end]
        if sl.empty:
            continue

        # Detect expansion event (within scan window)
        exp_ts, exp_dir = detect_expansion(sl, c["high"], c["low"], c["range_pts"])

        # Count touches BEFORE expansion (or all if no expansion)
        cutoff = exp_ts if exp_ts is not None else scan_end
        touches_pre = count_level_touches(sl, levels, end_ts=cutoff)
        # Count touches AFTER expansion (revisits post-expansion = chain back)
        touches_post = {ln: [] for ln in LEVEL_ORDER}
        if exp_ts is not None:
            sl_post = sl.loc[exp_ts:]
            touches_post = count_level_touches(sl_post, levels)

        # First level hit (any time)
        all_touches = count_level_touches(sl, levels, end_ts=scan_end)
        first_lvl, first_ts = first_level_hit(all_touches)

        per_candle_row = {
            "date": date.date(),
            "candle_high": c["high"], "candle_low": c["low"],
            "range_pts": c["range_pts"],
            "expanded": exp_ts is not None,
            "expansion_ts": exp_ts,
            "expansion_dir": exp_dir,
            "minutes_to_expansion": (
                int((exp_ts - scan_start).total_seconds() / 60)
                if exp_ts is not None else None),
            "first_level_hit": first_lvl,
            "first_level_hit_ts": first_ts,
            "first_level_hit_minutes_after_950": (
                int((first_ts - scan_start).total_seconds() / 60)
                if first_ts is not None else None),
        }
        for ln in LEVEL_ORDER:
            per_candle_row[f"touches_pre_exp_{ln}"] = len(touches_pre[ln])
            per_candle_row[f"touches_post_exp_{ln}"] = len(touches_post[ln])
        per_candle_row["total_touches_pre_exp"] = sum(
            len(touches_pre[ln]) for ln in LEVEL_ORDER)
        per_candle_row["total_touches_post_exp"] = sum(
            len(touches_post[ln]) for ln in LEVEL_ORDER)
        per_candle_rows.append(per_candle_row)

        # Long-format touch events (PRE-expansion)
        for ln, ts_list in touches_pre.items():
            for ord_, ts in enumerate(ts_list, start=1):
                touch_event_rows.append({
                    "candle_date": date.date(),
                    "level": ln, "level_price": levels[ln],
                    "phase": "pre_expansion",
                    "ordinal": ord_,
                    "touch_ts": ts,
                    "touch_hour": ts.hour,
                    "minutes_after_950": int((ts - scan_start).total_seconds() / 60),
                    "days_after_formation": (ts.date() - date.date()).days,
                })
        # Long-format touch events (POST-expansion)
        for ln, ts_list in touches_post.items():
            for ord_, ts in enumerate(ts_list, start=1):
                touch_event_rows.append({
                    "candle_date": date.date(),
                    "level": ln, "level_price": levels[ln],
                    "phase": "post_expansion",
                    "ordinal": ord_,
                    "touch_ts": ts,
                    "touch_hour": ts.hour,
                    "minutes_after_950": int((ts - scan_start).total_seconds() / 60),
                    "days_after_formation": (ts.date() - date.date()).days,
                })
        if first_lvl is not None:
            first_hit_rows.append({
                "candle_date": date.date(),
                "first_level": first_lvl,
                "first_ts": first_ts,
                "first_hour": first_ts.hour,
                "minutes_after_950": int((first_ts - scan_start).total_seconds() / 60),
            })

        # Chain test: after expansion, did price reach a different 9:45 range?
        if exp_ts is not None:
            chain = find_next_945_hit(df_1m, candles, exp_ts, date)
            chain_rows.append({
                "candle_date": date.date(),
                "expansion_ts": exp_ts,
                "expansion_dir": exp_dir,
                "chain_found": bool(chain),
                **(chain or {}),
            })

    pc = pd.DataFrame(per_candle_rows).set_index("date")
    te = pd.DataFrame(touch_event_rows)
    fh = pd.DataFrame(first_hit_rows)
    ch = pd.DataFrame(chain_rows)
    pc.to_csv(outdir / "per_candle.csv")
    te.to_csv(outdir / "touch_events.csv", index=False)
    fh.to_csv(outdir / "level_first_hit.csv", index=False)
    ch.to_csv(outdir / "chain_results.csv", index=False)

    # ---- aggregations ----

    print("\n=== Per-candle expansion overview ===")
    print(f"  expanded within {args.scan_days} days: "
          f"{int(pc['expanded'].sum())} / {len(pc)} "
          f"({pc['expanded'].mean()*100:.1f}%)")
    if pc["expanded"].any():
        ed = pc[pc["expanded"]]
        print(f"  median minutes to expansion: "
              f"{int(ed['minutes_to_expansion'].median())} ({int(ed['minutes_to_expansion'].median()/60)}h "
              f"{int(ed['minutes_to_expansion'].median()%60)}m after 9:50)")
        print(f"  expansion direction: "
              f"up={int((ed['expansion_dir']==1).sum())}  "
              f"down={int((ed['expansion_dir']==-1).sum())}")

    # First level hit distribution
    if not fh.empty:
        print("\n=== Which level gets touched FIRST after 9:50? ===")
        first_dist = fh["first_level"].value_counts(normalize=False).reindex(LEVEL_ORDER, fill_value=0)
        for ln in LEVEL_ORDER:
            print(f"  {ln}: {first_dist[ln]} ({first_dist[ln]/len(fh)*100:.1f}%)")
        print("\n  Hour of first touch:")
        hour_first = fh.groupby("first_hour").size().reset_index(name="n")
        print(hour_first.to_string(index=False))
        hour_first.to_csv(outdir / "first_hit_hour_dist.csv", index=False)

    # Touches per level (pre-expansion)
    if not te.empty:
        pre = te[te["phase"] == "pre_expansion"]
        print(f"\n=== Revisit count per level (PRE-expansion only, {len(pre):,} events) ===")
        per_level = pre.groupby("level").size().reindex(LEVEL_ORDER, fill_value=0)
        for ln in LEVEL_ORDER:
            print(f"  {ln}: {per_level[ln]:,} touches  "
                  f"({per_level[ln]/len(pc):.2f} per candle)")
        per_level.to_csv(outdir / "by_level_pre.csv")

        print("\n=== Touch count by HOUR-OF-DAY (pre-expansion) ===")
        hour_tot = pre.groupby("touch_hour").size().reset_index(name="n_touches")
        hour_tot["pct"] = hour_tot["n_touches"] / hour_tot["n_touches"].sum() * 100
        print(hour_tot.to_string(index=False))
        hour_tot.to_csv(outdir / "by_hour_pre_expansion.csv", index=False)

        print("\n=== Touch count by hour x level (pre-expansion, top 15) ===")
        hxl = pre.groupby(["touch_hour", "level"]).size().reset_index(name="n")
        hxl = hxl.sort_values("n", ascending=False)
        print(hxl.head(15).to_string(index=False))
        hxl.to_csv(outdir / "by_level.csv", index=False)

        # Distribution of total touches per candle (pre-expansion)
        print("\n=== Distribution of total revisits per candle (pre-expansion) ===")
        dist = pc["total_touches_pre_exp"].describe(percentiles=[0.25, 0.5, 0.75, 0.9])
        print(dist.to_string())

        # Days-since: when do most revisits happen?
        print("\n=== Revisits by days-since-formation ===")
        ds = pre.groupby("days_after_formation").size().reset_index(name="n")
        print(ds.to_string(index=False))
        ds.to_csv(outdir / "by_days_since.csv", index=False)

    # Chain analysis
    if not ch.empty:
        print(f"\n=== CHAIN: did expansion lead to another 9:45 candle range? ===")
        print(f"  candles that expanded: {len(ch)}")
        print(f"  chained to another 9:45 within {CHAIN_LOOKAHEAD_DAYS} days: "
              f"{int(ch['chain_found'].sum())} ({ch['chain_found'].mean()*100:.1f}%)")
        if ch["chain_found"].any():
            chf = ch[ch["chain_found"]]
            print(f"  median minutes to next-945 hit: "
                  f"{int(chf['minutes_from_expansion'].median())}")
            # which level of next 9:45 gets hit
            level_dist = chf["next_first_hit_level"].value_counts().reindex(LEVEL_ORDER, fill_value=0)
            print(f"  level of NEXT 9:45 hit first:")
            for ln in LEVEL_ORDER:
                print(f"    {ln}: {level_dist[ln]} ({level_dist[ln]/len(chf)*100:.1f}%)")

    # ---- print specific spot-check events ----
    print("\n=== Sample candles with revisit detail (first 8 days) ===")
    sample = pc.head(8)
    for d, row in sample.iterrows():
        exp_str = ""
        if row["expanded"]:
            exp_str = f" @{int(row['minutes_to_expansion'])}m"
        first_lvl = row["first_level_hit"] or "-"
        first_min = int(row["first_level_hit_minutes_after_950"]) if row["first_level_hit_minutes_after_950"] is not None else 0
        print(f"  {d} range={row['range_pts']:.1f}  "
              f"first_hit={first_lvl} @{first_min}m after 9:50  "
              f"total_pre_exp_touches={int(row['total_touches_pre_exp'])}  "
              f"expanded={'YES' if row['expanded'] else 'NO'}{exp_str}")

    print(f"\nAll outputs in {outdir.resolve()}")
    print("Done.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
