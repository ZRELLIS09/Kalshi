"""
9:45 Sweep + Reclaim Entry Backtest
====================================

Trades the exact rule we synthesized from the 1y MNQ studies:

  1. 9AM 1H bias        : close > open => long  /  close < open => short
                           (skip dojis)
  2. After 9:50 NY      : wait up to MAX_WAIT_MIN minutes for a SWEEP of
                           the 9:45 wick OPPOSITE the bias
                           - long  : 1m low  < 9:45_low
                           - short : 1m high > 9:45_high
  3. Reclaim            : after the sweep, first 1m bar that CLOSES back
                           inside [9:45_low, 9:45_high]
  4. Entry              : at the reclaim bar's close
  5. Stop               : long  -> swept_low  - STOP_BUFFER
                           short -> swept_high + STOP_BUFFER
  6. T1                 : opposite 9:45 wick
  7. T2                 : opposite 9AM 1H wick
  8. Time limit         : if neither stop nor target hit by 16:00, exit at
                           the 16:00 close
  9. Skip days when     : no sweep, no reclaim, sweep happened in the bias
                           direction (no fade setup), or stop distance is
                           < MIN_STOP pts (unrealistic).

Two parallel simulations are run per qualifying day:
  - target = T1 (opposite 9:45 wick)
  - target = T2 (opposite 9AM 1H wick)

If both stop and target are touched in the same 1m bar, we conservatively
assume the STOP was hit first (worst case).

Outputs:
  trades.csv         - one row per qualifying day (entry, exit, P&L)
  summary.csv        - headline stats per target
  by_bias.csv        - split by long/short
  by_month.csv       - month-of-year stability
  by_entry_hour.csv  - which entry hour produces best results
  pnl_curve.png      - cumulative P&L for T1 and T2
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
from nq_945_candle_test import extract_945_candles  # 9:45 5m candle

MAX_WAIT_MIN = 180     # minutes after 9:50 to wait for the sweep
STOP_BUFFER = 2.0      # pts beyond swept extreme
MIN_STOP = 4.0         # skip trades where stop distance < this
EOD_HOUR = 16          # NY session close


def find_sweep_reclaim(df_1m: pd.DataFrame,
                       date: pd.Timestamp,
                       c945: pd.Series,
                       bias: int,
                       max_wait_min: int = MAX_WAIT_MIN) -> dict | None:
    """Locate the first sweep (in fade direction) + first reclaim back into
    the 9:45 range. Returns dict with entry/stop or None if not qualifying.
    """
    start = pd.Timestamp(date.date(), tz=NY_TZ).replace(hour=9, minute=50)
    end = start + pd.Timedelta(minutes=max_wait_min)
    sl = df_1m.loc[start:end]
    if sl.empty:
        return None

    candle_low = float(c945["l"])
    candle_high = float(c945["h"])
    if bias > 0:
        sweep_mask = sl["low"] < candle_low
        if not sweep_mask.any():
            return None
        sweep_ts = sl.index[sweep_mask][0]
        after = sl.loc[sweep_ts:]
        reclaim_mask = after["close"] > candle_low
        if not reclaim_mask.any():
            return None
        reclaim_ts = after.index[reclaim_mask][0]
        seg = sl.loc[sweep_ts:reclaim_ts]
        swept_extreme = float(seg["low"].min())
        entry_px = float(sl.loc[reclaim_ts, "close"])
        if entry_px <= candle_low:
            return None
        return {
            "sweep_ts": sweep_ts,
            "reclaim_ts": reclaim_ts,
            "entry_px": entry_px,
            "swept_extreme": swept_extreme,
            "stop_px": swept_extreme - STOP_BUFFER,
        }
    else:
        sweep_mask = sl["high"] > candle_high
        if not sweep_mask.any():
            return None
        sweep_ts = sl.index[sweep_mask][0]
        after = sl.loc[sweep_ts:]
        reclaim_mask = after["close"] < candle_high
        if not reclaim_mask.any():
            return None
        reclaim_ts = after.index[reclaim_mask][0]
        seg = sl.loc[sweep_ts:reclaim_ts]
        swept_extreme = float(seg["high"].max())
        entry_px = float(sl.loc[reclaim_ts, "close"])
        if entry_px >= candle_high:
            return None
        return {
            "sweep_ts": sweep_ts,
            "reclaim_ts": reclaim_ts,
            "entry_px": entry_px,
            "swept_extreme": swept_extreme,
            "stop_px": swept_extreme + STOP_BUFFER,
        }


def simulate_to_target(df_1m: pd.DataFrame,
                       entry_ts: pd.Timestamp,
                       entry_px: float,
                       stop_px: float,
                       target_px: float,
                       bias: int,
                       eod_ts: pd.Timestamp) -> dict:
    """Simulate from entry_ts forward bar-by-bar.
    If both stop and target touched in same bar, assume STOP first (conservative).
    """
    sl = df_1m.loc[entry_ts:eod_ts]
    if sl.empty:
        return {"outcome": "no_data"}
    sl_after = sl.iloc[1:] if len(sl) > 1 else sl.iloc[0:0]
    for ts, bar in sl_after.iterrows():
        if bias > 0:
            stop_hit = bar["low"] <= stop_px
            target_hit = bar["high"] >= target_px
        else:
            stop_hit = bar["high"] >= stop_px
            target_hit = bar["low"] <= target_px
        if stop_hit:
            pnl = (stop_px - entry_px) if bias > 0 else (entry_px - stop_px)
            return {"outcome": "stop", "exit_ts": ts, "exit_px": stop_px,
                    "pnl": float(pnl), "minutes_held": int((ts - entry_ts).total_seconds() / 60)}
        if target_hit:
            pnl = (target_px - entry_px) if bias > 0 else (entry_px - target_px)
            return {"outcome": "target", "exit_ts": ts, "exit_px": target_px,
                    "pnl": float(pnl), "minutes_held": int((ts - entry_ts).total_seconds() / 60)}
    eod_close = float(sl["close"].iloc[-1])
    pnl = (eod_close - entry_px) if bias > 0 else (entry_px - eod_close)
    return {"outcome": "eod", "exit_ts": sl.index[-1], "exit_px": eod_close,
            "pnl": float(pnl), "minutes_held": int((sl.index[-1] - entry_ts).total_seconds() / 60)}


def run_backtest(df_1m: pd.DataFrame, daily: pd.DataFrame,
                 candles_945: pd.DataFrame) -> pd.DataFrame:
    rows = []
    skipped = {"no_bias": 0, "no_945": 0, "no_sweep_reclaim": 0,
               "tiny_stop": 0}
    for d in daily.index:
        bias = int(daily.loc[d, "bias_dir"])
        if bias == 0:
            skipped["no_bias"] += 1
            continue
        if d not in candles_945.index:
            skipped["no_945"] += 1
            continue
        c945 = candles_945.loc[d]
        sr = find_sweep_reclaim(df_1m, d, c945, bias)
        if sr is None:
            skipped["no_sweep_reclaim"] += 1
            continue
        stop_dist = abs(sr["entry_px"] - sr["stop_px"])
        if stop_dist < MIN_STOP:
            skipped["tiny_stop"] += 1
            continue

        # Targets
        if bias > 0:
            t1_px = float(c945["h"])              # opposite 9:45 wick
            t2_px = float(daily.loc[d, "nine_high"])  # 9AM 1H high
        else:
            t1_px = float(c945["l"])
            t2_px = float(daily.loc[d, "nine_low"])

        eod_ts = pd.Timestamp(d.date(), tz=NY_TZ).replace(hour=EOD_HOUR)

        sim_t1 = simulate_to_target(df_1m, sr["reclaim_ts"], sr["entry_px"],
                                     sr["stop_px"], t1_px, bias, eod_ts)
        sim_t2 = simulate_to_target(df_1m, sr["reclaim_ts"], sr["entry_px"],
                                     sr["stop_px"], t2_px, bias, eod_ts)

        rows.append({
            "date": d.date(),
            "bias": bias,
            "entry_ts": sr["reclaim_ts"],
            "entry_hour": sr["reclaim_ts"].hour,
            "entry_px": sr["entry_px"],
            "stop_px": sr["stop_px"],
            "stop_dist_pts": stop_dist,
            "t1_px": t1_px, "t2_px": t2_px,
            "t1_dist_pts": abs(t1_px - sr["entry_px"]),
            "t2_dist_pts": abs(t2_px - sr["entry_px"]),
            "t1_R_multiple": abs(t1_px - sr["entry_px"]) / stop_dist if stop_dist else np.nan,
            "t2_R_multiple": abs(t2_px - sr["entry_px"]) / stop_dist if stop_dist else np.nan,
            "t1_outcome": sim_t1["outcome"], "t1_pnl": sim_t1["pnl"],
            "t1_minutes_held": sim_t1.get("minutes_held"),
            "t2_outcome": sim_t2["outcome"], "t2_pnl": sim_t2["pnl"],
            "t2_minutes_held": sim_t2.get("minutes_held"),
            "minutes_to_sweep": int((sr["sweep_ts"] - pd.Timestamp(d.date(), tz=NY_TZ).replace(hour=9, minute=50)).total_seconds() / 60),
            "minutes_to_reclaim": int((sr["reclaim_ts"] - pd.Timestamp(d.date(), tz=NY_TZ).replace(hour=9, minute=50)).total_seconds() / 60),
        })
    print(f"[skip] {skipped}")
    return pd.DataFrame(rows)


def summarize(trades: pd.DataFrame, target: str) -> dict:
    """target in {'t1', 't2'}."""
    if trades.empty:
        return {}
    pnl_col = f"{target}_pnl"
    oc_col = f"{target}_outcome"
    n = len(trades)
    wins = (trades[oc_col] == "target").sum()
    losses = (trades[oc_col] == "stop").sum()
    eod = (trades[oc_col] == "eod").sum()
    win_rate = wins / n
    pnl = trades[pnl_col]
    total_pnl = float(pnl.sum())
    avg_pnl = float(pnl.mean())
    avg_win = float(pnl[pnl > 0].mean()) if (pnl > 0).any() else 0
    avg_loss = float(pnl[pnl < 0].mean()) if (pnl < 0).any() else 0
    expectancy = avg_pnl
    profit_factor = float(pnl[pnl > 0].sum() / abs(pnl[pnl < 0].sum())) if (pnl < 0).any() and pnl[pnl < 0].sum() != 0 else float("inf")
    median_R = float(trades[f"{target}_R_multiple"].median())
    return {
        "target": target,
        "n_trades": n,
        "wins": int(wins), "losses": int(losses), "eod_exits": int(eod),
        "win_rate": float(win_rate),
        "total_pnl_pts": total_pnl,
        "avg_pnl_pts": avg_pnl,
        "avg_win_pts": avg_win,
        "avg_loss_pts": avg_loss,
        "expectancy_pts": expectancy,
        "profit_factor": profit_factor,
        "median_target_R": median_R,
    }


def by_bias(trades: pd.DataFrame, target: str) -> pd.DataFrame:
    pnl_col = f"{target}_pnl"
    oc_col = f"{target}_outcome"
    return trades.groupby("bias").apply(
        lambda g: pd.Series({
            "n": len(g),
            "win_rate": (g[oc_col] == "target").mean(),
            "avg_pnl": g[pnl_col].mean(),
            "total_pnl": g[pnl_col].sum(),
        })
    ).reset_index()


def by_month(trades: pd.DataFrame, target: str) -> pd.DataFrame:
    pnl_col = f"{target}_pnl"
    oc_col = f"{target}_outcome"
    t = trades.copy()
    t["month"] = pd.to_datetime(t["date"]).dt.to_period("M").astype(str)
    return t.groupby("month").agg(
        n=(pnl_col, "size"),
        win_rate=(oc_col, lambda s: (s == "target").mean()),
        total_pnl=(pnl_col, "sum"),
        avg_pnl=(pnl_col, "mean"),
    ).reset_index()


def by_entry_hour(trades: pd.DataFrame, target: str) -> pd.DataFrame:
    pnl_col = f"{target}_pnl"
    oc_col = f"{target}_outcome"
    return trades.groupby("entry_hour").agg(
        n=(pnl_col, "size"),
        win_rate=(oc_col, lambda s: (s == "target").mean()),
        avg_pnl=(pnl_col, "mean"),
        total_pnl=(pnl_col, "sum"),
    ).reset_index()


def make_pnl_curves(trades: pd.DataFrame, outdir: Path) -> None:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    if trades.empty:
        return
    t = trades.sort_values("date").reset_index(drop=True)
    fig, ax = plt.subplots(figsize=(10, 5))
    ax.plot(t["date"], t["t1_pnl"].cumsum(), label="T1 (opp 9:45 wick)", lw=1.5)
    ax.plot(t["date"], t["t2_pnl"].cumsum(), label="T2 (opp 9AM 1H wick)", lw=1.5)
    ax.axhline(0, color="k", lw=0.5)
    ax.set_ylabel("cumulative P&L (pts)")
    ax.set_title("Sweep + Reclaim entry: cumulative P&L")
    ax.legend()
    fig.autofmt_xdate()
    fig.tight_layout()
    fig.savefig(outdir / "pnl_curve.png", dpi=120)
    plt.close(fig)


# ---------------------------------------------------------------------------

def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", default=None)
    ap.add_argument("--end", default=None)
    ap.add_argument("--cache", default="mnq_1m_clean.csv")
    ap.add_argument("--outdir", default="out_sweep_reclaim")
    args = ap.parse_args()

    end = pd.Timestamp(args.end) if args.end else pd.Timestamp.utcnow().normalize()
    start = pd.Timestamp(args.start) if args.start else (end - pd.Timedelta(days=380))
    cache_path = Path(args.cache)
    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    df_1m = load_databento_mnq_1m(start.strftime("%Y-%m-%d"),
                                   end.strftime("%Y-%m-%d"), cache_path)
    daily = build_daily_table(df_1m)
    candles_945 = extract_945_candles(df_1m, daily)
    print(f"[data] {len(daily)} sessions, {len(candles_945)} 9:45 candles")

    trades = run_backtest(df_1m, daily, candles_945)
    trades.to_csv(outdir / "trades.csv", index=False)
    print(f"\n[trades] {len(trades)} qualifying entries")

    if trades.empty:
        print("no trades to summarize")
        return 0

    sums = []
    for tg in ["t1", "t2"]:
        s = summarize(trades, tg)
        sums.append(s)
    sum_df = pd.DataFrame(sums)
    sum_df.to_csv(outdir / "summary.csv", index=False)
    print("\n=== SUMMARY ===")
    print(sum_df.to_string(index=False))

    print("\n=== By bias ===")
    for tg in ["t1", "t2"]:
        b = by_bias(trades, tg)
        b.insert(0, "target", tg)
        print(b.to_string(index=False))
        b.to_csv(outdir / f"by_bias_{tg}.csv", index=False)

    print("\n=== By entry hour ===")
    for tg in ["t1", "t2"]:
        h = by_entry_hour(trades, tg)
        h.insert(0, "target", tg)
        print(h.to_string(index=False))
        h.to_csv(outdir / f"by_entry_hour_{tg}.csv", index=False)

    print("\n=== By month (T1) ===")
    m = by_month(trades, "t1")
    print(m.to_string(index=False))
    m.to_csv(outdir / "by_month_t1.csv", index=False)

    make_pnl_curves(trades, outdir)
    print(f"\n[chart] pnl_curve.png written")

    # Plain-English headline
    s1 = summarize(trades, "t1")
    s2 = summarize(trades, "t2")
    print("\n" + "=" * 70)
    print("HEADLINE")
    print("=" * 70)
    print(f"Trades taken: {s1['n_trades']} (out of {len(daily)} sessions)")
    print()
    print(f"TARGET = opposite 9:45 wick (T1):")
    print(f"  win rate  : {s1['win_rate']*100:.1f}%")
    print(f"  expectancy: {s1['expectancy_pts']:+.2f} pts/trade")
    print(f"  total P&L : {s1['total_pnl_pts']:+.0f} pts over the year")
    print(f"  profit factor: {s1['profit_factor']:.2f}")
    print()
    print(f"TARGET = opposite 9AM 1H wick (T2):")
    print(f"  win rate  : {s2['win_rate']*100:.1f}%")
    print(f"  expectancy: {s2['expectancy_pts']:+.2f} pts/trade")
    print(f"  total P&L : {s2['total_pnl_pts']:+.0f} pts over the year")
    print(f"  profit factor: {s2['profit_factor']:.2f}")
    print()
    print("(MNQ: 1 pt = $2/contract. NQ: 1 pt = $20/contract.)")
    print(f"\nAll outputs in {outdir.resolve()}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
