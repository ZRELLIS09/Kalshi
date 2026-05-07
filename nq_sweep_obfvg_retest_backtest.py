"""
9:45 Sweep -> 1m OB + Engulfing + FVG -> Retest Entry Backtest
================================================================

Detects the strict ICT model after the 9:45 sweep:

  STEP 1 - SWEEP (in fade direction of 9AM 1H bias)
    bullish bias: first 1m bar with low < 9:45_low
    bearish bias: first 1m bar with high > 9:45_high

  STEP 2 - 3-bar OB + ENGULFING + FVG PATTERN (within 60 min after sweep)
    For BULLISH reversal:
      bar_{i-1}  : bearish candle  (close < open)         <- the OB
      bar_i      : bullish candle whose BODY fully
                   engulfs bar_{i-1}'s body
                   (close >= bar_{i-1}.open AND
                    open <= bar_{i-1}.close)
      bar_{i+1}  : low > bar_{i-1}.high                   <- creates FVG
                   (3-bar imbalance gap between
                    bar_{i-1}.high and bar_{i+1}.low)
    Mirror for BEARISH reversal.

  STEP 3 - RETEST  (within 60 min after pattern completes)
    bullish: first 1m bar with low <= FVG_high (price re-enters FVG)
    bearish: first 1m bar with high >= FVG_low

  STEP 4 - ENTRY at retest bar's close

  STEP 5 - STOP = OB.low - 2pts (bull) / OB.high + 2pts (bear)

  STEP 6 - TARGETS
    T1 = opposite 9:45 wick
    T2 = opposite 9AM 1H wick
    Time exit at 16:00 NY.

Conservative: same-bar stop + target -> stop hit first.

Outputs:
  pattern_log.csv    - one row per session showing each step's outcome
  trades.csv         - one row per qualifying entry
  summary.csv        - headline win rate / expectancy
  funnel.csv         - how many days made it to each step
  by_bias.csv, by_entry_hour.csv
  pnl_curve.png
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
from nq_945_candle_test import extract_945_candles

MAX_SWEEP_WAIT_MIN = 180
MAX_PATTERN_WAIT_MIN = 60
MAX_RETEST_WAIT_MIN = 60
STOP_BUFFER = 2.0
MIN_STOP = 4.0
EOD_HOUR = 16


def find_sweep(df_1m: pd.DataFrame, date: pd.Timestamp,
               c945: pd.Series, bias: int) -> pd.Timestamp | None:
    start = pd.Timestamp(date.date(), tz=NY_TZ).replace(hour=9, minute=50)
    end = start + pd.Timedelta(minutes=MAX_SWEEP_WAIT_MIN)
    sl = df_1m.loc[start:end]
    if sl.empty:
        return None
    candle_low = float(c945["l"]); candle_high = float(c945["h"])
    if bias > 0:
        m = sl["low"] < candle_low
    else:
        m = sl["high"] > candle_high
    if not m.any():
        return None
    return sl.index[m][0]


def detect_ob_engulf_fvg(df_1m: pd.DataFrame,
                          search_start: pd.Timestamp,
                          bias: int,
                          max_wait_min: int = MAX_PATTERN_WAIT_MIN) -> dict | None:
    """Strict 3-bar pattern detection.
    Returns dict with pattern_ts, ob_low, ob_high, fvg_low, fvg_high
    or None if no pattern found in window.
    """
    end = search_start + pd.Timedelta(minutes=max_wait_min + 3)
    sl = df_1m.loc[search_start:end]
    if len(sl) < 3:
        return None
    arr_o = sl["open"].to_numpy()
    arr_h = sl["high"].to_numpy()
    arr_l = sl["low"].to_numpy()
    arr_c = sl["close"].to_numpy()
    times = sl.index

    for i in range(1, len(sl) - 1):
        b1_o, b1_h, b1_l, b1_c = arr_o[i-1], arr_h[i-1], arr_l[i-1], arr_c[i-1]
        b2_o, b2_h, b2_l, b2_c = arr_o[i],   arr_h[i],   arr_l[i],   arr_c[i]
        b3_o, b3_h, b3_l, b3_c = arr_o[i+1], arr_h[i+1], arr_l[i+1], arr_c[i+1]

        if bias > 0:
            # bar1 bearish (OB), bar2 bullish engulfs body, bar3 leaves FVG up
            if not (b1_c < b1_o):
                continue
            if not (b2_c > b2_o):
                continue
            # body engulfing: bar2.open <= bar1.close AND bar2.close >= bar1.open
            if not (b2_o <= b1_c and b2_c >= b1_o):
                continue
            # FVG between bar1 and bar3
            if not (b3_l > b1_h):
                continue
            # constrain pattern to be within max_wait_min of search_start
            if (times[i+1] - search_start).total_seconds() / 60 > max_wait_min:
                return None
            return {
                "pattern_ts": times[i+1],
                "ob_ts": times[i-1],
                "engulf_ts": times[i],
                "ob_low": float(b1_l), "ob_high": float(b1_h),
                "fvg_low": float(b1_h), "fvg_high": float(b3_l),
                "fvg_size": float(b3_l - b1_h),
            }
        else:
            if not (b1_c > b1_o):
                continue
            if not (b2_c < b2_o):
                continue
            if not (b2_o >= b1_c and b2_c <= b1_o):
                continue
            if not (b3_h < b1_l):
                continue
            if (times[i+1] - search_start).total_seconds() / 60 > max_wait_min:
                return None
            return {
                "pattern_ts": times[i+1],
                "ob_ts": times[i-1],
                "engulf_ts": times[i],
                "ob_low": float(b1_l), "ob_high": float(b1_h),
                "fvg_low": float(b3_h), "fvg_high": float(b1_l),
                "fvg_size": float(b1_l - b3_h),
            }
    return None


def find_retest(df_1m: pd.DataFrame, pattern_ts: pd.Timestamp,
                fvg_low: float, fvg_high: float, bias: int,
                max_wait_min: int = MAX_RETEST_WAIT_MIN) -> pd.Timestamp | None:
    start = pattern_ts + pd.Timedelta(minutes=1)
    end = start + pd.Timedelta(minutes=max_wait_min)
    sl = df_1m.loc[start:end]
    if sl.empty:
        return None
    if bias > 0:
        m = sl["low"] <= fvg_high
    else:
        m = sl["high"] >= fvg_low
    if not m.any():
        return None
    return sl.index[m][0]


def simulate(df_1m: pd.DataFrame, entry_ts: pd.Timestamp, entry_px: float,
             stop_px: float, target_px: float, bias: int,
             eod_ts: pd.Timestamp) -> dict:
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
                    "pnl": float(pnl),
                    "minutes_held": int((ts - entry_ts).total_seconds() / 60)}
        if target_hit:
            pnl = (target_px - entry_px) if bias > 0 else (entry_px - target_px)
            return {"outcome": "target", "exit_ts": ts, "exit_px": target_px,
                    "pnl": float(pnl),
                    "minutes_held": int((ts - entry_ts).total_seconds() / 60)}
    eod_close = float(sl["close"].iloc[-1])
    pnl = (eod_close - entry_px) if bias > 0 else (entry_px - eod_close)
    return {"outcome": "eod", "exit_ts": sl.index[-1], "exit_px": eod_close,
            "pnl": float(pnl),
            "minutes_held": int((sl.index[-1] - entry_ts).total_seconds() / 60)}


def run(df_1m: pd.DataFrame, daily: pd.DataFrame,
        candles_945: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    funnel = {"sessions": 0, "with_bias": 0, "with_945": 0,
              "swept": 0, "pattern_formed": 0, "retested": 0,
              "trade_taken": 0}
    rows = []
    log_rows = []
    for d in daily.index:
        funnel["sessions"] += 1
        bias = int(daily.loc[d, "bias_dir"])
        if bias == 0:
            log_rows.append({"date": d.date(), "stage": "no_bias"})
            continue
        funnel["with_bias"] += 1
        if d not in candles_945.index:
            log_rows.append({"date": d.date(), "stage": "no_945"})
            continue
        funnel["with_945"] += 1
        c945 = candles_945.loc[d]

        sweep_ts = find_sweep(df_1m, d, c945, bias)
        if sweep_ts is None:
            log_rows.append({"date": d.date(), "bias": bias, "stage": "no_sweep"})
            continue
        funnel["swept"] += 1

        pat = detect_ob_engulf_fvg(df_1m, sweep_ts, bias)
        if pat is None:
            log_rows.append({"date": d.date(), "bias": bias,
                             "sweep_ts": sweep_ts, "stage": "no_pattern"})
            continue
        funnel["pattern_formed"] += 1

        retest_ts = find_retest(df_1m, pat["pattern_ts"],
                                 pat["fvg_low"], pat["fvg_high"], bias)
        if retest_ts is None:
            log_rows.append({"date": d.date(), "bias": bias,
                             "sweep_ts": sweep_ts,
                             "pattern_ts": pat["pattern_ts"],
                             "stage": "no_retest"})
            continue
        funnel["retested"] += 1

        entry_px = float(df_1m.loc[retest_ts, "close"])
        if bias > 0:
            stop_px = pat["ob_low"] - STOP_BUFFER
            t1 = float(c945["h"]); t2 = float(daily.loc[d, "nine_high"])
        else:
            stop_px = pat["ob_high"] + STOP_BUFFER
            t1 = float(c945["l"]); t2 = float(daily.loc[d, "nine_low"])
        stop_dist = abs(entry_px - stop_px)
        if stop_dist < MIN_STOP:
            log_rows.append({"date": d.date(), "bias": bias,
                             "stage": "tiny_stop", "stop_dist": stop_dist})
            continue
        funnel["trade_taken"] += 1

        eod_ts = pd.Timestamp(d.date(), tz=NY_TZ).replace(hour=EOD_HOUR)
        s1 = simulate(df_1m, retest_ts, entry_px, stop_px, t1, bias, eod_ts)
        s2 = simulate(df_1m, retest_ts, entry_px, stop_px, t2, bias, eod_ts)

        rows.append({
            "date": d.date(), "bias": bias,
            "sweep_ts": sweep_ts,
            "pattern_ts": pat["pattern_ts"],
            "retest_ts": retest_ts,
            "entry_hour": retest_ts.hour,
            "minutes_sweep_to_pattern": int((pat["pattern_ts"] - sweep_ts).total_seconds() / 60),
            "minutes_pattern_to_retest": int((retest_ts - pat["pattern_ts"]).total_seconds() / 60),
            "ob_low": pat["ob_low"], "ob_high": pat["ob_high"],
            "fvg_low": pat["fvg_low"], "fvg_high": pat["fvg_high"],
            "fvg_size": pat["fvg_size"],
            "entry_px": entry_px,
            "stop_px": stop_px, "stop_dist": stop_dist,
            "t1_px": t1, "t2_px": t2,
            "t1_dist": abs(t1 - entry_px), "t2_dist": abs(t2 - entry_px),
            "t1_R": abs(t1 - entry_px) / stop_dist,
            "t2_R": abs(t2 - entry_px) / stop_dist,
            "t1_outcome": s1["outcome"], "t1_pnl": s1["pnl"],
            "t2_outcome": s2["outcome"], "t2_pnl": s2["pnl"],
            "t1_minutes_held": s1.get("minutes_held"),
            "t2_minutes_held": s2.get("minutes_held"),
        })
        log_rows.append({"date": d.date(), "bias": bias, "stage": "TRADE",
                         "entry_px": entry_px, "t1_outcome": s1["outcome"],
                         "t1_pnl": s1["pnl"], "t2_outcome": s2["outcome"],
                         "t2_pnl": s2["pnl"]})

    return pd.DataFrame(rows), funnel


def make_pnl(trades: pd.DataFrame, outdir: Path) -> None:
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
    ax.set_title("Sweep -> 1m OB+Engulf+FVG -> Retest entry: cumulative P&L")
    ax.legend()
    fig.autofmt_xdate(); fig.tight_layout()
    fig.savefig(outdir / "pnl_curve.png", dpi=120)
    plt.close(fig)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", default=None)
    ap.add_argument("--end", default=None)
    ap.add_argument("--cache", default="mnq_1m_clean.csv")
    ap.add_argument("--outdir", default="out_obfvg_retest")
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

    trades, funnel = run(df_1m, daily, candles_945)
    trades.to_csv(outdir / "trades.csv", index=False)

    print("\n=== FUNNEL (how many sessions made it to each step) ===")
    funnel_df = pd.DataFrame([{"step": k, "count": v,
                                "pct_of_sessions": v / funnel["sessions"] * 100}
                               for k, v in funnel.items()])
    print(funnel_df.to_string(index=False))
    funnel_df.to_csv(outdir / "funnel.csv", index=False)

    if trades.empty:
        print("\nno trades to summarize")
        return 0

    print(f"\n[trades] {len(trades)}")
    for tg in ("t1", "t2"):
        n = len(trades)
        wins = (trades[f"{tg}_outcome"] == "target").sum()
        losses = (trades[f"{tg}_outcome"] == "stop").sum()
        eod = (trades[f"{tg}_outcome"] == "eod").sum()
        pnl = trades[f"{tg}_pnl"]
        wr = wins / n
        avg = pnl.mean()
        avg_w = pnl[pnl > 0].mean() if (pnl > 0).any() else 0
        avg_l = pnl[pnl < 0].mean() if (pnl < 0).any() else 0
        pf = pnl[pnl > 0].sum() / abs(pnl[pnl < 0].sum()) if (pnl < 0).any() and pnl[pnl < 0].sum() != 0 else float("inf")
        print(f"\n  TARGET = {tg}")
        print(f"    trades        : {n}")
        print(f"    wins / losses : {int(wins)} / {int(losses)} (eod={int(eod)})")
        print(f"    win rate      : {wr*100:.1f}%")
        print(f"    avg pnl       : {avg:+.2f} pts")
        print(f"    avg win / loss: {avg_w:+.2f} / {avg_l:+.2f}")
        print(f"    total pnl     : {pnl.sum():+.0f} pts")
        print(f"    profit factor : {pf:.2f}")

    print("\n=== By bias direction (T1) ===")
    bb = trades.groupby("bias").apply(
        lambda g: pd.Series({
            "n": len(g),
            "win_rate": (g["t1_outcome"] == "target").mean(),
            "avg_pnl": g["t1_pnl"].mean(),
            "total_pnl": g["t1_pnl"].sum(),
        })
    ).reset_index()
    print(bb.to_string(index=False))
    bb.to_csv(outdir / "by_bias_t1.csv", index=False)

    print("\n=== By entry hour (T1) ===")
    bh = trades.groupby("entry_hour").apply(
        lambda g: pd.Series({
            "n": len(g),
            "win_rate": (g["t1_outcome"] == "target").mean(),
            "avg_pnl": g["t1_pnl"].mean(),
            "total_pnl": g["t1_pnl"].sum(),
        })
    ).reset_index()
    print(bh.to_string(index=False))
    bh.to_csv(outdir / "by_entry_hour_t1.csv", index=False)

    make_pnl(trades, outdir)
    print(f"\n[chart] pnl_curve.png written")
    print(f"\nAll outputs in {outdir.resolve()}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
