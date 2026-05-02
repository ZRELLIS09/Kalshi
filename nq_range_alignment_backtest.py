"""
NQ/MNQ Range Alignment Backtest
================================

Tests whether the 9:00 AM 1-hour candle (BIAS) and the 9:45 AM 5-minute candle
(PRECISION) project range levels (0, 0.25, 0.50, 0.75, 1.00) that align across
prior days and current day, and whether that alignment predicts continuation.

Data: Databento MNQ 1-minute OHLCV (1 year), with TradingView CSV fallback.
Timezone: America/New_York, DST-aware.

Usage:
    export DATABENTO_API_KEY=db-...
    python3 nq_range_alignment_backtest.py
        [--start 2024-05-01] [--end 2025-05-01]
        [--cache mnq_1m_clean.csv]
        [--fallback-1h ~/Downloads/nq_1h_export.csv]
        [--fallback-5m ~/Downloads/nq_5m_export.csv]
        [--outdir ./out]
"""

from __future__ import annotations

import argparse
import os
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd

NY_TZ = "America/New_York"
LEVEL_MULTIPLIERS = [0.0, 0.25, 0.50, 0.75, 1.00]
LEVEL_NAMES = ["L0", "L25", "L50", "L75", "L100"]
TOLERANCES_POINTS = [2.0, 5.0, 10.0]
TOLERANCES_PCT = [0.00025, 0.0005, 0.0010]
LOOKBACKS = [1, 3, 5, 10, 20]
DEFAULT_LOOKBACK = 5
OUTCOME_WINDOWS_HOURS = [1, 2, 3, 5, 6]  # 10-11, 10-12, 10-13, 10-15, 10-16


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

def load_databento_mnq_1m(start: str, end: str, cache_path: Path) -> pd.DataFrame:
    """Pull 1m MNQ continuous front-month from Databento. Cache to CSV."""
    if cache_path.exists():
        print(f"[load] Reading cached Databento data from {cache_path}")
        df = pd.read_csv(cache_path)
        return _normalize_ohlcv(df)

    api_key = os.environ.get("DATABENTO_API_KEY")
    if not api_key:
        raise RuntimeError(
            "DATABENTO_API_KEY env var not set and no cache at "
            f"{cache_path}. Export your key or pass --fallback paths."
        )

    print(f"[load] Pulling MNQ 1m from Databento {start} -> {end} ...")
    import databento as db  # type: ignore
    client = db.Historical(api_key)
    data = client.timeseries.get_range(
        dataset="GLBX.MDP3",
        schema="ohlcv-1m",
        symbols=["MNQ.c.0"],
        stype_in="continuous",
        start=start,
        end=end,
    )
    df = data.to_df()
    df = df.reset_index()
    df.to_csv(cache_path, index=False)
    print(f"[load] Cached {len(df):,} rows -> {cache_path}")
    return _normalize_ohlcv(df)


def load_csv_fallback(path_1h: Optional[Path], path_5m: Optional[Path]) -> dict[str, pd.DataFrame]:
    """Load TradingView exports. Returns {'1h': df, '5m': df}."""
    out: dict[str, pd.DataFrame] = {}
    for label, p in (("1h", path_1h), ("5m", path_5m)):
        if not p:
            continue
        if not p.exists():
            print(f"[load] Fallback {label} file not found: {p}")
            continue
        print(f"[load] Reading TradingView {label} from {p}")
        df = pd.read_csv(p)
        out[label] = _normalize_ohlcv(df)
    return out


def _normalize_ohlcv(df: pd.DataFrame) -> pd.DataFrame:
    """Auto-detect timestamp/OHLCV columns, set NY-tz DatetimeIndex, dedupe."""
    cols = {c.lower(): c for c in df.columns}

    def pick(*candidates: str) -> Optional[str]:
        for c in candidates:
            if c in cols:
                return cols[c]
        return None

    ts_col = pick("ts_event", "timestamp", "time", "datetime", "date")
    if ts_col is None:
        raise ValueError(f"Cannot find timestamp column in {list(df.columns)}")

    open_c = pick("open")
    high_c = pick("high")
    low_c = pick("low")
    close_c = pick("close")
    vol_c = pick("volume", "vol")

    # Mixed-offset tolerant parse via utc=True, then convert to NY
    ts = pd.to_datetime(df[ts_col], utc=True, errors="coerce")
    if ts.isna().any():
        # Some Databento outputs have ts as int ns
        try:
            ts = pd.to_datetime(df[ts_col], utc=True, unit="ns")
        except Exception:
            pass
    ts = ts.dt.tz_convert(NY_TZ)

    out = pd.DataFrame({
        "open": pd.to_numeric(df[open_c], errors="coerce").to_numpy(),
        "high": pd.to_numeric(df[high_c], errors="coerce").to_numpy(),
        "low": pd.to_numeric(df[low_c], errors="coerce").to_numpy(),
        "close": pd.to_numeric(df[close_c], errors="coerce").to_numpy(),
    })
    if vol_c is not None:
        out["volume"] = pd.to_numeric(df[vol_c], errors="coerce").to_numpy()
    out.index = pd.DatetimeIndex(ts)  # preserves NY tz

    out.index.name = "ts"
    out = out[~out.index.isna()]
    out = out[~out.index.duplicated(keep="first")]
    out = out.sort_index()
    return out


# ---------------------------------------------------------------------------
# Candle extraction
# ---------------------------------------------------------------------------

def extract_window_candle(df_1m: pd.DataFrame, day: pd.Timestamp,
                          start_h: int, start_m: int,
                          end_h: int, end_m: int) -> Optional[dict]:
    """Aggregate 1m bars in [start, end) on `day` into a single OHLC candle.

    Returns None if window is incomplete (e.g. holiday, missing minutes).
    """
    start = pd.Timestamp(day.date(), tz=NY_TZ).replace(hour=start_h, minute=start_m)
    end = pd.Timestamp(day.date(), tz=NY_TZ).replace(hour=end_h, minute=end_m)
    sl = df_1m.loc[start:end - pd.Timedelta(seconds=1)]
    if sl.empty:
        return None
    expected_minutes = int((end - start).total_seconds() // 60)
    if len(sl) < max(2, int(expected_minutes * 0.5)):
        return None  # too sparse to trust
    return {
        "open": float(sl["open"].iloc[0]),
        "high": float(sl["high"].max()),
        "low": float(sl["low"].min()),
        "close": float(sl["close"].iloc[-1]),
        "n_minutes": int(len(sl)),
        "start": start,
        "end": end,
    }


def compute_levels(low: float, high: float) -> dict[str, float]:
    rng = high - low
    return {name: low + m * rng for name, m in zip(LEVEL_NAMES, LEVEL_MULTIPLIERS)}


# ---------------------------------------------------------------------------
# Daily table
# ---------------------------------------------------------------------------

def build_daily_table(df_1m: pd.DataFrame) -> pd.DataFrame:
    """One row per session date with 9AM 1H candle, 9:45 5m candle, levels,
    10:00 reference, and post-10AM excursions for each window."""
    days = sorted({ts.date() for ts in df_1m.index})
    rows = []
    for d in days:
        day_ts = pd.Timestamp(d, tz=NY_TZ)
        nine = extract_window_candle(df_1m, day_ts, 9, 0, 10, 0)   # 9:00-9:59
        n45 = extract_window_candle(df_1m, day_ts, 9, 45, 9, 50)   # 9:45-9:49
        if nine is None or n45 is None:
            continue
        # post-10AM bars
        ten = pd.Timestamp(d, tz=NY_TZ).replace(hour=10, minute=0)
        post = df_1m.loc[ten:ten.replace(hour=16, minute=0) - pd.Timedelta(seconds=1)]
        if post.empty:
            continue

        levels_1h = compute_levels(nine["low"], nine["high"])
        levels_5m = compute_levels(n45["low"], n45["high"])

        ten_open = float(post["open"].iloc[0])
        bias_dir = 1 if nine["close"] > nine["open"] else (-1 if nine["close"] < nine["open"] else 0)

        # outcome metrics for each window
        outcomes = {}
        for hours in OUTCOME_WINDOWS_HOURS:
            wend = ten + pd.Timedelta(hours=hours)
            w = post.loc[ten:wend - pd.Timedelta(seconds=1)]
            if w.empty:
                continue
            wclose = float(w["close"].iloc[-1])
            wmfe = float(w["high"].max()) - ten_open
            wmae = float(w["low"].min()) - ten_open
            wabs = float(w["high"].max() - w["low"].min())
            outcomes[f"close_10_{10+hours}"] = wclose - ten_open
            outcomes[f"mfe_10_{10+hours}"] = wmfe
            outcomes[f"mae_10_{10+hours}"] = wmae
            outcomes[f"range_10_{10+hours}"] = wabs

        # continuation: did price hit 1h high before 1h low (bull) or vice versa?
        cont = _continuation(post, nine["high"], nine["low"], bias_dir)

        # chop score
        chop = _chop_score(post, levels_1h, nine["high"] - nine["low"])

        rows.append({
            "date": pd.Timestamp(d),
            "nine_open": nine["open"], "nine_high": nine["high"],
            "nine_low": nine["low"], "nine_close": nine["close"],
            "nine_range": nine["high"] - nine["low"],
            "n45_open": n45["open"], "n45_high": n45["high"],
            "n45_low": n45["low"], "n45_close": n45["close"],
            "n45_range": n45["high"] - n45["low"],
            **{f"1h_{k}": v for k, v in levels_1h.items()},
            **{f"5m_{k}": v for k, v in levels_5m.items()},
            "ten_open": ten_open,
            "bias_dir": bias_dir,
            "above_50_at_10": int(ten_open > levels_1h["L50"]),
            "continuation": cont,
            "chop_score": chop,
            **outcomes,
        })
    out = pd.DataFrame(rows).set_index("date").sort_index()
    print(f"[daily] Built {len(out)} valid sessions "
          f"({out.index.min().date()} -> {out.index.max().date()})")
    return out


def _continuation(post: pd.DataFrame, hi: float, lo: float, bias: int) -> str:
    """Return 'continuation', 'reversal', 'inside' depending on which level
    is hit first relative to bias."""
    hits_hi_idx = post.index[post["high"] >= hi]
    hits_lo_idx = post.index[post["low"] <= lo]
    first_hi = hits_hi_idx.min() if len(hits_hi_idx) else None
    first_lo = hits_lo_idx.min() if len(hits_lo_idx) else None
    if first_hi is None and first_lo is None:
        return "inside"
    if first_hi is not None and (first_lo is None or first_hi < first_lo):
        return "continuation" if bias > 0 else ("reversal" if bias < 0 else "up")
    return "continuation" if bias < 0 else ("reversal" if bias > 0 else "down")


def _chop_score(post: pd.DataFrame, levels_1h: dict[str, float], nine_range: float) -> float:
    """0..1 — higher = chop. Combines L50 crossings, post-range vs 9am range,
    and whether expansion outside 9AM hi/lo occurred."""
    if post.empty or nine_range <= 0:
        return 0.0
    mid = levels_1h["L50"]
    s = np.sign(post["close"] - mid).values
    crossings = int(np.sum(np.abs(np.diff(np.sign(s))) > 0))
    post_range = float(post["high"].max() - post["low"].min())
    range_ratio = post_range / nine_range
    expanded = (post["high"].max() > levels_1h["L100"]) or (post["low"].min() < levels_1h["L0"])
    score = 0.0
    if crossings >= 3:
        score += 0.4
    if range_ratio < 0.5:
        score += 0.4
    if not expanded:
        score += 0.2
    return min(1.0, score)


# ---------------------------------------------------------------------------
# Alignment
# ---------------------------------------------------------------------------

@dataclass
class AlignmentHit:
    today_level: str          # e.g. "1h_L25"
    today_price: float
    prior_date: pd.Timestamp
    prior_level: str          # e.g. "5m_L75"
    prior_price: float
    distance: float


def _today_levels(row: pd.Series, source: str = "1h") -> dict[str, float]:
    return {ln: float(row[f"{source}_{ln}"]) for ln in LEVEL_NAMES}


def find_alignments(daily: pd.DataFrame, day: pd.Timestamp,
                    lookback: int, tolerance_pts: float,
                    today_source: str = "1h",
                    prior_source: str = "5m") -> list[AlignmentHit]:
    if day not in daily.index:
        return []
    today_lvls = _today_levels(daily.loc[day], source=today_source)
    idx = daily.index.get_indexer([day])[0]
    prior_slice = daily.iloc[max(0, idx - lookback):idx]
    hits = []
    for prior_date, prior_row in prior_slice.iterrows():
        prior_lvls = _today_levels(prior_row, source=prior_source)
        for tn, tp in today_lvls.items():
            for pn, pp in prior_lvls.items():
                d = abs(tp - pp)
                if d <= tolerance_pts:
                    hits.append(AlignmentHit(
                        today_level=f"{today_source}_{tn}",
                        today_price=tp,
                        prior_date=prior_date,
                        prior_level=f"{prior_source}_{pn}",
                        prior_price=pp,
                        distance=d,
                    ))
    return hits


# ---------------------------------------------------------------------------
# Statistics
# ---------------------------------------------------------------------------

def _summarize_group(g: pd.DataFrame, label: str) -> dict:
    move = g["close_10_13"]  # primary window: 10-13 (per 13:00 importance)
    mfe = g["mfe_10_13"]
    mae = g["mae_10_13"]
    cont = (g["continuation"] == "continuation").mean() if len(g) else np.nan
    chop = g["chop_score"].mean() if len(g) else np.nan
    win = (np.sign(move) == g["bias_dir"]).mean() if len(g) else np.nan
    return {
        "group": label,
        "count": int(len(g)),
        "avg_move": float(move.mean()) if len(g) else np.nan,
        "median_move": float(move.median()) if len(g) else np.nan,
        "win_rate": float(win),
        "continuation_rate": float(cont),
        "chop_rate": float((g["chop_score"] >= 0.6).mean()) if len(g) else np.nan,
        "avg_chop_score": float(chop),
        "avg_MFE": float(mfe.mean()) if len(g) else np.nan,
        "avg_MAE": float(mae.mean()) if len(g) else np.nan,
    }


def summary_aligned_vs_not(daily: pd.DataFrame, alignment_flag: pd.Series) -> pd.DataFrame:
    aligned = daily[alignment_flag]
    nonaligned = daily[~alignment_flag]
    return pd.DataFrame([
        _summarize_group(aligned, "aligned"),
        _summarize_group(nonaligned, "non_aligned"),
    ])


def scenario_table(daily: pd.DataFrame, lookback: int, tol_pts: float) -> pd.DataFrame:
    """Classify each day into 4 scenarios and summarize."""
    scenarios = []
    for day, row in daily.iterrows():
        hits_1h = find_alignments(daily, day, lookback, tol_pts, "1h", "1h")
        hits_5m = find_alignments(daily, day, lookback, tol_pts, "1h", "5m")
        has_1h = len(hits_1h) > 0
        has_5m = len(hits_5m) > 0
        if has_1h and has_5m:
            sc = "both"
        elif has_1h:
            sc = "only_1h"
        elif has_5m:
            sc = "only_5m"
        else:
            sc = "none"
        scenarios.append({"date": day, "scenario": sc,
                          "n_hits_1h": len(hits_1h), "n_hits_5m": len(hits_5m)})
    sdf = pd.DataFrame(scenarios).set_index("date")
    merged = daily.join(sdf)
    rows = []
    for sc in ["both", "only_1h", "only_5m", "none"]:
        g = merged[merged["scenario"] == sc]
        rows.append(_summarize_group(g, sc))
    return pd.DataFrame(rows), merged


def level_respect_stats(daily: pd.DataFrame, lookback: int, tol_pts: float) -> pd.DataFrame:
    """For each (today_1h_level, prior_5m_level) pair, count alignments and
    average post-10AM move on those days."""
    counts: dict[tuple[str, str], list[float]] = {}
    for day in daily.index:
        for h in find_alignments(daily, day, lookback, tol_pts, "1h", "5m"):
            counts.setdefault((h.today_level, h.prior_level), []).append(
                float(daily.loc[day, "close_10_13"]))
    rows = []
    for (tl, pl), moves in counts.items():
        rows.append({
            "today_1h_level": tl,
            "prior_5m_level": pl,
            "count": len(moves),
            "avg_move": float(np.mean(moves)),
            "median_move": float(np.median(moves)),
            "abs_avg_move": float(np.mean(np.abs(moves))),
        })
    df_out = pd.DataFrame(rows)
    if df_out.empty:
        return df_out
    return df_out.sort_values("abs_avg_move", ascending=False)


def alignment_detail(daily: pd.DataFrame, lookback: int, tol_pts: float) -> pd.DataFrame:
    """One row per alignment hit (long-format)."""
    rows = []
    for day, row in daily.iterrows():
        for h in find_alignments(daily, day, lookback, tol_pts, "1h", "5m"):
            rows.append({
                "date": day,
                "today_1h_level": h.today_level,
                "today_price": h.today_price,
                "prior_date": h.prior_date,
                "prior_5m_level": h.prior_level,
                "prior_price": h.prior_price,
                "distance_pts": h.distance,
                "bias_dir": int(row["bias_dir"]),
                "continuation": row["continuation"],
                "post_10_13_move": float(row["close_10_13"]),
                "MFE": float(row["mfe_10_13"]),
                "MAE": float(row["mae_10_13"]),
                "chop_score": float(row["chop_score"]),
            })
    return pd.DataFrame(rows)


def tolerance_sweep(daily: pd.DataFrame, lookback: int) -> pd.DataFrame:
    """Continuation/win rate at each tolerance bucket."""
    rows = []
    for tol in TOLERANCES_POINTS:
        flags = pd.Series(False, index=daily.index)
        for day in daily.index:
            if find_alignments(daily, day, lookback, tol, "1h", "5m"):
                flags.loc[day] = True
        s = _summarize_group(daily[flags], f"aligned_{tol}pt")
        s["tolerance_pts"] = tol
        rows.append(s)
    # also pct tolerances (computed per-day on price)
    for pct in TOLERANCES_PCT:
        flags = pd.Series(False, index=daily.index)
        for day in daily.index:
            tol_pts = pct * float(daily.loc[day, "ten_open"])
            if find_alignments(daily, day, lookback, tol_pts, "1h", "5m"):
                flags.loc[day] = True
        s = _summarize_group(daily[flags], f"aligned_{pct*100:.3f}pct")
        s["tolerance_pts"] = np.nan
        s["tolerance_pct"] = pct
        rows.append(s)
    return pd.DataFrame(rows)


def lookback_sweep(daily: pd.DataFrame, tol_pts: float) -> pd.DataFrame:
    rows = []
    for lb in LOOKBACKS:
        flags = pd.Series(False, index=daily.index)
        for day in daily.index:
            if find_alignments(daily, day, lb, tol_pts, "1h", "5m"):
                flags.loc[day] = True
        s = _summarize_group(daily[flags], f"lookback_{lb}d")
        s["lookback_days"] = lb
        rows.append(s)
    return pd.DataFrame(rows)


def unexpected_patterns(daily: pd.DataFrame, alignment_detail_df: pd.DataFrame) -> dict:
    """Search for: nearest-level magnet vs rejection, cluster-count effect,
    above/below current price, day-of-week, range-size filter."""
    out: dict = {}

    # Day of week
    dow = daily.copy()
    dow["dow"] = dow.index.dayofweek
    out["by_dow"] = dow.groupby("dow").apply(
        lambda g: pd.Series({
            "count": len(g),
            "avg_move": g["close_10_13"].mean(),
            "continuation_rate": (g["continuation"] == "continuation").mean(),
            "chop_rate": (g["chop_score"] >= 0.6).mean(),
        })
    ).reset_index()

    # 9AM range size buckets
    rng_q = daily["nine_range"].quantile([0.33, 0.66]).values
    def bucket(r):
        if r <= rng_q[0]:
            return "small"
        if r <= rng_q[1]:
            return "med"
        return "large"
    daily_b = daily.copy()
    daily_b["range_bucket"] = daily_b["nine_range"].apply(bucket)
    out["by_range_bucket"] = daily_b.groupby("range_bucket").apply(
        lambda g: pd.Series({
            "count": len(g),
            "avg_move": g["close_10_13"].mean(),
            "continuation_rate": (g["continuation"] == "continuation").mean(),
        })
    ).reset_index()

    # Per-level ranking
    if not alignment_detail_df.empty:
        out["per_today_level"] = alignment_detail_df.groupby("today_1h_level").apply(
            lambda g: pd.Series({
                "count": len(g),
                "avg_abs_move": g["post_10_13_move"].abs().mean(),
                "avg_move": g["post_10_13_move"].mean(),
                "continuation_rate": (g["continuation"] == "continuation").mean(),
            })
        ).reset_index()
        out["per_prior_level"] = alignment_detail_df.groupby("prior_5m_level").apply(
            lambda g: pd.Series({
                "count": len(g),
                "avg_abs_move": g["post_10_13_move"].abs().mean(),
                "avg_move": g["post_10_13_move"].mean(),
                "continuation_rate": (g["continuation"] == "continuation").mean(),
            })
        ).reset_index()

        # Cluster size: number of distinct prior dates aligning per day
        cluster = alignment_detail_df.groupby("date").agg(
            n_hits=("today_1h_level", "size"),
            n_prior_dates=("prior_date", "nunique"),
        )
        merged = daily.join(cluster, how="left").fillna({"n_hits": 0, "n_prior_dates": 0})
        out["by_cluster_size"] = merged.groupby(pd.cut(merged["n_hits"], [-0.1, 0, 1, 3, 6, 100])).apply(
            lambda g: pd.Series({
                "count": len(g),
                "avg_move": g["close_10_13"].mean(),
                "continuation_rate": (g["continuation"] == "continuation").mean(),
                "chop_rate": (g["chop_score"] >= 0.6).mean(),
            })
        ).reset_index()

    return out


def spot_check_dates(daily: pd.DataFrame, alignment_flag: pd.Series, n: int = 5) -> pd.DataFrame:
    a = daily[alignment_flag].copy()
    na = daily[~alignment_flag].copy()
    a["abs_move"] = a["close_10_13"].abs()
    na["abs_move"] = na["close_10_13"].abs()
    rows = []
    for label, df in [
        ("best_alignment_winner", a.nlargest(n, "abs_move")),
        ("worst_alignment_failure", a.nsmallest(n, "abs_move")),
        ("best_nonalignment_trend", na.nlargest(n, "abs_move")),
        ("choppiest_nonalignment", na.nsmallest(n, "abs_move")),
    ]:
        for d, r in df.iterrows():
            rows.append({
                "category": label, "date": d.date(),
                "ten_open": r["ten_open"], "post_10_13_move": r["close_10_13"],
                "MFE": r["mfe_10_13"], "MAE": r["mae_10_13"],
                "chop_score": r["chop_score"], "continuation": r["continuation"],
            })
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Charts
# ---------------------------------------------------------------------------

def make_charts(daily: pd.DataFrame, alignment_flag: pd.Series,
                tolerance_df: pd.DataFrame, level_respect_df: pd.DataFrame,
                alignment_detail_df: pd.DataFrame, outdir: Path) -> None:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    outdir.mkdir(parents=True, exist_ok=True)

    # 1) avg move aligned vs non-aligned
    a = daily.loc[alignment_flag, "close_10_13"]
    na = daily.loc[~alignment_flag, "close_10_13"]
    fig, ax = plt.subplots(figsize=(6, 4))
    ax.bar(["aligned", "non_aligned"], [a.abs().mean(), na.abs().mean()])
    ax.set_ylabel("avg |move| 10:00-13:00 (pts)")
    ax.set_title("Avg absolute post-10:00 move: aligned vs non-aligned")
    fig.tight_layout()
    fig.savefig(outdir / "01_avg_move_aligned_vs_not.png", dpi=120)
    plt.close(fig)

    # 2) continuation rate by tolerance
    fig, ax = plt.subplots(figsize=(7, 4))
    if not tolerance_df.empty:
        td = tolerance_df.dropna(subset=["tolerance_pts"])
        ax.plot(td["tolerance_pts"], td["continuation_rate"], marker="o")
        ax.set_xlabel("tolerance (pts)")
        ax.set_ylabel("continuation rate")
        ax.set_title("Continuation rate vs tolerance (aligned days)")
    fig.tight_layout()
    fig.savefig(outdir / "02_continuation_by_tolerance.png", dpi=120)
    plt.close(fig)

    # 3) range level ranking by abs_avg_move
    fig, ax = plt.subplots(figsize=(8, 5))
    if not level_respect_df.empty:
        top = level_respect_df.head(15).copy()
        top["pair"] = top["today_1h_level"] + " <-> " + top["prior_5m_level"]
        ax.barh(top["pair"], top["abs_avg_move"])
        ax.invert_yaxis()
        ax.set_xlabel("avg |post-10:00 move| (pts)")
        ax.set_title("Top 15 level pairs by reaction size")
    fig.tight_layout()
    fig.savefig(outdir / "03_level_pair_ranking.png", dpi=120)
    plt.close(fig)

    # 4) scatter: alignment distance vs post-10AM move
    fig, ax = plt.subplots(figsize=(7, 5))
    if not alignment_detail_df.empty:
        ax.scatter(alignment_detail_df["distance_pts"],
                   alignment_detail_df["post_10_13_move"], alpha=0.4, s=14)
        ax.axhline(0, color="k", lw=0.5)
        ax.set_xlabel("alignment distance (pts)")
        ax.set_ylabel("post 10-13 move (pts)")
        ax.set_title("Alignment tightness vs realized move")
    fig.tight_layout()
    fig.savefig(outdir / "04_distance_vs_move_scatter.png", dpi=120)
    plt.close(fig)

    # 5) boxplot post-10AM move aligned vs non-aligned
    fig, ax = plt.subplots(figsize=(6, 5))
    ax.boxplot([a.values, na.values], labels=["aligned", "non_aligned"])
    ax.axhline(0, color="k", lw=0.5)
    ax.set_ylabel("post 10-13 move (pts)")
    ax.set_title("Distribution of post-10:00 moves")
    fig.tight_layout()
    fig.savefig(outdir / "05_boxplot_aligned_vs_not.png", dpi=120)
    plt.close(fig)

    print(f"[charts] saved 5 PNGs to {outdir}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", default=None, help="YYYY-MM-DD (default: 1y ago)")
    ap.add_argument("--end", default=None, help="YYYY-MM-DD (default: today)")
    ap.add_argument("--cache", default="mnq_1m_clean.csv")
    ap.add_argument("--fallback-1h", default=None)
    ap.add_argument("--fallback-5m", default=None)
    ap.add_argument("--outdir", default="out_range_alignment")
    ap.add_argument("--lookback", type=int, default=DEFAULT_LOOKBACK)
    ap.add_argument("--tolerance-pts", type=float, default=5.0)
    args = ap.parse_args()

    end = pd.Timestamp(args.end) if args.end else pd.Timestamp.utcnow().normalize()
    start = pd.Timestamp(args.start) if args.start else (end - pd.Timedelta(days=380))
    cache_path = Path(args.cache)
    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    # ---- Load ----
    df_1m: Optional[pd.DataFrame] = None
    try:
        df_1m = load_databento_mnq_1m(start.strftime("%Y-%m-%d"),
                                       end.strftime("%Y-%m-%d"),
                                       cache_path)
    except Exception as e:
        print(f"[load] Databento path failed: {e}")
        # Fallback: TV exports — we expect 1h and 5m bars; we'll synthesize
        # a 1m-equivalent table by using the 5m as if it were 1m bars for
        # extraction. This is lossier and only used when no 1m source exists.
        fb = load_csv_fallback(
            Path(args.fallback_1h).expanduser() if args.fallback_1h else None,
            Path(args.fallback_5m).expanduser() if args.fallback_5m else None,
        )
        if "5m" in fb:
            print("[load] Using 5m TV export as fallback. Note: 9:00 1H candle "
                  "will be aggregated from 5m bars (less precise).")
            df_1m = fb["5m"]
        else:
            print("[load] No data available. Exiting.")
            return 2

    if df_1m is None or df_1m.empty:
        print("[load] Empty dataframe. Exiting.")
        return 2

    print(f"[data] {len(df_1m):,} bars, {df_1m.index.min()} -> {df_1m.index.max()}")

    # ---- Daily table ----
    daily = build_daily_table(df_1m)
    if daily.empty:
        print("[daily] No valid sessions. Exiting.")
        return 2
    daily.to_csv(outdir / "daily_levels.csv")

    # ---- Default alignment flag (1h current vs 5m prior, default tol/lookback) ----
    flag = pd.Series(False, index=daily.index)
    for d in daily.index:
        if find_alignments(daily, d, args.lookback, args.tolerance_pts, "1h", "5m"):
            flag.loc[d] = True
    print(f"[align] aligned days: {int(flag.sum())} / {len(daily)} "
          f"({flag.mean()*100:.1f}%) at lookback={args.lookback}, tol={args.tolerance_pts}pts")

    # ---- Tables ----
    summary = summary_aligned_vs_not(daily, flag)
    summary.to_csv(outdir / "summary_stats.csv", index=False)
    print("\n=== SUMMARY: aligned vs non-aligned ===")
    print(summary.to_string(index=False))

    scen_df, daily_with_scenario = scenario_table(daily, args.lookback, args.tolerance_pts)
    scen_df.to_csv(outdir / "scenario_comparison.csv", index=False)
    print("\n=== SCENARIO comparison ===")
    print(scen_df.to_string(index=False))

    detail = alignment_detail(daily, args.lookback, args.tolerance_pts)
    detail.to_csv(outdir / "alignment_results.csv", index=False)

    lvl = level_respect_stats(daily, args.lookback, args.tolerance_pts)
    lvl.to_csv(outdir / "level_respect_stats.csv", index=False)
    print("\n=== TOP level pairs ===")
    print(lvl.head(10).to_string(index=False))

    tol_df = tolerance_sweep(daily, args.lookback)
    tol_df.to_csv(outdir / "tolerance_sweep.csv", index=False)
    print("\n=== TOLERANCE sweep ===")
    print(tol_df.to_string(index=False))

    lb_df = lookback_sweep(daily, args.tolerance_pts)
    lb_df.to_csv(outdir / "lookback_sweep.csv", index=False)
    print("\n=== LOOKBACK sweep ===")
    print(lb_df.to_string(index=False))

    spot = spot_check_dates(daily, flag)
    spot.to_csv(outdir / "spot_check_dates.csv", index=False)
    print("\n=== SPOT-CHECK dates ===")
    print(spot.to_string(index=False))

    extras = unexpected_patterns(daily, detail)
    for k, v in extras.items():
        if isinstance(v, pd.DataFrame):
            v.to_csv(outdir / f"extra_{k}.csv", index=False)
            print(f"\n=== EXTRA: {k} ===")
            print(v.to_string(index=False))

    # ---- Charts ----
    make_charts(daily, flag, tol_df, lvl, detail, outdir)

    # ---- Plain-English interpretation ----
    print("\n" + "=" * 70)
    print("INTERPRETATION")
    print("=" * 70)

    a_move = float(daily.loc[flag, "close_10_13"].abs().mean())
    na_move = float(daily.loc[~flag, "close_10_13"].abs().mean())
    a_cont = float((daily.loc[flag, "continuation"] == "continuation").mean())
    na_cont = float((daily.loc[~flag, "continuation"] == "continuation").mean())
    a_chop = float((daily.loc[flag, "chop_score"] >= 0.6).mean())
    na_chop = float((daily.loc[~flag, "chop_score"] >= 0.6).mean())

    supported = (a_move > na_move) and (a_cont > na_cont)
    print(f"1) Hypothesis SUPPORTED? {'YES' if supported else 'NO'}")
    print(f"   aligned avg |move|     = {a_move:.1f} pts vs non-aligned {na_move:.1f}")
    print(f"   aligned continuation   = {a_cont*100:.1f}% vs non-aligned {na_cont*100:.1f}%")
    print(f"   aligned chop rate      = {a_chop*100:.1f}% vs non-aligned {na_chop*100:.1f}%")

    if not lvl.empty:
        print("\n2) Most-respected level pairs (top 5 by avg |move|):")
        for _, r in lvl.head(5).iterrows():
            print(f"   {r['today_1h_level']} <-> {r['prior_5m_level']}: "
                  f"n={int(r['count'])}, avg|move|={r['abs_avg_move']:.1f}")

    if not tol_df.empty:
        best = tol_df.dropna(subset=["continuation_rate"]).sort_values(
            "continuation_rate", ascending=False).head(1)
        if not best.empty:
            print(f"\n5) Best tolerance: {best.iloc[0]['group']} "
                  f"(continuation={best.iloc[0]['continuation_rate']*100:.1f}%)")

    if not lb_df.empty:
        best_lb = lb_df.sort_values("continuation_rate", ascending=False).head(1)
        if not best_lb.empty:
            print(f"6) Best lookback: {best_lb.iloc[0]['group']} "
                  f"(continuation={best_lb.iloc[0]['continuation_rate']*100:.1f}%)")

    if not scen_df.empty:
        both = scen_df[scen_df["group"] == "both"].iloc[0]
        only5 = scen_df[scen_df["group"] == "only_5m"].iloc[0]
        only1 = scen_df[scen_df["group"] == "only_1h"].iloc[0]
        none = scen_df[scen_df["group"] == "none"].iloc[0]
        print("\n7) Does 9:45 5m add precision to 9AM 1H?")
        print(f"   both:    cont={both['continuation_rate']*100:.1f}% mfe={both['avg_MFE']:.1f}")
        print(f"   only_1h: cont={only1['continuation_rate']*100:.1f}% mfe={only1['avg_MFE']:.1f}")
        print(f"   only_5m: cont={only5['continuation_rate']*100:.1f}% mfe={only5['avg_MFE']:.1f}")
        print(f"   none:    cont={none['continuation_rate']*100:.1f}% mfe={none['avg_MFE']:.1f}")
        adds = both["continuation_rate"] > only1["continuation_rate"]
        print(f"   -> 9:45 layer {'ADDS' if adds else 'does NOT add'} precision.")

    print("\n8) Dates to manually review on TradingView:")
    print(spot.head(20)[["category", "date", "post_10_13_move", "chop_score"]].to_string(index=False))

    print(f"\nAll CSVs and PNGs written to: {outdir.resolve()}")
    print("Done.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
