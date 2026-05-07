"""
SD Level Alignment Backtest — NQ / MNQ
======================================

Tests whether the 9:00 AM 1-hour candle range and the 9:45 AM 5-minute candle
range project SD (standard deviation / range projection) levels that ALIGN
across days, and whether that alignment predicts stronger intraday continuation.

Hypothesis (per @LimitlessgoatMi framework + prior research):
    - Today's 9AM 1H SD levels should align with a PRIOR day's 9:45 5m SD levels
      before a stronger intraday move continues.
    - Alignment = high conviction.
    - Non-alignment = chop or failure.

Data sources (in priority order):
    1. mnq_1m_clean.csv (cached pull)
    2. Databento API   (if DATABENTO_API_KEY set)
    3. nq_1h_export.csv + nq_5m_export.csv  (TradingView export fallback)
    4. --demo synthetic data  (smoke test only — NOT for real conclusions)

Run:
    python sd_alignment_backtest.py            # auto-detect data
    python sd_alignment_backtest.py --demo     # synthetic data smoke test
    python sd_alignment_backtest.py --pull     # force fresh Databento pull

All artifacts written to ./artifacts/ .
"""

from __future__ import annotations

import argparse
import os
import sys
import math
import warnings
from dataclasses import dataclass, field
from datetime import datetime, timedelta, time as dtime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt

warnings.filterwarnings("ignore", category=FutureWarning)

NY_TZ = "America/New_York"
ARTIFACTS = Path("artifacts")
ARTIFACTS.mkdir(exist_ok=True)

MULTIPLIERS = [-1.00, -0.75, -0.50, -0.25, 0.0, 0.25, 0.50, 0.75,
               1.00, 1.25, 1.50, 1.75, 2.00]
LOOKBACKS = [1, 3, 5, 10, 20]
DEFAULT_LOOKBACK = 5
TOLERANCES_PT = [2.0, 5.0, 10.0]
TOLERANCES_PCT = [0.00025, 0.0005, 0.0010]   # 0.025%, 0.05%, 0.10%
WINDOWS = [("10-11", dtime(10, 0), dtime(11, 0)),
           ("10-12", dtime(10, 0), dtime(12, 0)),
           ("10-13", dtime(10, 0), dtime(13, 0)),
           ("10-15", dtime(10, 0), dtime(15, 0)),
           ("10-16", dtime(10, 0), dtime(16, 0))]


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

COL_ALIASES = {
    "timestamp": ["timestamp", "ts_event", "datetime", "time", "date", "Date", "Time", "Datetime"],
    "open":      ["open", "Open", "o", "OPEN"],
    "high":      ["high", "High", "h", "HIGH"],
    "low":       ["low",  "Low",  "l", "LOW"],
    "close":     ["close", "Close", "c", "CLOSE"],
    "volume":    ["volume", "Volume", "v", "VOLUME"],
}


def _resolve_columns(df: pd.DataFrame) -> Dict[str, str]:
    """Auto-detect OHLCV column names, case-insensitive, with fallback aliases."""
    lower = {c.lower(): c for c in df.columns}
    out: Dict[str, str] = {}
    for canonical, options in COL_ALIASES.items():
        for opt in options:
            if opt.lower() in lower:
                out[canonical] = lower[opt.lower()]
                break
    missing = [k for k in ("timestamp", "open", "high", "low", "close")
               if k not in out]
    if missing:
        raise ValueError(f"Could not auto-detect columns {missing}. "
                         f"Saw: {list(df.columns)}")
    return out


def _normalize_ohlcv(df: pd.DataFrame) -> pd.DataFrame:
    """Return a clean OHLCV frame indexed by NY-aware DatetimeIndex."""
    cmap = _resolve_columns(df)
    out = pd.DataFrame({
        "timestamp": df[cmap["timestamp"]],
        "open":  pd.to_numeric(df[cmap["open"]],  errors="coerce"),
        "high":  pd.to_numeric(df[cmap["high"]],  errors="coerce"),
        "low":   pd.to_numeric(df[cmap["low"]],   errors="coerce"),
        "close": pd.to_numeric(df[cmap["close"]], errors="coerce"),
    })
    if "volume" in cmap:
        out["volume"] = pd.to_numeric(df[cmap["volume"]], errors="coerce")

    # Mixed-offset safe parsing (TradingView often exports +HH:MM in the string)
    ts = pd.to_datetime(out["timestamp"], utc=True, errors="coerce",
                        format="mixed")
    out["timestamp"] = ts.dt.tz_convert(NY_TZ)
    out = (out.dropna(subset=["timestamp", "open", "high", "low", "close"])
              .drop_duplicates(subset=["timestamp"])
              .sort_values("timestamp")
              .set_index("timestamp"))
    return out


def _load_csv(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path)
    return _normalize_ohlcv(df)


def _resample(df: pd.DataFrame, rule: str) -> pd.DataFrame:
    agg = {"open": "first", "high": "max", "low": "min", "close": "last"}
    if "volume" in df.columns:
        agg["volume"] = "sum"
    return (df.resample(rule, label="left", closed="left")
              .agg(agg)
              .dropna(subset=["open", "high", "low", "close"]))


def _databento_pull() -> Optional[pd.DataFrame]:
    """Pull ~1 year of MNQ continuous front-month 1-minute bars."""
    api_key = os.environ.get("DATABENTO_API_KEY")
    if not api_key:
        return None
    try:
        import databento as db  # type: ignore
    except ImportError:
        print("[data] databento not installed; skipping API path.")
        return None
    print("[data] pulling MNQ 1-minute bars from Databento (~1 yr)...")
    client = db.Historical(api_key)
    end = pd.Timestamp.utcnow().floor("D")
    start = end - pd.Timedelta(days=365)
    data = client.timeseries.get_range(
        dataset="GLBX.MDP3",
        symbols=["MNQ.c.0"],          # continuous front-month
        stype_in="continuous",
        schema="ohlcv-1m",
        start=start.isoformat(),
        end=end.isoformat(),
    )
    df = data.to_df()
    df = df.reset_index().rename(columns={"ts_event": "timestamp"})
    out = _normalize_ohlcv(df)
    out.to_csv("mnq_1m_clean.csv")
    print(f"[data] cached {len(out):,} rows → mnq_1m_clean.csv")
    return out


def load_data(force_pull: bool = False, demo: bool = False
              ) -> Tuple[pd.DataFrame, pd.DataFrame, str]:
    """
    Returns (bars_5m, bars_1h, source_label).
    Tries: cached 1m → Databento → TV exports → demo (if requested).
    """
    if demo:
        return _make_demo()

    cached = Path("mnq_1m_clean.csv")
    if cached.exists() and not force_pull:
        print(f"[data] using cached {cached}")
        m1 = _load_csv(cached)
        return _resample(m1, "5min"), _resample(m1, "60min"), "mnq_1m_clean.csv"

    if force_pull or os.environ.get("DATABENTO_API_KEY"):
        m1 = _databento_pull()
        if m1 is not None:
            return _resample(m1, "5min"), _resample(m1, "60min"), "databento:MNQ.c.0"

    # Last resort: TV exports
    home = Path.home()
    candidates = [Path("nq_1h_export.csv"), home / "Downloads/nq_1h_export.csv"]
    h1_path = next((p for p in candidates if p.exists()), None)
    candidates = [Path("nq_5m_export.csv"), home / "Downloads/nq_5m_export.csv"]
    m5_path = next((p for p in candidates if p.exists()), None)
    if h1_path and m5_path:
        print(f"[data] using TV exports {h1_path} + {m5_path}")
        return _load_csv(m5_path), _load_csv(h1_path), f"{m5_path.name}+{h1_path.name}"

    raise FileNotFoundError(
        "No data found. Provide one of:\n"
        "  • mnq_1m_clean.csv\n"
        "  • DATABENTO_API_KEY env var (for fresh pull)\n"
        "  • nq_5m_export.csv + nq_1h_export.csv (TV exports in cwd or ~/Downloads)\n"
        "Or run with --demo for a synthetic-data smoke test."
    )


# ---------------------------------------------------------------------------
# Synthetic demo data (smoke test only)
# ---------------------------------------------------------------------------

def _make_demo() -> Tuple[pd.DataFrame, pd.DataFrame, str]:
    """Generate ~1 year of synthetic 5m bars in NY tz with realistic intraday shape."""
    print("[data] generating synthetic 1-year MNQ-shaped data (DEMO ONLY)")
    rng = np.random.default_rng(42)
    days = pd.bdate_range("2024-04-01", "2025-04-30", tz=NY_TZ)
    rows = []
    px = 18000.0
    for d in days:
        # 6:00 AM → 4:00 PM NY, 5-minute bars
        session = pd.date_range(d.normalize() + pd.Timedelta(hours=6),
                                d.normalize() + pd.Timedelta(hours=16),
                                freq="5min", tz=NY_TZ, inclusive="left")
        # gentle daily drift + heteroskedastic intraday vol
        drift = rng.normal(0, 30)
        for i, ts in enumerate(session):
            hour = ts.hour + ts.minute / 60.0
            vol = 4 + 6 * math.exp(-((hour - 9.5) ** 2) / 4)   # punch around 9:30
            o = px
            ret = rng.normal(drift / len(session), vol)
            c = o + ret
            rng_intra = abs(rng.normal(0, vol * 0.7)) + 1
            h = max(o, c) + rng_intra
            l = min(o, c) - rng_intra
            rows.append((ts, o, h, l, c, int(rng.integers(50, 500))))
            px = c
        # overnight gap
        px += rng.normal(0, 25)
    m5 = pd.DataFrame(rows, columns=["timestamp", "open", "high", "low", "close", "volume"]
                      ).set_index("timestamp")
    m5.index.name = "timestamp"
    h1 = _resample(m5, "60min")
    return m5, h1, "synthetic-demo"


# ---------------------------------------------------------------------------
# Daily candle extraction
# ---------------------------------------------------------------------------

@dataclass
class DayLevels:
    date: pd.Timestamp                 # NY date
    h1_high: float
    h1_low: float
    h1_open: float
    h1_close: float
    m5_high: float
    m5_low: float
    m5_open: float
    m5_close: float
    h1_levels: Dict[float, float] = field(default_factory=dict)   # multiplier → price
    m5_levels: Dict[float, float] = field(default_factory=dict)

    @property
    def h1_range(self) -> float: return self.h1_high - self.h1_low
    @property
    def m5_range(self) -> float: return self.m5_high - self.m5_low
    @property
    def bias(self) -> str:
        return "bull" if self.h1_close > self.h1_open else (
            "bear" if self.h1_close < self.h1_open else "flat")


def _project(low: float, rng: float) -> Dict[float, float]:
    return {m: low + m * rng for m in MULTIPLIERS}


def extract_daily(bars_5m: pd.DataFrame) -> Dict[pd.Timestamp, DayLevels]:
    """
    Build the 9:00 1H candle (from 9:00–9:55 5m bars) and the 9:45 5m candle
    for every NY trading day. Drops days with incomplete data.
    """
    dl: Dict[pd.Timestamp, DayLevels] = {}
    by_date = bars_5m.groupby(bars_5m.index.date)
    skipped_incomplete = 0
    for d, g in by_date:
        d = pd.Timestamp(d)
        # 9:00 hour window — every 5m bar from 09:00 through 09:55 (12 bars)
        h_mask = (g.index.time >= dtime(9, 0)) & (g.index.time < dtime(10, 0))
        h_bars = g[h_mask]
        if len(h_bars) < 10:           # tolerate 1–2 missing bars
            skipped_incomplete += 1
            continue
        m5_mask = g.index.time == dtime(9, 45)
        m5_bar = g[m5_mask]
        if m5_bar.empty:
            skipped_incomplete += 1
            continue
        h1_high, h1_low = float(h_bars["high"].max()), float(h_bars["low"].min())
        h1_open  = float(h_bars.iloc[0]["open"])
        h1_close = float(h_bars.iloc[-1]["close"])
        m5_high  = float(m5_bar.iloc[0]["high"])
        m5_low   = float(m5_bar.iloc[0]["low"])
        m5_open  = float(m5_bar.iloc[0]["open"])
        m5_close = float(m5_bar.iloc[0]["close"])
        if h1_high <= h1_low or m5_high <= m5_low:
            skipped_incomplete += 1
            continue
        dl[d] = DayLevels(
            date=d,
            h1_high=h1_high, h1_low=h1_low, h1_open=h1_open, h1_close=h1_close,
            m5_high=m5_high, m5_low=m5_low, m5_open=m5_open, m5_close=m5_close,
            h1_levels=_project(h1_low, h1_high - h1_low),
            m5_levels=_project(m5_low, m5_high - m5_low),
        )
    print(f"[extract] valid days: {len(dl):,}   skipped (incomplete): {skipped_incomplete}")
    return dl


# ---------------------------------------------------------------------------
# Alignment scan
# ---------------------------------------------------------------------------

def _tolerance_pts(price: float, tol_key: str) -> float:
    if tol_key.endswith("pt"):
        return float(tol_key[:-2])
    pct = float(tol_key.replace("pct", "")) / 100.0
    return price * pct


def _tol_buckets() -> List[str]:
    keys = [f"{int(v)}pt" if v == int(v) else f"{v}pt" for v in TOLERANCES_PT]
    keys += [f"{p*100}pct" for p in TOLERANCES_PCT]
    return keys


def find_alignments(days: Dict[pd.Timestamp, DayLevels],
                    lookback: int = DEFAULT_LOOKBACK
                    ) -> pd.DataFrame:
    """
    For each day, compare today's 9AM 1H levels vs prior `lookback` days'
    9:45 5m levels. Record every pair within ANY tolerance bucket.
    """
    sorted_dates = sorted(days.keys())
    idx = {d: i for i, d in enumerate(sorted_dates)}
    rows: List[dict] = []
    tol_keys = _tol_buckets()

    for d in sorted_dates:
        today = days[d]
        i = idx[d]
        priors = sorted_dates[max(0, i - lookback): i]
        for pd_ in priors:
            prior = days[pd_]
            for m_today, p_today in today.h1_levels.items():
                for m_prior, p_prior in prior.m5_levels.items():
                    dist = abs(p_today - p_prior)
                    matched = []
                    for tol_pt in TOLERANCES_PT:
                        if dist <= tol_pt:
                            matched.append(f"{int(tol_pt)}pt"
                                           if tol_pt == int(tol_pt) else f"{tol_pt}pt")
                    for tol_pct in TOLERANCES_PCT:
                        if dist <= p_today * tol_pct:
                            matched.append(f"{tol_pct*100}pct")
                    if not matched:
                        continue
                    rows.append({
                        "date": d.date(),
                        "prior_date": pd_.date(),
                        "lag_days": (d - pd_).days,
                        "h1_mult": m_today,
                        "m5_mult": m_prior,
                        "h1_level_price": p_today,
                        "m5_level_price": p_prior,
                        "distance_pts": dist,
                        "tightest_tol": matched[0],
                        "tol_buckets": "|".join(matched),
                        "above_below": "above" if p_today > today.h1_close else "below",
                        "bias": today.bias,
                        "h1_range": today.h1_range,
                        "m5_range": prior.m5_range,
                    })
    df = pd.DataFrame(rows)
    print(f"[align] lookback={lookback}d → {len(df):,} aligned pairs across "
          f"{df['date'].nunique() if not df.empty else 0} days")
    return df


# ---------------------------------------------------------------------------
# Outcome metrics + chop
# ---------------------------------------------------------------------------

def _slice_window(bars_5m: pd.DataFrame, day: pd.Timestamp,
                  start: dtime, end: dtime) -> pd.DataFrame:
    mask = (bars_5m.index.date == day.date()) & \
           (bars_5m.index.time >= start) & (bars_5m.index.time < end)
    return bars_5m.loc[mask]


def compute_outcomes(bars_5m: pd.DataFrame,
                     days: Dict[pd.Timestamp, DayLevels],
                     align: pd.DataFrame) -> pd.DataFrame:
    """
    For every valid day, compute post-10AM outcome metrics.
    Tags whether the day had any alignment (for the default lookback).
    """
    aligned_dates = set(align["date"].unique()) if not align.empty else set()
    rows: List[dict] = []
    for d, dl in days.items():
        ten = _slice_window(bars_5m, d, dtime(10, 0), dtime(16, 0))
        if ten.empty:
            continue
        ten_open = float(ten.iloc[0]["open"])
        chop_50 = dl.h1_levels[0.5]
        crosses = ((ten["close"] > chop_50).astype(int).diff().abs().fillna(0).sum())
        post_high = float(ten["high"].max())
        post_low = float(ten["low"].min())
        post_range = post_high - post_low
        below_high = post_high < dl.h1_high
        above_low = post_low > dl.h1_low
        chop_score = 0
        if crosses >= 3: chop_score += 1
        if post_range < 0.5 * dl.h1_range: chop_score += 1
        if below_high and above_low: chop_score += 1
        # MFE / MAE
        mfe = post_high - ten_open
        mae = ten_open - post_low
        # Continuation tests
        bull_cont = bear_cont = False
        if dl.bias == "bull":
            up_lvls = [dl.h1_levels[m] for m in (1.25, 1.50, 2.00)]
            for _, bar in ten.iterrows():
                if bar["low"] < dl.h1_levels[0.0]:    # broke 9AM low
                    break
                if bar["high"] >= min(up_lvls):
                    bull_cont = True; break
        elif dl.bias == "bear":
            dn_lvls = [dl.h1_levels[m] for m in (-0.25, -0.50, -1.00)]
            for _, bar in ten.iterrows():
                if bar["high"] > dl.h1_levels[1.0]:   # broke 9AM high
                    break
                if bar["low"] <= max(dn_lvls):
                    bear_cont = True; break
        cont = bull_cont or bear_cont

        # Per-window snapshots
        per_window = {}
        for label, s, e in WINDOWS:
            w = _slice_window(bars_5m, d, s, e)
            if w.empty:
                continue
            per_window[f"close_{label}"] = float(w.iloc[-1]["close"]) - ten_open
            per_window[f"mfe_{label}"]  = float(w["high"].max()) - ten_open
            per_window[f"mae_{label}"]  = ten_open - float(w["low"].min())
            per_window[f"range_{label}"] = float(w["high"].max() - w["low"].min())

        rows.append({
            "date": d.date(),
            "weekday": d.day_name(),
            "bias": dl.bias,
            "h1_range": dl.h1_range,
            "m5_range": dl.m5_range,
            "ten_open": ten_open,
            "post_high": post_high,
            "post_low": post_low,
            "post_range": post_range,
            "post_close_chg": float(ten.iloc[-1]["close"]) - ten_open,
            "mfe": mfe, "mae": mae,
            "crosses_50": int(crosses),
            "chop_score": chop_score,
            "is_chop": chop_score >= 2,
            "continuation": cont,
            "aligned_today": (d.date() in aligned_dates),
            **per_window,
        })
    df = pd.DataFrame(rows)
    print(f"[outcomes] computed for {len(df):,} days")
    return df


# ---------------------------------------------------------------------------
# Level respect / touch counter
# ---------------------------------------------------------------------------

def level_respect_stats(bars_5m: pd.DataFrame,
                        days: Dict[pd.Timestamp, DayLevels],
                        atr_window: int = 14, touch_pts: float = 2.0
                        ) -> pd.DataFrame:
    """
    For each multiplier (separately for 9AM 1H and 9:45 5m), count touches,
    rejections (reverse 0.25 ATR within 6 bars) and breaks (continue 0.25 ATR).
    """
    # Daily ATR proxy from 5m → daily TR (key by date object, not tz-aware ts)
    daily = bars_5m.resample("1D").agg({"high": "max", "low": "min", "close": "last"}).dropna()
    daily["prev_close"] = daily["close"].shift(1)
    tr = pd.concat([
        daily["high"] - daily["low"],
        (daily["high"] - daily["prev_close"]).abs(),
        (daily["low"]  - daily["prev_close"]).abs(),
    ], axis=1).max(axis=1)
    atr = tr.rolling(atr_window, min_periods=5).mean()
    atr_by_date = {ts.date(): v for ts, v in atr.items()}

    rows: List[dict] = []
    for tag in ("h1", "m5"):
        for m in MULTIPLIERS:
            touches = rejections = breaks = 0
            move_after = []
            for d, dl in days.items():
                a = atr_by_date.get(d.date(), np.nan)
                if pd.isna(a):
                    continue
                lvl = (dl.h1_levels if tag == "h1" else dl.m5_levels)[m]
                # only use post-10AM window for both layers (clean apples-to-apples)
                bars = _slice_window(bars_5m, d, dtime(10, 0), dtime(16, 0))
                if bars.empty:
                    continue
                close = bars["close"].values
                high  = bars["high"].values
                low   = bars["low"].values
                last_touch_idx = -10
                for i in range(len(bars)):
                    near = (low[i] - touch_pts) <= lvl <= (high[i] + touch_pts)
                    if not near or i - last_touch_idx < 3:
                        continue
                    touches += 1
                    last_touch_idx = i
                    look = slice(i, min(i + 6, len(bars)))
                    seg_high = high[look].max()
                    seg_low  = low[look].min()
                    move_after.append(close[min(i + 5, len(bars) - 1)] - close[i])
                    threshold = 0.25 * a
                    if (close[i] >= lvl and (close[i] - seg_low) >= threshold) \
                       or (close[i] <= lvl and (seg_high - close[i]) >= threshold):
                        rejections += 1
                    elif (close[i] >= lvl and (seg_high - close[i]) >= threshold) \
                         or (close[i] <= lvl and (close[i] - seg_low) >= threshold):
                        breaks += 1
            rows.append({
                "layer": tag,
                "multiplier": m,
                "touch_count": touches,
                "rejections": rejections,
                "breaks": breaks,
                "respect_rate": rejections / touches if touches else np.nan,
                "break_rate": breaks / touches if touches else np.nan,
                "avg_move_after_touch": float(np.mean(move_after)) if move_after else np.nan,
            })
    return pd.DataFrame(rows).sort_values(["layer", "multiplier"]).reset_index(drop=True)


# ---------------------------------------------------------------------------
# Tolerance / lookback sweep + pattern search
# ---------------------------------------------------------------------------

def tolerance_sweep(days: Dict[pd.Timestamp, DayLevels],
                    bars_5m: pd.DataFrame,
                    outcomes_default: pd.DataFrame) -> pd.DataFrame:
    """Sweep all (lookback, tolerance) pairs, recompute alignment, summarize."""
    rows = []
    for lb in LOOKBACKS:
        align = find_alignments(days, lookback=lb)
        if align.empty:
            continue
        for tol in _tol_buckets():
            sub = align[align["tol_buckets"].str.contains(tol, regex=False)]
            aligned_dates = set(sub["date"].unique())
            df = outcomes_default.copy()
            df["aligned"] = df["date"].isin(aligned_dates)
            a = df[df["aligned"]]
            n = df[~df["aligned"]]
            rows.append({
                "lookback": lb,
                "tolerance": tol,
                "n_aligned": len(a),
                "n_nonaligned": len(n),
                "avg_move_aligned":  a["post_close_chg"].abs().mean(),
                "avg_move_nonalign": n["post_close_chg"].abs().mean(),
                "cont_rate_aligned":  a["continuation"].mean() if len(a) else np.nan,
                "cont_rate_nonalign": n["continuation"].mean() if len(n) else np.nan,
                "chop_rate_aligned":  a["is_chop"].mean() if len(a) else np.nan,
                "chop_rate_nonalign": n["is_chop"].mean() if len(n) else np.nan,
                "avg_mfe_aligned": a["mfe"].mean(),
                "avg_mae_aligned": a["mae"].mean(),
            })
    return pd.DataFrame(rows)


def pattern_search(align_default: pd.DataFrame,
                   outcomes: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    """A grab-bag of pattern checks per the 'unexpected' section."""
    out = {}
    if align_default.empty:
        return out

    # multiplier predictiveness — best 9AM mult & best 5m mult
    merged = align_default.merge(outcomes[["date", "post_close_chg",
                                           "continuation", "is_chop", "mfe", "mae"]],
                                 on="date", how="left")
    out["by_h1_multiplier"] = (merged.groupby("h1_mult")
                               .agg(n=("date", "count"),
                                    avg_abs_move=("post_close_chg",
                                                  lambda s: s.abs().mean()),
                                    cont_rate=("continuation", "mean"),
                                    chop_rate=("is_chop", "mean"))
                               .reset_index())
    out["by_m5_multiplier"] = (merged.groupby("m5_mult")
                               .agg(n=("date", "count"),
                                    avg_abs_move=("post_close_chg",
                                                  lambda s: s.abs().mean()),
                                    cont_rate=("continuation", "mean"),
                                    chop_rate=("is_chop", "mean"))
                               .reset_index())
    out["above_vs_below"] = (merged.groupby("above_below")
                             .agg(n=("date", "count"),
                                  avg_abs_move=("post_close_chg",
                                                lambda s: s.abs().mean()),
                                  cont_rate=("continuation", "mean"))
                             .reset_index())

    # cluster strength — how many aligned levels per day → outcome
    cluster = merged.groupby("date").agg(n_aligned=("h1_mult", "count")).reset_index()
    cluster = cluster.merge(outcomes[["date", "post_close_chg", "continuation"]],
                            on="date", how="left")
    cluster["abs_move"] = cluster["post_close_chg"].abs()
    out["cluster_strength"] = (cluster.assign(bucket=pd.cut(
        cluster["n_aligned"], [0, 1, 3, 6, 10, 999],
        labels=["1", "2-3", "4-6", "7-10", "11+"]))
        .groupby("bucket", observed=True)
        .agg(n_days=("date", "count"),
             avg_abs_move=("abs_move", "mean"),
             cont_rate=("continuation", "mean"))
        .reset_index())

    # day of week
    out["by_weekday"] = (outcomes.groupby("weekday")
                         .agg(n=("date", "count"),
                              avg_abs_move=("post_close_chg",
                                            lambda s: s.abs().mean()),
                              cont_rate=("continuation", "mean"),
                              chop_rate=("is_chop", "mean"))
                         .reset_index())

    # h1 range filter
    rng_q = outcomes["h1_range"].quantile([0.25, 0.5, 0.75]).to_dict()
    def _bucket(r):
        if r <= rng_q[0.25]: return "Q1"
        if r <= rng_q[0.50]: return "Q2"
        if r <= rng_q[0.75]: return "Q3"
        return "Q4"
    out["h1_range_buckets"] = (outcomes.assign(b=outcomes["h1_range"].map(_bucket))
                               .groupby("b")
                               .agg(n=("date", "count"),
                                    avg_abs_move=("post_close_chg",
                                                  lambda s: s.abs().mean()),
                                    cont_rate=("continuation", "mean"),
                                    chop_rate=("is_chop", "mean"))
                               .reset_index())
    return out


# ---------------------------------------------------------------------------
# Charts
# ---------------------------------------------------------------------------

def make_charts(outcomes: pd.DataFrame, sweep: pd.DataFrame,
                respect: pd.DataFrame, align_default: pd.DataFrame):
    if outcomes.empty:
        return
    a = outcomes[outcomes["aligned_today"]]
    n = outcomes[~outcomes["aligned_today"]]

    # 1 — avg abs post-10 move
    plt.figure(figsize=(6, 4))
    plt.bar(["aligned", "non-aligned"],
            [a["post_close_chg"].abs().mean() if len(a) else 0,
             n["post_close_chg"].abs().mean() if len(n) else 0])
    plt.title("Avg |post-10AM move| (close-to-close)")
    plt.ylabel("points"); plt.tight_layout()
    plt.savefig(ARTIFACTS / "01_avg_move.png", dpi=120); plt.close()

    # 2 — continuation rate by tolerance (5d lookback only for clarity)
    if not sweep.empty:
        s5 = sweep[sweep["lookback"] == DEFAULT_LOOKBACK]
        if not s5.empty:
            plt.figure(figsize=(7, 4))
            plt.plot(s5["tolerance"], s5["cont_rate_aligned"], "o-", label="aligned")
            plt.plot(s5["tolerance"], s5["cont_rate_nonalign"], "x--", label="non-aligned")
            plt.title(f"Continuation rate by tolerance (lookback={DEFAULT_LOOKBACK}d)")
            plt.ylabel("continuation rate"); plt.legend(); plt.xticks(rotation=30)
            plt.tight_layout()
            plt.savefig(ARTIFACTS / "02_cont_rate_by_tol.png", dpi=120); plt.close()

    # 3 — level respect by multiplier (h1 vs m5)
    plt.figure(figsize=(9, 4))
    for tag, color in [("h1", "tab:blue"), ("m5", "tab:orange")]:
        d = respect[respect["layer"] == tag]
        plt.plot(d["multiplier"], d["respect_rate"], "o-", label=tag, color=color)
    plt.title("Level respect (rejection) rate by multiplier")
    plt.xlabel("multiplier"); plt.ylabel("respect rate")
    plt.legend(); plt.grid(alpha=0.3); plt.tight_layout()
    plt.savefig(ARTIFACTS / "03_level_respect.png", dpi=120); plt.close()

    # 4 — alignment distance vs |move|
    if not align_default.empty:
        merged = align_default.merge(outcomes[["date", "post_close_chg"]],
                                     on="date", how="left").dropna()
        if not merged.empty:
            plt.figure(figsize=(7, 4))
            plt.scatter(merged["distance_pts"], merged["post_close_chg"].abs(),
                        s=10, alpha=0.4)
            plt.title("Alignment distance vs |post-10AM move|")
            plt.xlabel("distance (pts)"); plt.ylabel("|post-10AM move|")
            plt.tight_layout()
            plt.savefig(ARTIFACTS / "04_dist_vs_move.png", dpi=120); plt.close()

    # 5 — boxplot
    plt.figure(figsize=(6, 4))
    data = [a["post_close_chg"].abs().dropna() if len(a) else [],
            n["post_close_chg"].abs().dropna() if len(n) else []]
    plt.boxplot(data, tick_labels=["aligned", "non-aligned"], showfliers=False)
    plt.title("Post-10AM |move| distribution")
    plt.ylabel("points"); plt.tight_layout()
    plt.savefig(ARTIFACTS / "05_boxplot.png", dpi=120); plt.close()


# ---------------------------------------------------------------------------
# Reporting
# ---------------------------------------------------------------------------

def summarize(outcomes: pd.DataFrame) -> pd.DataFrame:
    a = outcomes[outcomes["aligned_today"]]
    n = outcomes[~outcomes["aligned_today"]]
    def _row(label, df):
        return {
            "group": label, "n": len(df),
            "avg_move": df["post_close_chg"].mean() if len(df) else np.nan,
            "avg_abs_move": df["post_close_chg"].abs().mean() if len(df) else np.nan,
            "median_abs_move": df["post_close_chg"].abs().median() if len(df) else np.nan,
            "win_rate_bull_bias": ((df["bias"] == "bull")
                                   & (df["post_close_chg"] > 0)).sum()
                                   / max((df["bias"] == "bull").sum(), 1),
            "win_rate_bear_bias": ((df["bias"] == "bear")
                                   & (df["post_close_chg"] < 0)).sum()
                                   / max((df["bias"] == "bear").sum(), 1),
            "continuation_rate": df["continuation"].mean() if len(df) else np.nan,
            "chop_rate": df["is_chop"].mean() if len(df) else np.nan,
            "avg_mfe": df["mfe"].mean() if len(df) else np.nan,
            "avg_mae": df["mae"].mean() if len(df) else np.nan,
        }
    return pd.DataFrame([_row("aligned", a), _row("non-aligned", n)])


def spot_checks(outcomes: pd.DataFrame) -> pd.DataFrame:
    if outcomes.empty:
        return pd.DataFrame()
    out = []
    a = outcomes[outcomes["aligned_today"]].copy()
    n = outcomes[~outcomes["aligned_today"]].copy()
    a["abs_move"] = a["post_close_chg"].abs()
    n["abs_move"] = n["post_close_chg"].abs()
    if not a.empty:
        for _, r in a.nlargest(5, "abs_move").iterrows():
            out.append({"category": "best_aligned_winner", **r.to_dict()})
        for _, r in a.nsmallest(5, "abs_move").iterrows():
            out.append({"category": "worst_aligned_failure", **r.to_dict()})
    if not n.empty:
        for _, r in n.nlargest(5, "abs_move").iterrows():
            out.append({"category": "best_nonaligned_trend", **r.to_dict()})
        for _, r in n.nsmallest(5, "abs_move").iterrows():
            out.append({"category": "choppiest_nonaligned", **r.to_dict()})
    keep = ["category", "date", "weekday", "bias", "post_close_chg", "abs_move",
            "mfe", "mae", "is_chop", "continuation", "h1_range", "m5_range"]
    return pd.DataFrame(out)[keep]


def interpret(summary: pd.DataFrame, sweep: pd.DataFrame,
              respect: pd.DataFrame, patterns: Dict[str, pd.DataFrame],
              source: str) -> str:
    lines = ["", "=" * 72,
             "SD ALIGNMENT BACKTEST — PLAIN-ENGLISH INTERPRETATION",
             "=" * 72, f"data source: {source}"]
    if summary.empty or summary["n"].sum() == 0:
        lines.append("No valid days produced — cannot interpret.")
        return "\n".join(lines)
    a = summary[summary["group"] == "aligned"].iloc[0]
    n = summary[summary["group"] == "non-aligned"].iloc[0]

    # 1 — supported?
    supported = (a["avg_abs_move"] > n["avg_abs_move"]
                 and a["continuation_rate"] >= n["continuation_rate"]
                 and a["chop_rate"] <= n["chop_rate"])
    lines.append(f"\n1. HYPOTHESIS: "
                 f"{'SUPPORTED' if supported else 'NOT SUPPORTED'} "
                 f"on this dataset.")
    lines.append(f"   aligned days     n={int(a['n']):>4}  "
                 f"avg|move|={a['avg_abs_move']:.1f}  "
                 f"cont={a['continuation_rate']:.1%}  "
                 f"chop={a['chop_rate']:.1%}")
    lines.append(f"   non-aligned days n={int(n['n']):>4}  "
                 f"avg|move|={n['avg_abs_move']:.1f}  "
                 f"cont={n['continuation_rate']:.1%}  "
                 f"chop={n['chop_rate']:.1%}")

    # 2 — best multipliers
    if "by_h1_multiplier" in patterns and not patterns["by_h1_multiplier"].empty:
        best_h1 = (patterns["by_h1_multiplier"]
                   .sort_values("avg_abs_move", ascending=False).head(3))
        lines.append("\n2. BEST 9AM 1H MULTIPLIERS (by avg post-10AM |move|):")
        for _, r in best_h1.iterrows():
            lines.append(f"     m={r['h1_mult']:>5.2f}  n={int(r['n']):>3}  "
                         f"avg|move|={r['avg_abs_move']:.1f}  "
                         f"cont={r['cont_rate']:.1%}")
    if "by_m5_multiplier" in patterns and not patterns["by_m5_multiplier"].empty:
        best_m5 = (patterns["by_m5_multiplier"]
                   .sort_values("avg_abs_move", ascending=False).head(3))
        lines.append("   BEST 9:45 5m MULTIPLIERS:")
        for _, r in best_m5.iterrows():
            lines.append(f"     m={r['m5_mult']:>5.2f}  n={int(r['n']):>3}  "
                         f"avg|move|={r['avg_abs_move']:.1f}  "
                         f"cont={r['cont_rate']:.1%}")

    # 3 — alignment vs continuation
    delta_cont = a["continuation_rate"] - n["continuation_rate"]
    lines.append(f"\n3. CONTINUATION LIFT FROM ALIGNMENT: "
                 f"{delta_cont:+.1%} ({a['continuation_rate']:.1%} vs "
                 f"{n['continuation_rate']:.1%}).")

    # 4 — non-alignment chop
    lines.append(f"\n4. NON-ALIGNMENT CHOP RATE: {n['chop_rate']:.1%} "
                 f"(aligned {a['chop_rate']:.1%}). "
                 f"{'CHOP HYPOTHESIS HOLDS' if n['chop_rate']>a['chop_rate'] else 'no chop edge'}.")

    # 5/6 — best tol & lookback
    if not sweep.empty:
        sweep["edge"] = (sweep["avg_move_aligned"] - sweep["avg_move_nonalign"])
        best = sweep.sort_values("edge", ascending=False).head(1).iloc[0]
        lines.append(f"\n5. BEST TOLERANCE: {best['tolerance']} "
                     f"(edge={best['edge']:+.1f} pts)")
        lines.append(f"6. BEST LOOKBACK: {int(best['lookback'])}-day prior 9:45 levels")

    # 7 — does 5m add precision?
    if not respect.empty:
        h1_resp = respect[respect["layer"] == "h1"]["respect_rate"].mean()
        m5_resp = respect[respect["layer"] == "m5"]["respect_rate"].mean()
        verdict = "ADDS PRECISION" if m5_resp >= h1_resp else "WEAKER than 9AM 1H alone"
        lines.append(f"\n7. 9:45 5m PRECISION LAYER: {verdict} "
                     f"(m5_respect={m5_resp:.1%} vs h1_respect={h1_resp:.1%})")

    lines.append("\n8. DATES TO REVIEW IN TRADINGVIEW: see "
                 "artifacts/spot_check_dates.csv (best/worst aligned + best/worst non-aligned)")

    # 9 — unexpected patterns
    lines.append("\n9. NOTABLE PATTERNS:")
    if "cluster_strength" in patterns and not patterns["cluster_strength"].empty:
        cs = patterns["cluster_strength"]
        lines.append("   alignment cluster size → avg |move|:")
        for _, r in cs.iterrows():
            lines.append(f"     bucket={r['bucket']:>5}  "
                         f"n={int(r['n_days']):>3}  "
                         f"avg|move|={r['avg_abs_move']:.1f}  "
                         f"cont={r['cont_rate']:.1%}")
    if "by_weekday" in patterns:
        wd = patterns["by_weekday"].sort_values("avg_abs_move", ascending=False)
        if not wd.empty:
            top = wd.iloc[0]
            lines.append(f"   strongest weekday: {top['weekday']} "
                         f"(avg|move|={top['avg_abs_move']:.1f})")

    lines.append("\n10. TRADING IMPLICATIONS:")
    if supported:
        lines.append("    • Bias intraday entries toward days where today's 9AM 1H levels")
        lines.append("      hit a prior-day 9:45 5m level within the best tolerance bucket.")
        lines.append("    • Use the most-respected multipliers (top of list above) as targets/")
        lines.append("      stops; treat their breaks as continuation triggers.")
        lines.append("    • Stand down on non-aligned days — chop rate is materially higher.")
    else:
        lines.append("    • Alignment did NOT produce an edge on this dataset. Don't gate")
        lines.append("      entries on it. The 9AM 1H structure may still work standalone.")
        lines.append("    • Re-test with more data, or with the 9AM FVG filter that was")
        lines.append("      already validated (78.8% reversal at 9AM touch).")

    lines.append("\n" + "=" * 72)
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# --tomorrow: forward-looking watch-level generator
# ---------------------------------------------------------------------------

def _next_session_date(last: pd.Timestamp) -> pd.Timestamp:
    """Skip weekends — return next NY business day after `last`."""
    nxt = last + pd.Timedelta(days=1)
    while nxt.weekday() >= 5:
        nxt += pd.Timedelta(days=1)
    return nxt.normalize()


def tomorrow_watch_levels(days: Dict[pd.Timestamp, DayLevels],
                          n_prior: int = 5,
                          cluster_tol: float = 5.0) -> pd.DataFrame:
    """
    Build a forward watch list for the next session by clustering prior days'
    9:45 SD levels. No 9AM 1H levels yet (tomorrow's candle hasn't formed) —
    the user marks these on chart, then re-runs analysis after 10 AM with
    chart_analyzer.py.

    Returns a DataFrame ranked by:
      • cluster size (more prior 9:45 levels stacking = stronger)
      • proximity to the most recent close
    """
    sorted_dates = sorted(days.keys())
    if not sorted_dates:
        return pd.DataFrame()

    priors = sorted_dates[-n_prior:]
    last = sorted_dates[-1]
    target_date = _next_session_date(last)

    # collect every 9:45 SD level from the prior window
    candidates: List[Tuple[float, str]] = []
    for d in priors:
        dl = days[d]
        for m, p in dl.m5_levels.items():
            candidates.append((p, f"{d.date()}_m{m:+.2f}"))
        # also include the 9:45 wicks themselves (top finding)
        candidates.append((dl.m5_high, f"{d.date()}_9:45_high"))
        candidates.append((dl.m5_low,  f"{d.date()}_9:45_low"))

    # greedy cluster — sort by price, merge anything within cluster_tol
    candidates.sort(key=lambda x: x[0])
    clusters: List[Dict] = []
    cur_prices: List[float] = []
    cur_sources: List[str] = []
    for price, src in candidates:
        if cur_prices and abs(price - cur_prices[0]) <= cluster_tol:
            cur_prices.append(price)
            cur_sources.append(src)
        else:
            if cur_prices:
                clusters.append({
                    "center_price": float(np.median(cur_prices)),
                    "cluster_size": len(cur_prices),
                    "spread_pts": max(cur_prices) - min(cur_prices),
                    "sources": "|".join(cur_sources),
                    "n_unique_days": len({s.split("_")[0] for s in cur_sources}),
                    "is_wick": any("9:45_" in s for s in cur_sources),
                })
            cur_prices = [price]
            cur_sources = [src]
    if cur_prices:
        clusters.append({
            "center_price": float(np.median(cur_prices)),
            "cluster_size": len(cur_prices),
            "spread_pts": max(cur_prices) - min(cur_prices),
            "sources": "|".join(cur_sources),
            "n_unique_days": len({s.split("_")[0] for s in cur_sources}),
            "is_wick": any("9:45_" in s for s in cur_sources),
        })

    df = pd.DataFrame(clusters)
    if df.empty:
        return df

    # rank: prefer multi-day clusters with ≥3 stacked levels and a wick included
    df["score"] = (df["cluster_size"]
                   + 2 * df["n_unique_days"]
                   + 3 * df["is_wick"].astype(int))
    df = df.sort_values("score", ascending=False).reset_index(drop=True)
    df.insert(0, "target_date", target_date.date())
    return df


def print_tomorrow(days: Dict[pd.Timestamp, DayLevels], n_prior: int = 5):
    """Console summary of the watch-list."""
    df = tomorrow_watch_levels(days, n_prior=n_prior)
    if df.empty:
        print("[tomorrow] no clusters generated — insufficient prior data.")
        return

    target_date = df.iloc[0]["target_date"]
    sorted_dates = sorted(days.keys())
    last_dl = days[sorted_dates[-1]]
    print("\n" + "=" * 72)
    print(f"TOMORROW WATCH LEVELS — target session: {target_date}")
    print(f"derived from prior {n_prior} sessions' 9:45 5m SD levels + wicks")
    print("=" * 72)

    print(f"\nlast session ({sorted_dates[-1].date()}):  "
          f"9AM 1H {last_dl.h1_low:.2f}–{last_dl.h1_high:.2f}  "
          f"(bias {last_dl.bias})  |  "
          f"9:45 5m {last_dl.m5_low:.2f}–{last_dl.m5_high:.2f}  "
          f"(body {_bias_for(last_dl.m5_open, last_dl.m5_close)})")

    # last few session biases — chain direction
    chain = [(d.date(), days[d].bias) for d in sorted_dates[-n_prior:]]
    print(f"\nrecent bias chain: " + ", ".join(f"{d}={b}" for d, b in chain))

    print(f"\n{'#':>2}  {'price':>10}  {'size':>4}  {'days':>4}  "
          f"{'spread':>6}  wick?  sources")
    print("-" * 72)
    for i, r in df.head(15).iterrows():
        wick = "YES" if r["is_wick"] else "no"
        # truncate sources for display
        src = r["sources"]
        if len(src) > 28:
            src = src[:25] + "..."
        print(f"{i+1:>2}  {r['center_price']:>10.2f}  "
              f"{int(r['cluster_size']):>4}  {int(r['n_unique_days']):>4}  "
              f"{r['spread_pts']:>6.2f}  {wick:>4}   {src}")

    # no-trade conditions for tomorrow
    print(f"\n-- NO-TRADE flags for {target_date} --")
    flags = []
    h1_ranges = [days[d].h1_range for d in sorted_dates[-20:]]
    h1_p25 = float(np.quantile(h1_ranges, 0.25))
    flags.append(f"  • If tomorrow's 9AM 1H range < {h1_p25:.1f} pt "
                 f"(bottom quartile of last 20 days) → likely chop")
    if df["cluster_size"].max() <= 2:
        flags.append("  • Top cluster size ≤ 2 — weak prior-day stacking, "
                     "alignment edge is minimal")
    bias_counts = pd.Series([b for _, b in chain]).value_counts()
    if bias_counts.max() / len(chain) >= 0.8:
        flags.append(f"  • {bias_counts.idxmax()} bias dominant "
                     f"({bias_counts.max()}/{len(chain)}) — watch for mean-reversion / exhaustion")
    if not flags:
        print("  (none triggered)")
    for f in flags: print(f)

    # findings reminder for the next session
    print("\n-- proven findings to apply at 9:45 tomorrow --")
    print("  • 9:45 wick first-touch reverses 84-96%")
    print("  • 9:45 HIGH from above reverses 96.4%")
    print("  • 9:45 body is CONTRARIAN (manipulation candle)")
    print("  • 9AM bias wins 59.8% when it disagrees with 9:45")
    print("  • Median expansion 54 min after 09:50 → key window 09:50-10:44")
    print("  • 13:00 touches on 9AM zones reverse 90%")

    out_path = ARTIFACTS / "tomorrow_watch_levels.csv"
    df.to_csv(out_path, index=False)
    print(f"\n[tomorrow] saved → {out_path}")


def _bias_for(o: float, c: float) -> str:
    return "bull" if c > o else ("bear" if c < o else "flat")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--demo", action="store_true",
                    help="run on synthetic demo data (smoke test, NOT real)")
    ap.add_argument("--pull", action="store_true",
                    help="force fresh Databento pull (needs DATABENTO_API_KEY)")
    ap.add_argument("--lookback", type=int, default=DEFAULT_LOOKBACK)
    ap.add_argument("--tomorrow", action="store_true",
                    help="print watch levels for the next session and exit")
    ap.add_argument("--tomorrow-priors", type=int, default=5,
                    help="number of prior sessions to cluster from (default 5)")
    args = ap.parse_args(argv)

    bars_5m, bars_1h, source = load_data(force_pull=args.pull, demo=args.demo)
    print(f"[main] source={source}  5m_bars={len(bars_5m):,}  "
          f"1h_bars={len(bars_1h):,}  "
          f"window={bars_5m.index.min()} → {bars_5m.index.max()}")

    days = extract_daily(bars_5m)
    if not days:
        print("[main] no valid days — aborting.")
        return 1

    if args.tomorrow:
        print_tomorrow(days, n_prior=args.tomorrow_priors)
        return 0

    # daily levels CSV
    rows = []
    for d, dl in days.items():
        for tag, lv in (("h1", dl.h1_levels), ("m5", dl.m5_levels)):
            for m, p in lv.items():
                rows.append({"date": d.date(), "layer": tag,
                             "multiplier": m, "price": p,
                             "h1_high": dl.h1_high, "h1_low": dl.h1_low,
                             "m5_high": dl.m5_high, "m5_low": dl.m5_low,
                             "bias": dl.bias})
    pd.DataFrame(rows).to_csv(ARTIFACTS / "daily_levels.csv", index=False)

    align_default = find_alignments(days, lookback=args.lookback)
    align_default.to_csv(ARTIFACTS / "alignment_results.csv", index=False)

    outcomes = compute_outcomes(bars_5m, days, align_default)
    outcomes.to_csv(ARTIFACTS / "outcomes.csv", index=False)

    respect = level_respect_stats(bars_5m, days)
    respect.to_csv(ARTIFACTS / "level_respect_stats.csv", index=False)

    sweep = tolerance_sweep(days, bars_5m, outcomes)
    sweep.to_csv(ARTIFACTS / "tolerance_lookback_sweep.csv", index=False)

    patterns = pattern_search(align_default, outcomes)
    for name, df in patterns.items():
        df.to_csv(ARTIFACTS / f"pattern_{name}.csv", index=False)

    summary = summarize(outcomes)
    summary.to_csv(ARTIFACTS / "summary_stats.csv", index=False)

    spots = spot_checks(outcomes)
    spots.to_csv(ARTIFACTS / "spot_check_dates.csv", index=False)

    make_charts(outcomes, sweep, respect, align_default)

    # console output
    pd.set_option("display.float_format", lambda x: f"{x:.3f}")
    print("\n--- SUMMARY (aligned vs non-aligned) ---")
    print(summary.to_string(index=False))
    print("\n--- LEVEL RESPECT (top 10 by respect_rate, h1) ---")
    print(respect[respect["layer"] == "h1"]
          .sort_values("respect_rate", ascending=False).head(10).to_string(index=False))
    print("\n--- LEVEL RESPECT (top 10 by respect_rate, m5) ---")
    print(respect[respect["layer"] == "m5"]
          .sort_values("respect_rate", ascending=False).head(10).to_string(index=False))
    print("\n--- TOLERANCE / LOOKBACK SWEEP (top 10 by edge) ---")
    if not sweep.empty:
        sweep["edge"] = sweep["avg_move_aligned"] - sweep["avg_move_nonalign"]
        print(sweep.sort_values("edge", ascending=False).head(10).to_string(index=False))
    print("\n--- SPOT CHECK DATES ---")
    if not spots.empty:
        print(spots.to_string(index=False))

    print(interpret(summary, sweep, respect, patterns, source))
    print(f"\n[done] artifacts written to: {ARTIFACTS.resolve()}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
