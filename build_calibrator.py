#!/usr/bin/env python3
"""
build_calibrator.py — Build isotonic μ-gap calibrator artifact
===============================================================
Walk-forward version: collects OOS (z, outcome) pairs by training
Ridge only on games BEFORE each fold, predicting on the fold.
Isotonic is fit on OOS pairs only — no in-sample leakage.

Output: calibrator_isotonic.pkl (joblib) — same format as before,
        bare IsotonicRegression object, drop-in replacement.

USAGE:
  python build_calibrator.py \
    --snapshots-dir . \
    --game-stats game_stats_with_vegas.csv

Run this nightly (or whenever game_stats is updated) to keep
the calibrator current. Daily runner loads the .pkl at startup.

SAFETY: Only uses games with date < cutoff. No future leakage.
"""

import argparse
import glob
import os
import re
import sys
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
from sklearn.isotonic import IsotonicRegression
from sklearn.linear_model import Ridge
import joblib


# ============================================================================
# CONSTANTS (frozen — matches daily_picks_simulator.py exactly)
# ============================================================================

FEATURE_COLS = [
    "adj_o_diff", "adj_d_diff",
    "off_efg_diff", "def_efg_diff", "off_to_diff", "def_to_diff",
    "tempo_avg",
    "barthag_diff", "wab_diff",
    "off_or_diff", "def_or_diff",
    "off_ftr_diff", "def_ftr_diff",
    "sos_diff", "hca",
]

RIDGE_ALPHA   = 1.0
MIN_TRAIN     = 500   # minimum games in train split per fold
MIN_HOLDOUT   = 50    # minimum games in holdout per fold
MIN_OOS_TOTAL = 200   # minimum total OOS pairs before isotonic fit
FOLD_DAYS     = 21    # walk-forward step size in days


# ============================================================================
# SNAPSHOT LOADING (identical to daily runner)
# ============================================================================

def load_snapshots(snapshots_dir):
    pattern = os.path.join(snapshots_dir, "team_power_snapshot_*.csv")
    files = sorted(glob.glob(pattern))
    files = [f for f in files if re.search(r'team_power_snapshot_\d{8}\.csv$', f)]
    if not files:
        print(f"  [ERROR] No snapshots in {snapshots_dir}")
        sys.exit(1)

    snapshots = {}
    snap_sources = {}
    for f in files:
        df = pd.read_csv(f)
        if "snapshot_date" not in df.columns:
            base = os.path.basename(f)
            ds = base.replace("team_power_snapshot_", "").replace(".csv", "")
            raw_date = pd.to_datetime(ds, format="%Y%m%d")
        else:
            raw_date = pd.to_datetime(df["snapshot_date"].iloc[0])

        was_offset = False
        if raw_date < pd.Timestamp("2025-11-01"):
            corrected = raw_date + timedelta(days=365)
            was_offset = True
        else:
            corrected = raw_date

        date_key = corrected.strftime("%Y-%m-%d")
        source = "offset" if was_offset else "native"

        if date_key in snapshots:
            existing = snap_sources[date_key]
            if existing == "native" and source == "offset":
                continue
            elif existing == "offset" and source == "native":
                pass
            else:
                print(f"  [HARD FAIL] Duplicate snapshot: {date_key}")
                sys.exit(1)

        df["_snap_date"] = date_key
        snapshots[date_key] = df
        snap_sources[date_key] = source

    print(f"  Loaded {len(snapshots)} snapshots")
    return snapshots


def find_nearest_snapshot(game_date_str, snapshot_dates):
    gd = pd.to_datetime(game_date_str)
    valid = [d for d in snapshot_dates if pd.to_datetime(d) < gd]
    return max(valid) if valid else None


# ============================================================================
# FEATURE ENGINEERING (identical to daily runner)
# ============================================================================

def build_features_for_game(home_team, away_team, snapshot_df):
    snap = snapshot_df.set_index("team")
    if home_team not in snap.index or away_team not in snap.index:
        return None
    h = snap.loc[home_team]
    a = snap.loc[away_team]
    needed = ["adj_o", "adj_d", "off_efg", "def_efg",
              "off_to", "def_to", "tempo", "sos",
              "barthag", "wab", "off_or", "def_or", "off_ftr", "def_ftr"]
    for col in needed:
        if pd.isna(h.get(col)) or pd.isna(a.get(col)):
            return None
    return {
        "adj_o_diff":  h["adj_o"]  - a["adj_o"],
        "adj_d_diff":  h["adj_d"]  - a["adj_d"],
        "off_efg_diff": h["off_efg"] - a["off_efg"],
        "def_efg_diff": h["def_efg"] - a["def_efg"],
        "off_to_diff":  h["off_to"]  - a["off_to"],
        "def_to_diff":  h["def_to"]  - a["def_to"],
        "tempo_avg":    (h["tempo"] + a["tempo"]) / 2,
        "barthag_diff": h["barthag"] - a["barthag"],
        "wab_diff":     h["wab"]     - a["wab"],
        "off_or_diff":  h["off_or"]  - a["off_or"],
        "def_or_diff":  h["def_or"]  - a["def_or"],
        "off_ftr_diff": h["off_ftr"] - a["off_ftr"],
        "def_ftr_diff": h["def_ftr"] - a["def_ftr"],
        "sos_diff":     h["sos"]     - a["sos"],
        "hca": 1.0,
    }


# ============================================================================
# MAIN
# ============================================================================

def main():
    parser = argparse.ArgumentParser(
        description="Build isotonic μ-gap calibrator (walk-forward OOS)")
    parser.add_argument("--snapshots-dir", default=".",
                        help="Directory with team_power_snapshot_*.csv")
    parser.add_argument("--game-stats", required=True,
                        help="game_stats_with_vegas.csv")
    parser.add_argument("--output", default="calibrator_isotonic.pkl",
                        help="Output calibrator path")
    parser.add_argument("--cutoff-date", default=None,
                        help="Only use games before this date (default: today)")
    parser.add_argument("--fold-days", type=int, default=FOLD_DAYS,
                        help=f"Walk-forward fold size in days (default: {FOLD_DAYS})")
    args = parser.parse_args()

    cutoff = args.cutoff_date or datetime.now().strftime("%Y-%m-%d")
    print("=" * 70)
    print(f"  BUILD ISOTONIC μ-GAP CALIBRATOR  (walk-forward OOS)")
    print(f"  Cutoff:     {cutoff}")
    print(f"  Fold size:  {args.fold_days} days")
    print("=" * 70)

    # ── [1] Load snapshots ───────────────────────────────────────────────────
    print("\n[1] Loading snapshots...")
    snapshots = load_snapshots(args.snapshots_dir)
    snap_dates = sorted(snapshots.keys())

    # ── [2] Load + filter game stats ─────────────────────────────────────────
    print("\n[2] Loading game stats...")
    gs = pd.read_csv(args.game_stats)
    gs["_gd"] = pd.to_datetime(gs["game_date"], format="mixed", errors="raise")
    gs = gs[gs["_gd"] < pd.to_datetime(cutoff)].copy()
    gs = gs.sort_values("_gd").reset_index(drop=True)
    print(f"  {len(gs)} games before {cutoff}")

    # ── [3] Build feature rows ───────────────────────────────────────────────
    print("\n[3] Building feature rows...")
    rows = []
    skipped = 0
    for _, g in gs.iterrows():
        gd = g["_gd"].strftime("%Y-%m-%d")
        sk = find_nearest_snapshot(gd, snap_dates)
        if sk is None:
            skipped += 1
            continue
        feats = build_features_for_game(g["team"], g["opponent"], snapshots[sk])
        if feats is None:
            skipped += 1
            continue
        spread = g.get("vegas_spread", np.nan)
        if pd.isna(spread):
            skipped += 1
            continue
        feats["_gd"]          = g["_gd"]
        feats["margin"]       = g["margin"]
        feats["spread"]       = float(spread)
        feats["home_covered"] = 1.0 if (g["margin"] + spread > 0) else 0.0
        rows.append(feats)

    df = pd.DataFrame(rows).sort_values("_gd").reset_index(drop=True)
    print(f"  {len(df)} usable games ({skipped} skipped)")

    if len(df) < MIN_TRAIN + MIN_OOS_TOTAL:
        print(f"  [ERROR] Need ≥{MIN_TRAIN + MIN_OOS_TOTAL} games, have {len(df)}")
        sys.exit(1)

    X_all      = df[FEATURE_COLS].values
    y_all      = df["margin"].values
    spread_all = df["spread"].values
    cover_all  = df["home_covered"].values
    dates_all  = pd.to_datetime(df["_gd"].values)

    # ── FEATURE SCALE AUDIT (Section 2) ────────────────────────────────────
    print(f"\n  ── FEATURE SCALE AUDIT ──")
    for _fc in ["adj_o_diff", "adj_d_diff", "barthag_diff"]:
        _ci = FEATURE_COLS.index(_fc)
        _v = X_all[:, _ci]
        print(f"    {_fc:<18} min={_v.min():+8.3f}  max={_v.max():+8.3f}  "
              f"std={_v.std():7.3f}  mean={_v.mean():+7.3f}")
    _sample_snap = list(snapshots.values())[0]
    if "rating" in _sample_snap.columns:
        _ratings = _sample_snap["rating"].dropna()
        print(f"    {'rating (raw)':<18} min={_ratings.min():+8.3f}  max={_ratings.max():+8.3f}  "
              f"std={_ratings.std():7.3f}  mean={_ratings.mean():+7.3f}")
    else:
        print(f"    rating column: NOT PRESENT in snapshots")

    # Build away-orientation features: flip *_diff columns, keep tempo_avg/hca
    DIFF_MASK = np.array([c.endswith("_diff") for c in FEATURE_COLS])
    X_away = X_all.copy()
    X_away[:, DIFF_MASK] *= -1

    # ── [4] Walk-forward: collect OOS (z, outcome) pairs ────────────────────
    print("\n[4] Walk-forward OOS collection...")

    oos_z        = []
    oos_outcomes = []
    oos_mu       = []          # AUDIT: track μ separately for magnitude diagnostics
    oos_spread_v = []          # AUDIT: track per-game spreads for correlation
    folds_run    = 0
    folds_skipped = 0
    n_active     = len(FEATURE_COLS)

    # First boundary: date of the MIN_TRAIN-th game (earliest viable warmup)
    warmup_cutoff = dates_all[MIN_TRAIN - 1]
    current = warmup_cutoff

    date_max = dates_all.max()

    while current < date_max:
        fold_end = current + pd.Timedelta(days=args.fold_days)

        tr_mask = dates_all < current
        te_mask = (dates_all >= current) & (dates_all < fold_end)

        n_tr = int(tr_mask.sum())
        n_te = int(te_mask.sum())

        if n_tr < MIN_TRAIN or n_te < MIN_HOLDOUT:
            current = fold_end
            folds_skipped += 1
            continue

        # Per-fold active columns: drop zero-variance features on train split
        col_std = np.nanstd(X_all[tr_mask], axis=0)
        active_idx = np.where(col_std > 1e-12)[0]
        n_active = len(active_idx)

        # Train Ridge strictly on past data (active columns only)
        m = Ridge(alpha=RIDGE_ALPHA)
        m.fit(X_all[tr_mask][:, active_idx], y_all[tr_mask])

        # Predict on holdout — flip-averaged μ (matches simulator)
        mu_home_te = m.predict(X_all[te_mask][:, active_idx])
        mu_away_te = m.predict(X_away[te_mask][:, active_idx])
        mu_te = (mu_home_te - mu_away_te) / 2
        z_te  = mu_te + spread_all[te_mask]   # μ-gap = mu + spread

        oos_z.extend(z_te.tolist())
        oos_outcomes.extend(cover_all[te_mask].tolist())
        oos_mu.extend(mu_te.tolist())                      # AUDIT
        oos_spread_v.extend(spread_all[te_mask].tolist())   # AUDIT

        folds_run += 1
        current = fold_end

    oos_z        = np.array(oos_z)
    oos_outcomes = np.array(oos_outcomes)
    oos_mu       = np.array(oos_mu)         # AUDIT
    oos_spread_v = np.array(oos_spread_v)   # AUDIT

    print(f"  Folds completed:  {folds_run}  (skipped: {folds_skipped})")
    print(f"  μ-mode: flip-avg  active_cols: {n_active}")
    print(f"  OOS pairs:        {len(oos_z)}")

    if len(oos_z) < MIN_OOS_TOTAL:
        print(f"  [ERROR] Only {len(oos_z)} OOS pairs — need ≥{MIN_OOS_TOTAL}")
        print(f"  Add more historical games or reduce --fold-days")
        sys.exit(1)

    print(f"  OOS z range:      [{oos_z.min():.1f}, {oos_z.max():.1f}]")
    print(f"  OOS cover rate:   {oos_outcomes.mean()*100:.1f}%")

    # Diagnostic: in-sample σ for reference
    full_model = Ridge(alpha=RIDGE_ALPHA)
    full_model.fit(X_all, y_all)
    insample_sigma = float(np.std(y_all - full_model.predict(X_all)))
    print(f"  σ in-sample (ref): {insample_sigma:.2f}")

    # ── MAGNITUDE AUDIT DIAGNOSTICS ────────────────────────────────────────
    print(f"\n  ── MAGNITUDE AUDIT (OOS) ──")
    print(f"  [A] OOS μ distribution (n={len(oos_mu)}):")
    print(f"      min:  {oos_mu.min():+.2f}")
    print(f"      max:  {oos_mu.max():+.2f}")
    print(f"      mean: {oos_mu.mean():+.2f}")
    print(f"      std:  {oos_mu.std():.2f}")
    print(f"      p01:  {np.percentile(oos_mu, 1):+.2f}")
    print(f"      p99:  {np.percentile(oos_mu, 99):+.2f}")
    _n_extreme = int(np.sum(np.abs(oos_mu) > 25))
    print(f"      |μ|>25: {_n_extreme} ({_n_extreme/len(oos_mu)*100:.1f}%)")
    print(f"  [B] OOS spread distribution:")
    print(f"      min:  {oos_spread_v.min():+.1f}")
    print(f"      max:  {oos_spread_v.max():+.1f}")
    print(f"  [C] corr(μ, spread): {np.corrcoef(oos_mu, oos_spread_v)[0, 1]:+.4f}")
    print(f"  [D] OOS z distribution (n={len(oos_z)}):")
    print(f"      min:  {oos_z.min():+.2f}")
    print(f"      max:  {oos_z.max():+.2f}")
    print(f"      p01:  {np.percentile(oos_z, 1):+.2f}")
    print(f"      p99:  {np.percentile(oos_z, 99):+.2f}")
    _n_z15 = int(np.sum(np.abs(oos_z) > 15))
    _n_z20 = int(np.sum(np.abs(oos_z) > 20))
    print(f"      |z|>15: {_n_z15} ({_n_z15/len(oos_z)*100:.1f}%)")
    print(f"      |z|>20: {_n_z20} ({_n_z20/len(oos_z)*100:.1f}%)")
    print(f"  [E] Intercept cancellation: algebraically verified")
    print(f"      flip-avg μ = Σ(coef_i · diff_i); intercept, tempo_avg, hca cancel")
    if folds_run > 0:
        print(f"      last-fold intercept: {m.intercept_:+.4f}  (cancelled by flip-avg)")

    # ── [5] Fit isotonic on OOS z → P(home_cover) ───────────────────────────
    print("\n[5] Fitting isotonic calibrator on OOS pairs...")
    iso = IsotonicRegression(y_min=0.01, y_max=0.99, out_of_bounds="clip")
    iso.fit(oos_z, oos_outcomes)

    # Spot-check
    test_z = np.array([-10, -5, -3, 0, 3, 5, 10])
    test_p = iso.predict(test_z)
    print(f"\n  Calibrator spot-check (OOS-trained):")
    print(f"    {'z':>6}  {'P(cover)':>9}")
    for z, p in zip(test_z, test_p):
        print(f"    {z:+6.1f}  {p*100:>8.1f}%")

    # Monotonicity check
    diffs = np.diff(test_p)
    if all(d >= 0 for d in diffs):
        print(f"  Monotonicity: ✓ (all increasing)")
    else:
        print(f"  Monotonicity: ⚠ non-monotone at spot-check points")

    # ── [6] Save — bare IsotonicRegression, identical format to original ─────
    print(f"\n[6] Saving calibrator...")
    joblib.dump(iso, args.output)
    print(f"  ✅ Saved: {args.output}")
    print(f"  Total games:    {len(df)}")
    print(f"  OOS pairs used: {len(oos_z)}")
    print(f"  Cutoff:         {cutoff}")
    print(f"\n  Daily runner command:")
    print(f"    python daily_picks_simulator.py --snapshots-dir . "
          f"--game-stats game_stats_with_vegas.csv "
          f"--calibrator {args.output}")


if __name__ == "__main__":
    main()
