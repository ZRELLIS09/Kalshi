#!/usr/bin/env python3
"""
daily_picks_simulator.py — Monte Carlo Game Simulator (Daily Runner)
====================================================================
Uses latest snapshot + live OddsAPI spreads to produce per-game:
  μ (predicted margin), σ, win%, cover%, recommended side, confidence

Trains Ridge regression on all historical games up to today,
then predicts today's slate with Monte Carlo simulation.

USAGE:
  python daily_picks_simulator.py \
    --snapshots-dir . \
    --game-stats game_stats_with_vegas.csv

  # Or skip live fetch and provide odds file:
  python daily_picks_simulator.py \
    --snapshots-dir . \
    --game-stats game_stats_with_vegas.csv \
    --no-fetch --odds odds_today.csv
"""

import argparse
import glob
import json
import os
import re
import sys
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
from sklearn.linear_model import Ridge
from sklearn.isotonic import IsotonicRegression
import joblib

# ── Import team mapping (same as existing system) ──
try:
    from team_name_mapping import odds_to_torvik, ODDS_TO_TORVIK
except ImportError:
    ODDS_TO_TORVIK = {}
    def odds_to_torvik(name):
        return ODDS_TO_TORVIK.get(name.strip(), name.strip())


# ============================================================================
# CONSTANTS (FROZEN — matches backtest_simulator_walkforward.py)
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

MC_SIMS    = 10_000
RIDGE_ALPHA = 1.0

# Walk-forward σ defaults (overridable via CLI --sigma-* args)
WF_MIN_TRAIN     = 500
WF_MIN_HOLDOUT   = 50
WF_MIN_OOS_TOTAL = 200
WF_FOLD_DAYS     = 21
BREAKEVEN_110 = 1.1 / 2.1  # 0.523809 — breakeven prob at -110 juice

# ── MAGNITUDE AUDIT FLAG (temporary — set False to suppress) ──
DEBUG_MAGNITUDE = True

# Known neutral-site showcase events (add as they arise)
# Format: frozenset of Torvik team names
# These get auto-flagged as neutral even without --neutral
KNOWN_NEUTRAL_EVENTS = [
    # 2025-26 showcases
    # Add pairs here as you encounter them, e.g.:
    # frozenset({"Michigan", "Duke"}),
]

# Month-based neutral site warning (conference tourneys + March Madness)
NEUTRAL_WARNING_MONTHS = [3, 4]  # March, April

# ── SPREAD SANITY GATE ──
# If |rating_implied_spread - actual_spread| exceeds these, flag/block.
# Rating diff ≈ point spread for D1 CBB (not exact, but directional).
SPREAD_WARN_THRESHOLD = 12.0   # Flag: "verify this spread"
SPREAD_BLOCK_THRESHOLD = 18.0  # Hard block: "almost certainly wrong data"

# OddsAPI
ODDS_API_KEY_ENV = "ODDS_API_KEY"
ODDS_API_URL = "https://api.the-odds-api.com/v4/sports/basketball_ncaab/odds"
BOOK_PRIORITY = ["fanduel", "draftkings", "betmgm", "pointsbetus",
                  "bovada", "betonlineag", "lowvig"]


# ============================================================================
# SNAPSHOT LOADING
# ============================================================================

def load_snapshots(snapshots_dir):
    """Load all snapshots with year-offset correction."""
    pattern = os.path.join(snapshots_dir, "team_power_snapshot_*.csv")
    files = sorted(glob.glob(pattern))
    # Filter to standard YYYYMMDD format only (exclude _enhanced, _v2, etc.)
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
            date_str = base.replace("team_power_snapshot_", "").replace(".csv", "")
            raw_date = pd.to_datetime(date_str, format="%Y%m%d")
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
                continue  # Keep native
            elif existing == "offset" and source == "native":
                pass  # Replace offset with native
            else:
                print(f"  [HARD FAIL] Duplicate snapshot date: {date_key}")
                print(f"    File: {f}")
                sys.exit(1)

        df["_snap_date"] = date_key
        snapshots[date_key] = df
        snap_sources[date_key] = source

    print(f"  Loaded {len(snapshots)} snapshots: "
          f"{min(snapshots.keys())} → {max(snapshots.keys())}")
    return snapshots


def get_latest_snapshot(snapshots, before_date=None):
    """Get the most recent snapshot (optionally before a date)."""
    dates = sorted(snapshots.keys())
    if before_date:
        bd = pd.to_datetime(before_date).strftime("%Y-%m-%d")
        dates = [d for d in dates if d <= bd]
    if not dates:
        return None, None
    latest = dates[-1]
    return latest, snapshots[latest]


def find_nearest_snapshot(game_date_str, snapshot_dates):
    gd = pd.to_datetime(game_date_str)
    valid = [d for d in snapshot_dates if pd.to_datetime(d) < gd]
    return max(valid) if valid else None


# ============================================================================
# FEATURE ENGINEERING
# ============================================================================

def build_features_for_game(home_team, away_team, snapshot_df, hca=1.0):
    """Build feature vector from snapshot. Returns dict or None."""
    snap = snapshot_df.set_index("team")
    if home_team not in snap.index or away_team not in snap.index:
        return None

    h = snap.loc[home_team]
    a = snap.loc[away_team]

    # Check required columns for NaN
    needed = ["adj_o", "adj_d", "rating", "barthag", "off_efg",
              "def_efg", "off_to", "def_to", "tempo", "sos",
              "wab", "off_or", "def_or", "off_ftr", "def_ftr"]
    for col in needed:
        if pd.isna(h.get(col)) or pd.isna(a.get(col)):
            return None

    return {
        "adj_o_diff":   h["adj_o"]   - a["adj_o"],
        "adj_d_diff":   h["adj_d"]   - a["adj_d"],
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
        "hca": hca,
    }


# ============================================================================
# TRAINING
# ============================================================================

def train_model(game_stats, snapshots, sigma_params=None):
    """Train Ridge regression on all available historical games.

    _gd is stored directly in each row during feature build — no
    date reconstruction loop. OOS sigma is computed from df["_gd"]
    directly. Hard error if OOS sample is insufficient.
    """
    snap_dates = sorted(snapshots.keys())
    rows = []
    skipped = 0

    for _, g in game_stats.iterrows():
        gd = pd.to_datetime(g["game_date"], format="mixed", errors="raise").strftime("%Y-%m-%d")
        snap_key = find_nearest_snapshot(gd, snap_dates)
        if snap_key is None:
            skipped += 1
            continue
        feats = build_features_for_game(g["team"], g["opponent"], snapshots[snap_key])
        if feats is None:
            skipped += 1
            continue
        feats["_gd"]    = pd.to_datetime(gd)   # C3: carried directly, no reconstruction
        feats["margin"] = g["margin"]
        rows.append(feats)

    df = pd.DataFrame(rows).sort_values("_gd").reset_index(drop=True)
    print(f"  Training on {len(df)} games ({skipped} skipped)")

    # Drop constant features (sos_diff=0 etc.)
    keep_cols = []
    for c in FEATURE_COLS:
        v = df[c].values
        if np.nanstd(v) > 1e-12:
            keep_cols.append(c)
        else:
            print(f"  ⚠ DROPPING CONST FEATURE: {c} (variance=0)")
    active_cols = keep_cols

    X = df[active_cols].values
    y = df["margin"].values

    model = Ridge(alpha=RIDGE_ALPHA)
    model.fit(X, y)

    preds     = model.predict(X)
    residuals = y - preds
    print(f"  Train RMSE: {np.sqrt(np.mean(residuals**2)):.2f}")

    # OOS sigma — strict walk-forward, no fallback, no reconstruction
    # Parameters come from CLI args (passed in via sigma_params dict)
    min_train   = sigma_params["min_train"]
    min_holdout = sigma_params["min_holdout"]
    min_oos     = sigma_params["min_oos_total"]
    fold_days   = sigma_params["fold_days"]

    print(f"  Computing OOS sigma (fold={fold_days}d, min_tr={min_train}, min_te={min_holdout})...")

    dates_all = pd.to_datetime(df["_gd"].values)

    if len(dates_all) < min_train + min_oos:
        print(f"  [HARD FAIL] Not enough games for OOS sigma: "
              f"have {len(dates_all)}, need >={min_train + min_oos}")
        sys.exit(1)

    warmup_cutoff = dates_all[min_train - 1]   # count-based warmup
    current       = warmup_cutoff
    date_max      = dates_all.max()
    oos_residuals = []
    folds_run     = 0

    while current < date_max:
        fold_end = current + pd.Timedelta(days=fold_days)
        tr_mask  = dates_all < current
        te_mask  = (dates_all >= current) & (dates_all < fold_end)

        if int(tr_mask.sum()) < min_train or int(te_mask.sum()) < min_holdout:
            current = fold_end
            continue

        m_fold = Ridge(alpha=RIDGE_ALPHA)
        m_fold.fit(X[tr_mask], y[tr_mask])
        resid_te = y[te_mask] - m_fold.predict(X[te_mask])
        oos_residuals.extend(resid_te.tolist())
        folds_run += 1
        current = fold_end

    oos_residuals = np.asarray(oos_residuals, dtype=float)

    if len(oos_residuals) < min_oos:
        print(f"  [HARD FAIL] OOS sigma: only {len(oos_residuals)} residuals "
              f"(need >={min_oos}). Lower --sigma-fold-days or add more game data.")
        sys.exit(1)

    sigma = float(np.std(oos_residuals))
    print(f"  OOS residuals: {len(oos_residuals)}  folds: {folds_run}")
    print(f"  sigma (OOS walk-forward): {sigma:.2f}  "
          f"[in-sample ref: {np.std(residuals):.2f}]")

    # Print coefficients
    print(f"\n  Feature weights:")
    for fname, coef in zip(active_cols, model.coef_):
        print(f"    {fname:<18} {coef:+.4f}")
    print(f"    {'intercept':<18} {model.intercept_:+.4f}")

    return model, sigma, active_cols


# ============================================================================
# ODDS FETCHING
# ============================================================================

def normalize_home_spread(home_team, away_team, spread_map):
    """Fix C: validate that home+away spreads are opposite sides (within 0.25).
    Returns (home_spread, ok_flag). If ok_flag False, skip the game.
    """
    if home_team not in spread_map or away_team not in spread_map:
        return (None, False)
    hs = float(spread_map[home_team])
    as_ = float(spread_map[away_team])
    if abs(hs + as_) > 0.25:
        return (None, False)
    return (hs, True)


def fetch_odds_api(api_key, date_str=None):
    """Fetch today's spreads from OddsAPI."""
    import urllib.request
    import urllib.parse

    params = {
        "apiKey": api_key,
        "regions": "us",
        "markets": "spreads",
        "oddsFormat": "american",
    }
    url = f"{ODDS_API_URL}?{urllib.parse.urlencode(params)}"

    print(f"  Fetching from OddsAPI...")
    req = urllib.request.Request(url)
    with urllib.request.urlopen(req) as resp:
        data = json.loads(resp.read().decode())
        remaining = resp.headers.get("x-requests-remaining", "?")
        print(f"  Got {len(data)} events, quota remaining: {remaining}")

    today = date_str or datetime.now().strftime("%Y-%m-%d")
    games = []

    for event in data:
        et_dt = None
        commence = event.get("commence_time", "")
        if not commence:
            continue

        # Convert UTC commence_time to ET (UTC-5 EST / UTC-4 EDT)
        # Use UTC-5 for college basketball season (Nov-Mar)
        try:
            from datetime import timezone
            utc_dt = datetime.strptime(commence[:19], "%Y-%m-%dT%H:%M:%S")
            utc_dt = utc_dt.replace(tzinfo=timezone.utc)
            et_offset = timedelta(hours=-5)
            et_dt = utc_dt + et_offset
            game_date_et = et_dt.strftime("%Y-%m-%d")
        except Exception:
            game_date_et = commence[:10]  # fallback to raw date

        # Only accept games on today's ET date
        if game_date_et != today:
            continue

        home_raw = event.get("home_team", "")
        away_raw = event.get("away_team", "")
        home = odds_to_torvik(home_raw)
        away = odds_to_torvik(away_raw)

        # Extract spread with book priority — build full spread_map for binding check
        spread_map = {}
        for book in event.get("bookmakers", []):
            book_key = book.get("key", "")
            if book_key not in BOOK_PRIORITY:
                continue
            for market in book.get("markets", []):
                if market.get("key") != "spreads":
                    continue
                for outcome in market.get("outcomes", []):
                    name = outcome.get("name")
                    point = outcome.get("point")
                    if name and point is not None:
                        spread_map[name] = point
                if spread_map:
                    break
            if spread_map:
                break

        # Fix C: validate home/away spread binding before accepting game
        home_spread, ok = normalize_home_spread(home_raw, away_raw, spread_map)
        if not ok:
            print(f"  ⚠ spread binding failed: {away_raw} @ {home_raw} spreads={spread_map} — SKIPPED")
            continue

        games.append({
            "home_team": home,
            "away_team": away,
            "spread": home_spread,
            "commence_time": commence,
            "tip_et": et_dt.strftime("%I:%M %p ET").lstrip("0") if et_dt is not None else "",
        })

    print(f"  Parsed {len(games)} games for {today}")
    # Print game list with tip times for verification
    for g in sorted(games, key=lambda x: x["commence_time"]):
        tip = g.get("tip_et", "")
        print(f"    {g['away_team']} @ {g['home_team']}  {g['spread']:+.1f}  {tip}")
    return games


# ============================================================================
# SPREAD SANITY GATE
# ============================================================================

def validate_spreads(games, snapshot_df):
    """Cross-reference spreads against Torvik power ratings.
    
    The 'rating' column in Torvik is roughly equivalent to point spread
    (higher rating = better team). If the spread says Team A is a big dog
    but ratings say Team A is far superior, the spread is almost certainly wrong.
    
    Returns: (clean_games, flagged_games, blocked_games)
    - clean: pass validation
    - flagged: warning but allowed (SPREAD_WARN_THRESHOLD)
    - blocked: removed from slate (SPREAD_BLOCK_THRESHOLD)
    """
    snap = snapshot_df.set_index("team")
    clean = []
    flagged = []
    blocked = []
    
    for game in games:
        home = game["home_team"]
        away = game["away_team"]
        spread = game["spread"]
        
        # Need both teams in snapshot
        if home not in snap.index or away not in snap.index:
            clean.append(game)  # Can't validate, pass through
            continue
        
        h_rating = snap.loc[home, "rating"]
        a_rating = snap.loc[away, "rating"]
        
        # Rating diff from home perspective (positive = home stronger)
        rating_diff = h_rating - a_rating
        
        # Implied spread from ratings (home advantage ≈ +3 for CBB)
        # rating_diff > 0 → home should be favored → spread < 0
        # So implied_spread ≈ -(rating_diff) - HCA_adjustment
        # We use rating_diff directly since the model intercept handles HCA
        implied_home_margin = rating_diff  # rough proxy
        
        # The spread is from home's perspective:
        # spread > 0 → home is dog
        # spread < 0 → home is fav
        # So implied_spread ≈ -rating_diff (if home is better, spread should be negative)
        
        # Discrepancy: how far is the actual spread from what ratings imply?
        # If home_margin ≈ +15 (home much better), spread should be ≈ -12 to -18
        # actual spread = -15 → discrepancy ≈ 0 (fine)
        # actual spread = +10 → discrepancy = 25 (WRONG — home is dog but should be big fav)
        discrepancy = abs(implied_home_margin - (-spread))
        
        # Also check direction: is the spread on the WRONG SIDE entirely?
        spread_says_home_dog = spread > 0
        ratings_say_home_much_better = rating_diff > 5  # 5+ rating points is clear
        ratings_say_away_much_better = rating_diff < -5
        
        direction_flip = (
            (spread_says_home_dog and ratings_say_home_much_better) or
            (not spread_says_home_dog and ratings_say_away_much_better)
        )
        
        game["_rating_diff"] = round(rating_diff, 1)
        game["_implied_margin"] = round(implied_home_margin, 1)
        game["_discrepancy"] = round(discrepancy, 1)
        game["_direction_flip"] = direction_flip
        
        if discrepancy >= SPREAD_BLOCK_THRESHOLD:
            blocked.append(game)
        elif discrepancy >= SPREAD_WARN_THRESHOLD or direction_flip:
            flagged.append(game)
        else:
            clean.append(game)
    
    return clean, flagged, blocked


def print_spread_validation(clean, flagged, blocked):
    """Print spread validation results."""
    print(f"\n  ── SPREAD SANITY GATE ──")
    print(f"  ✅ Clean: {len(clean)}  ⚠️ Flagged: {len(flagged)}  🚫 Blocked: {len(blocked)}")
    
    if blocked:
        print(f"\n  🚫 BLOCKED (discrepancy ≥ {SPREAD_BLOCK_THRESHOLD}pt — almost certainly bad data):")
        for g in blocked:
            print(f"     {g['away_team']} @ {g['home_team']}  "
                  f"spread={g['spread']:+.1f}  "
                  f"rating_diff={g['_rating_diff']:+.1f}  "
                  f"discrepancy={g['_discrepancy']:.1f}pt"
                  f"{'  ⛔ DIRECTION FLIP' if g.get('_direction_flip') else ''}")
    
    if flagged:
        print(f"\n  ⚠️ FLAGGED (discrepancy ≥ {SPREAD_WARN_THRESHOLD}pt or direction mismatch — verify before betting):")
        for g in flagged:
            print(f"     {g['away_team']} @ {g['home_team']}  "
                  f"spread={g['spread']:+.1f}  "
                  f"rating_diff={g['_rating_diff']:+.1f}  "
                  f"discrepancy={g['_discrepancy']:.1f}pt"
                  f"{'  ⛔ DIRECTION FLIP' if g.get('_direction_flip') else ''}")
    
    if not flagged and not blocked:
        print(f"  All spreads pass sanity check ✓")


# ============================================================================
# PREDICTION
# ============================================================================

def predict_games(games, model, sigma, snapshot_df, snap_date, calibrator=None,
                  neutral_games=None, active_cols=None, hca=1.0):
    """Run MC simulator on today's games. Applies isotonic calibrator if provided.
    
    neutral_games: set of frozenset({team1, team2}) for neutral site games.
                   For these, μ is averaged from both orientations.
    active_cols: feature columns actually used in training (after dropping constants).
    """
    if active_cols is None:
        active_cols = FEATURE_COLS
    results = []
    np.random.seed(42)
    neutral_games = neutral_games or set()

    for game in games:
        home = game["home_team"]
        away = game["away_team"]
        spread = game["spread"]

        feats = build_features_for_game(home, away, snapshot_df, hca=hca)
        if feats is None:
            results.append({
                "home_team": home, "away_team": away,
                "spread": spread, "error": f"Team not found",
            })
            continue

        # Check if explicitly flagged neutral site
        is_neutral = frozenset({home, away}) in neutral_games

        X = np.array([[feats[c] for c in active_cols]])
        mu_home = model.predict(X)[0]

        # ── HCA NEUTRALIZATION (v4, rest of season) ──
        # Since HCA is constant and drops as a feature, the intercept
        # absorbs a phantom HCA bias. To neutralize: average μ from
        # both orientations for ALL games. This cancels the phantom HCA
        # without requiring venue data. Next season: replace with learned HCA.
        feats_flip = build_features_for_game(away, home, snapshot_df, hca=hca)
        if feats_flip is not None:
            X_flip = np.array([[feats_flip[c] for c in active_cols]])
            mu_away = model.predict(X_flip)[0]
            mu = (mu_home - mu_away) / 2  # net rating differential, HCA-neutral
            flip_swing = abs(mu_home - mu_away)
        else:
            mu = mu_home
            flip_swing = 0.0

        if is_neutral:
            pass  # already averaged above — no additional adjustment needed

        # Monte Carlo
        sims = np.random.normal(mu, sigma, MC_SIMS)
        win_prob = (sims > 0).mean()
        raw_cover = float((sims + spread > 0).mean())

        # μ-gap (the calibration input signal)
        z_mugap = mu + spread

        # ── MAGNITUDE DEBUG (per-game) ──
        if DEBUG_MAGNITUDE:
            print(f"    [MAG] {away} @ {home}  μ={mu:+.2f}  spread={spread:+.1f}  "
                  f"z={z_mugap:+.2f}  flip_swing={flip_swing:.2f}")

        # ── SECOND SANITY GATE: z-gap magnitude ──
        # In real CBB, |z_mugap| > 15 is almost never a real edge —
        # it's almost always a spread data error. Flag these.
        Z_GAP_WARN = 12.0
        Z_GAP_BLOCK = 18.0
        z_gap_flag = ""
        if abs(z_mugap) >= Z_GAP_BLOCK:
            z_gap_flag = "BLOCKED_ZGAP"
        elif abs(z_mugap) >= Z_GAP_WARN:
            z_gap_flag = "WARN_ZGAP"

        # Apply isotonic calibration if available
        use_calibrated = calibrator is not None
        if use_calibrated:
            if np.isnan(z_mugap):
                use_calibrated = False
                cover_prob_home = raw_cover
            else:
                try:
                    cover_prob_home = float(calibrator.predict([z_mugap])[0])
                except Exception:
                    use_calibrated = False
                    cover_prob_home = raw_cover
        else:
            cover_prob_home = raw_cover

        # Pick side (uses calibrated prob if available)
        if cover_prob_home > 0.5:
            pick_side = "HOME"
            pick_conf = cover_prob_home
            pick_team = home
            pick_spread = spread
        else:
            pick_side = "AWAY"
            pick_conf = 1.0 - cover_prob_home
            pick_team = away
            pick_spread = -spread

        edge = pick_conf - BREAKEVEN_110  # Edge vs -110 juice

        # Raw conf for audit trail
        if raw_cover > 0.5:
            raw_conf = raw_cover
        else:
            raw_conf = 1.0 - raw_cover
        edge_raw = raw_conf - BREAKEVEN_110

        # Tier classification
        abs_sp = abs(spread)
        home_is_fav = spread < 0
        pick_is_home = pick_side == "HOME"
        pick_is_dog = (pick_is_home and not home_is_fav) or \
                      (not pick_is_home and home_is_fav)

        if 7.0 <= abs_sp <= 9.5 and pick_is_dog:
            tier = "POCKET"
        else:
            tier = "BOARD"

        # Unit sizing — evidence-based from walk-forward calibrated backtest
        # POCKET (7-9.5 dogs): edge ≥3% = 54.9%, +4.9% ROI (n=253)
        #   ≥8% DROPS to 49% — do NOT over-filter pocket
        # BOARD: only edge ≥8% = 57.4%, +9.6% ROI (n=634)
        #   Below 8% is coin flip on board
        
        # Z-gap block overrides everything
        if z_gap_flag == "BLOCKED_ZGAP":
            units = 0  # Force no-play: spread data is almost certainly wrong
        elif use_calibrated:
            if tier == "POCKET":
                # Pocket: lower floor, gentler sizing
                if edge >= 0.05:
                    units = 2
                elif edge >= 0.03:
                    units = 1
                else:
                    units = 0  # Below 3% = noise in pocket
            else:
                # Board: high floor only
                if edge >= 0.10:
                    units = 3
                elif edge >= 0.08:
                    units = 2
                else:
                    units = 0  # Below 8% = coin flip on board
        else:
            # Uncalibrated: wider thresholds (edges inflated)
            if edge >= 0.076:
                units = 3
            elif edge >= 0.036:
                units = 2
            elif edge >= 0.0:
                units = 1
            else:
                units = 0

        results.append({
            "home_team": home,
            "away_team": away,
            "spread": spread,
            "abs_spread": abs_sp,
            "mu": round(mu, 2),
            "sigma": round(sigma, 2),
            "z_mugap": round(z_mugap, 2),
            "win_prob": round(win_prob, 4),
            "cover_home_raw": round(raw_cover, 4),
            "cover_home_cal": round(cover_prob_home, 4) if calibrator else None,
            "pick_side": pick_side,
            "pick_team": pick_team,
            "pick_spread": round(pick_spread, 1),
            "pick_conf": round(pick_conf, 4),
            "pick_conf_raw": round(raw_conf, 4),
            "edge": round(edge, 4),
            "edge_raw": round(edge_raw, 4),
            "pick_is_dog": pick_is_dog,
            "tier": tier,
            "units": units,
            "snap_date": snap_date,
            "calibrated": calibrator is not None,
            "is_neutral": is_neutral,
            "flip_swing": round(flip_swing, 2),
            "spread_flagged": game.get("_spread_flagged", False),
            "spread_discrepancy": game.get("_discrepancy", 0.0),
            "z_gap_flag": z_gap_flag,
            "commence_time": game.get("commence_time", ""),
        })

    # ── MAGNITUDE AUDIT SUMMARY ──
    if DEBUG_MAGNITUDE:
        _valid = [r for r in results if "error" not in r]
        if _valid:
            _mus = [abs(r["mu"]) for r in _valid]
            _zs = [abs(r["z_mugap"]) for r in _valid]
            print(f"\n  ── MAGNITUDE AUDIT SUMMARY ──")
            print(f"    max |μ|:       {max(_mus):.2f}")
            print(f"    max |z|:       {max(_zs):.2f}")
            print(f"    count |z|>15:  {sum(1 for z in _zs if z > 15)}")
            print(f"    count |z|>20:  {sum(1 for z in _zs if z > 20)}")

    return results


# ============================================================================
# OUTPUT FORMATTING
# ============================================================================

def format_output(results, snap_date, date_str):
    """Format results for console + file output."""
    plays = [r for r in results if r.get("units", 0) > 0 and "error" not in r]
    pocket = [r for r in plays if r["tier"] == "POCKET"]
    board = [r for r in plays if r["tier"] == "BOARD"]
    no_play = [r for r in results if r.get("units", 0) == 0 and "error" not in r]
    errors = [r for r in results if "error" in r]

    # Detect calibration from results
    is_cal = any(r.get("calibrated") for r in results if "error" not in r)

    lines = []
    lines.append("=" * 78)
    lines.append(f"  \U0001f3c0 MC GAME SIMULATOR — {date_str}")
    lines.append(f"  Snapshot: {snap_date}")
    lines.append(f"  Model: Ridge margin + {MC_SIMS:,} Monte Carlo sims")
    lines.append(f"  Generated: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    lines.append("=" * 78)
    lines.append("")
    if is_cal:
        lines.append("  \u2705 CALIBRATED: Isotonic μ-gap calibrator applied")
        lines.append("  POCKET (7-9.5 dogs): ≥5% → 2u  |  3-4.9% → 1u  |  <3% → skip")
        lines.append("  BOARD:               ≥10% → 3u  |  8-9.9% → 2u  |  <8% → skip")
    else:
        lines.append("  \u26a0\ufe0f  UNCALIBRATED — raw normal CDF probabilities")
        lines.append("     Cover probs are INFLATED. Do NOT use for live staking.")
        lines.append("     Paper-track only. Run with --calibrator to enable.")
        lines.append("  UNITS: ≥7.6% edge → 3u  |  3.6-7.5% → 2u  |  0-3.5% → 1u  |  <0% → skip (vs -110)")
    lines.append("")

    if pocket:
        pu = sum(r["units"] for r in pocket)
        lines.append(f"\U0001f3af POCKET PLAYS (7-9.5 dog) — {len(pocket)} games ({pu}u)")
        lines.append("\u2500" * 78)
        for r in sorted(pocket, key=lambda x: x["edge"], reverse=True):
            lines.append(_format_pick(r))
            lines.append("")

    if board:
        bu = sum(r["units"] for r in board)
        lines.append(f"\u26aa BOARD PLAYS — {len(board)} games ({bu}u)")
        lines.append("\u2500" * 78)
        for r in sorted(board, key=lambda x: x["edge"], reverse=True):
            lines.append(_format_pick(r))
            lines.append("")

    if not pocket and not board:
        lines.append("\u26d4 NO PLAYS — insufficient edge on today's slate")
        lines.append("")

    # Near misses (top 5 by edge that didn't qualify)
    if no_play:
        near = sorted(no_play, key=lambda x: x.get("edge", 0), reverse=True)[:5]
        lines.append(f"\U0001f440 NEAR MISSES (top 5 by edge)")
        lines.append("\u2500" * 78)
        for r in near:
            side_label = "DOG" if r.get("pick_is_dog") else "FAV"
            lines.append(
                f"  {r['pick_team']} {r['pick_spread']:+.1f}  "
                f"edge={r['edge']*100:.1f}%  μ={r['mu']:+.1f}  "
                f"cover={r['pick_conf']*100:.1f}%  [{side_label}]"
                f"  |  {r['away_team']} @ {r['home_team']}")
        lines.append("")

    # Flip-sensitivity warnings (games where home/away swap changes pick)
    flip_threshold = 5.0  # Flag games with >5pt swing
    flip_risky = [r for r in results if "error" not in r
                  and r.get("flip_swing", 0) >= flip_threshold
                  and r.get("units", 0) > 0
                  and not r.get("is_neutral")]
    if flip_risky:
        lines.append(f"\u26a0\ufe0f  FLIP-SENSITIVE PLAYS ({len(flip_risky)} games swing ≥{flip_threshold:.0f}pt if home/away flipped)")
        lines.append("  These may be neutral-site games. Verify home court before betting.")
        lines.append("\u2500" * 78)
        for r in sorted(flip_risky, key=lambda x: x["flip_swing"], reverse=True):
            lines.append(
                f"  {r['pick_team']} {r['pick_spread']:+.1f}  "
                f"swing={r['flip_swing']:.1f}pt  μ={r['mu']:+.1f}  "
                f"{r['away_team']} @ {r['home_team']}")
        lines.append("")

    # Spread-flagged warnings
    spread_flagged = [r for r in results if r.get("spread_flagged") and "error" not in r
                      and r.get("units", 0) > 0]
    if spread_flagged:
        lines.append(f"\u26a0\ufe0f  SPREAD SANITY WARNINGS ({len(spread_flagged)} plays with suspect spreads)")
        lines.append("  These games have spread-vs-rating discrepancies. Verify before betting.")
        lines.append("\u2500" * 78)
        for r in spread_flagged:
            lines.append(
                f"  {r['pick_team']} {r['pick_spread']:+.1f}  "
                f"discrepancy={r.get('spread_discrepancy', 0):.1f}pt  "
                f"edge={r['edge']*100:.1f}%  "
                f"{r['away_team']} @ {r['home_team']}")
        lines.append("")

    # Z-gap blocked games (spread data errors caught by model)
    zgap_blocked = [r for r in results if r.get("z_gap_flag") == "BLOCKED_ZGAP" and "error" not in r]
    zgap_warned = [r for r in results if r.get("z_gap_flag") == "WARN_ZGAP" and "error" not in r
                   and r.get("units", 0) > 0]
    if zgap_blocked:
        lines.append(f"\U0001f6d1 Z-GAP BLOCKED ({len(zgap_blocked)} games — |μ+spread| ≥ {18.0})")
        lines.append("  Extreme model-vs-market gap = almost certainly bad spread data.")
        lines.append("\u2500" * 78)
        for r in zgap_blocked:
            lines.append(
                f"  {r['pick_team']} {r['pick_spread']:+.1f}  "
                f"z={r['z_mugap']:+.1f}  μ={r['mu']:+.1f}  spread={r['spread']:+.1f}  "
                f"{r['away_team']} @ {r['home_team']}")
        lines.append("")
    if zgap_warned:
        lines.append(f"\u26a0\ufe0f  Z-GAP WARNINGS ({len(zgap_warned)} plays — |μ+spread| ≥ {12.0})")
        lines.append("  Large model-vs-market gap. Could be real edge or bad data. Verify.")
        lines.append("\u2500" * 78)
        for r in zgap_warned:
            lines.append(
                f"  {r['pick_team']} {r['pick_spread']:+.1f}  "
                f"z={r['z_mugap']:+.1f}  μ={r['mu']:+.1f}  spread={r['spread']:+.1f}  "
                f"{r['away_team']} @ {r['home_team']}")
        lines.append("")

    if errors:
        lines.append(f"\u26a0\ufe0f  ERRORS: {len(errors)}")
        for r in errors:
            lines.append(f"  {r['away_team']} @ {r['home_team']}: {r['error']}")
        lines.append("")

    # Neutral site summary
    neutrals = [r for r in results if r.get("is_neutral") and "error" not in r]
    if neutrals:
        lines.append(f"\U0001f3df\ufe0f  NEUTRAL SITE: {len(neutrals)} games (μ averaged from both orientations)")
        for r in neutrals:
            lines.append(
                f"  {r['away_team']} vs {r['home_team']}  "
                f"μ={r['mu']:+.1f}  spread={r['spread']:+.1f}")
        lines.append("")

    lines.append("=" * 78)
    lines.append(f"  POCKET: {len(pocket)}  |  BOARD: {len(board)}  |  "
                 f"Skip: {len(no_play)}  |  Errors: {len(errors)}  |  "
                 f"Total: {len(results)}")
    lines.append("=" * 78)

    # Always remind about neutral sites
    non_neutral = [r for r in results if not r.get("is_neutral") and "error" not in r]
    n_neutral_flagged = len([r for r in results if r.get("is_neutral")])
    lines.append(f"\n  \U0001f3df\ufe0f  NEUTRAL SITE CHECK: {len(non_neutral)} games assume home-court "
                 f"(~2.9pt phantom HCA)")
    if n_neutral_flagged:
        lines.append(f"     {n_neutral_flagged} games flagged neutral (μ averaged)")
    lines.append(f"     Showcase/tournament? → --neutral Team1:Team2")

    return "\n".join(lines)


def _tip_et(commence_time):
    """Convert UTC ISO commence_time to ET tip string (EST = UTC-5)."""
    if not commence_time:
        return ""
    try:
        from datetime import timezone
        utc = datetime.strptime(commence_time[:19], "%Y-%m-%dT%H:%M:%S")
        utc = utc.replace(tzinfo=timezone.utc)
        et_h = (utc.hour - 5) % 24
        period = "PM" if et_h >= 12 else "AM"
        disp = et_h % 12 or 12
        return f"{disp}:{utc.minute:02d} {period} ET"
    except Exception:
        return ""


def _format_pick(r):
    """Format single pick line."""
    u = r["units"]
    icon = "\U0001f525" if u == 3 else ("\U0001f4ca" if u == 2 else "\u26aa")
    tier_tag = "\U0001f3af" if r["tier"] == "POCKET" else ""
    side_label = "DOG" if r.get("pick_is_dog") else "FAV"
    neutral_tag = " \U0001f3df\ufe0f NEUTRAL" if r.get("is_neutral") else ""
    spread_warn = " ⚠️SPREAD?" if r.get("spread_flagged") else ""
    zgap_warn = " 🚫ZGAP" if r.get("z_gap_flag") == "BLOCKED_ZGAP" else (
        " ⚠️ZGAP" if r.get("z_gap_flag") == "WARN_ZGAP" else "")
    warnings = f"{spread_warn}{zgap_warn}"
    tip = _tip_et(r.get("commence_time", ""))
    tip_str = f"  🕐 {tip}" if tip else ""

    # Show both raw and calibrated when available
    if r.get("calibrated"):
        conf_str = (f"cover={r['pick_conf']*100:.1f}% (cal)  "
                    f"raw={r['pick_conf_raw']*100:.1f}%  "
                    f"edge={r['edge']*100:.1f}%")
    else:
        conf_str = (f"cover={r['pick_conf']*100:.1f}% (raw)  "
                    f"edge={r['edge']*100:.1f}%")

    return (
        f"  {icon} {tier_tag} {r['pick_team']} {r['pick_spread']:+.1f}  "
        f"({u}u) [{side_label}]{neutral_tag}{warnings}\n"
        f"     {r['away_team']} @ {r['home_team']}{tip_str}\n"
        f"     μ={r['mu']:+.1f}  σ={r['sigma']:.1f}  z={r['z_mugap']:+.1f}  "
        f"{conf_str}  "
        f"win={r['win_prob']*100:.0f}%"
    )


# ============================================================================
# MAIN
# ============================================================================

def main():
    parser = argparse.ArgumentParser(
        description="Daily Monte Carlo Game Simulator")
    parser.add_argument("--snapshots-dir", default=".",
                        help="Directory with team_power_snapshot_*.csv")
    parser.add_argument("--game-stats", required=True,
                        help="game_stats_with_vegas.csv (for training)")
    parser.add_argument("--snapshot", default=None,
                        help="Specific snapshot file for today's predictions")
    parser.add_argument("--no-fetch", action="store_true",
                        help="Don't fetch from OddsAPI")
    parser.add_argument("--odds", default=None,
                        help="Pre-fetched odds CSV (home_team,away_team,spread)")
    parser.add_argument("--api-key", default=None,
                        help="OddsAPI key (or set ODDS_API_KEY env var)")
    parser.add_argument("--date", default=None,
                        help="Date to predict (default: today)")
    parser.add_argument("--output", default=None,
                        help="Output CSV path")
    parser.add_argument("--calibrator", default="calibrator_isotonic.pkl",
                        help="Path to isotonic calibrator artifact (joblib)")
    parser.add_argument("--no-calibration", action="store_true",
                        help="Disable calibration even if calibrator file exists")
    parser.add_argument("--neutral", nargs="*", default=[],
                        help="Neutral-site games as 'TeamA:TeamB' pairs. "
                             "e.g. --neutral Michigan:Duke 'Iowa St.:Kansas'")
    parser.add_argument("--hca", type=float, default=1.0,
                        help="Home court advantage weight (default 1.0). "
                             "Set to 0.0 for neutral-site slates (conf tourneys, March Madness).")
    parser.add_argument("--swap", nargs="*", default=[],
                        help="Swap home/away for games where OddsAPI has it wrong. "
                             "Format: 'RealHome:RealAway' (Torvik names). "
                             "e.g. --swap Houston:Arizona")
    parser.add_argument("--sigma-min-train", type=int, default=WF_MIN_TRAIN,
                        help=f"Min train games per σ fold (default {WF_MIN_TRAIN})")
    parser.add_argument("--sigma-min-holdout", type=int, default=WF_MIN_HOLDOUT,
                        help=f"Min holdout games per σ fold (default {WF_MIN_HOLDOUT})")
    parser.add_argument("--sigma-min-oos-total", type=int, default=WF_MIN_OOS_TOTAL,
                        help=f"Min total OOS residuals required (default {WF_MIN_OOS_TOTAL})")
    parser.add_argument("--sigma-fold-days", type=int, default=WF_FOLD_DAYS,
                        help=f"σ fold size in days (default {WF_FOLD_DAYS})")
    args = parser.parse_args()

    # Parse neutral games
    neutral_games = set()
    for pair in args.neutral:
        parts = pair.split(":")
        if len(parts) == 2:
            neutral_games.add(frozenset({parts[0].strip(), parts[1].strip()}))
        else:
            print(f"  [WARN] Bad neutral pair: {pair} (use Team1:Team2)")

    # Auto-add known neutral showcase events
    for pair in KNOWN_NEUTRAL_EVENTS:
        neutral_games.add(pair)

    if neutral_games:
        print(f"  \U0001f3df\ufe0f Neutral sites: {len(neutral_games)} games")

    # Parse swap overrides: key=frozenset, value=(real_home, real_away)
    swap_games = {}
    for pair in args.swap:
        parts = pair.split(":")
        if len(parts) == 2:
            real_home = parts[0].strip()
            real_away = parts[1].strip()
            swap_games[frozenset({real_home, real_away})] = (real_home, real_away)
        else:
            print(f"  [WARN] Bad swap pair: {pair} (use RealHome:RealAway)")
    if swap_games:
        print(f"  \U0001f504 Home/away swaps: {len(swap_games)} games")

    date_str = args.date or datetime.now().strftime("%Y-%m-%d")
    game_month = pd.to_datetime(date_str).month

    # Neutral site warning for tournament months
    if game_month in NEUTRAL_WARNING_MONTHS:
        print(f"\n  \u26a0\ufe0f  MARCH/APRIL: Most games are NEUTRAL SITE.")
        print(f"     Use --neutral Team1:Team2 for EVERY non-regular-season game.")
        print(f"     Phantom HCA ≈ 2.9 pts — this flips picks on close games.\n")

    date_str = args.date or datetime.now().strftime("%Y-%m-%d")
    print("=" * 78)
    print(f"  \U0001f3c0 MC GAME SIMULATOR — {date_str}")
    print("=" * 78)

    # ── Load snapshots ──
    print("\n[1] Loading snapshots...")
    snapshots = load_snapshots(args.snapshots_dir)

    # ── Get prediction snapshot ──
    if args.snapshot:
        snap_df = pd.read_csv(args.snapshot)
        snap_date = args.snapshot
    else:
        snap_date, snap_df = get_latest_snapshot(snapshots, before_date=date_str)
        if snap_df is None:
            print(f"  [ERROR] No snapshot before {date_str}")
            sys.exit(1)
    print(f"  Using snapshot: {snap_date}")

    # ── Train model ──
    print("\n[2] Loading + validating game stats...")
    gs = pd.read_csv(args.game_stats)

    # ══════════════════════════════════════════════════════════════
    # HARD MARGIN CONTRACT VALIDATOR — runs every time, no bypass
    # ══════════════════════════════════════════════════════════════
    print("  ── Margin orientation audit ──")

    # Check 1: location must be ALL 'H' (team = home)
    if "location" in gs.columns:
        non_home = gs[gs["location"] != "H"]
        if len(non_home) > 0:
            print(f"  [HARD FAIL] {len(non_home)} rows have location != 'H'")
            print(f"  team column is NOT always home. Cannot trust margin.")
            print(non_home[["team", "opponent", "location", "margin"]].head(10))
            sys.exit(1)
        print(f"    location=H for 100% of rows ({len(gs)}) ✓")
    else:
        print("  [HARD FAIL] No 'location' column — cannot verify orientation")
        sys.exit(1)

    # Check 2: parse result strings to verify margin
    if "result" in gs.columns:
        n_checked = 0
        n_mismatch = 0
        for idx, r in gs.sample(min(500, len(gs)), random_state=42).iterrows():
            try:
                res = r["result"]
                wl = res.split(",")[0].strip()
                scores = res.split(",")[1].strip().split("-")
                s1, s2 = int(scores[0]), int(scores[1])
                if wl == "W":
                    computed = s1 - s2
                else:
                    computed = s2 - s1
                if computed != r["margin"]:
                    n_mismatch += 1
                    if n_mismatch <= 3:
                        print(f"    MISMATCH: {r['team']} '{res}' "
                              f"computed={computed} csv={r['margin']}")
                n_checked += 1
            except Exception:
                pass  # Unparseable result string, skip
        if n_mismatch > 0:
            print(f"  [HARD FAIL] {n_mismatch}/{n_checked} margin mismatches")
            sys.exit(1)
        print(f"    margin verified vs result string: {n_checked}/{n_checked} ✓")

    # Check 3: sanity — home favorites should have positive avg margin
    if "vegas_spread" in gs.columns:
        big_fav = gs[gs["vegas_spread"] < -10]
        if len(big_fav) > 50:
            avg_m = big_fav["margin"].mean()
            if avg_m < 0:
                print(f"  [HARD FAIL] Home favored by 10+ but avg margin={avg_m:.1f}")
                print(f"  Spread/margin orientation mismatch.")
                sys.exit(1)
            print(f"    spread sanity: home fav>10 avg margin={avg_m:+.1f} ✓")

    print("  ── Margin contract: PASS ──")

    # ── Snapshot coverage ──
    gs["_gd"] = pd.to_datetime(gs["game_date"], format="mixed", errors="raise")
    snap_dates_list = sorted(snapshots.keys())
    earliest_snap = pd.to_datetime(snap_dates_list[0])
    n_covered = (gs["_gd"] > earliest_snap).sum()
    n_total = len(gs)
    print(f"  Snapshot coverage: {n_covered}/{n_total} games "
          f"({n_covered/n_total*100:.0f}%) after {earliest_snap.date()}")

    # Only train on games before today
    gs_train = gs[gs["_gd"] < pd.to_datetime(date_str)]
    print(f"  Training on games before {date_str}: {len(gs_train)}")
    sigma_params = {
        "min_train":     args.sigma_min_train,
        "min_holdout":   args.sigma_min_holdout,
        "min_oos_total": args.sigma_min_oos_total,
        "fold_days":     args.sigma_fold_days,
    }
    model, sigma, active_cols = train_model(gs_train, snapshots, sigma_params=sigma_params)

    # ── Load calibrator ──
    # Expects bare IsotonicRegression from build_calibrator.py.
    # Hard error on wrong type — no dict format, no σ override, no fallback.
    calibrator = None
    if not args.no_calibration:
        cal_path = args.calibrator
        if cal_path and os.path.exists(cal_path):
            artifact = joblib.load(cal_path)
            if not isinstance(artifact, IsotonicRegression):
                print(f"\n  [HARD FAIL] Calibrator at {cal_path} is not an IsotonicRegression "
                      f"(got {type(artifact).__name__}).")
                print(f"  Rebuild with: python build_calibrator.py --game-stats game_stats_with_vegas.csv")
                sys.exit(1)
            calibrator = artifact
            print(f"\n  ✅ CALIBRATION: ON — loaded {cal_path}")
        else:
            print(f"\n  🔴 CALIBRATION: OFF — {cal_path} not found")
            print(f"     Generate with: python build_calibrator.py --game-stats game_stats_with_vegas.csv")
    else:
        print(f"\n  🔴 CALIBRATION: OFF — disabled via --no-calibration")

    # ── Get today's games ──
    print(f"\n[3] Getting today's games...")
    if args.odds:
        odds_df = pd.read_csv(args.odds)
        games = odds_df.to_dict("records")
        print(f"  Loaded {len(games)} games from {args.odds}")
    elif args.no_fetch:
        print("  [ERROR] --no-fetch requires --odds")
        sys.exit(1)
    else:
        api_key = args.api_key or os.environ.get(ODDS_API_KEY_ENV)
        if not api_key:
            # Try reading from local file
            for kf in ["api_key.txt", ".odds_api_key"]:
                if os.path.exists(kf):
                    api_key = open(kf).read().strip()
                    break
        if not api_key:
            print("  [ERROR] No API key. Use --api-key or set ODDS_API_KEY")
            sys.exit(1)
        games = fetch_odds_api(api_key, date_str)

    if not games:
        print("  No games found for today.")
        sys.exit(0)

    # ── Apply home/away swaps ──
    if swap_games:
        for i, game in enumerate(games):
            pair_key = frozenset({game["home_team"], game["away_team"]})
            if pair_key in swap_games:
                real_home, real_away = swap_games[pair_key]
                old_home = game["home_team"]
                old_spread = game["spread"]
                games[i]["home_team"] = real_home
                games[i]["away_team"] = real_away
                games[i]["spread"] = -old_spread  # Flip spread to new home perspective
                print(f"  \U0001f504 SWAPPED: {old_home}(H) → {real_home}(H)  "
                      f"spread {old_spread:+.1f} → {-old_spread:+.1f}")

    # ── Validate spreads against ratings ──
    print(f"\n[3b] Spread sanity validation...")
    clean_games, flagged_games, blocked_games = validate_spreads(games, snap_df)
    print_spread_validation(clean_games, flagged_games, blocked_games)
    
    # Blocked games are removed from slate entirely
    if blocked_games:
        print(f"\n  🚫 Removing {len(blocked_games)} blocked games from slate.")
        print(f"     These have spread-vs-rating discrepancies too large to be real edges.")
        print(f"     Fix the spread data or use --swap to correct home/away.")
    
    # Flagged games get a marker but still run (user warned)
    for g in flagged_games:
        g["_spread_flagged"] = True
    
    # Combine clean + flagged for prediction (blocked excluded)
    games = clean_games + flagged_games

    # ── Predict ──
    print(f"\n[4] Running predictions ({len(games)} games)...")
    results = predict_games(games, model, sigma, snap_df, snap_date,
                            calibrator=calibrator, neutral_games=neutral_games,
                            active_cols=active_cols, hca=args.hca)

    # ── Output ──
    output_text = format_output(results, snap_date, date_str)
    print("\n" + output_text)

    # Save text
    txt_path = f"sim_picks_{date_str}.txt"
    with open(txt_path, "w") as f:
        f.write(output_text)
    print(f"\n  \U0001f4dd Picks: {txt_path}")

    # Save CSV
    csv_path = args.output or f"sim_predictions_{date_str}.csv"
    valid = [r for r in results if "error" not in r]
    if valid:
        pd.DataFrame(valid).to_csv(csv_path, index=False)
        print(f"  \U0001f4dd CSV: {csv_path}")


if __name__ == "__main__":
    main()
