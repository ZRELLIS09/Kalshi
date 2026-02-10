"""
Master NCAA Basketball Prediction Script
=========================================
Uses all project files to generate predictions with:
- Expected Margin
- Actual Spread (manual input or API)
- Confidence
- Model Edge

Usage:
    python run_predictions.py
"""

import pandas as pd
import numpy as np
import sys
import os
import glob
import re
from datetime import datetime

# Add project directory to path
sys.path.insert(0, '/mnt/project')

# Import project modules
try:
    from team_name_mapping import ODDS_TO_TORVIK, odds_to_torvik
    print("âœ… Successfully imported team_name_mapping")
except ImportError as e:
    print(f"âš ï¸ Warning: Could not import team_name_mapping: {e}")
    ODDS_TO_TORVIK = {}

# ============================================================================
# CONSTANTS
# ============================================================================

HOME_COURT_ADVANTAGE = 3.5  # Points added to home team's expected margin

FEATURE_WEIGHTS = {
    "barthag_diff":  0.25,
    "rating_diff":   0.30,
    "adj_o_diff":    0.08,
    "adj_d_diff":    0.08,
    "tempo_sum":     0.02,
    "off_efg_diff":  0.04,
    "off_to_diff":   0.03,
    "off_or_diff":   0.03,
    "off_ftr_diff":  0.02,
    "def_efg_diff":  0.05,
    "def_to_diff":   0.04,
    "def_or_diff":   0.03,
    "def_ftr_diff":  0.03,
}

FOUR_FACTOR_FEATURES = [
    "off_efg_diff", "off_to_diff", "off_or_diff", "off_ftr_diff",
    "def_efg_diff", "def_to_diff", "def_or_diff", "def_ftr_diff",
]

# ============================================================================
# SNAPSHOT LOADING
# ============================================================================

def load_latest_snapshot(snapshot_dir="/mnt/project"):
    """Load the most recent Torvik snapshot"""
    pattern = f"{snapshot_dir}/team_power_snapshot_*.csv"
    files = sorted(glob.glob(pattern))
    
    if not files:
        raise FileNotFoundError(f"No snapshot files found: {pattern}")
    
    latest_file = files[-1]
    df = pd.read_csv(latest_file)
    
    snapshot_date = latest_file.split('_')[-1].replace('.csv', '')
    
    # FIX 3: staleness check from filename date
    days_old = None
    m = re.search(r"team_power_snapshot_(\d{8})\.csv$", os.path.basename(latest_file))
    if m:
        try:
            snap_date = datetime.strptime(m.group(1), "%Y%m%d")
            days_old = (datetime.now() - snap_date).days
        except ValueError:
            days_old = None

    print(f"\n{'='*80}")
    print(f"DATA SOURCE")
    print(f"{'='*80}")
    print(f"Snapshot: {os.path.basename(latest_file)}")
    print(f"Date: {snapshot_date}")
    print(f"Teams: {len(df)}")
    if days_old is not None and days_old > 2:
        print(f"  *** STALE DATA WARNING: Snapshot is {days_old} days old! ***")
        print(f"  Edges may be inflated due to rating drift.")
    print(f"{'='*80}\n")
    
    if days_old is not None and days_old > 7:
        raise RuntimeError(
            f"HARD BLOCK: Snapshot is {days_old} days old (>7). "
            f"Stale data produces phantom edges of 10-20 pts. "
            f"Refresh: python fetch_barttorvik.py"
        )

    return df, latest_file

def get_team_stats(df, team_name):
    """Get team stats from snapshot, handling variations"""
    # Try exact match first
    team_data = df[df['team'] == team_name]
    
    if len(team_data) > 0:
        return team_data.iloc[0].to_dict()
    
    # Try case-insensitive
    team_data = df[df['team'].str.lower() == team_name.lower()]
    
    if len(team_data) > 0:
        return team_data.iloc[0].to_dict()
    
    # FIX 6: Use ODDS_TO_TORVIK mapping as fallback
    mapped_name = team_name
    try:
        mapped_name = odds_to_torvik(team_name)
    except Exception:
        mapped_name = ODDS_TO_TORVIK.get(team_name, team_name)
    
    if mapped_name != team_name:
        team_data = df[df['team'] == mapped_name]
        if len(team_data) > 0:
            return team_data.iloc[0].to_dict()
        team_data = df[df['team'].str.lower() == mapped_name.lower()]
        if len(team_data) > 0:
            return team_data.iloc[0].to_dict()

    return None

# ============================================================================
# FEATURE COMPUTATION
# ============================================================================

def compute_feature_differentials(home_stats, away_stats):
    """
    Compute all 13 Torvik feature differentials
    Following the exact inversion rules from constants_TORVIK.py
    """
    features = {}

    # FIX 2: NaN-safe subtraction helper
    def _diff(h, a):
        if h is None or a is None:
            return np.nan
        try:
            return float(h) - float(a)
        except (ValueError, TypeError):
            return np.nan

    # STANDARD FEATURES (higher = better): home - away
    features['barthag_diff'] = _diff(home_stats.get('barthag'), away_stats.get('barthag'))
    features['adj_o_diff'] = _diff(home_stats.get('adj_o'), away_stats.get('adj_o'))
    features['off_efg_diff'] = _diff(home_stats.get('off_efg'), away_stats.get('off_efg'))
    features['off_or_diff'] = _diff(home_stats.get('off_or'), away_stats.get('off_or'))
    features['off_ftr_diff'] = _diff(home_stats.get('off_ftr'), away_stats.get('off_ftr'))
    features['def_to_diff'] = _diff(home_stats.get('def_to'), away_stats.get('def_to'))

    # INVERTED FEATURES (lower = better): away - home
    features['adj_d_diff'] = _diff(away_stats.get('adj_d'), home_stats.get('adj_d'))
    features['off_to_diff'] = _diff(away_stats.get('off_to'), home_stats.get('off_to'))
    features['def_efg_diff'] = _diff(away_stats.get('def_efg'), home_stats.get('def_efg'))
    features['def_or_diff'] = _diff(away_stats.get('def_or'), home_stats.get('def_or'))
    features['def_ftr_diff'] = _diff(away_stats.get('def_ftr'), home_stats.get('def_ftr'))

    # SPECIAL FEATURES
    features['rating_diff'] = _diff(home_stats.get('rating'), away_stats.get('rating'))

    h_tempo = home_stats.get('tempo', 68)
    a_tempo = away_stats.get('tempo', 68)
    try:
        features['tempo_sum'] = round(float(h_tempo) + float(a_tempo), 4)
    except (ValueError, TypeError):
        features['tempo_sum'] = np.nan

    return features

def compute_win_probability(features, hca=HOME_COURT_ADVANTAGE):
    """
    Compute win probability using weighted feature composite.
    FIX 1: All 13 features now contribute to the logit.
    """
    logit = 0.0

    for feat_name, weight in FEATURE_WEIGHTS.items():
        val = features.get(feat_name, np.nan)
        if np.isnan(val):
            continue
        if feat_name == 'barthag_diff':
            logit += val * 30.0 * weight
        elif feat_name == 'rating_diff':
            logit += val * 0.15 * weight
        elif feat_name == 'tempo_sum':
            logit += (val - 136) * 0.01 * weight
        elif feat_name in ('adj_o_diff', 'adj_d_diff'):
            logit += val * 0.1 * weight
        else:
            logit += val * 0.08 * weight

    # Add home court advantage to composite
    composite_with_hca = logit + hca

    # Convert to probability using logistic function
    # Calibrated: 10-point composite ~ 76% win probability
    win_prob = 1 / (1 + np.exp(-composite_with_hca / 10))

    return win_prob, composite_with_hca

# ============================================================================
# PREDICTION GENERATION
# ============================================================================

def predict_game(home_team, away_team, df, spread=None):
    """
    Generate full prediction for a game
    
    Returns dict with all prediction metrics
    """
    # Get team stats
    home_stats = get_team_stats(df, home_team)
    away_stats = get_team_stats(df, away_team)
    
    if home_stats is None:
        return {'error': f'Team not found: {home_team}'}
    
    if away_stats is None:
        return {'error': f'Team not found: {away_team}'}
    
    # Compute features
    features = compute_feature_differentials(home_stats, away_stats)
    
    # FIX 2: four-factor NaN gate
    ff_missing = [f for f in FOUR_FACTOR_FEATURES if np.isnan(features.get(f, np.nan))]
    if len(ff_missing) >= 4:
        return {
            'home_team': home_team, 'away_team': away_team,
            'error': f"NO-GO: insufficient four-factor coverage ({len(ff_missing)}/8 missing)",
        }
    if ff_missing:
        print(f"  Warning: partial four-factor missingness for {away_team} @ {home_team}: {ff_missing}")

    # Compute win probability
    win_prob, composite = compute_win_probability(features)
    
    # Expected margin from HOME perspective (positive = home wins by X)
    expected_margin_home = features.get('rating_diff', 0.0)
    if np.isnan(expected_margin_home):
        expected_margin_home = 0.0
    expected_margin_home += HOME_COURT_ADVANTAGE
    
    # Determine pick and confidence
    if win_prob > 0.5:
        pick = home_team
        pick_type = "HOME"
        confidence = win_prob
        expected_margin = expected_margin_home  # Home favored
    else:
        pick = away_team
        pick_type = "AWAY"
        confidence = 1 - win_prob
        expected_margin = -expected_margin_home  # Away favored (negative from home perspective)
    
    # Calculate model edge
    # Spread convention: negative = home favored (e.g., Duke -5.5)
    # Model edge: expected_margin_home - (-spread) = expected_margin_home + spread
    # Positive edge = model likes home more than market
    # Negative edge = model likes away more than market
    if spread is not None:
        model_edge = expected_margin_home + spread
    else:
        model_edge = None
    
    return {
        'home_team': home_team,
        'away_team': away_team,
        'pick': pick,
        'pick_type': pick_type,
        'expected_margin': abs(expected_margin),  # Always positive for display
        'expected_margin_home': expected_margin_home,  # Keep signed version for calculations
        'actual_spread': spread,
        'model_edge': model_edge,
        'confidence': confidence,
        'home_win_prob': win_prob,
        'away_win_prob': 1 - win_prob,
        'composite_score': composite,
        'rating_diff': features['rating_diff'],
        'home_rating': home_stats['rating'],
        'away_rating': away_stats['rating'],
        'home_barthag': home_stats['barthag'],
        'away_barthag': away_stats['barthag'],
        'home_adj_o': home_stats['adj_o'],
        'away_adj_o': away_stats['adj_o'],
        'home_adj_d': home_stats['adj_d'],
        'away_adj_d': away_stats['adj_d'],
        # Removed 'features' dict to avoid CSV serialization issues
    }

# ============================================================================
# OUTPUT FORMATTING
# ============================================================================

def print_prediction(pred, rank=None):
    """Print formatted prediction"""
    if 'error' in pred:
        print(f"âŒ ERROR: {pred['error']}")
        return
    
    rank_str = f"#{rank} " if rank else ""
    
    print(f"\n{'='*90}")
    print(f"{rank_str}GAME: {pred['home_team']} vs {pred['away_team']}")
    print(f"{'='*90}")
    print(f"ðŸŽ¯ PICK: {pred['pick']} ({pred['pick_type']})")
    print(f"ðŸ“Š EXPECTED MARGIN: {pred['expected_margin']:.1f} points")
    
    if pred['actual_spread'] is not None:
        print(f"ðŸ’° ACTUAL SPREAD: {pred['actual_spread']}")
        if pred['model_edge'] is not None:
            edge_symbol = "+" if pred['model_edge'] >= 0 else ""
            stars = ""
            if pred['model_edge'] >= 10:
                stars = " â­â­â­ HUGE VALUE"
            elif pred['model_edge'] >= 5:
                stars = " â­â­ STRONG VALUE"
            elif pred['model_edge'] >= 2:
                stars = " â­ VALUE"
            print(f"ðŸ“ˆ MODEL EDGE: {edge_symbol}{pred['model_edge']:.1f} points{stars}")
    else:
        print(f"ðŸ’° ACTUAL SPREAD: Not available")
        print(f"ðŸ“ˆ MODEL EDGE: Not available (need spread)")
    
    print(f"âœ… CONFIDENCE: {pred['confidence']:.1%}")
    print(f"\n   Win Probabilities:")
    print(f"   {pred['home_team']}: {pred['home_win_prob']:.1%}")
    print(f"   {pred['away_team']}: {pred['away_win_prob']:.1%}")
    print(f"\n   Team Ratings:")
    print(f"   {pred['home_team']}: {pred['home_rating']:.1f}")
    print(f"   {pred['away_team']}: {pred['away_rating']:.1f}")
    print(f"{'='*90}")

def create_summary_table(predictions):
    """Create summary table of all predictions"""
    valid_preds = [p for p in predictions if 'error' not in p]
    
    if not valid_preds:
        print("\nâš ï¸ No valid predictions to summarize")
        return
    
    print(f"\n{'='*100}")
    print("PREDICTIONS SUMMARY - RANKED BY MODEL EDGE")
    print(f"{'='*100}\n")
    
    # Sort by model edge if available
    with_edges = [p for p in valid_preds if p['model_edge'] is not None]
    without_edges = [p for p in valid_preds if p['model_edge'] is None]
    
    sorted_preds = sorted(with_edges, key=lambda x: x['model_edge'], reverse=True)
    sorted_preds.extend(without_edges)
    
    print(f"{'Rank':<6} {'Game':<32} {'Pick':<15} {'Margin':<9} {'Spread':<9} {'Edge':<9} {'Conf':<8}")
    print(f"{'-'*6} {'-'*32} {'-'*15} {'-'*9} {'-'*9} {'-'*9} {'-'*8}")
    
    for i, pred in enumerate(sorted_preds, 1):
        game_str = f"{pred['home_team']} vs {pred['away_team']}"
        if len(game_str) > 32:
            game_str = game_str[:29] + "..."
        
        spread_str = str(pred['actual_spread']) if pred['actual_spread'] is not None else "N/A"
        
        if pred['model_edge'] is not None:
            edge_str = f"{pred['model_edge']:+.1f}"
            stars = ""
            if pred['model_edge'] >= 10:
                stars = " â­â­â­"
            elif pred['model_edge'] >= 5:
                stars = " â­â­"
            elif pred['model_edge'] >= 2:
                stars = " â­"
        else:
            edge_str = "N/A"
            stars = ""
        
        print(f"{i:<6} {game_str:<32} {pred['pick']:<15} {pred['expected_margin']:>7.1f}   "
              f"{spread_str:>7}   {edge_str:>7}   {pred['confidence']:>6.1%}{stars}")
    
    print(f"\n{'='*100}\n")

# ============================================================================
# MAIN EXECUTION
# ============================================================================

def main():
    print("="*100)
    print("NCAA BASKETBALL PREDICTIONS - Master Script")
    print("Using All Project Files: Snapshots + Team Mappings + Feature Computation")
    print("="*100)
    
    # Load snapshot data
    try:
        df, snapshot_file = load_latest_snapshot()
    except Exception as e:
        print(f"âŒ ERROR loading snapshot: {e}")
        return 1
    
    # ============================================================================
    # EDIT YOUR GAMES HERE
    # ============================================================================
    GAMES = [
        # Format: (home_team, away_team, spread)
        # Spread = negative means home team favored (e.g., Duke -5.5)
        # If spread unknown, use None
        
        ("Duke", "North Carolina", -5.5),
        ("Illinois", "Michigan St.", -8.5),
        ("Auburn", "Alabama", -5.0),
        
        # ADD MORE GAMES BELOW:
        # ("Home Team", "Away Team", -7.0),
        # ("Team A", "Team B", None),  # No spread available
    ]
    
    print(f"Generating predictions for {len(GAMES)} games...\n")
    
    # Generate predictions
    predictions = []
    errors = []
    
    for home, away, spread in GAMES:
        pred = predict_game(home, away, df, spread)
        predictions.append(pred)
        
        if 'error' in pred:
            errors.append(pred)
    
    # Print detailed predictions
    print(f"\n{'='*100}")
    print("DETAILED PREDICTIONS")
    print(f"{'='*100}")
    
    valid_preds = [p for p in predictions if 'error' not in p]
    with_edges = [p for p in valid_preds if p['model_edge'] is not None]
    sorted_preds = sorted(with_edges, key=lambda x: x['model_edge'], reverse=True)
    
    for i, pred in enumerate(sorted_preds, 1):
        print_prediction(pred, rank=i)
    
    # Print predictions without edges
    no_edges = [p for p in valid_preds if p['model_edge'] is None]
    if no_edges:
        print(f"\n{'='*100}")
        print("PREDICTIONS WITHOUT SPREADS")
        print(f"{'='*100}")
        for pred in no_edges:
            print_prediction(pred)
    
    # Print errors
    if errors:
        print(f"\n{'='*100}")
        print("ERRORS")
        print(f"{'='*100}")
        for err in errors:
            print(f"âŒ {err['error']}")
    
    # Summary table
    create_summary_table(predictions)
    
    # Save to CSV
    if valid_preds:
        pred_df = pd.DataFrame(valid_preds)
        output_file = f"/home/claude/predictions_{datetime.now().strftime('%Y%m%d')}.csv"
        pred_df.to_csv(output_file, index=False)
        print(f"ðŸ’¾ Saved predictions to: {output_file}\n")
    
    # Instructions
    print(f"{'='*100}")
    print("HOW TO USE THIS SCRIPT")
    print(f"{'='*100}")
    print("1. Edit the GAMES list above (around line 205) to add/remove games")
    print("2. Format: ('Home Team', 'Away Team', spread)")
    print("3. Spread convention: negative = home favored (e.g., Duke -5.5)")
    print("4. If no spread: ('Team A', 'Team B', None)")
    print("5. Run: python run_predictions.py")
    print("6. Look for highest model edge values for best plays")
    print(f"{'='*100}\n")
    
    return 0

if __name__ == "__main__":
    sys.exit(main())
