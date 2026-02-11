"""
constants_TORVIK.py
===================
Feature definitions, column mappings, and configuration for the
College Basketball ATS Phase 1 model using Barttorvik data.
"""

# ============================================================================
# SEASON CONFIGURATIONS (dual-season support)
# ============================================================================

SEASON_CONFIGS = {
    2025: {
        "TORVIK_YEAR": 2025,
        "SEASON_START": "2024-11-04",
        "SEASON_END": "2025-04-07",
        "TRAIN_CUTOFF": "2024-12-31",
        "VAL_START": "2025-01-01",
        "VAL_END": "2025-04-07",
        "label": "2024-25 Season (backtest)",
    },
    2026: {
        "TORVIK_YEAR": 2026,
        "SEASON_START": "2025-11-04",
        "SEASON_END": "2026-04-07",
        "TRAIN_CUTOFF": "2025-12-31",
        "VAL_START": "2026-01-01",
        "VAL_END": "2026-04-07",
        "label": "2025-26 Season (current)",
    },
}

# ============================================================================
# SNAPSHOT CONFIGURATION (defaults to 2026 - overridden by apply_season_config)
# ============================================================================

TORVIK_YEAR = 2026  # Barttorvik's year = spring graduation year
SEASON_START = "2025-11-04"
SEASON_END = "2026-04-07"
SNAPSHOT_FREQUENCY_DAYS = 7  # Weekly snapshots


def apply_season_config(year):
    """
    Override module-level constants for a specific season.
    Call this BEFORE importing from constants_TORVIK in other modules.
    
    Usage:
        import constants_TORVIK
        constants_TORVIK.apply_season_config(2025)
    """
    import constants_TORVIK as _self
    if year not in SEASON_CONFIGS:
        raise ValueError(f"Unknown season {year}. Available: {list(SEASON_CONFIGS.keys())}")
    cfg = SEASON_CONFIGS[year]
    _self.TORVIK_YEAR = cfg["TORVIK_YEAR"]
    _self.SEASON_START = cfg["SEASON_START"]
    _self.SEASON_END = cfg["SEASON_END"]
    _self.TRAIN_CUTOFF = cfg["TRAIN_CUTOFF"]
    _self.VAL_START = cfg["VAL_START"]
    _self.VAL_END = cfg["VAL_END"]
    # Update derived URLs
    _self.CSV_URL = f"http://barttorvik.com/{cfg['TORVIK_YEAR']}_team_results.csv"
    print(f"  â†’ Season config applied: {cfg['label']}")
    print(f"    Data:  {cfg['SEASON_START']} â†’ {cfg['SEASON_END']}")
    print(f"    Train: {cfg['SEASON_START']} â†’ {cfg['TRAIN_CUTOFF']}")
    print(f"    Val:   {cfg['VAL_START']} â†’ {cfg['VAL_END']}")

# Required columns in each snapshot CSV
SNAPSHOT_COLUMNS = [
    "team", "adj_o", "adj_d", "barthag", "tempo", "rating",
    "off_efg", "def_efg", "off_to", "def_to", "off_or", "def_or",
    "off_ftr", "def_ftr", "wab", "sos", "snapshot_date"
]

# ============================================================================
# trank.php JSON COLUMN MAPPING (array index â†’ field name)
# Verified against HTML table for multiple teams
# NOTE: The JSON from trank.php returns nested arrays per team.
# This mapping is for the standard trank.php?json=1 endpoint.
# ============================================================================

TRANK_JSON_MAP = {
    0: "rank",
    1: "team",
    2: "conf",
    3: "record",
    # Efficiency
    4: "adj_o",        # Adjusted Offensive Efficiency
    5: "adj_o_rank",
    6: "adj_d",        # Adjusted Defensive Efficiency
    7: "adj_d_rank",
    8: "barthag",      # Torvik power rating (0-1 scale)
    9: "barthag_rank",
    # Projections
    10: "proj_w",
    11: "proj_l",
    12: "proj_conf_w",
    13: "proj_conf_l",
    14: "conf_rec",
    # SOS metrics
    15: "sos",
    16: "nc_sos",
    17: "conf_sos",
    18: "proj_sos",
    19: "proj_nc_sos",
    20: "proj_conf_sos",
    21: "elite_sos",
    22: "elite_nc_sos",
    # Opponent metrics
    23: "opp_oe",
    24: "opp_de",
    25: "opp_proj_oe",
    26: "opp_proj_de",
    # Conference adjusted
    27: "conf_adj_oe",
    28: "conf_adj_de",
    # Quality metrics
    29: "qual_o",
    30: "qual_d",
    31: "qual_barthag",
    32: "qual_games",
    # Misc
    33: "fun",
    34: "conf_pf",
    35: "conf_pa",
    36: "conf_poss",
    37: "conf_oe",
    38: "conf_de",
    39: "conf_sos_remain",
    40: "conf_win_pct",
    # WAB
    41: "wab",
    42: "wab_rank",
    43: "fun_rank",
    # Tempo
    44: "adj_t",        # Adjusted Tempo (index 44 in full CSV)
}

# ============================================================================
# CSV COLUMN MAPPING (from 2026_team_results.csv)
# The CSV has named columns, but this maps to our standard names
# ============================================================================

CSV_COLUMN_MAP = {
    "rank": "rank",
    "team": "team",
    "conf": "conf",
    "record": "record",
    "adjoe": "adj_o",
    "adjde": "adj_d",
    "barthag": "barthag",
    "adjt": "adj_t",
    "sos": "sos",
    "WAB": "wab",
}

# ============================================================================
# teamstats.php JSON COLUMN MAPPING (four factors endpoint)
# This is the endpoint that returns off/def four factors
# URL: barttorvik.com/teamstats.php?year=2026&json=1
# ============================================================================

TEAMSTATS_JSON_MAP = {
    0: "team",
    1: "conf",
    2: "adj_o",
    3: "adj_d",
    4: "barthag",
    5: "adj_t",
    6: "off_efg",    # Offensive eFG%
    7: "off_to",     # Offensive TO%
    8: "off_or",     # Offensive Rebound %
    9: "off_ftr",    # Offensive Free Throw Rate
    10: "def_efg",   # Defensive eFG% allowed
    11: "def_to",    # Defensive TO% forced
    12: "def_or",    # Defensive ORB% allowed
    13: "def_ftr",   # Defensive FTR allowed
    14: "two_pt_pct",
    15: "three_pt_pct",
    16: "three_pt_rate",
    17: "avg_hgt",
    18: "eff_hgt",
    19: "exp",
    20: "bench_mins",
}

# ============================================================================
# PHASE 1 FEATURES (20 total)
# ============================================================================

# Historical features computed from game database (8)
HISTORICAL_FEATURES = [
    "margin_diff",          # Home avg margin - Away avg margin
    "adj_margin_diff",      # SOS-adjusted margins
    "home_std_margin",      # Home team margin volatility
    "away_std_margin",      # Away team margin volatility
    "hca_diff",             # Home court advantage differential
    "rest_advantage_adj",   # Days rest advantage (adjusted)
    "pace_diff",            # Tempo differential
    "close_game_diff",      # Close game performance differential
]

# Torvik Core Metrics (5) - from snapshots
TORVIK_CORE_FEATURES = [
    "barthag_diff",     # Power rating differential
    "adj_o_diff",       # Adjusted offensive efficiency diff
    "adj_d_diff",       # Adjusted defensive efficiency diff (INVERTED)
    "rating_diff",      # Expected scoring margin diff
    "tempo_sum",        # Combined adjusted tempo
]

# Offensive Four Factors (4) - from snapshots
OFFENSIVE_FOUR_FACTORS = [
    "off_efg_diff",     # Offensive eFG% differential
    "off_to_diff",      # Offensive TO% differential (INVERTED - lower is better)
    "off_or_diff",      # Offensive rebound % differential
    "off_ftr_diff",     # Offensive free throw rate differential
]

# Defensive Four Factors (4) - from snapshots (ALL INVERTED EXCEPT def_to)
DEFENSIVE_FOUR_FACTORS = [
    "def_efg_diff",     # Defensive eFG% allowed diff (INVERTED - lower is better)
    "def_to_diff",      # Defensive TO% forced diff (NOT inverted - higher is better)
    "def_or_diff",      # Defensive ORB% allowed diff (INVERTED - lower is better)
    "def_ftr_diff",     # Defensive FTR allowed diff (INVERTED - lower is better)
]

# All Phase 1 features
ALL_PHASE1_FEATURES = (
    HISTORICAL_FEATURES +
    TORVIK_CORE_FEATURES +
    OFFENSIVE_FOUR_FACTORS +
    DEFENSIVE_FOUR_FACTORS
)

# User spec header says 20 but lists 21 (8+5+4+4). Using actual count.
assert len(ALL_PHASE1_FEATURES) == 21, f"Expected 21 features, got {len(ALL_PHASE1_FEATURES)}"

# Torvik-sourced features only (for feature_builder_TORVIK.py)
TORVIK_FEATURES = TORVIK_CORE_FEATURES + OFFENSIVE_FOUR_FACTORS + DEFENSIVE_FOUR_FACTORS
assert len(TORVIK_FEATURES) == 13, f"Expected 13 Torvik features, got {len(TORVIK_FEATURES)}"

# ============================================================================
# INVERSION RULES
# For "lower is better" stats, we invert the differential so that
# POSITIVE values always favor the HOME team.
#
# Convention: home_feature - away_feature (or inverted equivalent)
# ============================================================================

# Features where LOWER raw value = BETTER performance
# For these, we compute: away_stat - home_stat (so positive = home team is better)
INVERTED_FEATURES = {
    "adj_d_diff":   "adj_d",      # Lower adj_d = better defense
    "off_to_diff":  "off_to",     # Lower TO% = fewer turnovers = better
    "def_efg_diff": "def_efg",    # Lower def eFG% = better defense
    "def_or_diff":  "def_or",     # Lower def ORB% allowed = better defense
    "def_ftr_diff": "def_ftr",    # Lower def FTR allowed = better defense
}

# Features where HIGHER raw value = BETTER performance (standard diff)
# For these, we compute: home_stat - away_stat
STANDARD_FEATURES = {
    "barthag_diff": "barthag",
    "adj_o_diff":   "adj_o",
    "off_efg_diff": "off_efg",
    "off_or_diff":  "off_or",
    "off_ftr_diff": "off_ftr",
    "def_to_diff":  "def_to",    # Higher TO% forced = better defense
}

# Special features
SPECIAL_FEATURES = {
    "rating_diff": ("adj_o", "adj_d"),  # rating = adj_o - adj_d
    "tempo_sum": ("adj_t",),            # tempo_sum = home_tempo + away_tempo
}

# ============================================================================
# TRAINING CONFIGURATION
# ============================================================================

TRAIN_CUTOFF = "2025-12-31"  # Train on Nov-Dec 2025
VAL_START = "2026-01-01"     # Validate on Jan+ 2026
VAL_END = "2026-04-07"

TARGET_COLUMN = "home_covers"  # 1 = home team covers spread, 0 = doesn't

# LightGBM hyperparameters (conservative for small dataset)
LGBM_PARAMS = {
    "objective": "binary",
    "metric": ["auc", "binary_logloss"],
    "boosting_type": "gbdt",
    "num_leaves": 15,          # Conservative - prevent overfitting
    "max_depth": 4,            # Shallow trees
    "learning_rate": 0.05,
    "n_estimators": 300,
    "min_child_samples": 30,   # High minimum for small dataset
    "subsample": 0.8,
    "colsample_bytree": 0.7,
    "reg_alpha": 1.0,          # L1 regularization
    "reg_lambda": 2.0,         # L2 regularization
    "verbose": -1,
    "random_state": 42,
    "importance_type": "gain",
}

# ============================================================================
# FILE PATHS (relative to working directory)
# ============================================================================

CLEANED_DATA_DIR = "cleaned_data"
MODELS_DIR = "models"
SNAPSHOT_PREFIX = "team_power_snapshot_TORVIK_"
TRAINING_FILE = "training_games_TORVIK.csv"

# Data retrieval
POLITE_DELAY_SECONDS = 2.0  # Delay between barttorvik requests
MAX_RETRIES = 3
REQUEST_TIMEOUT = 30

# URLs
TORVIK_BASE_URL = "https://barttorvik.com"
TRANK_JSON_URL = f"{TORVIK_BASE_URL}/trank.php"
TEAMSTATS_URL = f"{TORVIK_BASE_URL}/teamstats.php"
CSV_URL = f"http://barttorvik.com/{TORVIK_YEAR}_team_results.csv"
