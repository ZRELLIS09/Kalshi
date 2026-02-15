#!/usr/bin/env python3
"""
daily_picks.py â€” All-in-One CBB ATS Prediction Tool
=====================================================
Fetches fresh Torvik data + OddsAPI spreads, runs model, outputs picks.

SETUP (one-time):
  pip install requests pandas numpy
  set ODDS_API_KEY=your_key_here      (Windows)
  export ODDS_API_KEY=your_key_here   (Mac/Linux)

DAILY USAGE:
  # Fully automatic â€” fetches Torvik ratings + OddsAPI spreads:
  python daily_picks.py

  # With RotoWire CSV instead of OddsAPI:
  python daily_picks.py --odds rotowire.csv

  # Skip Torvik fetch (use local snapshot):
  python daily_picks.py --no-fetch --snapshot team_power_snapshot_20250209.csv

OUTPUT:
  picks_YYYY-MM-DD.txt   â€” Formatted picks sheet
  predictions_YYYY-MM-DD.csv â€” Full CSV for Excel

Author: Built for Zdub's CBB ATS system
"""

import argparse
import csv
import json
import logging
import os
import re
import sys
from datetime import datetime
from math import exp

import numpy as np
import pandas as pd

try:
    import requests
    HAS_REQUESTS = True
except ImportError:
    HAS_REQUESTS = False

# Import full Odds API â†’ Torvik mapping if available
try:
    from team_name_mapping import ODDS_TO_TORVIK as FULL_ODDS_MAP
except ImportError:
    FULL_ODDS_MAP = None

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("daily_picks")

# ============================================================================
# CONSTANTS
# ============================================================================

HOME_COURT_ADVANTAGE = 1.75       # Margin HCA (optimized via MAE minimization over 4125 games)
PROB_HCA = 3.5                    # Probability HCA (used inside sigmoid, calibration-optimal)
PROB_TEMPERATURE = 5.0            # Logistic temperature (ECE-optimal from sweep: 0.167 → 0.068)
TORVIK_YEAR = 2026
BASE_URL = "https://barttorvik.com"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Referer": f"https://barttorvik.com/trank.php?year={TORVIK_YEAR}",
    "Accept-Language": "en-US,en;q=0.9",
}

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
# TEAM NAME MAPPING (RotoWire â†’ Torvik)
# ============================================================================

ROTOWIRE_TO_TORVIK = {
    "UNC-Greensboro": "UNC Greensboro",
    "Central Florida": "UCF",
    "UNC-Charlotte": "Charlotte",
    "Louisiana State": "LSU",
    "Wisconsin-Milwaukee": "Milwaukee",
    "Cal-Poly San Luis Obispo": "Cal Poly",
    "Cal-Santa Barbara": "UC Santa Barbara",
    "McNeese State": "McNeese St.",
    "Massachusetts-Lowell": "UMass Lowell",
    "IPFW": "Purdue Fort Wayne",
    "SE Missouri State": "Southeast Missouri St.",
    "SE Louisiana": "Southeastern Louisiana",
    "Mississippi Valley St.": "Mississippi Valley St.",
    "Texas Rio Grande Valley": "UT Rio Grande Valley",
    "Arkansas-Little Rock": "Little Rock",
    "Arkansas-Pine Bluff": "Arkansas Pine Bluff",
    "St. Mary's (CAL)": "Saint Mary's",
    "Mount St. Mary's": "Mount St. Mary's",
    "Prairie View": "Prairie View A&M",
    "Cal State Bakersfield": "Cal St. Bakersfield",
    "Cal State-Fullerton": "Cal St. Fullerton",
    "Tenn-Martin": "Tennessee Martin",
    "SIU-Edwardsville": "SIU Edwardsville",
    "St. Thomas (MN)": "St. Thomas",
    "San Jose State": "San Jose St.",
    "San JosÃ© St Spartans": "San Jose St.",
    "San Jose St Spartans": "San Jose St.",
    "San Jos\u00e9 St": "San Jose St.",
    "East Texas A&M Lions": "East Texas A&M",
    "East Texas A&M": "East Texas A&M",
    "Texas A&M Commerce": "East Texas A&M",
    "Grambling": "Grambling St.",
    "Alcorn State": "Alcorn St.",
    "Alabama State": "Alabama St.",
    "Jackson State": "Jackson St.",
    "Delaware State": "Delaware St.",
    "Coppin State": "Coppin St.",
    "South Carolina State": "South Carolina St.",
    "Kennesaw State": "Kennesaw St.",
    "Northwestern State": "Northwestern St.",
    "Southern Indiana": "Southern Indiana",
    "Tarleton State": "Tarleton St.",
    "Idaho State": "Idaho St.",
    "New Mexico State": "New Mexico St.",
    "Northern Colorado": "Northern Colorado",
    "Portland State": "Portland St.",
    "Sacramento State": "Sacramento St.",
    "Weber State": "Weber St.",
    "South Dakota State": "South Dakota St.",
    "Montana State": "Montana St.",
    "Eastern Washington": "Eastern Washington",
    "Tennessee State": "Tennessee St.",
    "Western Illinois": "Western Illinois",
    "Houston Christian": "Houston Christian",
    "North Alabama": "North Alabama",
    "Norfolk State": "Norfolk St.",
    "North Carolina Central": "North Carolina Central",
    "Oral Roberts": "Oral Roberts",
    "Morehead State": "Morehead St.",
    "Murray State": "Murray St.",
    "Western Kentucky": "Western Kentucky",
    "Middle Tennessee": "Middle Tennessee",
    "Youngstown State": "Youngstown St.",
    "Wright State": "Wright St.",
    "Cleveland State": "Cleveland St.",
    "Northern Kentucky": "Northern Kentucky",
    "Green Bay": "Green Bay",
    "Robert Morris": "Robert Morris",
    "Central Michigan": "Central Michigan",
    "Eastern Michigan": "Eastern Michigan",
    "Western Michigan": "Western Michigan",
    "Northern Illinois": "Northern Illinois",
    "Southern Illinois": "Southern Illinois",
    "Indiana State": "Indiana St.",
    "Illinois State": "Illinois St.",
    "Missouri State": "Missouri St.",
    "Long Beach State": "Long Beach St.",
    "San Diego State": "San Diego St.",
    "Fresno State": "Fresno St.",
    "Boise State": "Boise St.",
    "Colorado State": "Colorado St.",
    "Utah State": "Utah St.",
    "Arizona State": "Arizona St.",
    "Michigan State": "Michigan St.",
    "Ohio State": "Ohio St.",
    "Penn State": "Penn St.",
    "Iowa State": "Iowa St.",
    "Kansas State": "Kansas St.",
    "Oklahoma State": "Oklahoma St.",
    "Oregon State": "Oregon St.",
    "Florida State": "Florida St.",
    "Wichita State": "Wichita St.",
    "Appalachian State": "Appalachian St.",
    "Georgia State": "Georgia St.",
    "Kent State": "Kent St.",
    "Ball State": "Ball St.",
    "North Dakota State": "North Dakota St.",
    "Sam Houston State": "Sam Houston St.",
    "Stephen F. Austin": "Stephen F. Austin",
    "Central Arkansas": "Central Arkansas",
    "Bethune-Cookman": "Bethune Cookman",
    "Florida A&M": "Florida A&M",
    "Texas A&M-Commerce": "East Texas A&M",
    "Fairleigh Dickinson": "Fairleigh Dickinson",
    "Gardner-Webb": "Gardner Webb",
    "California Baptist": "Cal Baptist",
    "Jacksonville State": "Jacksonville St.",
    "Abilene Christian": "Abilene Christian",
    "Washington State": "Washington St.",
    "Mississippi State": "Mississippi St.",
    "Charleston Southern": "Charleston Southern",
    "Coastal Carolina": "Coastal Carolina",
    "Georgia Southern": "Georgia Southern",
    "East Tennessee State": "East Tennessee St.",
    "Tennessee Tech": "Tennessee Tech",
    "Austin Peay": "Austin Peay",
    "Eastern Kentucky": "Eastern Kentucky",
    "Texas A&M-CC": "Texas A&M Corpus Chris",
    "Southern Miss": "Southern Miss",
    "Louisiana Tech": "Louisiana Tech",
    "North Texas": "North Texas",
    "Florida International": "FIU",
    "Florida Atlantic": "Florida Atlantic",
    "Florida Gulf Coast": "Florida Gulf Coast",
    "Loyola (Chi)": "Loyola Chicago",
    "Loyola (MD)": "Loyola MD",
    "Loyola Marymount": "Loyola Marymount",
    "Saint Joseph's": "Saint Joseph's",
    "Saint Louis": "Saint Louis",
    "Saint Peter's": "Saint Peter's",
    "St. Bonaventure": "St. Bonaventure",
    "St. Francis (PA)": "Saint Francis",
    "St. John's": "St. John's",
    "Ole Miss": "Mississippi",
    "UConn": "Connecticut",
    "Pitt": "Pittsburgh",
    "Miami (FL)": "Miami FL",
    "Miami (OH)": "Miami OH",
    "NC State": "N.C. State",
    "USC": "USC",
    "UCLA": "UCLA",
    "UNLV": "UNLV",
    "SMU": "SMU",
    "BYU": "BYU",
    "TCU": "TCU",
    "UCF": "UCF",
    "LSU": "LSU",
    "Southern Utah": "Southern Utah",
    "Utah Tech": "Utah Tech",
    "Utah Valley": "Utah Valley",
    "Hawaii": "Hawaii",
    "Hawai'i": "Hawaii",
    "Northern Arizona": "Northern Arizona",
    "UC Irvine": "UC Irvine",
    "UC Davis": "UC Davis",
    "UC Riverside": "UC Riverside",
    "UC San Diego": "UC San Diego",
    "Binghamton": "Binghamton",
    "Stony Brook": "Stony Brook",
    "UMBC": "UMBC",
    "Vermont": "Vermont",
    "Albany": "Albany",
    "New Hampshire": "New Hampshire",
    "Maine": "Maine",
    "Hartford": "Hartford",
    "NJIT": "NJIT",
    "LIU": "LIU",
    "Merrimack": "Merrimack",
    "Omaha": "Nebraska Omaha",
    "Fort Wayne": "Purdue Fort Wayne",
}


def torvik_name(rw_name):
    """Convert RotoWire team name to Torvik canonical name."""
    rw_name = rw_name.strip()
    if rw_name in ROTOWIRE_TO_TORVIK:
        return ROTOWIRE_TO_TORVIK[rw_name]
    # Try "State" â†’ "St." conversion
    if " State" in rw_name and rw_name not in ("Ohio State", "Penn State", "Michigan State",
        "Iowa State", "Kansas State", "Oklahoma State", "Oregon State", "Florida State",
        "Arizona State", "Colorado State", "Boise State", "Utah State", "San Diego State",
        "Fresno State", "Wichita State"):
        alt = rw_name.replace(" State", " St.")
        if alt != rw_name:
            return alt
    return rw_name


# ============================================================================
# STEP 1: FETCH FRESH TORVIK DATA
# ============================================================================

def fetch_torvik_snapshot(output_dir="."):
    """
    Fetch fresh team ratings + four factors from barttorvik.com.
    Returns path to saved snapshot CSV, or None on failure.
    """
    if not HAS_REQUESTS:
        log.error("'requests' package not installed. Run: pip install requests")
        return None

    today = datetime.now().strftime("%Y%m%d")
    today_iso = datetime.now().strftime("%Y-%m-%d")
    filename = f"team_power_snapshot_{today}.csv"
    filepath = os.path.join(output_dir, filename)

    # Check if already fetched today
    if os.path.exists(filepath):
        log.info(f"Today's snapshot already exists: {filepath}")
        return filepath

    log.info("Fetching fresh Torvik ratings...")

    try:
        url = f"{BASE_URL}/trank.php"
        resp = requests.get(url, headers=HEADERS, params={"year": TORVIK_YEAR, "json": 1}, timeout=30)
        resp.raise_for_status()

        if resp.text.strip().startswith("<!"):
            log.error("Got HTML instead of JSON â€” site may be blocking. Try again in a minute.")
            return None

        trank = resp.json()
        log.info(f"  Got {len(trank)} teams from trank.php")
    except Exception as e:
        log.error(f"Failed to fetch Torvik data: {e}")
        return None

    # Parse into snapshot format
    fieldnames = [
        "team", "adj_o", "adj_d", "barthag", "tempo", "rating",
        "off_efg", "def_efg", "off_to", "def_to", "off_or", "def_or",
        "off_ftr", "def_ftr", "wab", "sos", "record", "snapshot_date"
    ]

    rows = []
    for t in trank:
        try:
            ao = float(t[1]) if t[1] is not None else 0
            ad = float(t[2]) if t[2] is not None else 0
            rows.append({
                "team":          str(t[0]).strip(),
                "adj_o":         ao,
                "adj_d":         ad,
                "barthag":       t[3],
                "tempo":         t[15],
                "rating":        round(ao - ad, 2),
                "off_efg":       t[7],
                "def_efg":       t[8],
                "off_to":        t[11],
                "def_to":        t[12],
                "off_or":        t[13],
                "def_or":        t[14],
                "off_ftr":       t[9],
                "def_ftr":       t[10],
                "wab":           t[34] if t[34] is not None else 0,
                "sos":           t[35] if len(t) > 35 and t[35] is not None else 0,
                "record":        t[4] if t[4] is not None else "",
                "snapshot_date": today_iso,
            })
        except (ValueError, IndexError, TypeError):
            continue

    os.makedirs(output_dir, exist_ok=True)
    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    log.info(f"  Saved {len(rows)} teams â†’ {filepath}")
    return filepath


# ============================================================================
# STEP 2: LOAD TEAM RATINGS
# ============================================================================

def load_team_ratings(snapshot_path, four_factors_path=None):
    """
    Load team ratings from EITHER format:
      A) Snapshot CSV (from fetch_barttorvik.py) â€” has adj_o, adj_d, four factors
      B) Team Results CSV (from barttorvik download) â€” has adjoe, adjde, adjt (no four factors)
    
    If format B, optionally merge four factors from a separate snapshot file.
    
    Returns: dict of {team_name: stats_dict}
    """
    log.info(f"Loading team ratings from: {snapshot_path}")
    df = pd.read_csv(snapshot_path)
    
    # Auto-detect format
    cols = [c.lower().strip() for c in df.columns]
    is_team_results = "adjoe" in cols or "oe rank" in " ".join(cols).lower()
    
    if is_team_results:
        log.info("  Detected: team_results format (adjoe/adjde)")
    else:
        log.info("  Detected: snapshot format (adj_o/adj_d)")

    # Normalize column names for team_results format
    col_map = {}
    for c in df.columns:
        cl = c.lower().strip()
        if cl == "adjoe": col_map[c] = "adj_o"
        elif cl == "adjde": col_map[c] = "adj_d"
        elif cl == "adjt": col_map[c] = "tempo"
        elif cl == "wab" or c == "WAB": col_map[c] = "wab"
    df = df.rename(columns=col_map)

    # Compute rating if missing
    if "rating" not in df.columns and "adj_o" in df.columns and "adj_d" in df.columns:
        df["rating"] = df["adj_o"] - df["adj_d"]

    team_lookup = {}
    for _, row in df.iterrows():
        team = str(row["team"]).strip()
        adj_o = float(row["adj_o"]) if pd.notna(row.get("adj_o")) else 100.0
        adj_d = float(row["adj_d"]) if pd.notna(row.get("adj_d")) else 100.0
        team_lookup[team] = {
            "team": team,
            "adj_o": adj_o,
            "adj_d": adj_d,
            "barthag": float(row["barthag"]) if pd.notna(row.get("barthag")) else 0.5,
            "tempo": float(row["tempo"]) if pd.notna(row.get("tempo")) else 68.0,
            "rating": round(adj_o - adj_d, 2),
            "sos": float(row["sos"]) if pd.notna(row.get("sos")) else 0.0,
            "wab": float(row["wab"]) if pd.notna(row.get("wab")) else 0.0,
            "record": str(row.get("record", "")),
            "off_efg": float(row["off_efg"]) if "off_efg" in row.index and pd.notna(row.get("off_efg")) else None,
            "def_efg": float(row["def_efg"]) if "def_efg" in row.index and pd.notna(row.get("def_efg")) else None,
            "off_to": float(row["off_to"]) if "off_to" in row.index and pd.notna(row.get("off_to")) else None,
            "def_to": float(row["def_to"]) if "def_to" in row.index and pd.notna(row.get("def_to")) else None,
            "off_or": float(row["off_or"]) if "off_or" in row.index and pd.notna(row.get("off_or")) else None,
            "def_or": float(row["def_or"]) if "def_or" in row.index and pd.notna(row.get("def_or")) else None,
            "off_ftr": float(row["off_ftr"]) if "off_ftr" in row.index and pd.notna(row.get("off_ftr")) else None,
            "def_ftr": float(row["def_ftr"]) if "def_ftr" in row.index and pd.notna(row.get("def_ftr")) else None,
        }

    log.info(f"  Loaded {len(team_lookup)} teams (core ratings)")
    
    # Merge four factors from separate snapshot if needed
    if four_factors_path and os.path.exists(four_factors_path):
        log.info(f"  Merging four factors from: {four_factors_path}")
        snap = pd.read_csv(four_factors_path)
        ff_cols = ["off_efg", "def_efg", "off_to", "def_to",
                   "off_or", "def_or", "off_ftr", "def_ftr"]
        merged = 0
        for _, row in snap.iterrows():
            team = str(row["team"]).strip()
            if team in team_lookup:
                for col in ff_cols:
                    if col in row.index and pd.notna(row[col]):
                        team_lookup[team][col] = float(row[col])
                merged += 1
        log.info(f"  Merged four factors for {merged} teams")
    elif is_team_results:
        # Count how many have four factors from the primary file
        ff_count = sum(1 for t in team_lookup.values() if t["off_efg"] is not None)
        if ff_count == 0:
            log.warning("  âš  No four factors in team_results file.")
            log.warning("    Predictions will use core ratings only (barthag/adj_o/adj_d).")
            log.warning("    For full model, also provide --four-factors snapshot.csv")

    return team_lookup


# ============================================================================
# STEP 3: PARSE ROTOWIRE ODDS CSV
# ============================================================================

def parse_rotowire(filepath, target_date=None):
    """
    Parse RotoWire odds CSV into list of game dicts.
    
    RotoWire format: paired rows (away team first, home team second).
    Each pair shares a timestamp.
    
    Args:
        filepath: Path to RotoWire CSV
        target_date: Optional "YYYY-MM-DD" to filter specific date
        
    Returns: list of {home_team, away_team, spread, game_time}
    """
    log.info(f"Parsing RotoWire odds: {filepath}")
    df = pd.read_csv(filepath, skiprows=1)
    df.columns = ["Team", "Date", "Moneyline", "Bet1", "Spread", "Bet2",
                   "OverUnder", "Bet3", "WinPct", "ImpliedTeam", "ImpliedOpp"]

    # Filter by date if specified
    if target_date:
        df["date_only"] = pd.to_datetime(df["Date"]).dt.strftime("%Y-%m-%d")
        df = df[df["date_only"] == target_date]
        log.info(f"  Filtered to {target_date}: {len(df)} rows")

    games = []
    for i in range(0, len(df) - 1, 2):
        row1 = df.iloc[i]
        row2 = df.iloc[i + 1]

        away_name = torvik_name(str(row1["Team"]))
        home_name = torvik_name(str(row2["Team"]))
        home_spread = float(row2["Spread"])
        game_time = str(row2["Date"])

        games.append({
            "home_team": home_name,
            "away_team": away_name,
            "spread": home_spread,
            "game_time": game_time,
        })

    log.info(f"  Parsed {len(games)} games")
    return games


# ============================================================================
# STEP 3b: FETCH ODDS FROM ODDSAPI
# ============================================================================

ODDS_API_KEY = os.getenv("ODDS_API_KEY", "")

def fetch_oddsapi_spreads(api_key=None, target_date=None):
    """
    Fetch today's NCAAB spreads from The Odds API.
    Uses the upcoming games endpoint (1 API call).
    Filters to only games on target_date (defaults to today).
    
    Returns: list of {home_team, away_team, spread, game_time}
    """
    if not HAS_REQUESTS:
        log.error("'requests' package not installed. Run: pip install requests")
        return None

    key = api_key or ODDS_API_KEY
    if not key:
        log.error("No OddsAPI key found.")
        log.error("Set it:  set ODDS_API_KEY=your_key   (Windows)")
        log.error("    or:  export ODDS_API_KEY=your_key (Mac/Linux)")
        log.error("Get one: https://the-odds-api.com/")
        return None

    # Determine target date for filtering
    if target_date:
        filter_date = target_date  # "YYYY-MM-DD"
    else:
        filter_date = datetime.now().strftime("%Y-%m-%d")

    url = "https://api.the-odds-api.com/v4/sports/basketball_ncaab/odds"
    params = {
        "apiKey": key,
        "regions": "us",
        "markets": "spreads",
        "oddsFormat": "american",
    }

    log.info("Fetching spreads from OddsAPI...")
    try:
        resp = requests.get(url, params=params, timeout=30)

        # Show remaining quota
        remaining = resp.headers.get("x-requests-remaining", "?")
        used = resp.headers.get("x-requests-used", "?")
        log.info(f"  API quota: {remaining} remaining, {used} used this month")

        if resp.status_code == 401:
            log.error("  Invalid API key")
            return None
        elif resp.status_code == 429:
            log.error("  Rate limited â€” too many requests. Wait and retry.")
            return None
        elif resp.status_code != 200:
            log.error(f"  HTTP {resp.status_code}: {resp.text[:200]}")
            return None

        data = resp.json()
        log.info(f"  Got {len(data)} upcoming games from OddsAPI")

    except Exception as e:
        log.error(f"  Request failed: {e}")
        return None

    # Build the ODDS_TO_TORVIK lookup
    if FULL_ODDS_MAP:
        name_map = FULL_ODDS_MAP
    else:
        name_map = ROTOWIRE_TO_TORVIK  # Fallback

    def _resolve_odds_name(odds_name):
        """Convert OddsAPI team name to Torvik name."""
        odds_name = odds_name.strip()
        # Normalize accented characters
        odds_name = odds_name.replace("Ã©", "e").replace("Ã±", "n").replace("Ã³", "o")
        # Try full ODDS_TO_TORVIK map first (handles "Team Mascot" format)
        if name_map and odds_name in name_map:
            return name_map[odds_name]
        # Try RotoWire map
        if odds_name in ROTOWIRE_TO_TORVIK:
            return ROTOWIRE_TO_TORVIK[odds_name]
        # Try without mascot (OddsAPI sometimes uses "School Mascot" format)
        # Check if the full name minus common suffixes matches
        for suffix in [" Wildcats", " Bulldogs", " Tigers", " Bears", " Eagles",
                       " Hawks", " Panthers", " Knights", " Cougars", " Rebels",
                       " Huskies", " Cardinals", " Aggies", " Rams", " Mustangs",
                       " Dons", " Gaels", " Friars", " Hoyas", " Bruins",
                       " Trojans", " Seminoles", " Terriers", " Lancers",
                       " Broncos", " Mavericks", " Wolves", " Lions",
                       " Spartans", " Wolverines", " Crimson Tide",
                       " Jayhawks", " Sooners", " Longhorns", " Cyclones"]:
            if odds_name.endswith(suffix):
                base = odds_name[:-len(suffix)].strip()
                # Check if base matches any Torvik name
                if base in ROTOWIRE_TO_TORVIK:
                    return ROTOWIRE_TO_TORVIK[base]
                return base
        return odds_name

    games = []
    skipped_other_dates = 0
    for event in data:
        home_team_raw = event.get("home_team", "")
        away_team_raw = event.get("away_team", "")
        commence = event.get("commence_time", "")

        # Filter to target date only
        # commence_time is UTC like "2026-02-09T23:00:00Z"
        # US games on Feb 9 could be UTC Feb 9 afternoon through UTC Feb 10 early AM
        if commence and filter_date:
            try:
                from datetime import timedelta
                game_utc = datetime.strptime(commence[:19], "%Y-%m-%dT%H:%M:%S")
                # Convert UTC to approximate ET (UTC - 5 hours)
                game_et = game_utc - timedelta(hours=5)
                game_date_et = game_et.strftime("%Y-%m-%d")
                if game_date_et != filter_date:
                    skipped_other_dates += 1
                    continue
            except (IndexError, ValueError):
                pass

        home_team = _resolve_odds_name(home_team_raw)
        away_team = _resolve_odds_name(away_team_raw)

        # Find the home team spread from bookmakers
        # Prefer: FanDuel â†’ DraftKings â†’ BetMGM â†’ first available
        preferred_books = ["fanduel", "draftkings", "betmgm", "pointsbetus",
                           "bovada", "williamhill_us", "barstool"]
        home_spread = None

        bookmakers = event.get("bookmakers", [])
        # Sort bookmakers by preference
        book_lookup = {b["key"]: b for b in bookmakers}
        ordered_books = [book_lookup[k] for k in preferred_books if k in book_lookup]
        ordered_books += [b for b in bookmakers if b["key"] not in preferred_books]

        for book in ordered_books:
            for market in book.get("markets", []):
                if market["key"] != "spreads":
                    continue
                for outcome in market.get("outcomes", []):
                    if outcome["name"] == home_team_raw:
                        home_spread = outcome.get("point")
                        break
                if home_spread is not None:
                    break
            if home_spread is not None:
                break

        if home_spread is None:
            log.warning(f"  âš  No spread found for {away_team_raw} @ {home_team_raw}")
            continue

        games.append({
            "home_team": home_team,
            "away_team": away_team,
            "spread": float(home_spread),
            "game_time": commence,
            "home_team_raw": home_team_raw,
            "away_team_raw": away_team_raw,
        })

    log.info(f"  Parsed {len(games)} games for {filter_date}")
    if skipped_other_dates:
        log.info(f"  Skipped {skipped_other_dates} games on other dates")

    # Show what we got
    if games:
        log.info(f"  Today's slate ({filter_date}):")
        for g in games:
            game_time = ""
            if g.get("game_time"):
                try:
                    # Convert UTC to rough ET display
                    utc_str = g["game_time"]
                    hour = int(utc_str[11:13])
                    minute = utc_str[14:16]
                    et_hour = (hour - 5) % 24  # UTC to ET (rough)
                    ampm = "PM" if et_hour >= 12 else "AM"
                    display_hour = et_hour % 12 or 12
                    game_time = f"  {display_hour}:{minute} {ampm} ET"
                except:
                    pass
            log.info(f"    {g['away_team']:25s} @ {g['home_team']:25s}  spread: {g['spread']:+.1f}{game_time}")

    return games


# ============================================================================
# STEP 4: PREDICTION ENGINE
# ============================================================================

def compute_features(home_stats, away_stats):
    """Compute all 13 Torvik feature differentials."""
    features = {}

    def _diff(h, a):
        if h is None or a is None:
            return np.nan
        try:
            return float(h) - float(a)
        except (ValueError, TypeError):
            return np.nan

    # Standard (higher = better): home - away
    features["barthag_diff"] = _diff(home_stats.get("barthag"), away_stats.get("barthag"))
    features["adj_o_diff"] = _diff(home_stats.get("adj_o"), away_stats.get("adj_o"))
    features["off_efg_diff"] = _diff(home_stats.get("off_efg"), away_stats.get("off_efg"))
    features["off_or_diff"] = _diff(home_stats.get("off_or"), away_stats.get("off_or"))
    features["off_ftr_diff"] = _diff(home_stats.get("off_ftr"), away_stats.get("off_ftr"))
    features["def_to_diff"] = _diff(home_stats.get("def_to"), away_stats.get("def_to"))

    # Inverted (lower = better): away - home
    features["adj_d_diff"] = _diff(away_stats.get("adj_d"), home_stats.get("adj_d"))
    features["off_to_diff"] = _diff(away_stats.get("off_to"), home_stats.get("off_to"))
    features["def_efg_diff"] = _diff(away_stats.get("def_efg"), home_stats.get("def_efg"))
    features["def_or_diff"] = _diff(away_stats.get("def_or"), home_stats.get("def_or"))
    features["def_ftr_diff"] = _diff(away_stats.get("def_ftr"), home_stats.get("def_ftr"))

    # Special
    try:
        home_r = float(home_stats["adj_o"]) - float(home_stats["adj_d"])
        away_r = float(away_stats["adj_o"]) - float(away_stats["adj_d"])
        features["rating_diff"] = round(home_r - away_r, 4)
    except (ValueError, TypeError, KeyError):
        features["rating_diff"] = np.nan

    try:
        features["tempo_sum"] = round(
            float(home_stats.get("tempo", 68)) + float(away_stats.get("tempo", 68)), 4
        )
    except (ValueError, TypeError):
        features["tempo_sum"] = np.nan

    return features


def compute_logit(features):
    """Compute raw logit from all 13 weighted features. Positive = home advantage."""
    logit = 0.0
    for feat, weight in FEATURE_WEIGHTS.items():
        val = features.get(feat, np.nan)
        if not np.isnan(val):
            if feat == "barthag_diff":
                logit += val * 30.0 * weight
            elif feat == "rating_diff":
                logit += val * 0.15 * weight
            elif feat == "tempo_sum":
                logit += (val - 136) * 0.01 * weight
            elif feat in ("adj_o_diff", "adj_d_diff"):
                logit += val * 0.1 * weight
            else:
                logit += val * 0.08 * weight
    return logit


def compute_probability(features):
    """Home win probability from calibrated sigmoid.
    
    P = sigmoid((logit + PROB_HCA) / PROB_TEMPERATURE)
    """
    logit = compute_logit(features)
    return 1.0 / (1.0 + exp(-(logit + PROB_HCA) / PROB_TEMPERATURE))


def compute_expected_margin(features):
    """Expected home margin in POINTS. Codex-validated formula.
    
    margin = rating_diff + HOME_COURT_ADVANTAGE (1.75)
    """
    rd = features.get("rating_diff", 0.0)
    if np.isnan(rd):
        rd = 0.0
    return rd + HOME_COURT_ADVANTAGE


def resolve_team(name, team_lookup):
    """Resolve team name to Torvik canonical. Returns (name, found)."""
    name = name.strip()
    if name in team_lookup:
        return name, True
    tv = ROTOWIRE_TO_TORVIK.get(name, name)
    if tv in team_lookup:
        return tv, True
    # Case-insensitive
    for tk in team_lookup:
        if tk.lower() == name.lower():
            return tk, True
    # State â†’ St.
    if " State" in name:
        alt = name.replace(" State", " St.")
        if alt in team_lookup:
            return alt, True
    if " St." in name:
        alt = name.replace(" St.", " State")
        if alt in team_lookup:
            return alt, True
    return name, False


def predict_game(home_name, away_name, spread, team_lookup):
    """Generate full prediction for a single game."""
    home_norm, home_found = resolve_team(home_name, team_lookup)
    away_norm, away_found = resolve_team(away_name, team_lookup)

    if not home_found or not away_found:
        missing = []
        if not home_found: missing.append(home_name)
        if not away_found: missing.append(away_name)
        return {"home_team": home_name, "away_team": away_name, "error": f"Team not found: {missing}"}

    home_stats = team_lookup[home_norm]
    away_stats = team_lookup[away_norm]
    features = compute_features(home_stats, away_stats)

    # FIX 4: explicit four-factor NaN gate
    ff_missing = [f for f in FOUR_FACTOR_FEATURES if np.isnan(features.get(f, np.nan))]
    if len(ff_missing) >= 4:
        return {
            "home_team": home_norm, "away_team": away_norm,
            "error": f"NO-GO: insufficient four-factor coverage ({len(ff_missing)}/8 missing)",
        }
    if ff_missing:
        log.warning(
            "Partial four-factor missingness for %s @ %s: %s",
            away_norm, home_norm, ", ".join(ff_missing),
        )

    prob_home = compute_probability(features)

    # Expected margin derived from SAME logit as probability (all 13 features)
    # This ensures margin and probability always agree on direction
    expected_margin = compute_expected_margin(features)
    if np.isnan(expected_margin):
        expected_margin = 0.0

    model_edge = None
    if spread is not None and not np.isnan(spread):
        model_edge = expected_margin - spread

    # ATS PICK LOGIC:
    # Edge direction determines the ATS pick (who covers), not SU probability.
    # model_edge > 0 → model says home covers → bet HOME spread
    # model_edge < 0 → model says away covers → bet AWAY spread

    su_confidence = prob_home if prob_home > 0.5 else 1.0 - prob_home
    su_pick = home_norm if prob_home > 0.5 else away_norm

    if model_edge is not None and model_edge != 0:
        if model_edge > 0:
            # Model likes HOME to cover
            pick, pick_side = home_norm, "HOME"
            pick_spread = f"{home_norm} {spread:+.1f}"
        else:
            # Model likes AWAY to cover
            pick, pick_side = away_norm, "AWAY"
            away_spread = -spread
            pick_spread = f"{away_norm} {away_spread:+.1f}"
    else:
        # No spread or zero edge — fall back to SU pick
        if prob_home > 0.5:
            pick, pick_side = home_norm, "HOME"
            pick_spread = f"{home_norm} {spread:+.1f}" if spread is not None else home_norm
        else:
            pick, pick_side = away_norm, "AWAY"
            away_spread = -spread if spread is not None else None
            pick_spread = f"{away_norm} {away_spread:+.1f}" if away_spread is not None else away_norm

    # Confidence = SU confidence for the ATS-picked side
    if pick_side == "HOME":
        confidence = prob_home
    else:
        confidence = 1.0 - prob_home

    conf_pct = confidence * 100
    if conf_pct >= 90:    conf_tier = f"🔒 {conf_pct:.1f}%"
    elif conf_pct >= 80:  conf_tier = f"📊 {conf_pct:.1f}%"
    elif conf_pct >= 65:  conf_tier = f"📉 {conf_pct:.1f}%"
    else:                 conf_tier = f"⚪ {conf_pct:.1f}%"

    abs_edge = abs(model_edge) if model_edge is not None else 0
    if abs_edge >= 5.0:   edge_tier = "Strong"
    elif abs_edge >= 3.0: edge_tier = "Lean"
    else:                 edge_tier = "Watch"


    return {
        "home_team": home_norm, "away_team": away_norm,
        "home_record": home_stats.get("record", ""),
        "away_record": away_stats.get("record", ""),
        "home_rating": home_stats.get("rating", 0),
        "away_rating": away_stats.get("rating", 0),
        "spread": spread,
        "prob_home_covers": round(prob_home, 4),
        "expected_margin": round(expected_margin, 1),
        "model_edge": round(model_edge, 1) if model_edge is not None else None,
        "pick": pick, "pick_side": pick_side, "pick_spread": pick_spread,
        "confidence": round(confidence, 4),
        "confidence_pct": round(confidence * 100, 1),
        "conf_tier": conf_tier, "edge_tier": edge_tier,
        "barthag_diff": round(features.get("barthag_diff", 0), 4),
        "rating_diff": round(features.get("rating_diff", 0), 4),
        "adj_o_diff": round(features.get("adj_o_diff", 0), 4),
        "adj_d_diff": round(features.get("adj_d_diff", 0), 4),
        "off_efg_diff": features.get("off_efg_diff"),
        "def_efg_diff": features.get("def_efg_diff"),
        "def_to_diff": features.get("def_to_diff"),
        "error": None,
    }


# ============================================================================
# STEP 5: OUTPUT
# ============================================================================

def format_pick_line(row):
    """Format single pick with confidence %. Edge retains true sign."""
    if row["pick_side"] == "AWAY":
        pick_team = row["away_team"]
        spread_display = -row["spread"]
    else:
        pick_team = row["home_team"]
        spread_display = row["spread"]

    matchup = f"{row['away_team']} @ {row['home_team']}"
    conf = row["confidence"] * 100
    edge = row["model_edge"]

    if conf >= 90:   icon = "ðŸ”’"
    elif conf >= 80: icon = "ðŸ“Š"
    elif conf >= 65: icon = "ðŸ“‰"
    else:            icon = "âšª"

    return f"  {icon} {conf:5.1f}%  {pick_team} {spread_display:+.1f}   edge: {edge:+.1f}   {matchup}"


def generate_output(predictions, snapshot_name, date_str, stale_banner=None):
    """Generate formatted picks text and save CSV."""
    valid = [p for p in predictions if not p.get("error")]
    errors = [p for p in predictions if p.get("error")]

    df = pd.DataFrame(valid)
    df["abs_edge"] = df["model_edge"].abs()
    df = df.sort_values("abs_edge", ascending=False)

    strong = df[df["abs_edge"] >= 5.0]
    lean = df[(df["abs_edge"] >= 3.0) & (df["abs_edge"] < 5.0)]
    watch = df[df["abs_edge"] < 3.0]


    lines = []
    lines.append("=" * 72)
    lines.append(f"  CBB ATS PICKS â€” {date_str}")
    lines.append(f"  Model: Torvik Power Ratings + Four Factors")
    lines.append(f"  Snapshot: {snapshot_name} | HCA: {HOME_COURT_ADVANTAGE} pts")
    lines.append(f"  Generated: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    lines.append("=" * 72)
    if stale_banner:
        lines.append("")
        lines.append(stale_banner)
    lines.append("")
    lines.append("  UNIT GUIDE:")
    lines.append("  ðŸ”’ 90%+  = 3u    ðŸ“Š 80-89% = 2u    ðŸ“‰ 65-79% = 1u    âšª <65% = Skip")
    lines.append("")

    if len(strong) > 0:
        lines.append(f"ðŸ”¥ STRONG PLAYS (Edge â‰¥ 5.0 pts) â€” {len(strong)} games")
        lines.append("â”€" * 72)
        for _, row in strong.iterrows():
            lines.append(format_pick_line(row))

    if len(lean) > 0:
        lines.append(f"\nðŸ“Š LEAN PLAYS (Edge 3.0-4.9 pts) â€” {len(lean)} games")
        lines.append("â”€" * 72)
        for _, row in lean.iterrows():
            lines.append(format_pick_line(row))

    if len(watch) > 0:
        lines.append(f"\nðŸ‘€ WATCH LIST (Edge < 3.0 pts) â€” {len(watch)} games")
        lines.append("â”€" * 72)
        for _, row in watch.iterrows():
            lines.append(format_pick_line(row))

    if errors:
        lines.append(f"\nâš ï¸  ERRORS ({len(errors)} games)")
        lines.append("â”€" * 72)
        for e in errors:
            lines.append(f"  {e['home_team']} vs {e['away_team']}: {e['error']}")

    lines.append(f"\n{'='*72}")
    lines.append(f"  TOTAL: {len(valid)} predicted | Strong: {len(strong)} | Lean: {len(lean)} | Watch: {len(watch)}")
    if errors:
        lines.append(f"  âš ï¸  {len(errors)} games failed (team name mismatch)")
    lines.append("=" * 72)

    return "\n".join(lines), df


# ============================================================================
# MAIN
# ============================================================================

def main():
    parser = argparse.ArgumentParser(
        description="Daily CBB ATS Picks â€” All-in-One",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Fully automatic (OddsAPI spreads + auto-fetch Torvik):
  python daily_picks.py

  # With RotoWire CSV instead of OddsAPI:
  python daily_picks.py --odds rotowire.csv

  # Use local snapshot (skip Torvik fetch):
  python daily_picks.py --no-fetch --snapshot team_power_snapshot_20250209.csv

  # Use team_results + separate four factors file:
  python daily_picks.py --no-fetch --snapshot 2026_team_results_1.csv --four-factors team_power_snapshot_20250209.csv

  # Specify OddsAPI key inline (instead of env variable):
  python daily_picks.py --api-key YOUR_KEY_HERE
        """,
    )
    parser.add_argument("--odds", default=None, help="RotoWire odds CSV (if omitted, uses OddsAPI)")
    parser.add_argument("--api-key", default=None, dest="api_key", help="OddsAPI key (or set ODDS_API_KEY env var)")
    parser.add_argument("--snapshot", default=None, help="Team ratings file (snapshot or team_results CSV)")
    parser.add_argument("--four-factors", default=None, dest="four_factors",
                        help="Separate snapshot CSV with four factors (if main file lacks them)")
    parser.add_argument("--no-fetch", action="store_true", help="Skip Torvik fetch (requires --snapshot)")
    parser.add_argument("--date", default=None, help="Filter RotoWire to this date (YYYY-MM-DD)")
    parser.add_argument("--output-dir", default=".", help="Output directory for picks files")
    args = parser.parse_args()

    today = datetime.now().strftime("%Y-%m-%d")
    date_label = args.date or today

    print(f"\n{'='*72}")
    print(f"  ðŸ€ CBB ATS DAILY PICKS â€” {date_label}")
    print(f"{'='*72}\n")

    # Step 1: Get Torvik data
    snapshot_path = args.snapshot
    if not args.no_fetch and not snapshot_path:
        log.info("STEP 1: Fetching fresh Torvik ratings...")
        snapshot_path = fetch_torvik_snapshot(args.output_dir)
        if not snapshot_path:
            log.warning("Auto-fetch failed. Looking for most recent local snapshot...")
            # Try to find any existing snapshot in output dir
            import glob
            snaps = sorted(glob.glob(os.path.join(args.output_dir, "team_power_snapshot_*.csv")))
            if snaps:
                snapshot_path = snaps[-1]
                log.info(f"  Using existing snapshot: {snapshot_path}")
            else:
                log.error("No snapshot available. Use --snapshot to provide a local file.")
                log.error("  Or run:  python fetch_barttorvik.py")
                return 1
    elif not snapshot_path:
        log.error("--no-fetch requires --snapshot. Provide a snapshot CSV path.")
        return 1

    if not os.path.exists(snapshot_path):
        log.error(f"Snapshot file not found: {snapshot_path}")
        return 1

    snapshot_name = os.path.basename(snapshot_path)

    # Step 2: Load ratings
    log.info("STEP 2: Loading team ratings...")
    team_lookup = load_team_ratings(snapshot_path, four_factors_path=args.four_factors)

    # FIX 6: mapping drift cross-check
    mapped_values = set(ROTOWIRE_TO_TORVIK.values())
    missing_canonical = sorted(t for t in mapped_values if t not in team_lookup)
    if missing_canonical:
        log.warning("Identity drift: %d mapped Torvik names missing from team_lookup", len(missing_canonical))
        for t in missing_canonical[:10]:
            log.warning("  missing: %s", t)
    else:
        log.info("Identity drift check: 0 mismatches")

    # FIX 5: pre-prediction staleness gate (moved from generate_output)
    stale_banner = None
    days_old = None
    snap_match = re.search(r"(\d{8}|\d{4}-\d{2}-\d{2})", snapshot_name)
    if snap_match:
        snap_str = snap_match.group(1).replace("-", "")
        try:
            snap_date = datetime.strptime(snap_str, "%Y%m%d")
            days_old = (datetime.now() - snap_date).days
        except ValueError:
            days_old = None

    if days_old is not None and days_old > 7:
        log.error("HARD BLOCK: snapshot is %d days old (>7). Refusing to run.", days_old)
        log.error("Stale data produces phantom edges of 10-20 points.")
        log.error("Refresh: python fetch_barttorvik.py")
        return 1
    if days_old is not None and days_old > 2:
        warn_msg = f"  STALE DATA WARNING: Snapshot is {days_old} days old!"
        log.warning(warn_msg)
        if days_old >= 3:
            stale_banner = (
                f"  *** STALE SNAPSHOT: {days_old} days old ***\n"
                f"  Edges >10 pts are likely phantom. Proceed with caution."
            )

    # Step 3: Get odds (OddsAPI or RotoWire)
    if args.odds:
        log.info("STEP 3: Parsing RotoWire odds...")
        games = parse_rotowire(args.odds, target_date=args.date)
    else:
        log.info("STEP 3: Fetching spreads from OddsAPI...")
        games = fetch_oddsapi_spreads(api_key=args.api_key, target_date=date_label)

    if not games:
        log.error("No games found. Check your odds source.")
        if not args.odds:
            log.error("  Make sure ODDS_API_KEY is set, or use --odds rotowire.csv")
        return 1

    # Step 4: Run predictions
    log.info(f"STEP 4: Running {len(games)} predictions...")
    predictions = []
    for game in games:
        pred = predict_game(game["home_team"], game["away_team"], game["spread"], team_lookup)
        predictions.append(pred)

    # Step 5: Output
    picks_text, results_df = generate_output(predictions, snapshot_name, date_label, stale_banner=stale_banner)
    print(picks_text)

    # Save files
    os.makedirs(args.output_dir, exist_ok=True)
    picks_path = os.path.join(args.output_dir, f"picks_{date_label}.txt")
    csv_path = os.path.join(args.output_dir, f"predictions_{date_label}.csv")

    with open(picks_path, "w", encoding="utf-8-sig") as f:
        f.write(picks_text)

    if len(results_df) > 0:
        # Drop helper column before saving
        save_df = results_df.drop(columns=["abs_edge"], errors="ignore")
        save_df.to_csv(csv_path, index=False)

    valid = [p for p in predictions if not p.get("error")]
    print(f"\n  ðŸ“ Picks saved: {picks_path}")
    print(f"  ðŸ“ CSV saved:   {csv_path}")
    print(f"  âœ… {len(valid)}/{len(predictions)} games predicted\n")

    return 0


if __name__ == "__main__":
    sys.exit(main())
