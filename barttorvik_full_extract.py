"""
Barttorvik Full Data Extractor

Pulls ALL available data from barttorvik.com using the direct
CSV/JSON endpoints that Bart Torvik has publicly documented.

NO LOGIN REQUIRED. NO SUBSCRIPTION NEEDED.

These are the official bulk data endpoints Bart has shared publicly
on his blog (adamcwisports.blogspot.com/p/data.html). They bypass
the browser verification page entirely.

REQUIREMENTS:
    pip install requests pandas

USAGE:
    python barttorvik_full_extract.py

    # Pull specific season:
    python barttorvik_full_extract.py --season 2026

    # Pull multiple seasons:
    python barttorvik_full_extract.py --seasons 2022,2023,2024,2025,2026

    # Pull time machine snapshots:
    python barttorvik_full_extract.py --timemachine

All CSVs land in ./torvik_data/
"""

import os
import sys
import io
import json
import gzip
import time
import argparse
import requests
import pandas as pd
from datetime import datetime, timedelta

# ═══════════════════════════════════════════════════════════════════════════════
# CONFIGURATION
# ═══════════════════════════════════════════════════════════════════════════════

BASE_URL = "https://barttorvik.com"
OUTPUT_DIR = "torvik_data"
REQUEST_DELAY = 1.5  # Be respectful

# Headers to mimic a normal browser request
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/json,text/csv,*/*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://barttorvik.com/",
}

# ═══════════════════════════════════════════════════════════════════════════════
# HELPERS
# ═══════════════════════════════════════════════════════════════════════════════

def fetch_url(url, name, as_json=False, as_gzip=False):
    """Fetch a URL with error handling and delay."""
    print(f"  Fetching: {name}…", end=" ", flush=True)
    try:
        resp = requests.get(url, headers=HEADERS, timeout=30)
        resp.raise_for_status()
        time.sleep(REQUEST_DELAY)

        if as_gzip:
            data = gzip.decompress(resp.content)
            print(f"✅ ({len(data):,} bytes decompressed)")
            return json.loads(data)
        elif as_json:
            data = resp.json()
            print(f"✅ ({len(data)} records)" if isinstance(data, list) else "✅")
            return data
        else:
            text = resp.text
            if "Verifying your browser" in text or len(text) < 100:
                print("⚠️  Got browser verification page (blocked)")
                return None
            print(f"✅ ({len(text):,} chars)")
            return text
    except Exception as e:
        print(f"❌ {e}")
        time.sleep(REQUEST_DELAY)
        return None


def csv_text_to_df(text, name=""):
    """Convert CSV text to dataframe."""
    if text is None:
        return None
    try:
        df = pd.read_csv(io.StringIO(text))
        if len(df) > 0:
            return df
    except Exception as e:
        print(f"    ⚠️  Parse error for {name}: {e}")
    return None


def json_to_df(data, name=""):
    """Convert JSON data to dataframe."""
    if data is None:
        return None
    try:
        if isinstance(data, list):
            df = pd.DataFrame(data)
        elif isinstance(data, dict):
            df = pd.DataFrame(data)
        else:
            return None
        if len(df) > 0:
            return df
    except Exception as e:
        print(f"    ⚠️  JSON parse error for {name}: {e}")
    return None


def save_df(df, filename, output_dir):
    """Save dataframe to CSV."""
    if df is not None and len(df) > 0:
        path = os.path.join(output_dir, filename)
        df.to_csv(path, index=False)
        print(f"    💾 Saved: {filename} ({len(df):,} rows × {len(df.columns)} cols)")
        return True
    return False


# ═══════════════════════════════════════════════════════════════════════════════
# ENDPOINT 1: Team Results (the main bulk data file)
# Bart says: "http://barttorvik.com/YYYY_team_results.csv"
# Contains: team, conf, record, adj_o, adj_d, barthag, adj_t,
#           wab, sos, four factors, and more
# ═══════════════════════════════════════════════════════════════════════════════

def pull_team_results(season, output_dir):
    """
    Pull the main team results CSV for a season.
    This is the primary bulk data file — updated constantly during the season.

    Known columns (based on Bart's documentation and community reverse-engineering):
    team, conf, g, wins, losses, adj_o, adj_d, barthag, adj_t, wab,
    off_efg, off_to, off_or, off_ftr, def_efg, def_to, def_or, def_ftr,
    two_pt_pct, three_pt_pct, ft_pct, blk_pct, stl_pct, ...
    """
    # Try CSV first
    url = f"{BASE_URL}/{season}_team_results.csv"
    text = fetch_url(url, f"Team Results CSV ({season})")
    df = csv_text_to_df(text, f"team_results_{season}")
    if df is not None:
        save_df(df, f"team_results_{season}.csv", output_dir)
        return df

    # Try JSON fallback
    url = f"{BASE_URL}/{season}_team_results.json"
    data = fetch_url(url, f"Team Results JSON ({season})", as_json=True)
    df = json_to_df(data, f"team_results_{season}")
    if df is not None:
        save_df(df, f"team_results_{season}.csv", output_dir)
        return df

    return None


# ═══════════════════════════════════════════════════════════════════════════════
# ENDPOINT 2: Team Slice JSON (filterable team stats)
# Bart says: "teamslicejson.php?year=YYYY&json=1&type=R"
# type: A=All, R=Regular Season, P=Postseason, T=Tournament
# ═══════════════════════════════════════════════════════════════════════════════

def pull_team_slice(season, output_dir, game_type="A"):
    """Pull team stats with game type filter."""
    type_labels = {"A": "All", "R": "RegSeason", "P": "Postseason", "T": "Tournament"}
    label = type_labels.get(game_type, game_type)

    url = f"{BASE_URL}/teamslicejson.php?year={season}&json=1&type={game_type}"
    data = fetch_url(url, f"Team Slice ({label}, {season})", as_json=True)
    df = json_to_df(data, f"team_slice_{label}_{season}")
    if df is not None:
        save_df(df, f"team_slice_{label.lower()}_{season}.csv", output_dir)
        return df

    # Try CSV version
    url = f"{BASE_URL}/teamslicejson.php?year={season}&csv=1&type={game_type}"
    text = fetch_url(url, f"Team Slice CSV ({label}, {season})")
    df = csv_text_to_df(text, f"team_slice_{label}_{season}")
    if df is not None:
        save_df(df, f"team_slice_{label.lower()}_{season}.csv", output_dir)
        return df

    return None


# ═══════════════════════════════════════════════════════════════════════════════
# ENDPOINT 3: T-Rank JSON (main ratings page data)
# trank.php?year=YYYY&json=1
# ═══════════════════════════════════════════════════════════════════════════════

def pull_trank(season, output_dir):
    """Pull main T-Rank ratings."""
    url = f"{BASE_URL}/trank.php?year={season}&json=1"
    data = fetch_url(url, f"T-Rank Ratings ({season})", as_json=True)
    df = json_to_df(data, f"trank_{season}")
    if df is not None:
        save_df(df, f"trank_ratings_{season}.csv", output_dir)
        return df
    return None


# ═══════════════════════════════════════════════════════════════════════════════
# ENDPOINT 4: Game Stats (advanced game-by-game stats)
# getgamestats.php?year=YYYY
# ═══════════════════════════════════════════════════════════════════════════════

def pull_game_stats(season, output_dir):
    """Pull advanced game-by-game stats for the season."""
    url = f"{BASE_URL}/getgamestats.php?year={season}"
    data = fetch_url(url, f"Game Stats ({season})", as_json=True)
    df = json_to_df(data, f"game_stats_{season}")
    if df is not None:
        save_df(df, f"game_stats_{season}.csv", output_dir)
        return df
    return None


# ═══════════════════════════════════════════════════════════════════════════════
# ENDPOINT 5: Player Stats
# playerstat.php in various formats
# ═══════════════════════════════════════════════════════════════════════════════

def pull_player_stats(season, output_dir):
    """Pull player-level statistics."""
    # Main player stats endpoint
    url = f"{BASE_URL}/playerstat.php?year={season}&minmin=0&csv=1"
    text = fetch_url(url, f"Player Stats CSV ({season})")
    df = csv_text_to_df(text, f"player_stats_{season}")
    if df is not None:
        save_df(df, f"player_stats_{season}.csv", output_dir)
        return df

    # Try JSON version
    url = f"{BASE_URL}/playerstat.php?year={season}&minmin=0&json=1"
    data = fetch_url(url, f"Player Stats JSON ({season})", as_json=True)
    df = json_to_df(data, f"player_stats_{season}")
    if df is not None:
        save_df(df, f"player_stats_{season}.csv", output_dir)
        return df

    return None


# ═══════════════════════════════════════════════════════════════════════════════
# ENDPOINT 6: Transfer Portal Stats
# playerstat.php?link=y&year=trans
# ═══════════════════════════════════════════════════════════════════════════════

def pull_transfer_stats(output_dir):
    """Pull committed transfer player stats."""
    url = f"{BASE_URL}/playerstat.php?link=y&year=trans&minmin=0&csv=1"
    text = fetch_url(url, "Transfer Portal Stats")
    df = csv_text_to_df(text, "transfers")
    if df is not None:
        save_df(df, "transfer_portal_stats.csv", output_dir)
        return df
    return None


# ═══════════════════════════════════════════════════════════════════════════════
# ENDPOINT 7: SOS (Strength of Schedule)
# sos.php
# ═══════════════════════════════════════════════════════════════════════════════

def pull_sos(season, output_dir):
    """Pull strength of schedule data."""
    url = f"{BASE_URL}/sos.php?year={season}&csv=1"
    text = fetch_url(url, f"Strength of Schedule ({season})")
    df = csv_text_to_df(text, f"sos_{season}")
    if df is not None:
        save_df(df, f"sos_{season}.csv", output_dir)
        return df
    return None


# ═══════════════════════════════════════════════════════════════════════════════
# ENDPOINT 8: Returning Production / Experience
# trankpure17.php (returning minutes %, possession %)
# ═══════════════════════════════════════════════════════════════════════════════

def pull_returning_production(season, output_dir):
    """Pull returning production / experience data."""
    url = f"{BASE_URL}/trankpure17.php?year={season}&csv=1"
    text = fetch_url(url, f"Returning Production ({season})")
    df = csv_text_to_df(text, f"returning_{season}")
    if df is not None:
        save_df(df, f"returning_production_{season}.csv", output_dir)
        return df
    return None


# ═══════════════════════════════════════════════════════════════════════════════
# ENDPOINT 9: Time Machine Snapshots (historical point-in-time ratings)
# /timemachine/team_results/YYYYMMDD_team_results.json.gz
# CRITICAL for backtesting — gives you the actual ratings on a given date
# ═══════════════════════════════════════════════════════════════════════════════

def pull_time_machine(date_str, output_dir):
    """
    Pull time machine snapshot for a specific date.
    date_str: YYYYMMDD format
    Returns point-in-time team ratings as they were on that date.
    """
    url = f"{BASE_URL}/timemachine/team_results/{date_str}_team_results.json.gz"
    data = fetch_url(url, f"Time Machine ({date_str})", as_gzip=True)
    df = json_to_df(data, f"timemachine_{date_str}")
    if df is not None:
        save_df(df, f"timemachine_{date_str}.csv", output_dir)
        return df
    return None


def pull_time_machine_range(start_date, end_date, output_dir, interval_days=7):
    """
    Pull weekly time machine snapshots over a date range.
    Perfect for building point-in-time snapshots for your model.
    """
    tm_dir = os.path.join(output_dir, "timemachine")
    os.makedirs(tm_dir, exist_ok=True)

    current = start_date
    all_snapshots = []
    while current <= end_date:
        date_str = current.strftime("%Y%m%d")
        df = pull_time_machine(date_str, tm_dir)
        if df is not None:
            df["snapshot_date"] = current.strftime("%Y-%m-%d")
            all_snapshots.append(df)
        current += timedelta(days=interval_days)

    if all_snapshots:
        combined = pd.concat(all_snapshots, ignore_index=True)
        save_df(combined, "timemachine_all_snapshots.csv", tm_dir)
        print(f"\n    📊 Combined time machine: {len(all_snapshots)} snapshots, "
              f"{len(combined):,} total rows")
        return combined
    return None


# ═══════════════════════════════════════════════════════════════════════════════
# ENDPOINT 10: Schedule / Game Results
# ═══════════════════════════════════════════════════════════════════════════════

def pull_schedule(season, output_dir):
    """Pull game schedule/results."""
    url = f"{BASE_URL}/schedule.php?year={season}&csv=1"
    text = fetch_url(url, f"Schedule/Results ({season})")
    df = csv_text_to_df(text, f"schedule_{season}")
    if df is not None:
        save_df(df, f"schedule_results_{season}.csv", output_dir)
        return df
    return None


# ═══════════════════════════════════════════════════════════════════════════════
# ENDPOINT 11: Conference Stats
# ═══════════════════════════════════════════════════════════════════════════════

def pull_conference_stats(season, output_dir):
    """Pull conference-level aggregate stats."""
    url = f"{BASE_URL}/confstats.php?year={season}&csv=1"
    text = fetch_url(url, f"Conference Stats ({season})")
    df = csv_text_to_df(text, f"conf_stats_{season}")
    if df is not None:
        save_df(df, f"conference_stats_{season}.csv", output_dir)
        return df
    return None


# ═══════════════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(description="Barttorvik Full Data Extractor")
    parser.add_argument("--season", type=int, default=2026,
                        help="Season to pull (default: 2026)")
    parser.add_argument("--seasons", type=str, default=None,
                        help="Comma-separated seasons (e.g., 2022,2023,2024,2025,2026)")
    parser.add_argument("--timemachine", action="store_true",
                        help="Pull weekly time machine snapshots for the season")
    parser.add_argument("--output", type=str, default=OUTPUT_DIR,
                        help=f"Output directory (default: {OUTPUT_DIR})")
    args = parser.parse_args()

    output_dir = args.output
    os.makedirs(output_dir, exist_ok=True)

    # Determine seasons to pull
    if args.seasons:
        seasons = [int(s.strip()) for s in args.seasons.split(",")]
    else:
        seasons = [args.season]

    print("=" * 70)
    print("  BARTTORVIK FULL DATA EXTRACTOR")
    print(f"  Seasons: {seasons}")
    print(f"  Date: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"  Output: {os.path.abspath(output_dir)}")
    print("=" * 70)
    print()
    print("  Using Bart Torvik's official bulk data endpoints.")
    print("  No login. No subscription. No browser verification.")
    print()

    results = {}

    for season in seasons:
        print(f"\n{'━' * 70}")
        print(f"📊 SEASON {season}")
        print(f"{'━' * 70}")

        # 1. Team Results (main bulk file)
        df = pull_team_results(season, output_dir)
        if df is not None:
            results[f"team_results_{season}"] = df

        # 2. Team Slice — All Games
        df = pull_team_slice(season, output_dir, game_type="A")
        if df is not None:
            results[f"team_slice_all_{season}"] = df

        # 3. Team Slice — Regular Season Only
        df = pull_team_slice(season, output_dir, game_type="R")
        if df is not None:
            results[f"team_slice_reg_{season}"] = df

        # 4. T-Rank Ratings
        df = pull_trank(season, output_dir)
        if df is not None:
            results[f"trank_{season}"] = df

        # 5. Game Stats (game-by-game advanced)
        df = pull_game_stats(season, output_dir)
        if df is not None:
            results[f"game_stats_{season}"] = df

        # 6. Player Stats
        df = pull_player_stats(season, output_dir)
        if df is not None:
            results[f"player_stats_{season}"] = df

        # 7. Strength of Schedule
        df = pull_sos(season, output_dir)
        if df is not None:
            results[f"sos_{season}"] = df

        # 8. Returning Production
        df = pull_returning_production(season, output_dir)
        if df is not None:
            results[f"returning_{season}"] = df

        # 9. Schedule / Game Results
        df = pull_schedule(season, output_dir)
        if df is not None:
            results[f"schedule_{season}"] = df

        # 10. Conference Stats
        df = pull_conference_stats(season, output_dir)
        if df is not None:
            results[f"conf_stats_{season}"] = df

    # Transfer Portal (not season-specific)
    print(f"\n{'━' * 70}")
    print("📊 TRANSFER PORTAL")
    print(f"{'━' * 70}")
    df = pull_transfer_stats(output_dir)
    if df is not None:
        results["transfers"] = df

    # Time Machine Snapshots (optional)
    if args.timemachine:
        print(f"\n{'━' * 70}")
        print("📊 TIME MACHINE SNAPSHOTS")
        print(f"{'━' * 70}")
        print("  Pulling weekly snapshots for backtesting...")

        for season in seasons:
            # Season typically runs Nov through April
            start = datetime(season - 1, 11, 1)
            end = datetime(season, 4, 15)
            if end > datetime.now():
                end = datetime.now() - timedelta(days=1)

            print(f"\n  Season {season}: {start.strftime('%Y-%m-%d')} → {end.strftime('%Y-%m-%d')}")
            df = pull_time_machine_range(start, end, output_dir, interval_days=7)
            if df is not None:
                results[f"timemachine_{season}"] = df

    # ══════════════════════════════════════════════════════════════════════
    # SUMMARY
    # ══════════════════════════════════════════════════════════════════════
    print(f"\n{'=' * 70}")
    print("  EXTRACTION COMPLETE")
    print(f"{'=' * 70}")
    print(f"\n  📁 Output: {os.path.abspath(output_dir)}")
    print(f"  📊 Datasets extracted: {len(results)}")

    # List files
    all_csvs = []
    for root, dirs, files in os.walk(output_dir):
        for f in sorted(files):
            if f.endswith('.csv'):
                fpath = os.path.join(root, f)
                all_csvs.append(fpath)

    if all_csvs:
        print(f"\n  Saved files ({len(all_csvs)}):")
        total_rows = 0
        for fpath in all_csvs:
            try:
                nrows = sum(1 for _ in open(fpath)) - 1
                total_rows += max(nrows, 0)
                rel = os.path.relpath(fpath, output_dir)
                print(f"    ✅ {rel} ({nrows:,} rows)")
            except Exception:
                print(f"    ✅ {os.path.relpath(fpath, output_dir)}")
        print(f"\n  Total rows: {total_rows:,}")

    # Data quality check
    for season in seasons:
        key = f"team_results_{season}"
        if key in results:
            df = results[key]
            print(f"\n  📋 Quality Check — Team Results {season}:")
            print(f"     Teams: {len(df)}")
            print(f"     Columns: {len(df.columns)}")
            print(f"     Sample cols: {list(df.columns[:15])}")

    print(f"\n  🎯 WHAT YOU NOW HAVE:")
    print(f"  ─────────────────────")
    print(f"  • Team Results — AdjO, AdjD, Barthag, AdjT, Four Factors, WAB")
    print(f"  • Team Slice — Filterable by game type (all/regular/post/tourney)")
    print(f"  • T-Rank Ratings — Full ratings table")
    print(f"  • Game Stats — Game-by-game advanced box scores")
    print(f"  • Player Stats — Individual player metrics")
    print(f"  • Transfer Portal — Committed transfer stats")
    print(f"  • SOS — Strength of schedule")
    print(f"  • Returning Production — Minutes% and possession% returning")
    print(f"  • Schedule/Results — Full game results")
    print(f"  • Conference Stats — Conference aggregates")
    if args.timemachine:
        print(f"  • Time Machine — Weekly point-in-time snapshots (for backtesting!)")
    else:
        print(f"  • Time Machine — Run with --timemachine flag to get historical snapshots")

    print(f"\n  FOR CLAUDE CODE: Copy this entire torvik_data/ folder, then tell Claude:")
    print(f'  "Load the Barttorvik CSVs from torvik_data/ and build my CBB model features"')
    print(f"\n  Done! 🏀")


if __name__ == "__main__":
    main()
