#!/usr/bin/env python3
"""
daily_recap.py — Grade yesterday's picks against OddsAPI final scores
======================================================================
Usage:
  py daily_recap.py --api-key YOUR_KEY
  py daily_recap.py --api-key YOUR_KEY --date 2026-02-27
  py daily_recap.py --api-key YOUR_KEY --min-units 2
"""

import json, sys, os, csv, argparse, urllib.request, urllib.parse
from datetime import datetime, timedelta, timezone

ODDS_API_SCORES = "https://api.the-odds-api.com/v4/sports/basketball_ncaab/scores"
JUICE = 1.1

ODDS_TO_TORVIK = {
    "Jacksonville State": "Jacksonville St.", "Morehead State": "Morehead St.",
    "Missouri State": "Missouri St.", "Sacramento State": "Sacramento St.",
    "Cleveland State": "Cleveland St.", "Tennessee State": "Tennessee St.",
    "East Tennessee State": "East Tennessee St.", "UT Martin": "Tennessee Martin",
    "Tarleton State": "Tarleton St.", "Gardner-Webb": "Gardner Webb",
    "Kennesaw State": "Kennesaw St.", "New Mexico State": "New Mexico St.",
    "Miami (OH)": "Miami OH", "North Carolina A&T": "NC A&T",
    "Louisiana": "Louisiana Lafayette", "LIU": "Long Island University",
    "UTSA": "UT San Antonio", "UTRGV": "UT Rio Grande Valley",
    "SIU Edwardsville": "SIU-Edwardsville", "St. John's (NY)": "St. John's",
    "Saint Mary's (CA)": "Saint Mary's", "Loyola Chicago": "Loyola-Chicago",
    "Loyola Maryland": "Loyola MD", "Texas A&M-Corpus Christi": "Texas A&M-CC",
    "McNeese": "McNeese St.", "Southeast Missouri State": "Southeast Missouri St.",
    "Illinois State": "Illinois St.", "Indiana State": "Indiana St.",
    "Murray State": "Murray St.", "Michigan State": "Michigan St.",
    "Ohio State": "Ohio St.", "Penn State": "Penn St.",
    "Iowa State": "Iowa St.", "Kansas State": "Kansas St.",
    "Oklahoma State": "Oklahoma St.", "Mississippi State": "Mississippi St.",
    "Florida State": "Florida St.", "South Dakota State": "South Dakota St.",
    "North Dakota State": "North Dakota St.", "Little Rock": "Little Rock",
    "Louisiana Tech": "Louisiana Tech", "Utah Valley": "Utah Valley",
    "Oral Roberts": "Oral Roberts", "Northern Kentucky": "Northern Kentucky",
    "USC Upstate": "USC Upstate", "UMass Lowell": "UMass Lowell",
    "UMass": "Massachusetts", "UMBC": "UMBC", "UAB": "UAB", "UTEP": "UTEP",
    "FIU": "Florida International", "FAU": "Florida Atlantic",
    "VCU": "VCU", "TCU": "TCU", "SMU": "SMU", "LSU": "LSU",
    "BYU": "BYU", "UCF": "UCF", "USF": "South Florida", "UNLV": "UNLV",
    "Cal Poly": "Cal Poly", "Charleston": "Col. of Charleston",
    "Coastal Carolina": "Coastal Carolina", "Western Kentucky": "Western Kentucky",
    "Western Michigan": "Western Michigan", "Eastern Kentucky": "Eastern Kentucky",
    "Eastern Washington": "Eastern Washington", "Eastern Michigan": "Eastern Michigan",
    "Central Michigan": "Central Michigan", "Central Connecticut": "Central Connecticut",
    "South Dakota": "South Dakota", "North Dakota": "North Dakota",
    "Detroit Mercy": "Detroit Mercy", "Robert Morris": "Robert Morris",
    "Saint Joseph's": "Saint Joseph's", "St. Bonaventure": "St. Bonaventure",
    "Boston University": "Boston University", "Middle Tennessee": "Middle Tennessee",
    "Florida Gulf Coast": "Florida Gulf Coast", "North Florida": "North Florida",
    "Nebraska Omaha": "Nebraska Omaha", "Oakland": "Oakland",
    "Wright State": "Wright St.", "Youngstown State": "Youngstown St.",
    "IU Indianapolis": "IU Indy", "Valparaiso": "Valparaiso",
    "Evansville": "Evansville", "Merrimack": "Merrimack",
    "Canisius": "Canisius", "Rhode Island": "Rhode Island",
    "Saint Peter's": "Saint Peter's", "Manhattan": "Manhattan",
}

def odds_to_torvik(name):
    return ODDS_TO_TORVIK.get(name, name)

def fetch_scores(api_key, date_str):
    target = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    days_back = min(3, max(1, (datetime.now(timezone.utc) - target).days + 1))

    params = {"apiKey": api_key, "daysFrom": days_back}
    url = f"{ODDS_API_SCORES}?{urllib.parse.urlencode(params)}"
    print(f"  Fetching OddsAPI scores (daysFrom={days_back})...")
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read().decode())
            remaining = resp.headers.get("x-requests-remaining", "?")
            print(f"  Got {len(data)} records, quota remaining: {remaining}")
    except Exception as e:
        print(f"  [ERROR] {e}")
        return {}

    games = {}
    for event in data:
        commence = event.get("commence_time", "")
        if not commence or not event.get("completed"):
            continue
        try:
            utc_dt = datetime.strptime(commence[:19], "%Y-%m-%dT%H:%M:%S").replace(tzinfo=timezone.utc)
            et_date = (utc_dt + timedelta(hours=-5)).strftime("%Y-%m-%d")
        except Exception:
            et_date = commence[:10]
        if et_date != date_str:
            continue

        home_raw = event.get("home_team", "")
        away_raw = event.get("away_team", "")
        score_list = event.get("scores") or []
        score_map = {s["name"]: int(s["score"]) for s in score_list if s.get("score") is not None}
        hs = score_map.get(home_raw)
        as_ = score_map.get(away_raw)
        if hs is None or as_ is None:
            continue

        ht = odds_to_torvik(home_raw)
        at = odds_to_torvik(away_raw)
        games[frozenset({ht, at})] = {
            "home": ht, "away": at,
            "home_score": hs, "away_score": as_,
        }

    print(f"  Matched {len(games)} completed games for {date_str}")
    return games

def load_predictions(date_str):
    fname = f"sim_predictions_{date_str}.csv"
    if not os.path.exists(fname):
        print(f"  [ERROR] {fname} not found.")
        sys.exit(1)
    with open(fname, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))

def grade(pick_team, pick_spread, home, home_score, away_score):
    margin = home_score - away_score
    team_margin = margin if pick_team == home else -margin
    result = team_margin + pick_spread
    return "WIN" if result > 0 else ("PUSH" if result == 0 else "LOSS")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--api-key", required=True)
    parser.add_argument("--date", default=None)
    parser.add_argument("--min-units", type=int, default=1)
    args = parser.parse_args()

    date_str = args.date or (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

    print("=" * 70)
    print(f"  DAILY RECAP — {date_str}")
    print("=" * 70)

    print("\n[1] Loading predictions...")
    preds = load_predictions(date_str)
    plays = [p for p in preds if int(float(p.get("units", 0))) >= args.min_units]
    print(f"  {len(preds)} predictions | {len(plays)} plays (>={args.min_units}u)")

    print("\n[2] Fetching scores...")
    scores = fetch_scores(args.api_key, date_str)

    print("\n[3] Grading...")
    results = []
    unmatched = []

    for p in plays:
        ht = p["home_team"]; at = p["away_team"]
        pick_team = p["pick_team"]
        pick_sprd = float(p["pick_spread"])
        units = int(float(p["units"]))
        edge = float(p["edge"]) * 100
        cal = float(p["pick_conf"]) * 100

        key = frozenset({ht, at})
        if key not in scores:
            unmatched.append(f"{at} @ {ht}")
            continue

        g = scores[key]
        result = grade(pick_team, pick_sprd, g["home"], g["home_score"], g["away_score"])
        pnl = units if result == "WIN" else (-units * JUICE if result == "LOSS" else 0)

        results.append({
            "away": at, "home": ht,
            "pick_team": pick_team, "pick_spread": pick_sprd,
            "units": units, "edge": edge, "cal": cal,
            "home_score": g["home_score"], "away_score": g["away_score"],
            "result": result, "pnl": pnl,
        })

    print()
    print("=" * 70)
    print(f"  PICKS RECAP — {date_str}")
    print("=" * 70)
    print()

    results.sort(key=lambda r: (-r["units"], -r["edge"]))

    for r in results:
        icon = "✅" if r["result"] == "WIN" else ("❌" if r["result"] == "LOSS" else "➖")
        pnl_str = f"+{r['pnl']:.1f}u" if r["pnl"] >= 0 else f"{r['pnl']:.1f}u"
        print(f"  {icon}  {r['pick_team']} {r['pick_spread']:+.1f}  ({r['units']}u)  {r['result']}  {pnl_str}")
        print(f"       {r['away']} @ {r['home']}")
        print(f"       Final: {r['away']} {r['away_score']} - {r['home']} {r['home_score']}")
        print(f"       cal={r['cal']:.1f}%  edge={r['edge']:.1f}%")
        print()

    if unmatched:
        print(f"  UNMATCHED ({len(unmatched)}) — not in OddsAPI scores:")
        for u in unmatched:
            print(f"    {u}")
        print()

    wins = [r for r in results if r["result"] == "WIN"]
    losses = [r for r in results if r["result"] == "LOSS"]
    pushes = [r for r in results if r["result"] == "PUSH"]
    total_pnl = sum(r["pnl"] for r in results)
    units_bet = sum(r["units"] for r in results)
    roi = (total_pnl / units_bet * 100) if units_bet > 0 else 0

    print("=" * 70)
    print(f"  RECORD:  {len(wins)}W - {len(losses)}L - {len(pushes)}P")
    print(f"  UNITS:   {total_pnl:+.1f}u on {units_bet}u risked")
    print(f"  ROI:     {roi:+.1f}%")
    print("=" * 70)

if __name__ == "__main__":
    main()
