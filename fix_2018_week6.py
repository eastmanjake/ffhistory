#!/usr/bin/env python3
"""
fix_2018_week6.py — Targeted re-scrape of 2018 Goatse X (Jake) week 6 lineup.

Fetches week 6 roster + fantasy points for Goatse X and appends to
docs/data/2018/weekly_lineups.json.

Usage:
  python fix_2018_week6.py
"""

import os
import re
import json
import time
from datetime import datetime, timezone
import pandas as pd
from requests_oauthlib import OAuth2Session

CONSUMER_KEY    = "dj0yJmk9Q0pnTU1pNE42ako5JmQ9WVdrOVdraFZSMnAwWVZnbWNHbzlNQT09JnM9Y29uc3VtZXJzZWNyZXQmc3Y9MCZ4PTdh"
CONSUMER_SECRET = "6717467220c40d91faba43ca37bc2e1a2982b7a6"
TOKEN_FILE      = os.path.expanduser("~/ffhistory/token.json")
REDIRECT_URI    = "https://localhost"
AUTHORIZATION_BASE_URL = "https://api.login.yahoo.com/oauth2/request_auth"
TOKEN_URL       = "https://api.login.yahoo.com/oauth2/get_token"
BASE            = "https://fantasysports.yahooapis.com/fantasy/v2"

GAME_ID    = "380"
LEAGUE_ID  = "208659"
TARGET_WEEK = 6
TARGET_TEAM = "Goatse X"
TARGET_OWNER = "Jake"
LINEUPS_PATH = os.path.expanduser("~/ffhistory/docs/data/2018/weekly_lineups.json")

OWNER_OVERRIDES = {
    "380.l.208659.t.6": "Steve",  # Raaaaaaaandy
}


def get_token():
    oauth = OAuth2Session(CONSUMER_KEY, redirect_uri=REDIRECT_URI)
    authorization_url, state = oauth.authorization_url(AUTHORIZATION_BASE_URL)
    print("\n--- AUTHORIZATION REQUIRED ---")
    print("Open this URL in your browser:")
    print(authorization_url)
    print("\nAfter approving, paste the verifier code from the redirect URL.")
    verifier = input("Enter verifier: ").strip()
    token = oauth.fetch_token(TOKEN_URL, code=verifier, client_secret=CONSUMER_SECRET)
    with open(TOKEN_FILE, "w") as f:
        json.dump(token, f)
    print("Token saved.")
    return token


def load_token():
    if os.path.exists(TOKEN_FILE):
        with open(TOKEN_FILE) as f:
            return json.load(f)
    return None


def get_session():
    token = load_token()
    if not token:
        token = get_token()
    return OAuth2Session(CONSUMER_KEY, token=token)


def yahoo_get(session, url, _retries=5):
    from oauthlib.oauth2.rfc6749.errors import TokenExpiredError
    for attempt in range(_retries):
        try:
            response = session.get(url, headers={"Accept": "application/json"})
        except TokenExpiredError:
            print("Token expired, re-authenticating...")
            token = get_token()
            session.__dict__.update(OAuth2Session(CONSUMER_KEY, token=token).__dict__)
            response = session.get(url, headers={"Accept": "application/json"})
        if response.status_code == 401:
            print("Token expired (401), re-authenticating...")
            token = get_token()
            session.__dict__.update(OAuth2Session(CONSUMER_KEY, token=token).__dict__)
            response = session.get(url, headers={"Accept": "application/json"})
        if response.status_code == 429 or not response.text.strip():
            wait = 30 * (attempt + 1)
            print(f"  Rate limited (empty response), waiting {wait}s...")
            time.sleep(wait)
            continue
        try:
            return response.json()
        except Exception:
            wait = 30 * (attempt + 1)
            print(f"  Bad response (status {response.status_code}), waiting {wait}s...")
            time.sleep(wait)
    raise RuntimeError(f"Yahoo API failed after {_retries} retries: {url}")


def normalize_name(name):
    name = name.lower().strip()
    name = name.replace('.', '')
    name = re.sub(r'\s+(jr|sr|ii|iii|iv)$', '', name)
    name = re.sub(r'\s+', ' ', name)
    return name


def build_nfl_team_lookup(year):
    try:
        import nfl_data_py as nfl
        rosters = nfl.import_weekly_rosters([year])
        by_yahoo_id, by_name = {}, {}
        for _, row in rosters.iterrows():
            week_val = row.get('week')
            team_val = row.get('team')
            if pd.isna(week_val) or pd.isna(team_val):
                continue
            week = int(week_val)
            team = str(team_val)
            yid = row.get('yahoo_id')
            try:
                if yid is not None and str(yid).strip() != '' and not pd.isna(yid):
                    by_yahoo_id.setdefault(int(yid), {})[week] = team
            except (ValueError, TypeError):
                pass
            pname = str(row.get('player_name') or '')
            norm = normalize_name(pname)
            if norm:
                by_name.setdefault(norm, {})[week] = team
        return by_yahoo_id, by_name
    except Exception as e:
        print(f"  Warning: nfl_data_py unavailable: {e}")
        return {}, {}


def lookup_nfl_team(player_key, player_name, week, by_yahoo_id, by_name, fallback=""):
    if player_key:
        try:
            yid = int(player_key.split('.')[-1])
            if yid in by_yahoo_id:
                week_map = by_yahoo_id[yid]
                if week in week_map:
                    return week_map[week]
                nearest = min(week_map, key=lambda w: abs(w - week))
                return week_map[nearest]
        except (ValueError, IndexError):
            pass
    norm = normalize_name(player_name)
    if norm in by_name:
        week_map = by_name[norm]
        if week in week_map:
            return week_map[week]
        nearest = min(week_map, key=lambda w: abs(w - week))
        return week_map[nearest]
    return fallback


def main():
    league_key = f"{GAME_ID}.l.{LEAGUE_ID}"
    session = get_session()

    # Step 1: Get all team keys for 2018 league
    print(f"Fetching 2018 league standings to find team keys...")
    data = yahoo_get(session, f"{BASE}/league/{league_key}/standings?format=json")
    teams_raw = data["fantasy_content"]["league"][1]["standings"][0]["teams"]
    team_count = teams_raw["count"]

    target_team_key = None
    for i in range(team_count):
        t = teams_raw[str(i)]["team"]
        info = t[0]
        name = next((x["name"] for x in info if isinstance(x, dict) and "name" in x), "")
        team_key = next((x["team_key"] for x in info if isinstance(x, dict) and "team_key" in x), "")
        print(f"  {team_key}: {name}")
        if name == TARGET_TEAM:
            target_team_key = team_key

    if not target_team_key:
        print(f"\nERROR: Could not find team '{TARGET_TEAM}' in 2018 league.")
        return

    print(f"\nFound {TARGET_TEAM} team key: {target_team_key}")

    # Step 2: Build NFL team lookup
    print(f"\nLoading nfl_data_py roster data for 2018...")
    nfl_by_yahoo_id, nfl_by_name = build_nfl_team_lookup(2018)

    # Step 3: Fetch week 6 roster for Goatse X
    print(f"\nFetching week {TARGET_WEEK} roster for {TARGET_TEAM}...")
    roster_data = yahoo_get(session, f"{BASE}/team/{target_team_key}/roster;week={TARGET_WEEK}?format=json")
    roster_players = roster_data["fantasy_content"]["team"][1]["roster"]["0"]["players"]

    players_this_week = []
    for i in range(roster_players["count"]):
        p = roster_players[str(i)]["player"]
        info = p[0]
        p_selected = p[1].get("selected_position", [])
        pk    = next((x["player_key"] for x in info if isinstance(x, dict) and "player_key" in x), None)
        pname = next((x["name"]["full"] for x in info if isinstance(x, dict) and "name" in x and isinstance(x["name"], dict)), "")
        ppos  = next((x["display_position"] for x in info if isinstance(x, dict) and "display_position" in x), "")
        pteam = next((x["editorial_team_abbr"] for x in info if isinstance(x, dict) and "editorial_team_abbr" in x), "")
        slot = ""
        if isinstance(p_selected, list):
            for s in p_selected:
                if isinstance(s, dict) and "position" in s:
                    slot = s["position"]
        elif isinstance(p_selected, dict):
            slot = p_selected.get("position", "")
        if pk:
            players_this_week.append((pk, slot, pname, ppos, pteam))
            print(f"  {slot:<4} {pname} ({ppos})")

    print(f"\n  {len(players_this_week)} players found")

    # Step 4: Fetch fantasy points for those players
    time.sleep(0.5)
    keys_str = ",".join(pk for pk, *_ in players_this_week)
    print(f"\nFetching week {TARGET_WEEK} fantasy points...")
    pts_data = yahoo_get(session, f"{BASE}/league/{league_key}/players;player_keys={keys_str}/stats;type=week;week={TARGET_WEEK}?format=json")
    pts_players = pts_data["fantasy_content"]["league"][1]["players"]
    pts_by_key = {}
    for i in range(pts_players["count"]):
        p = pts_players[str(i)]["player"]
        info = p[0]
        sb = p[1] if len(p) > 1 else {}
        pk = next((x["player_key"] for x in info if isinstance(x, dict) and "player_key" in x), "")
        pts_by_key[pk] = sb.get("player_points", {}).get("total", "")

    # Step 5: Build new lineup rows
    new_rows = []
    for pk, slot, pname, ppos, pteam in players_this_week:
        nfl_team = lookup_nfl_team(pk, pname, TARGET_WEEK, nfl_by_yahoo_id, nfl_by_name, pteam)
        pts = pts_by_key.get(pk, "")
        new_rows.append({
            "week": TARGET_WEEK,
            "team": TARGET_TEAM,
            "owner": TARGET_OWNER,
            "slot": slot,
            "player": pname,
            "pos": ppos,
            "nfl_team": nfl_team,
            "fantasy_pts": pts
        })
        print(f"  {slot:<4} {pname:<30} {nfl_team:<4} {pts}")

    # Step 6: Load existing weekly_lineups.json and append
    print(f"\nLoading existing {LINEUPS_PATH}...")
    with open(LINEUPS_PATH) as f:
        existing = json.load(f)

    # Sanity check: make sure Goatse X week 6 isn't already there
    already = [e for e in existing if e["week"] == TARGET_WEEK and e["team"] == TARGET_TEAM]
    if already:
        print(f"\nWARNING: {TARGET_TEAM} week {TARGET_WEEK} already has {len(already)} entries! Aborting.")
        return

    combined = existing + new_rows
    # Sort by week, then team for consistency
    combined.sort(key=lambda e: (e["week"], e["team"]))

    with open(LINEUPS_PATH, "w") as f:
        json.dump(combined, f, indent=2)

    print(f"\nDone! Added {len(new_rows)} entries for {TARGET_TEAM} week {TARGET_WEEK}.")
    print(f"weekly_lineups.json now has {len(combined)} total records.")


if __name__ == "__main__":
    main()
