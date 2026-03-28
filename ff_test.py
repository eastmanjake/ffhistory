import os
import json
import openpyxl
from requests_oauthlib import OAuth2Session

# Your credentials
CONSUMER_KEY = "dj0yJmk9Q0pnTU1pNE42ako5JmQ9WVdrOVdraFZSMnAwWVZnbWNHbzlNQT09JnM9Y29uc3VtZXJzZWNyZXQmc3Y9MCZ4PTdh"
CONSUMER_SECRET = "6717467220c40d91faba43ca37bc2e1a2982b7a6"
LEAGUE_ID = "163099"
GAME_ID = "257"
SEASON_YEAR = 2011
PLAYOFF_START_WEEK = 14
NUM_WEEKS = 16

TOKEN_FILE = os.path.expanduser("~/ffhistory/token.json")
REDIRECT_URI = "https://localhost"
AUTHORIZATION_BASE_URL = "https://api.login.yahoo.com/oauth2/request_auth"
TOKEN_URL = "https://api.login.yahoo.com/oauth2/get_token"
BASE = "https://fantasysports.yahooapis.com/fantasy/v2"
LEAGUE_KEY = f"{GAME_ID}.l.{LEAGUE_ID}"

def get_token():
    oauth = OAuth2Session(CONSUMER_KEY, redirect_uri=REDIRECT_URI)
    authorization_url, state = oauth.authorization_url(AUTHORIZATION_BASE_URL)
    print("\n--- AUTHORIZATION REQUIRED ---")
    print("Open this URL in your browser:")
    print(authorization_url)
    print("\nAfter approving, you'll see a verification code.")
    verifier = input("Enter verifier: ").strip()
    token = oauth.fetch_token(TOKEN_URL, code=verifier, client_secret=CONSUMER_SECRET)
    with open(TOKEN_FILE, "w") as f:
        json.dump(token, f)
    print("Token saved!")
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

def yahoo_get(session, url):
    response = session.get(url, headers={"Accept": "application/json"})
    if response.status_code == 401:
        print("Token expired, re-authenticating...")
        token = get_token()
        session = OAuth2Session(CONSUMER_KEY, token=token)
        response = session.get(url, headers={"Accept": "application/json"})
    return response.json()

session = get_session()

# ── 1. STANDINGS ──────────────────────────────────────────────────────────────
print("Fetching standings...")
data = yahoo_get(session, f"{BASE}/league/{LEAGUE_KEY}/standings?format=json")
teams_raw = data["fantasy_content"]["league"][1]["standings"][0]["teams"]
team_count = teams_raw["count"]

standings_rows = []
for i in range(team_count):
    t = teams_raw[str(i)]["team"]
    info = t[0]
    standings = t[2]["team_standings"]
    name = next((x["name"] for x in info if isinstance(x, dict) and "name" in x), "")
    manager = ""
    for x in info:
        if isinstance(x, dict) and "managers" in x:
            manager = x["managers"][0]["manager"].get("nickname", "")
    outcomes = standings["outcome_totals"]
    standings_rows.append([
        name, manager,
        outcomes["wins"], outcomes["losses"], outcomes["ties"],
        standings["points_for"], standings["points_against"],
        standings["rank"]
    ])
    print(f"  {name} - {outcomes['wins']}W {outcomes['losses']}L")

# ── 2. DRAFT RESULTS ─────────────────────────────────────────────────────────
print("Fetching draft results...")
data = yahoo_get(session, f"{BASE}/league/{LEAGUE_KEY}/draftresults?format=json")
picks_raw = data["fantasy_content"]["league"][1]["draft_results"]
pick_count = picks_raw["count"]

# Build team key -> team name map from standings data
team_key_to_name = {}
for i in range(team_count):
    t = teams_raw[str(i)]["team"]
    info = t[0]
    name = next((x["name"] for x in info if isinstance(x, dict) and "name" in x), "")
    team_key = next((x["team_key"] for x in info if isinstance(x, dict) and "team_key" in x), "")
    team_key_to_name[team_key] = name

# Collect all pick data and player keys
raw_picks = []
player_keys = []
for i in range(pick_count):
    pick = picks_raw[str(i)]["draft_result"]
    raw_picks.append(pick)
    player_keys.append(pick["player_key"])

# Batch fetch player names (25 at a time)
print(f"  Resolving {len(player_keys)} player names...")
player_key_to_name = {}
batch_size = 25
for i in range(0, len(player_keys), batch_size):
    batch = player_keys[i:i+batch_size]
    keys_str = ",".join(batch)
    url = f"{BASE}/players;player_keys={keys_str}?format=json"
    pdata = yahoo_get(session, url)
    players = pdata["fantasy_content"]["players"]
    pcount = players["count"]
    for j in range(pcount):
        p = players[str(j)]["player"][0]
        pkey = next((x["player_key"] for x in p if isinstance(x, dict) and "player_key" in x), "")
        pname = ""
        pposition = ""
        for x in p:
            if isinstance(x, dict) and "name" in x and isinstance(x["name"], dict):
                pname = x["name"].get("full", "")
            if isinstance(x, dict) and "display_position" in x:
                pposition = x["display_position"]
        player_key_to_name[pkey] = (pname, pposition)
    print(f"  Fetched players {i+1} to {min(i+batch_size, len(player_keys))}")

# Build final draft rows
draft_rows = []
for pick in raw_picks:
    team_name = team_key_to_name.get(pick["team_key"], pick["team_key"])
    player_info = player_key_to_name.get(pick["player_key"], (pick["player_key"], ""))
    player_name, player_position = player_info if isinstance(player_info, tuple) else (player_info, "")
    draft_rows.append([
        pick["round"],
        pick["pick"],
        team_name,
        player_name,
        player_position
    ])
print(f"  {pick_count} draft picks fetched")
# ── 3. WEEKLY MATCHUPS ───────────────────────────────────────────────────────
print("Fetching weekly matchups...")
matchup_rows = []
for week in range(1, NUM_WEEKS + 1):
    print(f"  Week {week}...")
    try:
        data = yahoo_get(session, f"{BASE}/league/{LEAGUE_KEY}/scoreboard;week={week}?format=json")
        matchups = data["fantasy_content"]["league"][1]["scoreboard"]["0"]["matchups"]
        matchup_count = matchups["count"]
        for i in range(matchup_count):
            m = matchups[str(i)]["matchup"]
            teams = m["0"]["teams"]
            t1 = teams["0"]["team"]
            t2 = teams["1"]["team"]
            t1_name = next((x["name"] for x in t1[0] if isinstance(x, dict) and "name" in x), "")
            t2_name = next((x["name"] for x in t2[0] if isinstance(x, dict) and "name" in x), "")
            t1_pts = t1[1]["team_points"]["total"]
            t2_pts = t2[1]["team_points"]["total"]
            is_playoff = week >= PLAYOFF_START_WEEK
            matchup_rows.append([week, t1_name, t1_pts, t2_name, t2_pts, "Yes" if is_playoff else "No", ""])
    except Exception as e:
        print(f"  Week {week} error: {e}")

# ── 4. TRANSACTIONS (TRADES, ADDS, DROPS) ────────────────────────────────────
import time
# ── PLAYOFF BRACKET LABELING ─────────────────────────────────────────────────
print("Labeling playoff rounds...")

# ── PLAYOFF BRACKET LABELING ─────────────────────────────────────────────────
print("Labeling playoff rounds...")

# Get seeds from regular-season record (wins desc, points_for desc as tiebreaker)
seeded_teams = [row[0] for row in sorted(standings_rows, key=lambda r: (-int(r[2]), -float(r[5])))]
playoff_teams = set(seeded_teams[:6])
top_two = set(seeded_teams[:2])

# Track bracket progression
week14_winners = set()
week14_losers = set()
week15_winners = set()
week15_losers = set()

for i, row in enumerate(matchup_rows):
    week, t1, s1, t2, s2, is_playoff, _ = row
    if not is_playoff == "Yes":
        continue

    try:
        s1 = float(s1)
        s2 = float(s2)
    except:
        continue

    winner = t1 if s1 > s2 else t2
    loser = t1 if s1 < s2 else t2

    if week == PLAYOFF_START_WEEK:
        # Week 14 - Quarterfinals
        if t1 in top_two or t2 in top_two:
            label = "Consolation"
        elif t1 in playoff_teams and t2 in playoff_teams:
            label = "Quarterfinal"
            week14_winners.add(winner)
            week14_losers.add(loser)
        else:
            label = "Consolation"

    elif week == PLAYOFF_START_WEEK + 1:
        # Week 15 - Semifinals
        semifinal_teams = top_two | week14_winners
        if t1 in semifinal_teams and t2 in semifinal_teams:
            label = "Semifinal"
            week15_winners.add(winner)
            week15_losers.add(loser)
        else:
            label = "Consolation"

    elif week == PLAYOFF_START_WEEK + 2:
        # Week 16 - Championship
        if t1 in week15_winners and t2 in week15_winners:
            label = "Championship"
        elif t1 in week15_losers and t2 in week15_losers:
            label = "3rd Place"
        else:
            label = "Consolation"
    else:
        label = ""

    matchup_rows[i][6] = label
print("Fetching transactions...")
transaction_rows = []

transaction_types = ["add", "drop", "trade"]

for t_type in transaction_types:
    print(f"  Fetching {t_type}s...")
    try:
        url = f"{BASE}/league/{LEAGUE_KEY}/transactions;type={t_type}?format=json"
        data = yahoo_get(session, url)
        transactions = data["fantasy_content"]["league"][1]["transactions"]
        t_count = transactions["count"]
        print(f"  Found {t_count} {t_type}s")
        for i in range(t_count):
            t = transactions[str(i)]["transaction"]
            t_info = t[0]
            t_players = t[1].get("players", {})
            p_count = t_players.get("count", 0)
            timestamp = t_info.get("timestamp", "")
            week = t_info.get("transaction_data", [{}])
            # Get team name involved
            for j in range(p_count):
                p = t_players[str(j)]["player"]
                p_info = p[0]
                p_data = p[1].get("transaction_data", {})
                pname = ""
                for x in p_info:
                    if isinstance(x, dict) and "name" in x and isinstance(x["name"], dict):
                        pname = x["name"].get("full", "")
                if isinstance(p_data, list):
                    p_data = p_data[0] if p_data else {}
                source_team = p_data.get("source_team_name", "FA/Waiver")
                dest_team = p_data.get("destination_team_name", "Dropped")
                transaction_rows.append([
                    t_type.capitalize(),
                    timestamp,
                    pname,
                    source_team,
                    dest_team
                ])
        time.sleep(0.5)
    except Exception as e:
        print(f"  Error fetching {t_type}s: {e}")

print(f"  {len(transaction_rows)} total transactions fetched")

# ── 5. END OF SEASON ROSTERS ─────────────────────────────────────────────────
team_keys = list(team_key_to_name.keys())
print("Fetching end of season rosters...")
roster_rows = []

for team_key in team_keys:
    team_name = team_key_to_name.get(team_key, team_key)
    try:
        url = f"{BASE}/team/{team_key}/roster;week={NUM_WEEKS}?format=json"
        data = yahoo_get(session, url)
        players = data["fantasy_content"]["team"][1]["roster"]["0"]["players"]
        p_count = players["count"]
        for i in range(p_count):
            p = players[str(i)]["player"][0]
            pname = ""
            pposition = ""
            pteam = ""
            pstatus = ""
            for x in p:
                if isinstance(x, dict) and "name" in x and isinstance(x["name"], dict):
                    pname = x["name"].get("full", "")
                if isinstance(x, dict) and "display_position" in x:
                    pposition = x["display_position"]
                if isinstance(x, dict) and "editorial_team_abbr" in x:
                    pteam = x["editorial_team_abbr"]
                if isinstance(x, dict) and "status" in x:
                    pstatus = x["status"]
            roster_rows.append([SEASON_YEAR, team_name, pname, pposition, pteam, pstatus])
        print(f"  {team_name}: {p_count} players")
        time.sleep(0.5)
    except Exception as e:
        print(f"  Error fetching roster for {team_name}: {e}")

print(f"  {len(roster_rows)} total roster spots fetched")


# ── 5. EXPORT TO EXCEL ───────────────────────────────────────────────────────
print("Writing to Excel...")
wb = openpyxl.Workbook()

ws = wb.active
ws.title = "Standings"
ws.append(["Team", "Manager", "Wins", "Losses", "Ties", "Points For", "Points Against", "Rank"])
for row in standings_rows:
    ws.append(row)

ws2 = wb.create_sheet("Draft")
ws2.append(["Round", "Pick", "Team", "Player", "Position"])
for row in draft_rows:
    ws2.append(row)

ws3 = wb.create_sheet("Matchups")
ws3.append(["Week", "Team 1", "Score 1", "Team 2", "Score 2", "Playoff", "Round"])

for row in matchup_rows:
    ws3.append(row)

ws5 = wb.create_sheet("Transactions")
ws5.append(["Type", "Timestamp", "Player", "From", "To"])
for row in transaction_rows:
    ws5.append(row)

ws6 = wb.create_sheet("End of Season Rosters")
ws6.append(["Season", "Fantasy Team", "Player", "Position", "NFL Team", "Status"])
for row in roster_rows:
    ws6.append(row)

ws7 = wb.create_sheet("Playoffs")
ws7.append(["Week", "Team 1", "Score 1", "Team 2", "Score 2", "Round"])
for row in matchup_rows:
    if row[0] >= PLAYOFF_START_WEEK:
        ws7.append([row[0], row[1], row[2], row[3], row[4], row[6]])
output = os.path.expanduser("~/ffhistory/ff_2011.xlsx")
wb.save(output)
print(f"\nDone! Saved to {output}")
