"""Quick verification of Section 6 logic — fetches week 1 for all teams only."""
import os, json, time
from requests_oauthlib import OAuth2Session
from oauthlib.oauth2.rfc6749.errors import TokenExpiredError

CONSUMER_KEY = "dj0yJmk9Q0pnTU1pNE42ako5JmQ9WVdrOVdraFZSMnAwWVZnbWNHbzlNQT09JnM9Y29uc3VtZXJzZWNyZXQmc3Y9MCZ4PTdh"
CONSUMER_SECRET = "6717467220c40d91faba43ca37bc2e1a2982b7a6"
GAME_ID = "257"
LEAGUE_ID = "163099"
LEAGUE_KEY = f"{GAME_ID}.l.{LEAGUE_ID}"
BASE = "https://fantasysports.yahooapis.com/fantasy/v2"
TOKEN_FILE = os.path.expanduser("~/ffhistory/token.json")
REDIRECT_URI = "https://localhost"
AUTHORIZATION_BASE_URL = "https://api.login.yahoo.com/oauth2/request_auth"
TOKEN_URL = "https://api.login.yahoo.com/oauth2/get_token"

def get_token():
    oauth = OAuth2Session(CONSUMER_KEY, redirect_uri=REDIRECT_URI)
    auth_url, _ = oauth.authorization_url(AUTHORIZATION_BASE_URL)
    print("\nOpen this URL in your browser:")
    print(auth_url)
    verifier = input("\nEnter verifier: ").strip()
    token = oauth.fetch_token(TOKEN_URL, code=verifier, client_secret=CONSUMER_SECRET)
    with open(TOKEN_FILE, "w") as f:
        json.dump(token, f)
    return token

token = json.load(open(TOKEN_FILE)) if os.path.exists(TOKEN_FILE) else None
session = OAuth2Session(CONSUMER_KEY, token=token)
try:
    session.get(f"{BASE}/league/{LEAGUE_KEY}/standings?format=json", headers={"Accept": "application/json"})
except TokenExpiredError:
    print("Token expired, re-authenticating...")
    token = get_token()
    session = OAuth2Session(CONSUMER_KEY, token=token)

def yahoo_get(url):
    return session.get(url, headers={"Accept": "application/json"}).json()

# Get all team keys
data = yahoo_get(f"{BASE}/league/{LEAGUE_KEY}/standings?format=json")
teams_raw = data["fantasy_content"]["league"][1]["standings"][0]["teams"]
team_key_to_name = {}
for i in range(teams_raw["count"]):
    info = teams_raw[str(i)]["team"][0]
    name = next((x["name"] for x in info if isinstance(x, dict) and "name" in x), "")
    key  = next((x["team_key"] for x in info if isinstance(x, dict) and "team_key" in x), "")
    team_key_to_name[key] = name

WEEK = 1
print(f"\nFetching week {WEEK} for all {len(team_key_to_name)} teams...\n")

for team_key, team_name in team_key_to_name.items():
    # Call 1: week-specific roster → player_keys, slots, and player info
    roster_data = yahoo_get(f"{BASE}/team/{team_key}/roster;week={WEEK}?format=json")
    roster_players = roster_data["fantasy_content"]["team"][1]["roster"]["0"]["players"]
    players_this_week = []  # list of (pk, slot, name, pos, nfl_team)
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
    time.sleep(0.2)

    # Call 2: fetch points for exactly those player_keys using league/players endpoint
    keys_str = ",".join(pk for pk, *_ in players_this_week)
    pts_data = yahoo_get(f"{BASE}/league/{LEAGUE_KEY}/players;player_keys={keys_str}/stats;type=week;week={WEEK}?format=json")
    pts_by_key = {}
    pts_players = pts_data["fantasy_content"]["league"][1]["players"]
    for i in range(pts_players["count"]):
        p = pts_players[str(i)]["player"]
        info = p[0]
        sb   = p[1] if len(p) > 1 else {}
        pk   = next((x["player_key"] for x in info if isinstance(x, dict) and "player_key" in x), "")
        pts  = sb.get("player_points", {}).get("total", "")
        pts_by_key[pk] = pts
    time.sleep(0.2)

    print(f"-- {team_name} --")
    print(f"  {'Slot':<6} {'Player':<25} {'Pos':<5} {'NFL':<5} {'Pts':>7}")
    print(f"  {'-'*52}")
    for pk, slot, pname, ppos, pteam in players_this_week:
        pts = pts_by_key.get(pk, "?")
        print(f"  {slot:<6} {pname:<25} {ppos:<5} {pteam:<5} {str(pts):>7}")
    print()
