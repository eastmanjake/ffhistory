#!/usr/bin/env python3
"""
debug_trades.py — Diagnose why 2023-2025 trade transactions return 0.

Fetches the raw Yahoo API response for type=trade for 2022 (known-working)
and 2023 (broken) and prints the full response structure for comparison.

Usage:
  python debug_trades.py
"""

import os
import json
import time
from requests_oauthlib import OAuth2Session

CONSUMER_KEY    = "dj0yJmk9Q0pnTU1pNE42ako5JmQ9WVdrOVdraFZSMnAwWVZnbWNHbzlNQT09JnM9Y29uc3VtZXJzZWNyZXQmc3Y9MCZ4PTdh"
CONSUMER_SECRET = "6717467220c40d91faba43ca37bc2e1a2982b7a6"
TOKEN_FILE      = os.path.expanduser("~/ffhistory/token.json")
REDIRECT_URI    = "https://localhost"
AUTHORIZATION_BASE_URL = "https://api.login.yahoo.com/oauth2/request_auth"
TOKEN_URL       = "https://api.login.yahoo.com/oauth2/get_token"
BASE            = "https://fantasysports.yahooapis.com/fantasy/v2"

SEASONS = [
    {"year": 2022, "game_id": "414", "league_id": "247217"},  # last year with known trades
    {"year": 2023, "game_id": "423", "league_id": "251046"},  # first year with 0 trades
    {"year": 2024, "game_id": "449", "league_id": "215072"},
]


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


def yahoo_get_raw(session, url, _retries=5):
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
            print(f"  Rate limited or empty, waiting {wait}s...")
            time.sleep(wait)
            continue
        print(f"  HTTP {response.status_code}")
        return response.status_code, response.text
    return None, None


def summarize_structure(obj, depth=0, max_depth=4):
    """Recursively summarize a JSON structure to understand shape."""
    indent = "  " * depth
    if depth > max_depth:
        return f"{indent}...(truncated)"
    if isinstance(obj, dict):
        lines = [f"{indent}{{  # {len(obj)} keys"]
        for k, v in list(obj.items())[:10]:
            lines.append(f"{indent}  {repr(k)}:")
            lines.append(summarize_structure(v, depth + 1, max_depth))
        if len(obj) > 10:
            lines.append(f"{indent}  ... ({len(obj) - 10} more keys)")
        lines.append(f"{indent}}}")
        return "\n".join(lines)
    elif isinstance(obj, list):
        lines = [f"{indent}[  # {len(obj)} items"]
        for item in obj[:3]:
            lines.append(summarize_structure(item, depth + 1, max_depth))
        if len(obj) > 3:
            lines.append(f"{indent}  ... ({len(obj) - 3} more items)")
        lines.append(f"{indent}]")
        return "\n".join(lines)
    else:
        return f"{indent}{repr(obj)}"


def main():
    session = get_session()

    for s in SEASONS:
        year = s["year"]
        league_key = f"{s['game_id']}.l.{s['league_id']}"
        url = f"{BASE}/league/{league_key}/transactions;type=trade?format=json"

        print(f"\n{'='*60}")
        print(f"  {year} trades  ({league_key})")
        print(f"{'='*60}")
        print(f"  URL: {url}")

        status, text = yahoo_get_raw(session, url)
        if text is None:
            print("  FAILED: no response")
            continue

        print(f"\n--- RAW RESPONSE (first 2000 chars) ---")
        print(text[:2000])

        try:
            parsed = json.loads(text)
            print(f"\n--- STRUCTURE SUMMARY ---")
            print(summarize_structure(parsed))

            # Drill into the transactions key specifically
            league_content = parsed.get("fantasy_content", {}).get("league", [])
            print(f"\n--- league content type: {type(league_content)}, len: {len(league_content) if isinstance(league_content, list) else 'n/a'} ---")
            if isinstance(league_content, list) and len(league_content) > 1:
                transactions_raw = league_content[1].get("transactions")
                print(f"  transactions_raw type: {type(transactions_raw)}")
                print(f"  transactions_raw value: {repr(transactions_raw)[:500]}")
        except json.JSONDecodeError as e:
            print(f"\nFailed to parse JSON: {e}")

        time.sleep(1)

    print("\n\nDone. Check the output above to see what differs between 2022 and 2023+.")


if __name__ == "__main__":
    main()
