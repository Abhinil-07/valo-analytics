import os
import json
import requests

# --- Config ---
puuid = "59bae8f3-025c-5dcc-9a1c-c903279e4145"
region = "ap"
url = f"https://api.henrikdev.xyz/valorant/v3/by-puuid/matches/{region}/{puuid}"

params = {
    "mode": "competitive",
    "size": 2
}

# Recommended: keep your key out of source code.
# Set it once in your terminal: export HENRIKDEV_API_KEY="your_key_here"
api_key = os.environ.get("HENRIKDEV_API_KEY", "HDEV-c897243f-3b2e-4962-b1f4-4f9f9e272b44")

headers = {
    "Authorization": api_key,
    "Accept": "application/json"
}

output_path = "matches.json"

# --- Request ---
response = requests.get(url, params=params, headers=headers)

if response.status_code == 200:
    data = response.json()

    # Write nicely formatted, multi-line JSON
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(
            data,
            f,
            indent=4,           # 4-space indentation for readability
            ensure_ascii=False, # keep non-ASCII characters (e.g. player names) as-is
            sort_keys=False     # preserve the API's original key order
        )

    print(f"Saved {len(data.get('data', []))} matches to {os.path.abspath(output_path)}")
else:
    print(f"Error {response.status_code}: {response.text}")