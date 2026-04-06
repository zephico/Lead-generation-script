#!/usr/bin/env python3

import uuid
from datetime import datetime, timezone

import pandas as pd
import requests


SUBREDDITS = ["forhire"]
OUTPUT_CSV = "reddit_subreddits.csv"


HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json,text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.reddit.com/",
}


def to_iso_utc(created_utc):
    if created_utc in (None, ""):
        return ""
    return datetime.fromtimestamp(created_utc, tz=timezone.utc).isoformat()


def fetch_subreddit_about(subreddit: str):
    url = f"https://www.reddit.com/r/{subreddit}/about.json"
    response = requests.get(url, headers=HEADERS, timeout=30)
    response.raise_for_status()
    return response.json()


def build_row(subreddit: str):
    payload = fetch_subreddit_about(subreddit)
    data = payload.get("data", {})

    return {
        "id": str(uuid.uuid4()),
        "reddit_subreddit_id": data.get("id", ""),
        "display_name": data.get("display_name", ""),
        "display_name_prefixed": data.get("display_name_prefixed", ""),
        "title": data.get("title", ""),
        "public_description": data.get("public_description", ""),
        "description": data.get("description", ""),
        "subscribers": data.get("subscribers", 0),
        "active_user_count": data.get("active_user_count", 0),
        "created_utc": to_iso_utc(data.get("created_utc")),
        "over18": bool(data.get("over18")),
        "subreddit_type": data.get("subreddit_type", ""),
        "submission_type": data.get("submission_type", ""),
        "url": f"https://www.reddit.com{data.get('url', '')}" if data.get("url") else "",
        "lang": data.get("lang", ""),
        "raw_payload": data,
        "fetched_at": datetime.now(timezone.utc).isoformat(),
    }


def main():
    rows = [build_row(subreddit) for subreddit in SUBREDDITS]
    df = pd.DataFrame(rows)
    print(df)
    df.to_csv(OUTPUT_CSV, index=False)
    print(f"Wrote {len(df)} subreddits to {OUTPUT_CSV}")


if __name__ == "__main__":
    main()
