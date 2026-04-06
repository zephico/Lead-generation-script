#!/usr/bin/env python3

import uuid
from datetime import datetime, timezone

import pandas as pd
import requests


SOURCE_TYPE = "subreddit"
SOURCE_VALUES = ["forhire"]
ENDPOINT_NAME = "/new"
PAGE_SIZE = 100
START_DATE = "2026-03-27"
OUTPUT_CSV = "reddit_checkpoints.csv"
DEFAULT_NOTES = "Checkpoint seeded from subreddit new listing for incremental load."


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


def parse_start_date(start_date: str) -> datetime:
    return datetime.strptime(start_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)


def to_iso_utc(created_utc):
    if created_utc in (None, ""):
        return ""
    return datetime.fromtimestamp(created_utc, tz=timezone.utc).isoformat()


def fetch_page(source_value: str, page_size: int, after: str | None = None):
    url = f"https://www.reddit.com/r/{source_value}/new.json?limit={page_size}"
    if after:
        url = f"{url}&after={after}"

    response = requests.get(url, headers=HEADERS, timeout=30)
    response.raise_for_status()
    return response.json()


def build_checkpoint_row(source_value: str):
    cutoff = parse_start_date(START_DATE)
    after = None
    last_after = ""
    last_post_created_utc = ""
    last_post_fullname = ""
    processed_count = 0
    reached_cutoff = False

    while True:
        payload = fetch_page(source_value=source_value, page_size=PAGE_SIZE, after=after)
        listing_data = payload.get("data", {})
        posts = listing_data.get("children", [])
        if not posts:
            break

        current_after = listing_data.get("after")

        for post in posts:
            post_data = post.get("data", {})
            created_dt = datetime.fromtimestamp(post_data.get("created_utc", 0), tz=timezone.utc)
            if created_dt < cutoff:
                reached_cutoff = True
                break

            processed_count += 1
            last_post_created_utc = to_iso_utc(post_data.get("created_utc"))
            last_post_fullname = post_data.get("name", "")

        if reached_cutoff:
            last_after = after or ""
            break

        if not current_after:
            last_after = ""
            break

        last_after = current_after
        after = current_after

    now_iso = datetime.now(timezone.utc).isoformat()
    notes = f"{DEFAULT_NOTES} Start date={START_DATE}. Records seen={processed_count}."

    return {
        "id": str(uuid.uuid4()),
        "source_type": SOURCE_TYPE,
        "source_value": source_value,
        "endpoint_name": ENDPOINT_NAME,
        "last_after": last_after,
        "last_post_created_utc": last_post_created_utc,
        "last_post_fullname": last_post_fullname,
        "last_success_at": now_iso,
        "notes": notes,
    }


def main():
    rows = [build_checkpoint_row(source_value) for source_value in SOURCE_VALUES]
    df = pd.DataFrame(rows)
    print(df)
    df.to_csv(OUTPUT_CSV, index=False)
    print(f"Wrote {len(df)} checkpoints to {OUTPUT_CSV}")


if __name__ == "__main__":
    main()
