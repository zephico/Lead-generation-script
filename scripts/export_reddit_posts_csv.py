#!/usr/bin/env python3

import uuid
from datetime import datetime, timezone

import pandas as pd
import requests


SUBREDDIT = "forhire"
PAGE_SIZE = 100
START_DATE = "2026-03-27"
OUTPUT_CSV = "1.csv"


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


def build_comments_url(permalink: str) -> str:
    permalink = permalink.rstrip("/")
    return f"https://www.reddit.com{permalink}.json?sort=new"


def to_iso_utc(created_utc):
    if created_utc in (None, ""):
        return ""
    return datetime.fromtimestamp(created_utc, tz=timezone.utc).isoformat()


def parse_start_date(start_date: str) -> datetime:
    return datetime.strptime(start_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)


def fetch_page(subreddit: str, page_size: int, after: str | None = None):
    url = f"https://www.reddit.com/r/{subreddit}/new.json?limit={page_size}"
    if after:
        url = f"{url}&after={after}"
    response = requests.get(url, headers=HEADERS, timeout=30)
    response.raise_for_status()
    return response.json()


def fetch_posts(subreddit: str, start_date: str, page_size: int):
    cutoff = parse_start_date(start_date)
    rows = []
    after = None

    while True:
        data = fetch_page(subreddit=subreddit, page_size=page_size, after=after)
        posts = data["data"]["children"]
        if not posts:
            break

        reached_cutoff = False
        for post in posts:
            p = post["data"]
            created_dt = datetime.fromtimestamp(p.get("created_utc", 0), tz=timezone.utc)
            if created_dt < cutoff:
                reached_cutoff = True
                break

            permalink = p.get("permalink", "")
            rows.append(
                {
                    "id": str(uuid.uuid4()),
                    "reddit_post_id": p.get("id", ""),
                    "reddit_fullname": p.get("name", ""),
                    "subreddit_name": p.get("subreddit", ""),
                    "subreddit_id": p.get("subreddit_id", ""),
                    "author_name": p.get("author", ""),
                    "author_fullname": p.get("author_fullname", ""),
                    "title": p.get("title", ""),
                    "selftext": p.get("selftext", ""),
                    "selftext_html": p.get("selftext_html", ""),
                    "post_url": p.get("url", ""),
                    "permalink": f"https://www.reddit.com{permalink}" if permalink else "",
                    "domain": p.get("domain", ""),
                    "created_utc": to_iso_utc(p.get("created_utc")),
                    "edited": bool(p.get("edited")),
                    "is_self": bool(p.get("is_self")),
                    "post_hint": p.get("post_hint", ""),
                    "score": p.get("score", 0),
                    "ups": p.get("ups", 0),
                    "downs": p.get("downs", 0),
                    "upvote_ratio": p.get("upvote_ratio", 0),
                    "num_comments": p.get("num_comments", 0),
                    "num_crossposts": p.get("num_crossposts", 0),
                    "link_flair_text": p.get("link_flair_text", ""),
                    "author_flair_text": p.get("author_flair_text", ""),
                    "over_18": bool(p.get("over_18")),
                    "spoiler": bool(p.get("spoiler")),
                    "locked": bool(p.get("locked")),
                    "archived": bool(p.get("archived")),
                    "stickied": bool(p.get("stickied")),
                    "distinguished": p.get("distinguished", ""),
                    "removed_by_category": p.get("removed_by_category", ""),
                    "thumbnail": p.get("thumbnail", ""),
                    "raw_payload": p,
                    "fetched_at": datetime.now(timezone.utc).isoformat(),
                    "comments": build_comments_url(permalink) if permalink else "",
                }
            )

        if reached_cutoff:
            break

        after = data["data"].get("after")
        if not after:
            break

    return rows


def main():
    rows = fetch_posts(SUBREDDIT, START_DATE, PAGE_SIZE)
    df = pd.DataFrame(rows)
    print(df)
    df.to_csv(OUTPUT_CSV, index=False)
    print(f"Wrote {len(df)} posts from {START_DATE} onward to {OUTPUT_CSV}")


if __name__ == "__main__":
    main()
