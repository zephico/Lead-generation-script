#!/usr/bin/env python3

import json
import subprocess
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import psycopg2
import requests
from psycopg2.extras import Json, execute_values


SUBREDDITS = ["forhire"]
PAGE_SIZE = 100
START_DATE = "2026-03-27"

POSTS_OUTPUT_CSV = "reddit_posts.csv"
SUBREDDITS_OUTPUT_CSV = "reddit_subreddits.csv"
CHECKPOINTS_OUTPUT_CSV = "reddit_checkpoints.csv"
PROJECT_ROOT = Path(__file__).resolve().parent.parent
COMPOSE_FILE = PROJECT_ROOT / "docker-compose.yml"

DB_HOST = "127.0.0.1"
DB_PORT = 5434
DB_NAME = "leadgen"
DB_USER = "postgres"
DB_PASSWORD = "postgres"

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

POST_COLUMNS = [
    "id",
    "reddit_post_id",
    "reddit_fullname",
    "subreddit_name",
    "subreddit_id",
    "author_name",
    "author_fullname",
    "title",
    "selftext",
    "selftext_html",
    "post_url",
    "permalink",
    "domain",
    "created_utc",
    "edited",
    "is_self",
    "post_hint",
    "score",
    "ups",
    "downs",
    "upvote_ratio",
    "num_comments",
    "num_crossposts",
    "link_flair_text",
    "author_flair_text",
    "over_18",
    "spoiler",
    "locked",
    "archived",
    "stickied",
    "distinguished",
    "removed_by_category",
    "thumbnail",
    "raw_payload",
    "fetched_at",
    "comments",
]

SUBREDDIT_COLUMNS = [
    "id",
    "reddit_subreddit_id",
    "display_name",
    "display_name_prefixed",
    "title",
    "public_description",
    "description",
    "subscribers",
    "active_user_count",
    "created_utc",
    "over18",
    "subreddit_type",
    "submission_type",
    "url",
    "lang",
    "raw_payload",
    "fetched_at",
]

CHECKPOINT_COLUMNS = [
    "id",
    "source_type",
    "source_value",
    "endpoint_name",
    "last_after",
    "last_post_created_utc",
    "last_post_fullname",
    "last_success_at",
    "notes",
]


def parse_start_date(start_date: str) -> datetime:
    return datetime.strptime(start_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)


def ensure_postgres_container():
    subprocess.run(
        ["docker", "compose", "-f", str(COMPOSE_FILE), "up", "-d", "postgres"],
        check=True,
        cwd=PROJECT_ROOT,
    )


def get_connection():
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
    )


def wait_for_postgres(max_wait_seconds: int = 60):
    deadline = time.time() + max_wait_seconds
    while time.time() < deadline:
        try:
            with get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT 1")
                    cur.fetchone()
            return
        except psycopg2.OperationalError:
            time.sleep(2)
    raise RuntimeError("PostgreSQL did not become ready in time.")


def create_tables():
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS reddit_posts (
                    id uuid PRIMARY KEY,
                    reddit_post_id varchar(20) UNIQUE NOT NULL,
                    reddit_fullname varchar(30),
                    subreddit_name varchar(100),
                    subreddit_id varchar(30),
                    author_name varchar(100),
                    author_fullname varchar(30),
                    title text,
                    selftext text,
                    selftext_html text,
                    post_url text,
                    permalink text,
                    domain varchar(255),
                    created_utc timestamptz,
                    edited boolean,
                    is_self boolean,
                    post_hint varchar(100),
                    score integer,
                    ups integer,
                    downs integer,
                    upvote_ratio numeric(5,2),
                    num_comments integer,
                    num_crossposts integer,
                    link_flair_text varchar(255),
                    author_flair_text varchar(255),
                    over_18 boolean,
                    spoiler boolean,
                    locked boolean,
                    archived boolean,
                    stickied boolean,
                    distinguished varchar(50),
                    removed_by_category varchar(100),
                    thumbnail text,
                    raw_payload jsonb,
                    fetched_at timestamptz,
                    comments text
                );
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS reddit_subreddits (
                    id uuid PRIMARY KEY,
                    reddit_subreddit_id varchar(30) UNIQUE NOT NULL,
                    display_name varchar(100),
                    display_name_prefixed varchar(120),
                    title text,
                    public_description text,
                    description text,
                    subscribers bigint,
                    active_user_count bigint,
                    created_utc timestamptz,
                    over18 boolean,
                    subreddit_type varchar(50),
                    submission_type varchar(50),
                    url text,
                    lang varchar(20),
                    raw_payload jsonb,
                    fetched_at timestamptz
                );
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS reddit_checkpoints (
                    id uuid PRIMARY KEY,
                    source_type varchar(50) NOT NULL,
                    source_value varchar(255) NOT NULL,
                    endpoint_name varchar(100) NOT NULL,
                    last_after varchar(50),
                    last_post_created_utc timestamptz,
                    last_post_fullname varchar(30),
                    last_success_at timestamptz,
                    notes text,
                    UNIQUE (source_type, source_value, endpoint_name)
                );
                """
            )
        conn.commit()


def normalize_json_rows(rows, json_columns):
    normalized = []
    for row in rows:
        item = row.copy()
        for column in json_columns:
            item[column] = Json(item[column])
        normalized.append(item)
    return normalized


def upsert_rows(table_name: str, columns: list[str], rows: list[dict], conflict_target: str, update_columns: list[str]):
    if not rows:
        return

    insert_sql = f"""
        INSERT INTO {table_name} ({", ".join(columns)})
        VALUES %s
        ON CONFLICT ({conflict_target}) DO UPDATE SET
        {", ".join(f"{column} = EXCLUDED.{column}" for column in update_columns)}
    """

    values = [[row.get(column) for column in columns] for row in rows]
    with get_connection() as conn:
        with conn.cursor() as cur:
            execute_values(cur, insert_sql, values)
        conn.commit()


def to_iso_utc(created_utc):
    if created_utc in (None, ""):
        return ""
    return datetime.fromtimestamp(created_utc, tz=timezone.utc).isoformat()


def build_comments_url(permalink: str) -> str:
    permalink = permalink.rstrip("/")
    return f"https://www.reddit.com{permalink}.json?sort=new"


def fetch_subreddit_new_page(subreddit: str, page_size: int, after: str | None = None):
    url = f"https://www.reddit.com/r/{subreddit}/new.json?limit={page_size}"
    if after:
        url = f"{url}&after={after}"
    response = requests.get(url, headers=HEADERS, timeout=30)
    response.raise_for_status()
    return response.json()


def fetch_subreddit_about(subreddit: str):
    url = f"https://www.reddit.com/r/{subreddit}/about.json"
    response = requests.get(url, headers=HEADERS, timeout=30)
    response.raise_for_status()
    return response.json()


def collect_posts_and_checkpoint(subreddit: str):
    cutoff = parse_start_date(START_DATE)
    posts_rows = []
    after = None
    last_after = ""
    last_post_created_utc = ""
    last_post_fullname = ""
    processed_count = 0
    reached_cutoff = False

    while True:
        payload = fetch_subreddit_new_page(subreddit=subreddit, page_size=PAGE_SIZE, after=after)
        listing_data = payload.get("data", {})
        posts = listing_data.get("children", [])
        if not posts:
            break

        current_after = listing_data.get("after")

        for post in posts:
            p = post.get("data", {})
            created_dt = datetime.fromtimestamp(p.get("created_utc", 0), tz=timezone.utc)
            if created_dt < cutoff:
                reached_cutoff = True
                break

            processed_count += 1
            last_post_created_utc = to_iso_utc(p.get("created_utc"))
            last_post_fullname = p.get("name", "")

            permalink = p.get("permalink", "")
            posts_rows.append(
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
            last_after = after or ""
            break

        if not current_after:
            last_after = ""
            break

        last_after = current_after
        after = current_after

    checkpoint_row = {
        "id": str(uuid.uuid4()),
        "source_type": "subreddit",
        "source_value": subreddit,
        "endpoint_name": "/new",
        "last_after": last_after,
        "last_post_created_utc": last_post_created_utc,
        "last_post_fullname": last_post_fullname,
        "last_success_at": datetime.now(timezone.utc).isoformat(),
        "notes": (
            "Checkpoint seeded from subreddit new listing for incremental load. "
            f"Start date={START_DATE}. Records seen={processed_count}."
        ),
    }

    return posts_rows, checkpoint_row


def collect_subreddit_row(subreddit: str):
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
    all_posts = []
    checkpoint_rows = []
    subreddit_rows = []

    for subreddit in SUBREDDITS:
        posts_rows, checkpoint_row = collect_posts_and_checkpoint(subreddit)
        all_posts.extend(posts_rows)
        checkpoint_rows.append(checkpoint_row)
        subreddit_rows.append(collect_subreddit_row(subreddit))

    posts_df = pd.DataFrame(all_posts)
    subreddits_df = pd.DataFrame(subreddit_rows)
    checkpoints_df = pd.DataFrame(checkpoint_rows)

    posts_path = PROJECT_ROOT / POSTS_OUTPUT_CSV
    subreddits_path = PROJECT_ROOT / SUBREDDITS_OUTPUT_CSV
    checkpoints_path = PROJECT_ROOT / CHECKPOINTS_OUTPUT_CSV

    posts_df.to_csv(posts_path, index=False)
    subreddits_df.to_csv(subreddits_path, index=False)
    checkpoints_df.to_csv(checkpoints_path, index=False)

    ensure_postgres_container()
    wait_for_postgres()
    create_tables()

    upsert_rows(
        "reddit_posts",
        POST_COLUMNS,
        normalize_json_rows(all_posts, ["raw_payload"]),
        "reddit_post_id",
        [column for column in POST_COLUMNS if column not in {"id", "reddit_post_id"}],
    )
    upsert_rows(
        "reddit_subreddits",
        SUBREDDIT_COLUMNS,
        normalize_json_rows(subreddit_rows, ["raw_payload"]),
        "reddit_subreddit_id",
        [column for column in SUBREDDIT_COLUMNS if column not in {"id", "reddit_subreddit_id"}],
    )
    upsert_rows(
        "reddit_checkpoints",
        CHECKPOINT_COLUMNS,
        checkpoint_rows,
        "source_type, source_value, endpoint_name",
        [column for column in CHECKPOINT_COLUMNS if column not in {"id", "source_type", "source_value", "endpoint_name"}],
    )

    print(f"Wrote {len(posts_df)} rows to {posts_path}")
    print(f"Wrote {len(subreddits_df)} rows to {subreddits_path}")
    print(f"Wrote {len(checkpoints_df)} rows to {checkpoints_path}")
    print("PostgreSQL tables created/updated in Docker successfully.")


if __name__ == "__main__":
    main()
