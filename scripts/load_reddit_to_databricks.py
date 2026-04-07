#!/usr/bin/env python3

"""Databricks/PySpark Reddit bronze loader.

Fetches Reddit data on the driver and merges it into Delta tables under the
`bronze_leads` schema.
"""

from __future__ import annotations

import json
import time
import uuid
from datetime import datetime, timezone

import requests
from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql import types as T


SUBREDDITS = ["forhire"]
PAGE_SIZE = 100
DEFAULT_START_DATE = "2026-03-27"
TARGET_SCHEMA = "bronze_leads"

POSTS_TABLE = f"{TARGET_SCHEMA}.reddit_posts"
SUBREDDITS_TABLE = f"{TARGET_SCHEMA}.reddit_subreddits"
CHECKPOINTS_TABLE = f"{TARGET_SCHEMA}.reddit_checkpoints"

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

POSTS_SCHEMA = T.StructType(
    [
        T.StructField("id", T.StringType(), False),
        T.StructField("reddit_post_id", T.StringType(), False),
        T.StructField("reddit_fullname", T.StringType(), True),
        T.StructField("subreddit_name", T.StringType(), True),
        T.StructField("subreddit_id", T.StringType(), True),
        T.StructField("author_name", T.StringType(), True),
        T.StructField("author_fullname", T.StringType(), True),
        T.StructField("title", T.StringType(), True),
        T.StructField("selftext", T.StringType(), True),
        T.StructField("selftext_html", T.StringType(), True),
        T.StructField("post_url", T.StringType(), True),
        T.StructField("permalink", T.StringType(), True),
        T.StructField("domain", T.StringType(), True),
        T.StructField("created_utc", T.TimestampType(), True),
        T.StructField("edited", T.BooleanType(), True),
        T.StructField("is_self", T.BooleanType(), True),
        T.StructField("post_hint", T.StringType(), True),
        T.StructField("score", T.IntegerType(), True),
        T.StructField("ups", T.IntegerType(), True),
        T.StructField("downs", T.IntegerType(), True),
        T.StructField("upvote_ratio", T.DoubleType(), True),
        T.StructField("num_comments", T.IntegerType(), True),
        T.StructField("num_crossposts", T.IntegerType(), True),
        T.StructField("link_flair_text", T.StringType(), True),
        T.StructField("author_flair_text", T.StringType(), True),
        T.StructField("over_18", T.BooleanType(), True),
        T.StructField("spoiler", T.BooleanType(), True),
        T.StructField("locked", T.BooleanType(), True),
        T.StructField("archived", T.BooleanType(), True),
        T.StructField("stickied", T.BooleanType(), True),
        T.StructField("distinguished", T.StringType(), True),
        T.StructField("removed_by_category", T.StringType(), True),
        T.StructField("thumbnail", T.StringType(), True),
        T.StructField("raw_payload", T.StringType(), True),
        T.StructField("fetched_at", T.TimestampType(), True),
        T.StructField("comments", T.StringType(), True),
    ]
)

SUBREDDITS_SCHEMA = T.StructType(
    [
        T.StructField("id", T.StringType(), False),
        T.StructField("reddit_subreddit_id", T.StringType(), False),
        T.StructField("display_name", T.StringType(), True),
        T.StructField("display_name_prefixed", T.StringType(), True),
        T.StructField("title", T.StringType(), True),
        T.StructField("public_description", T.StringType(), True),
        T.StructField("description", T.StringType(), True),
        T.StructField("subscribers", T.LongType(), True),
        T.StructField("active_user_count", T.LongType(), True),
        T.StructField("created_utc", T.TimestampType(), True),
        T.StructField("over18", T.BooleanType(), True),
        T.StructField("subreddit_type", T.StringType(), True),
        T.StructField("submission_type", T.StringType(), True),
        T.StructField("url", T.StringType(), True),
        T.StructField("lang", T.StringType(), True),
        T.StructField("raw_payload", T.StringType(), True),
        T.StructField("fetched_at", T.TimestampType(), True),
    ]
)

CHECKPOINTS_SCHEMA = T.StructType(
    [
        T.StructField("id", T.StringType(), False),
        T.StructField("source_type", T.StringType(), False),
        T.StructField("source_value", T.StringType(), False),
        T.StructField("endpoint_name", T.StringType(), False),
        T.StructField("last_after", T.StringType(), True),
        T.StructField("last_post_created_utc", T.TimestampType(), True),
        T.StructField("last_post_fullname", T.StringType(), True),
        T.StructField("last_success_at", T.TimestampType(), True),
        T.StructField("notes", T.StringType(), True),
    ]
)


def get_spark() -> SparkSession:
    try:
        return spark  # type: ignore[name-defined]
    except NameError:
        return SparkSession.builder.appName("reddit-bronze-loader").getOrCreate()


def parse_start_date(value: str) -> datetime:
    return datetime.strptime(value, "%Y-%m-%d").replace(tzinfo=timezone.utc)


def to_utc_datetime(epoch_seconds):
    if epoch_seconds in (None, ""):
        return None
    return datetime.fromtimestamp(epoch_seconds, tz=timezone.utc)


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


def table_exists(spark_session: SparkSession, table_name: str) -> bool:
    return spark_session.catalog.tableExists(table_name)


def get_cutoff_for_subreddit(spark_session: SparkSession, subreddit: str) -> datetime:
    if not table_exists(spark_session, CHECKPOINTS_TABLE):
        return parse_start_date(DEFAULT_START_DATE)

    row = (
        spark_session.table(CHECKPOINTS_TABLE)
        .where("source_type = 'subreddit'")
        .where(f"source_value = '{subreddit}'")
        .where("endpoint_name = '/new'")
        .select("last_post_created_utc")
        .limit(1)
        .collect()
    )
    if row and row[0]["last_post_created_utc"]:
        return row[0]["last_post_created_utc"]
    return parse_start_date(DEFAULT_START_DATE)


def collect_posts_and_checkpoint(subreddit: str, cutoff: datetime):
    posts_rows = []
    after = None
    last_after = ""
    last_post_created_utc = None
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
            post_data = post.get("data", {})
            created_dt = to_utc_datetime(post_data.get("created_utc"))
            if created_dt and created_dt < cutoff:
                reached_cutoff = True
                break

            processed_count += 1
            last_post_created_utc = created_dt
            last_post_fullname = post_data.get("name", "")
            permalink = post_data.get("permalink", "")

            posts_rows.append(
                {
                    "id": str(uuid.uuid4()),
                    "reddit_post_id": post_data.get("id", ""),
                    "reddit_fullname": post_data.get("name", ""),
                    "subreddit_name": post_data.get("subreddit", ""),
                    "subreddit_id": post_data.get("subreddit_id", ""),
                    "author_name": post_data.get("author", ""),
                    "author_fullname": post_data.get("author_fullname", ""),
                    "title": post_data.get("title", ""),
                    "selftext": post_data.get("selftext", ""),
                    "selftext_html": post_data.get("selftext_html", ""),
                    "post_url": post_data.get("url", ""),
                    "permalink": f"https://www.reddit.com{permalink}" if permalink else "",
                    "domain": post_data.get("domain", ""),
                    "created_utc": created_dt,
                    "edited": bool(post_data.get("edited")),
                    "is_self": bool(post_data.get("is_self")),
                    "post_hint": post_data.get("post_hint", ""),
                    "score": int(post_data.get("score", 0) or 0),
                    "ups": int(post_data.get("ups", 0) or 0),
                    "downs": int(post_data.get("downs", 0) or 0),
                    "upvote_ratio": float(post_data.get("upvote_ratio", 0) or 0),
                    "num_comments": int(post_data.get("num_comments", 0) or 0),
                    "num_crossposts": int(post_data.get("num_crossposts", 0) or 0),
                    "link_flair_text": post_data.get("link_flair_text", ""),
                    "author_flair_text": post_data.get("author_flair_text", ""),
                    "over_18": bool(post_data.get("over_18")),
                    "spoiler": bool(post_data.get("spoiler")),
                    "locked": bool(post_data.get("locked")),
                    "archived": bool(post_data.get("archived")),
                    "stickied": bool(post_data.get("stickied")),
                    "distinguished": post_data.get("distinguished", ""),
                    "removed_by_category": post_data.get("removed_by_category", ""),
                    "thumbnail": post_data.get("thumbnail", ""),
                    "raw_payload": json.dumps(post_data, ensure_ascii=False),
                    "fetched_at": datetime.now(timezone.utc),
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
        time.sleep(1)

    checkpoint_row = {
        "id": str(uuid.uuid4()),
        "source_type": "subreddit",
        "source_value": subreddit,
        "endpoint_name": "/new",
        "last_after": last_after,
        "last_post_created_utc": last_post_created_utc,
        "last_post_fullname": last_post_fullname,
        "last_success_at": datetime.now(timezone.utc),
        "notes": (
            "Checkpoint maintained by Databricks Reddit bronze loader. "
            f"Cutoff={cutoff.isoformat()}. Records seen={processed_count}."
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
        "subscribers": int(data.get("subscribers", 0) or 0),
        "active_user_count": int(data.get("active_user_count", 0) or 0),
        "created_utc": to_utc_datetime(data.get("created_utc")),
        "over18": bool(data.get("over18")),
        "subreddit_type": data.get("subreddit_type", ""),
        "submission_type": data.get("submission_type", ""),
        "url": f"https://www.reddit.com{data.get('url', '')}" if data.get("url") else "",
        "lang": data.get("lang", ""),
        "raw_payload": json.dumps(data, ensure_ascii=False),
        "fetched_at": datetime.now(timezone.utc),
    }


def create_or_merge_table(
    spark_session: SparkSession,
    table_name: str,
    dataframe,
    merge_condition: str,
    key_columns: list[str],
):
    if dataframe.rdd.isEmpty():
        return

    if not table_exists(spark_session, table_name):
        dataframe.write.format("delta").mode("overwrite").saveAsTable(table_name)
        return

    delta_table = DeltaTable.forName(spark_session, table_name)
    source_alias = "source"
    target_alias = "target"
    columns = dataframe.columns

    update_map = {
        column: f"{source_alias}.{column}"
        for column in columns
        if column not in key_columns
    }
    insert_map = {
        column: f"{source_alias}.{column}"
        for column in columns
    }

    (
        delta_table.alias(target_alias)
        .merge(dataframe.alias(source_alias), merge_condition)
        .whenMatchedUpdate(set=update_map)
        .whenNotMatchedInsert(values=insert_map)
        .execute()
    )


def main():
    spark_session = get_spark()
    spark_session.sql(f"CREATE SCHEMA IF NOT EXISTS {TARGET_SCHEMA}")

    all_posts = []
    subreddit_rows = []
    checkpoint_rows = []

    for subreddit in SUBREDDITS:
        cutoff = get_cutoff_for_subreddit(spark_session, subreddit)
        posts_rows, checkpoint_row = collect_posts_and_checkpoint(subreddit, cutoff)
        all_posts.extend(posts_rows)
        subreddit_rows.append(collect_subreddit_row(subreddit))
        checkpoint_rows.append(checkpoint_row)

    posts_df = spark_session.createDataFrame(all_posts, POSTS_SCHEMA)
    subreddits_df = spark_session.createDataFrame(subreddit_rows, SUBREDDITS_SCHEMA)
    checkpoints_df = spark_session.createDataFrame(checkpoint_rows, CHECKPOINTS_SCHEMA)

    create_or_merge_table(
        spark_session,
        POSTS_TABLE,
        posts_df,
        "target.reddit_post_id = source.reddit_post_id",
        ["reddit_post_id"],
    )
    create_or_merge_table(
        spark_session,
        SUBREDDITS_TABLE,
        subreddits_df,
        "target.reddit_subreddit_id = source.reddit_subreddit_id",
        ["reddit_subreddit_id"],
    )
    create_or_merge_table(
        spark_session,
        CHECKPOINTS_TABLE,
        checkpoints_df,
        """
        target.source_type = source.source_type
        AND target.source_value = source.source_value
        AND target.endpoint_name = source.endpoint_name
        """,
        ["source_type", "source_value", "endpoint_name"],
    )

    print(f"Loaded {len(all_posts)} reddit posts into {POSTS_TABLE}")
    print(f"Loaded {len(subreddit_rows)} subreddit rows into {SUBREDDITS_TABLE}")
    print(f"Loaded {len(checkpoint_rows)} checkpoint rows into {CHECKPOINTS_TABLE}")


if __name__ == "__main__":
    main()
