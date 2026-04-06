# Lead Generation Script

This project fetches Reddit data for configured subreddits, generates CSV exports, starts PostgreSQL in Docker, creates the required tables, and loads the generated data into PostgreSQL.

## Included Scripts

- `scripts/export_reddit_all_csvs.py`
- `scripts/export_reddit_posts_csv.py`
- `scripts/export_reddit_subreddits_csv.py`
- `scripts/export_reddit_checkpoints_csv.py`

## Setup

Install dependencies:

```bash
pip install -r requirements.txt
```

Make sure Docker Desktop is running.

## Main Run

Run the combined workflow:

```bash
python3 scripts/export_reddit_all_csvs.py
```

This will:

- generate `reddit_posts.csv`
- generate `reddit_subreddits.csv`
- generate `reddit_checkpoints.csv`
- start PostgreSQL in Docker
- create the three tables
- load the exported data into PostgreSQL

## PostgreSQL Connection

Docker PostgreSQL is exposed on:

- Host: `127.0.0.1`
- Port: `5434`
- Database: `leadgen`
- User: `postgres`
- Password: `postgres`

## Postico URL

```text
postgresql://postgres:postgres@127.0.0.1:5434/leadgen
```
