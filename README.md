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
- create the `bronze_leads` schema and the three tables inside it
- load the exported data into PostgreSQL

## PostgreSQL Connection

Docker PostgreSQL is exposed on:

- Host: `127.0.0.1`
- Port: `5434`
- Database: `leadgen`
- User: `postgres`
- Password: `postgres`

## PostgreSQL Schema

The loader creates and writes tables into:

- Schema: `bronze_leads`

## Postico URL

```text
postgresql://postgres:postgres@127.0.0.1:5434/leadgen
```

## Databricks / PySpark

For Databricks, use:

- `scripts/load_reddit_to_databricks.py`

What it does:

- fetches Reddit data on the driver
- creates Spark DataFrames
- creates schema `bronze_leads`
- merges into Delta tables:
  - `bronze_leads.reddit_posts`
  - `bronze_leads.reddit_subreddits`
  - `bronze_leads.reddit_checkpoints`

Recommended usage:

```python
# Databricks notebook or Databricks Python job
%run ./scripts/load_reddit_to_databricks.py
```

The Databricks loader uses the checkpoint table for incremental cutoff lookup when it already exists.
