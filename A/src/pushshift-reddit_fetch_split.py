#!/usr/bin/env python3
"""Fetch/sample posts from Hugging Face dataset fddemarco/pushshift-reddit using streaming.

Output file: pushshift-reddit_post.parquet
Output columns (exact order):
['kind', 'type', 'id', 'subreddit_id', 'subreddit_name', 'created_utc', 'score', 'selftext', 'title', 'num_comments']

Filtering:
- Drop rows where cleaned selftext length < 20 characters

Notes:
- This script samples (does NOT download full dataset) by iterating a streaming dataset
  and stopping at --max_posts.
"""

import argparse
import os
from pathlib import Path

import pandas as pd
from datasets import load_dataset

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_OUT_DIR = PROJECT_ROOT / "data" / "processed"


def clean_str(x) -> str:
    """Normalize text fields: stringify, normalize newlines, strip."""
    if x is None:
        return ""
    return str(x).replace("\r\n", "\n").replace("\r", "\n").strip()


def to_utc_datetime(x):
    """Accept datetime, numeric seconds, or parseable string; return UTC timestamp."""
    if x is None:
        return None

    # already a python datetime
    if hasattr(x, "tzinfo"):
        ts = pd.Timestamp(x)
        return ts.tz_convert("UTC") if ts.tzinfo else pd.Timestamp(x, tz="UTC")

    # numeric seconds
    if isinstance(x, (int, float)):
        return pd.to_datetime(x, unit="s", utc=True)

    # string-like
    try:
        return pd.to_datetime(x, utc=True)
    except Exception:
        return None


def main():
    ap = argparse.ArgumentParser(description="Stream-sample Pushshift Reddit posts and write to parquet")
    ap.add_argument("--dataset", default="fddemarco/pushshift-reddit", help="HF dataset name")
    ap.add_argument("--split", default="train", help="Dataset split (default: train)")
    ap.add_argument("--out_dir", default=str(DEFAULT_OUT_DIR), help="Output directory")
    ap.add_argument("--max_posts", type=int, default=2000, help="Max posts to write")
    ap.add_argument(
        "--to_datetime",
        action="store_true",
        default=False,
        help="Convert created_utc to UTC pandas Timestamp",
    )
    args = ap.parse_args()

    os.makedirs(args.out_dir, exist_ok=True)

    ds = load_dataset(args.dataset, split=args.split, streaming=True)

    rows = []
    for rec in ds:
        # Per your spec
        subreddit_id = rec.get("subreddit_id")
        subreddit_name = rec.get("subreddit")  # dataset uses `subreddit` as name

        selftext = clean_str(rec.get("selftext"))
        if len(selftext) < 20:
            continue

        title = clean_str(rec.get("title"))

        row = {
            "kind": "post_internal",
            "type": "post",
            "id": rec.get("id"),
            "subreddit_id": subreddit_id,
            "subreddit_name": subreddit_name,
            "created_utc": to_utc_datetime(rec.get("created_utc")) if args.to_datetime else rec.get("created_utc"),
            "score": rec.get("score"),
            "selftext": selftext,
            "title": title,
            "num_comments": rec.get("num_comments"),
        }

        rows.append(row)
        if len(rows) >= args.max_posts:
            break

    out_path = os.path.join(args.out_dir, "pushshift-reddit_post.parquet")

    df = pd.DataFrame(rows)

    # Force exact column order
    col_order = [
        "kind",
        "type",
        "id",
        "subreddit_id",
        "subreddit_name",
        "created_utc",
        "score",
        "selftext",
        "title",
        "num_comments",
    ]

    for c in col_order:
        if c not in df.columns:
            df[c] = pd.NA

    df = df[col_order]

    # Optional: ensure (type,id) unique if duplicates appear
    df = df.drop_duplicates(subset=["type", "id"], keep="first")

    df.to_parquet(out_path, index=False)

    print(f"Wrote {len(df)} posts to {out_path}")


if __name__ == "__main__":
    main()
