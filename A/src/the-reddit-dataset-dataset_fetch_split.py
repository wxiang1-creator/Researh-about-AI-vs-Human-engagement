#!/usr/bin/env python3
import argparse
import os
import pandas as pd
from datasets import load_dataset, get_dataset_config_names, ClassLabel
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent  
DEFAULT_OUT_DIR = PROJECT_ROOT / "data" / "processed"

SUBREDDIT_ID = "2r97t"
SUBREDDIT_NAME = "datasets"


def add_subreddit_cols(df):
    df["subreddit_id"] = SUBREDDIT_ID
    df["subreddit_name"] = SUBREDDIT_NAME
    return df

def clean_str(x):
    if x is None:
        return ""
    return str(x).replace("\r\n", "\n").replace("\r", "\n").strip()

def is_removed(s):
    s2 = str(s).strip().lower()
    if s2 in ["[deleted]", "[removed]"]:
        return True
    if "removed by reddit" in s2:
        return True
    return False


def normalize_type(raw_type, features):
    if isinstance(raw_type, str):
        return raw_type
    if isinstance(raw_type, int) and features is not None:
        ft = features.get("type")
        if isinstance(ft, ClassLabel):
            return ft.int2str(raw_type)
    if raw_type == 1:
        return "comment"
    if raw_type == 0:
        return "post"
    return None

def to_utc_datetime(x):
    """Accept datetime, numeric seconds, or parseable string; return UTC timestamp."""
    if x is None:
        return None
    # already a python datetime (streaming gives this)
    if hasattr(x, "tzinfo"):
        return pd.Timestamp(x).tz_convert("UTC") if pd.Timestamp(x).tzinfo else pd.Timestamp(x, tz="UTC")
    # numeric seconds
    if isinstance(x, (int, float)):
        return pd.to_datetime(x, unit="s", utc=True)
    # string-like
    try:
        return pd.to_datetime(x, utc=True)
    except Exception:
        return None

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dataset", default="SocialGrep/the-reddit-dataset-dataset")
    ap.add_argument("--posts_config", default="posts")
    ap.add_argument("--comments_config", default="comments")
    ap.add_argument("--out_dir", default=str(DEFAULT_OUT_DIR))
    ap.add_argument("--max_posts", type=int, default=2000)
    ap.add_argument("--max_comments", type=int, default=2000)
    ap.add_argument("--to_datetime", action="store_true", default=False)
    args = ap.parse_args()

    # (optional) validate configs
    try:
        configs = get_dataset_config_names(args.dataset)
        if args.posts_config not in configs or args.comments_config not in configs:
            print("Available configs:", configs)
    except Exception:
        pass

    os.makedirs(args.out_dir, exist_ok=True)

    posts = load_dataset(args.dataset, name=args.posts_config, split="train", streaming=True)
    comments = load_dataset(args.dataset, name=args.comments_config, split="train", streaming=True)

    # ---------- COMMENTS ----------
    comment_rows = []
    for rec in comments:
        t = normalize_type(rec.get("type"), getattr(comments, "features", None))
        if t is not None and t != "comment":
            continue

        permalink = clean_str(rec.get("permalink"))
        body = clean_str(rec.get("body"))

        if is_removed(body):
            continue

        # B) empty body => drop
        if len(body) < 20:
            continue

        row = {
            "kind": "comment",
            "type": "comment",
            "id": rec.get("id"),
            "created_utc": to_utc_datetime(rec.get("created_utc")) if args.to_datetime else rec.get("created_utc"),
            "score": rec.get("score"),
            "permalink": permalink,
            "body": body,
            "sentiment": rec.get("sentiment", None),
        }

        comment_rows.append(row)
        if len(comment_rows) >= args.max_comments:
            break

    df_comments = pd.DataFrame(comment_rows)
    df_comments = df_comments[["kind","type","id","created_utc","score","permalink","body","sentiment"]]
    df_comments.to_parquet(os.path.join(args.out_dir, "base_comments.parquet"), index=False)

    # ---------- POSTS ----------
    post_rows = []
    for rec in posts:
        t = normalize_type(rec.get("type"), getattr(posts, "features", None))
        if t is not None and t != "post":
            continue

        # 先 clean
        permalink = clean_str(rec.get("permalink"))
        title = clean_str(rec.get("title"))
        selftext = clean_str(rec.get("selftext"))
        url = clean_str(rec.get("url"))
        domain = clean_str(rec.get("domain"))

        # 用 clean 后的 domain 判 kind
        kind = "post_internal" if domain == "self.datasets" else "post_external"

        # B) 第一级：关键字段 strip 为空就丢
        if kind == "post_internal":
            # 你定义 internal 必须有 title + selftext
            if len(title) == 0 or len(selftext) < 20:
                continue
            if is_removed(selftext) or is_removed(title):
                continue

        else:
            # external 必须有 title + url + domain
            if len(title) < 5 or len(url) == 0 or len(domain) == 0:
                continue
            if is_removed(title):
                continue

        row = {
            "kind": kind,
            "type": "post",
            "id": rec.get("id"),
            "created_utc": to_utc_datetime(rec.get("created_utc")) if args.to_datetime else rec.get("created_utc"),
            "score": rec.get("score"),
            "permalink": permalink,
            "title": title,
            "selftext": selftext,
            "url": url,
            "domain": domain,
        }

        post_rows.append(row)
        if len(post_rows) >= args.max_posts:
            break

    df_posts = pd.DataFrame(post_rows)

    df_post_internal = df_posts[df_posts["kind"]=="post_internal"].copy()
    df_post_external = df_posts[df_posts["kind"]=="post_external"].copy()

    df_post_internal = df_post_internal[["kind","type","id","created_utc","score","permalink","selftext","title"]]
    df_post_external = df_post_external[["kind","type","id","created_utc","score","permalink","title","url","domain"]]

    df_post_internal.to_parquet(os.path.join(args.out_dir, "base_post_internal.parquet"), index=False)
    df_post_external.to_parquet(os.path.join(args.out_dir, "base_post_external.parquet"), index=False)

    # ---------- ALL ----------
    all_cols = [
        "kind","type","id",
        "subreddit_id","subreddit_name",
        "created_utc","score","permalink",
        "body","sentiment","selftext","title","url","domain"
        ]

    def ensure_cols(df):
        for c in all_cols:
            if c not in df.columns:
                df[c] = pd.NA
        return df[all_cols]

    df_all = pd.concat([ensure_cols(df_comments), ensure_cols(df_post_internal), ensure_cols(df_post_external)], ignore_index=True)

    df_all["subreddit_id"] = SUBREDDIT_ID
    df_all["subreddit_name"] = SUBREDDIT_NAME
    df_all = df_all[all_cols]   # 保证列顺序

    df_all.to_parquet(os.path.join(args.out_dir, "base_all.parquet"), index=False)

    print("Wrote 4 files to", args.out_dir)
    print("comments:", len(df_comments), "internal:", len(df_post_internal), "external:", len(df_post_external), "all:", len(df_all))

if __name__ == "__main__":
    main()

    