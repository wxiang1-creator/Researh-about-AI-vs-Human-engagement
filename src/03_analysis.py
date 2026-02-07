import pandas as pd
import numpy as np
from pathlib import Path

df = pd.read_parquet("data/raw/reddit_posts.parquet")

# 清洗文本
df["text"] = (df["title"].fillna("") + " " + df["selftext"].fillna("")).str.lower()

# engagement 指标
df["engagement"] = np.log1p(df["score"].clip(lower=0))

print(df[["score", "engagement"]].describe())

Path("outputs/tables").mkdir(parents=True, exist_ok=True)
df.to_csv("outputs/tables/reddit_with_engagement.csv", index=False)

print("Saved: outputs/tables/reddit_with_engagement.csv")
