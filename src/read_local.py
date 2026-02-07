import pandas as pd

df = pd.read_parquet("data/raw/reddit_posts.parquet")
print("Columns:", df.columns.tolist())
print(df.head())
