import numpy as np
import pandas as pd
import torch
from transformers import AutoTokenizer, AutoModelForSequenceClassification

IN_PATH  = "A/data/processed/base_all.parquet"
OUT_PATH = "A/data/processed/labeled_all.parquet"

MODEL_NAME = "roberta-base-openai-detector"  # 开源 detector（会自动下载）
MIN_CHARS = 200        # 太短文本不打（不准+浪费时间）
THR = 0.5
BATCH_SIZE = 32

def build_text(row):
    typ = str(row.get("type") or "").lower()
    if typ == "comment":
        body = row.get("body")
        return body if isinstance(body, str) else ""
    else:
        title = row.get("title") if isinstance(row.get("title"), str) else ""
        selftext = row.get("selftext") if isinstance(row.get("selftext"), str) else ""
        txt = (title + "\n\n" + selftext).strip() if selftext else title.strip()
        return txt

def main():
    df = pd.read_parquet(IN_PATH)

    # 组装要检测的文本
    df["content_text"] = df.apply(build_text, axis=1)
    texts = df["content_text"].fillna("").astype(str).tolist()

    device = "cuda" if torch.cuda.is_available() else "cpu"
    print("device:", device)

    tokenizer = AutoTokenizer.from_pretrained(MODEL_NAME)
    model = AutoModelForSequenceClassification.from_pretrained(MODEL_NAME).to(device)
    model.eval()

    probs = np.full(len(texts), np.nan, dtype=float)
    idxs = [i for i, t in enumerate(texts) if len(t) >= MIN_CHARS]

    for start in range(0, len(idxs), BATCH_SIZE):
        batch_ids = idxs[start:start+BATCH_SIZE]
        batch_txt = [texts[i] for i in batch_ids]

        enc = tokenizer(batch_txt, truncation=True, padding=True, max_length=512, return_tensors="pt").to(device)
        with torch.no_grad():
            out = model(**enc)
            logits = out.logits.detach().cpu()

        # 二分类：取 AI 类（label=1）的概率
        p_ai = torch.softmax(logits, dim=1)[:, 1].numpy()
        probs[batch_ids] = p_ai

        if start % (BATCH_SIZE * 20) == 0:
            print(f"progress: {start}/{len(idxs)}")

    df["ai_prob"] = probs
    df["ai_label"] = np.where(np.isfinite(df["ai_prob"]),
                              np.where(df["ai_prob"] >= THR, "ai", "human"),
                              "unknown")

    df.to_parquet(OUT_PATH, index=False)
    print("saved:", OUT_PATH)
    print(df["ai_label"].value_counts(dropna=False))

if __name__ == "__main__":
    main()
