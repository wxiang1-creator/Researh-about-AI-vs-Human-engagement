import pandas as pd
from pathlib import Path

AIRULES_DIR = Path("data/raw/data/raw/AIRules")

def main():
    # 递归列出所有 csv/json
    files = list(AIRULES_DIR.rglob("*.csv")) + list(AIRULES_DIR.rglob("*.json"))
    print("Found files:")
    for f in files[:50]:
        print(" -", f)

    # 尝试读取第一个 csv 看 schema（你之后把最关键的 csv 改成固定文件）
    csv_files = list(AIRULES_DIR.rglob("*.csv"))
    if csv_files:
        f = csv_files[0]
        df = pd.read_csv(f)
        print("\nPreview:", f)
        print("Columns:", df.columns.tolist())
        print(df.head(5))

if __name__ == "__main__":
    main()
