import os
import pandas as pd

SRC = os.environ.get("SRC", "./data/adult.csv")

OUT_MASTER  = "./master_data/adult_master.csv"
OUT_WORKER1 = "./worker1_data/adult_worker1.csv"
OUT_WORKER2 = "./worker2_data/adult_worker2.csv"

os.makedirs(os.path.dirname(OUT_MASTER), exist_ok=True)
os.makedirs(os.path.dirname(OUT_WORKER1), exist_ok=True)
os.makedirs(os.path.dirname(OUT_WORKER2), exist_ok=True)

def main():
    df = pd.read_csv(SRC).sample(frac=1, random_state=42).reset_index(drop=True)

    n = len(df)
    master  = df.iloc[: int(0.4 * n)]
    worker1 = df.iloc[int(0.4 * n) : int(0.7 * n)]
    worker2 = df.iloc[int(0.7 * n) :]

    master.to_csv (OUT_MASTER,  index=False)
    worker1.to_csv(OUT_WORKER1, index=False)
    worker2.to_csv(OUT_WORKER2, index=False)

    print(f"✅ Wrote: {OUT_MASTER}   ({len(master)} rows)")
    print(f"✅ Wrote: {OUT_WORKER1}  ({len(worker1)} rows)")
    print(f"✅ Wrote: {OUT_WORKER2}  ({len(worker2)} rows)")

if __name__ == "__main__":
    main()
