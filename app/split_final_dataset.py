
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import rand, row_number, monotonically_increasing_id
from pyspark.sql.window import Window

# --- Config via environment (with sensible defaults) ---
SRC = os.environ.get("SRC", "/app/output/final_unified/final_dataset.parquet")

OUT_MASTER  = os.environ.get("OUT_MASTER",  "/app/data/master")
OUT_WORKER1 = os.environ.get("OUT_WORKER1", "/app/data/worker1")
OUT_WORKER2 = os.environ.get("OUT_WORKER2", "/app/data/worker2")

def main():
    spark = (SparkSession.builder
             .appName("SplitParquetIntoThreeDatasets")
             .config("spark.sql.adaptive.enabled", "true")
             .getOrCreate())

    # Read Parquet (supports single file or dataset directory)
    df = spark.read.parquet(SRC)

    # Deterministic shuffle, similar to pandas .sample(frac=1, random_state=42)
    df = df.orderBy(rand(seed=42))

    # Count rows
    n = df.count()
    if n == 0:
        print(f"⚠️ No rows found in {SRC}. Nothing to write.")
        spark.stop()
        return

    n_master  = int(0.40 * n)
    n_worker1 = int(0.30 * n)
    # worker2 gets the remainder

    # Assign a stable row index after shuffle
    w = Window.orderBy(monotonically_increasing_id())
    df_idx = df.withColumn("_rn", row_number().over(w) - 1)

    # Slice by index ranges
    master  = df_idx.filter(df_idx._rn < n_master).drop("_rn")
    worker1 = df_idx.filter((df_idx._rn >= n_master) & (df_idx._rn < n_master + n_worker1)).drop("_rn")
    worker2 = df_idx.filter(df_idx._rn >= n_master + n_worker1).drop("_rn")

    # Write outputs as Parquet datasets (directories). Overwrite if they exist.
    master.write.mode("overwrite").parquet(OUT_MASTER)
    worker1.write.mode("overwrite").parquet(OUT_WORKER1)
    worker2.write.mode("overwrite").parquet(OUT_WORKER2)

    print(f"✅ Wrote: {OUT_MASTER}   ({master.count()} rows)")
    print(f"✅ Wrote: {OUT_WORKER1}  ({worker1.count()} rows)")
    print(f"✅ Wrote: {OUT_WORKER2}  ({worker2.count()} rows)")

    spark.stop()

if __name__ == "__main__":
    main()