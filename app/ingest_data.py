
# /app/ingest_node.py
import os
from pyspark.sql import SparkSession

# For each node, set these via environment variables in the command line:
#   NODE_ID   = master | worker1 | worker2
#   NODE_FILE = /master_data/adult_master.csv OR /worker1_data/... OR /worker2_data/...
# Output base (shared by all): /app/data
NODE_ID   = os.environ.get("NODE_ID", "unknown")
NODE_FILE = os.environ.get("NODE_FILE")  # must be set per node
OUT_BASE  = os.environ.get("OUT_BASE", "/app/data")

def main():
    if not NODE_FILE or not os.path.exists(NODE_FILE):
        raise FileNotFoundError(f"Input not found: {NODE_FILE}")

    spark = SparkSession.builder.appName(f"Ingest_{NODE_ID}").getOrCreate()

    df = (spark.read
                .option("header", True)
                .option("inferSchema", True)
                .csv(NODE_FILE))

    print(f"[{NODE_ID}] File: {NODE_FILE}")
    print(f"[{NODE_ID}] Schema:")
    df.printSchema()
    print(f"[{NODE_ID}] Count: {df.count()}")

    out_dir = f"{OUT_BASE}/{NODE_ID}"
    (df.write.mode("overwrite").parquet(out_dir))
    print(f"[{NODE_ID}] ✅ Wrote Parquet to {out_dir}")

    spark.stop()

if __name__ == "__main__":
    main()
