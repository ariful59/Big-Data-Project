
from pyspark.sql import SparkSession

STAGE_DIR   = "/app/data"
UNIFIED_OUT = "/app/output/unified"

def main():
    spark = SparkSession.builder.appName("Unify").getOrCreate()

    master  = spark.read.parquet(f"{STAGE_DIR}/master")
    worker1 = spark.read.parquet(f"{STAGE_DIR}/worker1")
    worker2 = spark.read.parquet(f"{STAGE_DIR}/worker2")

    unified = master.union(worker1).union(worker2)

    print("Unified schema:")
    unified.printSchema()
    print("Unified count:", unified.count())

    (unified.coalesce(1).write.mode("overwrite").parquet(UNIFIED_OUT))
    print(f"✅ Unified Parquet written to: {UNIFIED_OUT}")

    spark.stop()

from pyspark.sql import SparkSession

STAGE_DIR   = "/app/data"
UNIFIED_OUT = "/app/output/unified"

def main():
    spark = SparkSession.builder.appName("Unify").getOrCreate()

    master  = spark.read.parquet(f"{STAGE_DIR}/master")
    worker1 = spark.read.parquet(f"{STAGE_DIR}/worker1")
    worker2 = spark.read.parquet(f"{STAGE_DIR}/worker2")

    unified = master.union(worker1).union(worker2)

    print("Unified schema:")
    unified.printSchema()
    print("Unified count:", unified.count())

    (unified.coalesce(1).write.mode("overwrite").parquet(UNIFIED_OUT))
    print(f"✅ Unified Parquet written to: {UNIFIED_OUT}")

    import os, glob, shutil
    part = glob.glob(os.path.join(UNIFIED_OUT, "part-*.parquet"))[0]
    shutil.move(part, os.path.join(UNIFIED_OUT, "unified.parquet"))

    spark.stop()

if __name__ == "__main__":
    main()

if __name__ == "__main__":
    main()
