
from pyspark.sql import SparkSession

INPUT_FILE  = "/app/output/unified/unified.parquet"  # your single parquet
UNIFIED_OUT = "/app/output/final_unified"

spark = SparkSession.builder.appName("Unify-SQL-Adult").getOrCreate()

df = spark.read.parquet(INPUT_FILE)

# Register for SQL
df.createOrReplaceTempView("unified")

# --- Exploration ---
spark.sql("""
  SELECT COUNT(*) AS rows, COUNT(DISTINCT fnlwgt) AS unique_ids
  FROM unified
""").show()

spark.sql("""
  SELECT workclass, COUNT(*) AS n
  FROM unified
  GROUP BY workclass
  ORDER BY n DESC
""").show()






# --- Cleaning ---



cleaned = spark.sql("""
  SELECT
    CAST(age AS INT) AS age,
    NULLIF(UPPER(workclass), '?')         AS workclass,
    CAST(fnlwgt AS INT)                AS fnlwgt,
    NULLIF(UPPER(education), '?')         AS education,
    CAST(`education.num` AS INT)          AS education_num,
    NULLIF(UPPER(`marital.status`), '?')  AS marital_status,
    NULLIF(UPPER(occupation), '?')        AS occupation,
    NULLIF(UPPER(relationship), '?')      AS relationship,
    NULLIF(UPPER(race), '?')              AS race,
    NULLIF(UPPER(sex), '?')               AS sex,
    CAST(`capital.gain` AS INT)           AS capital_gain,
    CAST(`capital.loss` AS INT)           AS capital_loss,
    CAST(`hours.per.week` AS INT)         AS hours_per_week,
    NULLIF(UPPER(`native.country`), '?')  AS native_country,
    UPPER(income)                         AS income
  FROM unified
  WHERE age IS NOT NULL AND age >= 0
""")
cleaned.createOrReplaceTempView("cleaned")

# --- Feature engineering ---
features = spark.sql("""
  SELECT
    age,
    workclass,
    education,
    education_num,
    marital_status,
    occupation,
    relationship,
    race,
    sex,
    income,
    -- target
    CASE WHEN income = '>50K' THEN 1 ELSE 0 END AS is_high_income,

    -- buckets
    CASE
      WHEN age < 30 THEN 'young'
      WHEN age < 50 THEN 'mid'
      ELSE 'senior'
    END AS age_bucket,

    -- simple ratio feature
    (COALESCE(capital_gain, 0) - COALESCE(capital_loss, 0)) / (COALESCE(hours_per_week, 0) + 1.0) AS capital_ratio,

    -- keep numeric columns (renamed without dots)
    capital_gain,
    capital_loss,
    hours_per_week,
    fnlwgt,

    -- convenience categorical
    native_country
  FROM cleaned
""")

# --- Repartitioning ---
spark.conf.set("spark.sql.shuffle.partitions", "200")  # tune to your cluster/data size
features = features.repartition(200, features["workclass"])  # or income/race/etc.

print("Schema:")
features.printSchema()
print("Count:", features.count())

# --- Write ---
features.coalesce(1).write.mode("overwrite").parquet(UNIFIED_OUT)
print(f"✅ Wrote to: {UNIFIED_OUT}")

import os, glob, shutil
part = glob.glob(os.path.join(UNIFIED_OUT, "part-*.parquet"))[0]
shutil.move(part, os.path.join(UNIFIED_OUT, "final_dataset.parquet"))


