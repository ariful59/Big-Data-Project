
from pyspark.sql import SparkSession

INPUT_FILE  = "/app/output/unified/unified.parquet"  # your single parquet
UNIFIED_OUT = "/app/output/final_unified"

spark = SparkSession.builder.appName("Unify-SQL-Adult").getOrCreate()

df = spark.read.parquet(INPUT_FILE)

rename_map = {
    "education.num": "education_num",
    "marital.status": "marital_status",
    "hours.per.week": "hours_per_week",
    "capital.gain": "capital_gain",
    "capital.loss": "capital_loss",
    "native.country": "native_country",
}

for old, new in rename_map.items():
    if old in df.columns:
        df = df.withColumnRenamed(old, new)

# --- Repartitioning ---
spark.conf.set("spark.sql.shuffle.partitions", "100")
df = df.repartition(100, df["workclass"])  #you can choose other feature or no feature, fix value

# Register for SQL
df.createOrReplaceTempView("unified")

# --- Exploration ---
#Counting total rows
spark.sql("""
    DESCRIBE TABLE unified;
""").show()

#Describing the schema
spark.sql("""
  SELECT COUNT(*)
  FROM unified
""").show()

# Finding the maximum capital_gain for each unique combination of demographic and work-related columns
spark.sql("""
SELECT 
  age, 
  education, 
  native_country, 
  hours_per_week, 
  occupation, 
  MAX(capital_gain) AS max_capital_gain
FROM unified
GROUP BY 
  age, 
  education, 
  native_country, 
  hours_per_week, 
  occupation
LIMIT 10;
""").show()

spark.sql("""
  SELECT workclass, COUNT(*) AS n
  FROM unified
  GROUP BY workclass
  ORDER BY n DESC
""").show()

# --- Cleaning ---
from pyspark.sql.functions import count, when, isnull

cleaned = spark.sql("""
    SELECT
      CAST(age AS INT) AS age,
      NULLIF(UPPER(workclass), '?') AS workclass,
      CAST(fnlwgt AS INT) AS fnlwgt,
      NULLIF(UPPER(education), '?') AS education,
      CAST(education_num AS INT) AS education_num,
      NULLIF(UPPER(marital_status), '?') AS marital_status,
      NULLIF(UPPER(occupation), '?') AS occupation,
      NULLIF(UPPER(relationship), '?') AS relationship,
      NULLIF(UPPER(race), '?') AS race,
      NULLIF(UPPER(sex), '?') AS sex,
      CAST(capital_gain AS INT) AS capital_gain,
      CAST(capital_loss AS INT) AS capital_loss,
      CAST(hours_per_week AS INT) AS hours_per_week,
      NULLIF(UPPER(native_country), '?') AS native_country,
      UPPER(income) AS income
      FROM unified
""")

cleaned.select([count(when(isnull(c), c)).alias(c) for c in df.columns]).show()

cleaned = cleaned.fillna({"workclass":"UNKNOWN", "occupation":"UNKNOWN", "native_country":"UNKNOWN"})

cleaned.createOrReplaceTempView("cleaned")

# --- Feature engineering ---

features = spark.sql("""
  SELECT
    workclass,
    education,
    education_num,
    marital_status,
    occupation,
    relationship,
    race,
    sex,

    CASE WHEN income = '>50K' THEN 1 ELSE 0 END AS is_high_income,

    CASE
      WHEN age < 30 THEN 'young'
      WHEN age < 50 THEN 'mid'
      ELSE 'senior'
    END AS age_bucket,
    
    (COALESCE(capital_gain, 0) - COALESCE(capital_loss, 0)) / (COALESCE(hours_per_week, 0) + 1.0) AS capital_ratio,

    hours_per_week,
       fnlwgt,
    native_country
  FROM cleaned
  """)

print("Schema:")
features.printSchema()
print("Count:", features.count())

# --- Write ---
features.coalesce(1).write.mode("overwrite").parquet(UNIFIED_OUT)
print(f"✅ Wrote to: {UNIFIED_OUT}")

import os, glob, shutil
part = glob.glob(os.path.join(UNIFIED_OUT, "part-*.parquet"))[0]
shutil.move(part, os.path.join(UNIFIED_OUT, "final_dataset.parquet"))


