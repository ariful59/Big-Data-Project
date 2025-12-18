
# app/ml_pipeline_node.py
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, lit, coalesce, count, isnull, upper, trim
)
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, OneHotEncoder, Imputer, VectorAssembler, StandardScaler
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator

# --- Read node context from environment ---
NODE_ID   = os.environ.get("NODE_ID", "unknown")              # master | worker1 | worker2
DATA_BASE = os.environ.get("OUT_BASE", "/app/data")           # same default used by ingest
INPUT_PATH = os.environ.get("INPUT_PATH", f"{DATA_BASE}/{NODE_ID}")  # Parquet directory
MODEL_DIR  = os.environ.get("MODEL_DIR", f"/app/models/{NODE_ID}_model")

# # Optional tuning via env
# SHUFFLE_PARTS = os.environ.get("SPARK_SQL_SHUFFLE_PARTITIONS", "32")
# ADAPTIVE      = os.environ.get("SPARK_SQL_ADAPTIVE_ENABLED", "true")

# Expected schema
label_col = "is_high_income"
cat_cols = [
    "workclass","education","marital_status","occupation",
    "relationship","race","sex","native_country","age_bucket"
]
num_cols = ["education_num","hours_per_week","capital_ratio"]

def main():
    spark = (SparkSession.builder
             .appName(f"ML_{NODE_ID}")
             # .config("spark.sql.shuffle.partitions", SHUFFLE_PARTS)
             # .config("spark.sql.adaptive.enabled", ADAPTIVE)
             .getOrCreate())

    # Expect a parquet folder (not a single file)
    df = spark.read.parquet(INPUT_PATH)

    # ---- Rename columns to match expected names ----
    rename_map = {
        "education.num": "education_num",
        "marital.status": "marital_status",
        "hours.per.week": "hours_per_week",
        "capital.gain": "capital_gain",
        "capital.loss": "capital_loss",
        "native.country": "native_country",
        "work-class": "workclass",
        "marital-status": "marital_status",
        "native-country": "native_country",
        "education-num": "education_num",
        "hours-per-week": "hours_per_week",
        "capital-gain": "capital_gain",
        "capital-loss": "capital_loss",
        # pass-through (if already present)
        "education": "education",
        "occupation": "occupation",
        "relationship": "relationship",
        "race": "race",
        "sex": "sex",
        "age": "age",
        "fnlwgt": "fnlwgt",
        "income": "income",
        "age-bucket": "age_bucket",
    }
    for old, new in rename_map.items():
        if old in df.columns and new not in df.columns:
            df = df.withColumnRenamed(old, new)

    # ---- Clean categoricals: uppercase, trim, convert '?' or '' to NULL ----
    cats_to_clean = ["workclass","education","marital_status","occupation",
                     "relationship","race","sex","native_country","income"]
    for c in cats_to_clean:
        if c in df.columns:
            df = df.withColumn(
                c,
                when(upper(trim(col(c))) == lit("?"), lit(None))
                .when(trim(col(c)) == lit(""), lit(None))
                .otherwise(upper(trim(col(c))))
            )

    # ---- Cast numerics to DOUBLE ----
    numeric_sources = ["age", "fnlwgt", "education_num", "capital_gain", "capital_loss", "hours_per_week"]
    for c in numeric_sources:
        if c in df.columns:
            df = df.withColumn(c, col(c).cast("double"))

    # ---- Feature engineering ----
    # 1) is_high_income from income
    if "income" in df.columns and label_col not in df.columns:
        df = df.withColumn(
            label_col,
            when(col("income") == lit(">50K"), lit(1.0)).otherwise(lit(0.0))
        )

    # 2) age_bucket from age if not present
    if "age_bucket" not in df.columns and "age" in df.columns:
        df = df.withColumn(
            "age_bucket",
            when(col("age") < 30, lit("YOUNG"))
            .when(col("age") < 50, lit("MID"))
            .otherwise(lit("SENIOR"))
        )

    # 3) capital_ratio
    if "capital_ratio" not in df.columns:
        df = df.withColumn(
            "capital_ratio",
            (coalesce(col("capital_gain"), lit(0.0)) - coalesce(col("capital_loss"), lit(0.0))) /
            (coalesce(col("hours_per_week"), lit(0.0)) + lit(1.0))
        )

    # ---- Drop rows with nulls in required columns ----
    required_cols = [label_col] + cat_cols + num_cols
    existing_required = [c for c in required_cols if c in df.columns]
    if existing_required:
        df = df.dropna(subset=existing_required)

    # ---- Drop duplicates ----
    df = df.dropDuplicates()

    # ---- Diagnostics ----
    print("\n" + "="*70)
    print(f"[{NODE_ID}] NULL COUNTS AFTER CLEANING")
    print("="*70)
    null_counts = df.select([count(when(isnull(col(c)), c)).alias(c) for c in df.columns])
    null_counts.show(truncate=False)

    print("\n" + "="*70)
    print(f"[{NODE_ID}] INPUT SUMMARY AFTER CLEANING")
    print("="*70)
    print(f"• Input: {INPUT_PATH}")
    print(f"• Rows:  {df.count():,}")
    print(f"• Cols:  {len(df.columns)}")
    print(f"• Using label: {label_col}")
    print("="*70 + "\n")

    # ---- Label as double ----
    df = df.withColumn("label", col(label_col).cast("double"))

    # ---- Feature pipeline ----
    present_num_cols = [c for c in num_cols if c in df.columns]
    present_cat_cols = [c for c in cat_cols if c in df.columns]

    imputer = Imputer(
        inputCols=present_num_cols,
        outputCols=[c + "_imp" for c in present_num_cols],
        strategy="median"
    )

    indexers = [
        StringIndexer(inputCol=c, outputCol=f"{c}_idx", handleInvalid="keep")
        for c in present_cat_cols
    ]

    encoder = OneHotEncoder(
        inputCols=[f"{c}_idx" for c in present_cat_cols],
        outputCols=[f"{c}_oh" for c in present_cat_cols],
        handleInvalid="keep"
    )

    assembler = VectorAssembler(
        inputCols=[f"{c}_imp" for c in present_num_cols] + [f"{c}_oh" for c in present_cat_cols],
        outputCol="features_raw"
    )

    scaler = StandardScaler(inputCol="features_raw", outputCol="features", withMean=False, withStd=True)

    lr = LogisticRegression(
        featuresCol="features",
        labelCol="label",
        maxIter=50,
        elasticNetParam=0.5,
        regParam=0.01,
        standardization=False
    )

    pipeline = Pipeline(stages=[imputer] + indexers + [encoder, assembler, scaler, lr])

    # ---- Train/test split & fit ----
    train_df, test_df = df.randomSplit([0.8, 0.2], seed=42)
    model = pipeline.fit(train_df)
    preds = model.transform(test_df)

    # ---- Metrics ----
    auc_roc = BinaryClassificationEvaluator(labelCol="label", metricName="areaUnderROC").evaluate(preds)
    auc_pr  = BinaryClassificationEvaluator(labelCol="label", metricName="areaUnderPR").evaluate(preds)
    accuracy = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="accuracy").evaluate(preds)

    print("\n" + "="*70)
    print(f"[{NODE_ID}] EVALUATION SUMMARY")
    print("="*70)
    print(f"• Input: {INPUT_PATH}")
    print(f"• AUC (ROC): {auc_roc:.4f}")
    print(f"• AUC (PR):  {auc_pr:.4f}")
    print(f"• Accuracy:  {accuracy:.4f}")
    print("="*70 + "\n")

    # ---- Save fitted pipeline model per node ----
    model.write().overwrite().save(MODEL_DIR)
    print(f"NODE_ID  {NODE_ID} ✅ Saved model to {MODEL_DIR}")

    spark.stop()

if __name__ == "__main__":
    main()