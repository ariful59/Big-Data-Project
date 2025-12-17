
#not complete yet

from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, OneHotEncoder, Imputer, VectorAssembler, StandardScaler
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.sql import SparkSession

INPUT_FILE  = "/app/output/unified/unified.parquet"  # your single parquet
UNIFIED_OUT = "/app/output/final_unified"

spark = SparkSession.builder.appName("ML_Pipeline").getOrCreate()

df = spark.read.parquet(INPUT_FILE)

# --- Repartitioning ---
spark.conf.set("spark.sql.shuffle.partitions", "200")
features = df.repartition(200, df["workclass"])  #you can choose other feature or no feature, fix value


# Categorical columns (after engineering)
cat_cols = [
    "workclass","education","marital_group","occupation","relationship","race","sex","native_country_group","age_bucket"
]
num_cols = [
    "age","education_num","hours_per_week","log1p_capital_gain","log1p_capital_loss","net_capital","cap_per_hour"
]
# (Optional) exclude fnlwgt or treat as sample weight; many pipelines drop it

# Impute numeric nulls (if any)
imputer = Imputer(strategy="median", inputCols=num_cols, outputCols=[c+"_imp" for c in num_cols])

# Index + OneHot for categoricals (handleInvalid='keep' to retain unseen/missing)
indexers = [StringIndexer(inputCol=c, outputCol=c+"_idx", handleInvalid="keep") for c in cat_cols]
encoders = [OneHotEncoder(inputCols=[c+"_idx"], outputCols=[c+"_oh"], handleInvalid="keep") for c in cat_cols]

# Assemble features
assembler = VectorAssembler(
    inputCols=[c+"_imp" for c in num_cols] + [c+"_oh" for c in cat_cols],
    outputCol="features_raw"
)

# Scale numeric features
scaler = StandardScaler(inputCol="features_raw", outputCol="features", withMean=False, withStd=True)

# Classifier
lr = LogisticRegression(featuresCol="features", labelCol="label", weightCol=None)  # set weightCol to 'fnlwgt' if you want sample weighting

# Pipeline
stages = [imputer] + indexers + encoders + [assembler, scaler, lr]
pipeline = Pipeline(stages=stages)

# Train/test split
train_df, test_df = df.randomSplit([0.8, 0.2], seed=42)

# Hyperparameter grid
param_grid = (
    ParamGridBuilder()
      .addGrid(lr.regParam, [0.0, 0.01, 0.1])
      .addGrid(lr.elasticNetParam, [0.0, 0.5, 1.0])
      .build()
)

# Evaluator
evaluator = BinaryClassificationEvaluator(labelCol="label", metricName="areaUnderROC")

# Cross‑validation
cv = CrossValidator(
    estimator=pipeline,
    estimatorParamMaps=param_grid,
    evaluator=evaluator,
    numFolds=3,
    parallelism=2
)

cv_model = cv.fit(train_df)
preds = cv_model.transform(test_df)

print("AUC (ROC):", evaluator.evaluate(preds))
