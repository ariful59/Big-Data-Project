
"""
Binary income classification with a full Spark ML pipeline:
- Numeric imputation
- Categorical indexing & one-hot encoding
- Feature assembly & scaling
- Logistic Regression with CV
- Metrics: AUC(ROC/PR), Accuracy, F1, Precision/Recall (weighted & binary)
- Model saving (CV model + best pipeline)
"""

from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, OneHotEncoder, Imputer, VectorAssembler, StandardScaler
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when

# -----------------------------
# Paths & basic setup
# -----------------------------
INPUT_FILE  = "/app/output/final_unified/final_dataset.parquet"
CV_MODEL_PATH = "/app/models/income_lr_cv_model"
BEST_MODEL_PATH = "/app/models/income_lr_best_pipeline"

spark = SparkSession.builder.appName("ML_Pipeline").getOrCreate()
df = spark.read.parquet(INPUT_FILE)

# Improve parallelism (optional)
spark.conf.set("spark.sql.shuffle.partitions", "200")

# If you expect skew by workclass, consider not using a partitioning column
# or pick something with higher cardinality. We'll keep your choice here:
df = df.repartition(200, df["workclass"])

# -----------------------------
# Columns configuration
# -----------------------------
label_col = "is_high_income"  # original label (expected 0/1)
cat_cols = [
    "workclass","education","marital_status","occupation",
    "relationship","race","sex","native_country","age_bucket"
]
num_cols = [
    "education_num","hours_per_week","capital_ratio"
]

# -----------------------------
# Label prep (cast to double)
# -----------------------------
df = df.withColumn("label", col(label_col).cast("double"))

# Optional: cache if dataset fits in memory to speed up CV
df.cache()

# -----------------------------
# Feature transformers
# -----------------------------
# Imputer for numeric features
imputer = Imputer(
    inputCols=num_cols,
    outputCols=[c + "_imp" for c in num_cols],
    strategy="median"  # "mean" is also fine; "median" is robust to outliers
)

# StringIndexers for ALL categorical columns (handle unseen categories safely)
indexers = [
    StringIndexer(inputCol=c, outputCol=f"{c}_idx", handleInvalid="keep")
    for c in cat_cols
]

# OneHotEncoder for ALL categorical columns (sparse vectors)
encoder = OneHotEncoder(
    inputCols=[f"{c}_idx" for c in cat_cols],
    outputCols=[f"{c}_oh" for c in cat_cols],
    handleInvalid="keep"
)

# Assemble numeric (imputed) + categorical (encoded)
assembler = VectorAssembler(
    inputCols=[f"{c}_imp" for c in num_cols] + [f"{c}_oh" for c in cat_cols],
    outputCol="features_raw"
)

# Scale features for LR (tree models don't need scaling; LR benefits)
# withMean=False is required to keep sparse vectors sparse
scaler = StandardScaler(inputCol="features_raw", outputCol="features", withMean=False, withStd=True)

# -----------------------------
# Estimator: Logistic Regression
# -----------------------------
# You can tune threshold, class weights, etc. Example: set weightCol='fnlwgt'
lr = LogisticRegression(
    featuresCol="features",
    labelCol="label",
    maxIter=50,           # a sensible default for convergence
    elasticNetParam=0.5,  # will be tuned via CV grid anyway
    regParam=0.01,        # ditto
    standardization=False # already scaling features explicitly
)

# Pipeline stages
stages = [imputer] + indexers + [encoder, assembler, scaler, lr]
pipeline = Pipeline(stages=stages)

# -----------------------------
# Train/test split
# -----------------------------
train_df, test_df = df.randomSplit([0.8, 0.2], seed=42)

# -----------------------------
# Hyperparameter grid (CV)
# -----------------------------
param_grid = (
    ParamGridBuilder()
      .addGrid(lr.regParam, [0.0, 0.01, 0.1])
      .addGrid(lr.elasticNetParam, [0.0, 0.5, 1.0])
      .build()
)

# Primary evaluator (AUC ROC)
evaluator_roc = BinaryClassificationEvaluator(
    labelCol="label", metricName="areaUnderROC"
)

# Cross-validation setup
cv = CrossValidator(
    estimator=pipeline,
    estimatorParamMaps=param_grid,
    evaluator=evaluator_roc,
    numFolds=3,
    parallelism=2,
    seed=42
)

# -----------------------------
# Fit & predict
# -----------------------------
cv_model = cv.fit(train_df)
preds = cv_model.transform(test_df)

# -----------------------------
# Metrics
# -----------------------------
# Threshold-independent metrics
auc_roc = evaluator_roc.evaluate(preds)

evaluator_pr = BinaryClassificationEvaluator(
    labelCol="label", metricName="areaUnderPR"
)
auc_pr = evaluator_pr.evaluate(preds)

# Accuracy & weighted metrics
acc_eval = MulticlassClassificationEvaluator(
    labelCol="label", predictionCol="prediction", metricName="accuracy"
)
accuracy = acc_eval.evaluate(preds)

f1_eval = MulticlassClassificationEvaluator(
    labelCol="label", predictionCol="prediction", metricName="f1"
)
f1_weighted = f1_eval.evaluate(preds)

prec_eval = MulticlassClassificationEvaluator(
    labelCol="label", predictionCol="prediction", metricName="weightedPrecision"
)
precision_weighted = prec_eval.evaluate(preds)

rec_eval = MulticlassClassificationEvaluator(
    labelCol="label", predictionCol="prediction", metricName="weightedRecall"
)
recall_weighted = rec_eval.evaluate(preds)

# Optional: binary confusion matrix metrics at default threshold (0.5)
tp = preds.filter((col("label") == 1.0) & (col("prediction") == 1.0)).count()
fp = preds.filter((col("label") == 0.0) & (col("prediction") == 1.0)).count()
tn = preds.filter((col("label") == 0.0) & (col("prediction") == 0.0)).count()
fn = preds.filter((col("label") == 1.0) & (col("prediction") == 0.0)).count()

precision_pos = tp / (tp + fp) if (tp + fp) > 0 else 0.0
recall_pos = tp / (tp + fn) if (tp + fn) > 0 else 0.0
f1_pos = (2 * precision_pos * recall_pos / (precision_pos + recall_pos)) if (precision_pos + recall_pos) > 0 else 0.0
accuracy_conf = (tp + tn) / (tp + fp + tn + fn) if (tp + fp + tn + fn) > 0 else 0.0

# -----------------------------
#  summary output
# -----------------------------
print("\n" + "="*70)
print("EVALUATION SUMMARY")
print("="*70)
print(f"• AUC (ROC):           {auc_roc:.4f}")
print(f"• AUC (PR):            {auc_pr:.4f}")
print(f"• Accuracy:            {accuracy:.4f}")
print(f"• F1 (weighted):       {f1_weighted:.4f}")
print(f"• Precision (weighted):{precision_weighted:.4f}")
print(f"• Recall (weighted):   {recall_weighted:.4f}")
print("-"*70)
print(f"• Confusion Matrix: TP={tp}, FP={fp}, TN={tn}, FN={fn}")
print(f"• Precision (positive class): {precision_pos:.4f}")
print(f"• Recall    (positive class): {recall_pos:.4f}")
print(f"• F1        (positive class): {f1_pos:.4f}")
print(f"• Accuracy  (from confusion): {accuracy_conf:.4f}")
print("="*70 + "\n")

# -----------------------------
# Save models
# -----------------------------
# Save the full CrossValidatorModel (contains CV metadata + bestModel)
cv_model.write().overwrite().save(CV_MODEL_PATH)

# Save only the best fitted PipelineModel (lighter; ideal for inference)
best_model = cv_model.bestModel
best_model.write().overwrite().save(BEST_MODEL_PATH)

print("Models saved:")
print(f"   • CV model:    {CV_MODEL_PATH}")
print(f"   • Best model:  {BEST_MODEL_PATH}")

# Optional: print best LR params chosen by CV
lr_model = best_model.stages[-1]  # last stage is LogisticRegressionModel
print("\n Best Logistic Regression hyperparameters (from CV):")
print(f"   • regParam:        {lr_model.getRegParam()}")
print(f"   • elasticNetParam: {lr_model.getElasticNetParam()}")
print(f"   • maxIter:         {lr_model.getMaxIter()}")
print(f"   • threshold:       {lr_model.getThreshold()}")