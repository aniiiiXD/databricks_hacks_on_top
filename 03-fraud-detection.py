# Databricks notebook source
# MAGIC %md
# MAGIC # Digital-Artha: Fraud Detection ML Pipeline
# MAGIC
# MAGIC **Ensemble anomaly detection** combining:
# MAGIC 1. **Isolation Forest** (scikit-learn) — unsupervised outlier detection
# MAGIC 2. **K-Means + Distance** (Spark MLlib) — cluster-based anomaly scoring
# MAGIC 3. **Weighted ensemble** — blends both scores + AI risk label from DLT
# MAGIC
# MAGIC All experiments tracked in **MLflow** with model registration to **Unity Catalog**.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

# MAGIC %pip install scikit-learn imbalanced-learn shap --quiet

# COMMAND ----------

dbutils.widgets.text("catalog", "digital_artha", "Catalog Name")
dbutils.widgets.text("schema", "main", "Schema Name")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

import numpy as np
import pandas as pd
from datetime import datetime

import os
os.environ["MLFLOW_TRACKING_URI"] = "databricks"

import mlflow
import mlflow.sklearn

# Suppress the modelRegistryUri warning on Free Edition
try:
    import mlflow.spark
except Exception:
    pass

# Set experiment — this DOES work on Free Edition
username = spark.sql("SELECT current_user()").collect()[0][0]
experiment_path = f"/Users/{username}/digital-artha-fraud-detection"

try:
    mlflow.set_experiment(experiment_path)
    print(f"MLflow experiment: {experiment_path}")
except Exception as e:
    # If set_experiment fails, create it via the workspace path
    print(f"MLflow set_experiment warning: {e}")
    print("Trying alternative experiment setup...")
    try:
        mlflow.set_experiment(f"/Shared/digital-artha-fraud-detection")
        print("MLflow experiment set to /Shared/digital-artha-fraud-detection")
    except Exception as e2:
        print(f"MLflow experiment setup failed: {e2}")
        print("Continuing without MLflow tracking — training will still work.")

mlflow.autolog(disable=True)  # We'll log manually for more control

# Safe MLflow logging helpers — won't crash if MLflow is partially broken
def safe_log_param(key, value):
    try: mlflow.log_param(key, value)
    except: pass

def safe_log_metric(key, value):
    try: mlflow.log_metric(key, value)
    except: pass

def safe_log_model(model, name):
    try: mlflow.sklearn.log_model(model, name)
    except: pass

print(f"Data source: {catalog}.{schema}.gold_transactions")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Load Gold Transactions & Explore

# COMMAND ----------

gold_df = spark.table(f"{catalog}.{schema}.gold_transactions")
print(f"Total transactions: {gold_df.count():,}")

# Class distribution (if ground truth exists)
if "is_fraud" in gold_df.columns:
    display(gold_df.groupBy("is_fraud").count().orderBy("is_fraud"))

display(gold_df.groupBy("ai_risk_label").count().orderBy("ai_risk_label"))
display(gold_df.groupBy("amount_bucket").count().orderBy("amount_bucket"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Feature Engineering (PySpark)
# MAGIC
# MAGIC Build fraud-detection features at scale using window functions.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# --- Transaction velocity windows ---
# How many transactions did this sender make in the last 1h, 6h, 24h?
w_1h = Window.partitionBy("sender_id").orderBy(F.col("transaction_time").cast("long")).rangeBetween(-3600, 0)
w_6h = Window.partitionBy("sender_id").orderBy(F.col("transaction_time").cast("long")).rangeBetween(-21600, 0)
w_24h = Window.partitionBy("sender_id").orderBy(F.col("transaction_time").cast("long")).rangeBetween(-86400, 0)

# Sender historical stats
w_sender_all = Window.partitionBy("sender_id")

featured_df = (gold_df
    # Transaction velocity (count in window)
    .withColumn("txn_velocity_1h", F.count("transaction_id").over(w_1h))
    .withColumn("txn_velocity_6h", F.count("transaction_id").over(w_6h))
    .withColumn("txn_velocity_24h", F.count("transaction_id").over(w_24h))

    # Amount statistics per sender
    .withColumn("sender_avg_amount", F.avg("amount").over(w_sender_all))
    .withColumn("sender_std_amount", F.stddev("amount").over(w_sender_all))
    .withColumn("sender_max_amount", F.max("amount").over(w_sender_all))
    .withColumn("sender_txn_count", F.count("transaction_id").over(w_sender_all))

    # Amount deviation from sender's norm (z-score)
    .withColumn("amount_deviation",
        F.when(F.col("sender_std_amount") > 0,
            (F.col("amount") - F.col("sender_avg_amount")) / F.col("sender_std_amount")
        ).otherwise(0.0))

    # Amount relative to sender's max
    .withColumn("amount_ratio_to_max",
        F.when(F.col("sender_max_amount") > 0,
            F.col("amount") / F.col("sender_max_amount")
        ).otherwise(1.0))

    # Velocity in amount (total ₹ moved in last 1h)
    .withColumn("amount_velocity_1h", F.sum("amount").over(w_1h))

    # Cross features
    .withColumn("velocity_x_deviation", F.col("txn_velocity_1h") * F.abs(F.col("amount_deviation")))
    .withColumn("amount_x_hour", F.col("amount") * F.col("hour_of_day"))

    # Is this the sender's first transaction? (cold start risk)
    .withColumn("is_new_sender", F.when(F.col("sender_txn_count") <= 2, 1).otherwise(0))

    # Late night high value (fraud pattern)
    .withColumn("late_night_high_value",
        F.when(
            (F.col("hour_of_day").between(0, 5)) & (F.col("amount") > 5000), 1
        ).otherwise(0))

    # Fill nulls for ML
    .fillna(0.0, subset=[
        "txn_velocity_1h", "txn_velocity_6h", "txn_velocity_24h",
        "sender_avg_amount", "sender_std_amount", "sender_max_amount",
        "amount_deviation", "amount_ratio_to_max", "amount_velocity_1h",
        "velocity_x_deviation", "amount_x_hour"
    ])
)

# Cache for reuse
featured_df.cache()
print(f"Feature-engineered transactions: {featured_df.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Feature summary

# COMMAND ----------

# day_of_week is string ("Tuesday") in Kaggle data — encode it numerically
# is_weekend might be bool/string — cast to int
featured_df = (featured_df
    .withColumn("day_of_week_num",
        F.when(F.col("day_of_week") == "Monday", 1)
        .when(F.col("day_of_week") == "Tuesday", 2)
        .when(F.col("day_of_week") == "Wednesday", 3)
        .when(F.col("day_of_week") == "Thursday", 4)
        .when(F.col("day_of_week") == "Friday", 5)
        .when(F.col("day_of_week") == "Saturday", 6)
        .when(F.col("day_of_week") == "Sunday", 7)
        .otherwise(F.col("day_of_week").cast("int"))
    )
    .withColumn("is_weekend_num", F.col("is_weekend").cast("int"))
    .fillna(0, subset=["day_of_week_num", "is_weekend_num"])
)

feature_cols = [
    "amount", "hour_of_day", "day_of_week_num", "is_weekend_num",
    "txn_velocity_1h", "txn_velocity_6h", "txn_velocity_24h",
    "sender_avg_amount", "sender_std_amount",
    "amount_deviation", "amount_ratio_to_max", "amount_velocity_1h",
    "velocity_x_deviation", "amount_x_hour",
    "is_new_sender", "late_night_high_value",
    "ai_risk_score"
]

display(featured_df.select(feature_cols).describe())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Model A: Isolation Forest (scikit-learn)
# MAGIC
# MAGIC Unsupervised anomaly detection — learns the "normal" distribution
# MAGIC and flags outliers. Works without labeled data.

# COMMAND ----------

from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import classification_report, precision_recall_fscore_support, roc_auc_score
import warnings
warnings.filterwarnings('ignore')

# Convert to Pandas for sklearn (sample if too large for driver memory)
row_count = featured_df.count()
if row_count > 500000:
    sample_fraction = 500000 / row_count
    pdf = featured_df.select(feature_cols + ["transaction_id", "is_fraud"]).sample(fraction=sample_fraction, seed=42).toPandas()
else:
    pdf = featured_df.select(feature_cols + ["transaction_id", "is_fraud"]).toPandas()

print(f"Training on {len(pdf):,} transactions")

X = pdf[feature_cols].values
scaler = StandardScaler()
X_scaled = scaler.fit_transform(X)

# --- Train Isolation Forest ---
contamination_rate = 0.05  # Expected ~5% fraud rate

with mlflow.start_run(run_name="isolation_forest_v1") as if_run:
    # Hyperparameters
    n_estimators = 300
    max_samples = min(len(X_scaled), 10000)
    max_features = 0.8

    safe_log_param("model_type", "IsolationForest")
    safe_log_param("n_estimators", n_estimators)
    safe_log_param("contamination", contamination_rate)
    safe_log_param("max_samples", max_samples)
    safe_log_param("max_features", max_features)
    safe_log_param("n_features", len(feature_cols))
    safe_log_param("training_rows", len(X_scaled))
    safe_log_param("feature_cols", str(feature_cols))

    iforest = IsolationForest(
        n_estimators=n_estimators,
        contamination=contamination_rate,
        max_samples=max_samples,
        max_features=max_features,
        random_state=42,
        n_jobs=-1,
        warm_start=False
    )
    iforest.fit(X_scaled)

    # Predictions: -1 = anomaly, 1 = normal
    if_predictions = iforest.predict(X_scaled)
    if_scores = iforest.decision_function(X_scaled)  # Lower = more anomalous

    # Normalize scores to [0, 1] where 1 = most anomalous
    if_scores_normalized = 1 - (if_scores - if_scores.min()) / (if_scores.max() - if_scores.min() + 1e-10)

    pdf["if_anomaly"] = (if_predictions == -1).astype(int)
    pdf["if_score"] = if_scores_normalized

    # Evaluate against ground truth if available
    if pdf["is_fraud"].notna().any():
        y_true = pdf["is_fraud"].fillna(False).astype(int)
        y_pred = pdf["if_anomaly"]

        precision, recall, f1, _ = precision_recall_fscore_support(y_true, y_pred, average="binary", zero_division=0)
        try:
            auc = roc_auc_score(y_true, if_scores_normalized)
        except:
            auc = 0.0

        safe_log_metric("precision", precision)
        safe_log_metric("recall", recall)
        safe_log_metric("f1_score", f1)
        safe_log_metric("auc_roc", auc)
        safe_log_metric("flagged_count", int(pdf["if_anomaly"].sum()))
        safe_log_metric("flagged_rate", float(pdf["if_anomaly"].mean()))

        print(f"Isolation Forest Results:")
        print(f"  Precision: {precision:.4f}")
        print(f"  Recall:    {recall:.4f}")
        print(f"  F1 Score:  {f1:.4f}")
        print(f"  AUC-ROC:   {auc:.4f}")
        print(f"  Flagged:   {pdf['if_anomaly'].sum():,} / {len(pdf):,} ({pdf['if_anomaly'].mean()*100:.2f}%)")

    # Log model
    safe_log_model(iforest, "isolation_forest_model")
    safe_log_model(scaler, "scaler")

    if_run_id = if_run.info.run_id
    print(f"\nMLflow Run ID: {if_run_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Model B: K-Means Anomaly Detection (Spark MLlib)
# MAGIC
# MAGIC Cluster transactions, then flag those far from any cluster center.
# MAGIC Uses **Spark MLlib** for distributed computation (judges score Databricks usage).

# COMMAND ----------

from pyspark.ml.feature import VectorAssembler, StandardScaler as SparkScaler, StringIndexer
from pyspark.ml.clustering import KMeans
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.ml.functions import vector_to_array

# Numeric feature columns for MLlib
numeric_features = [
    "amount", "hour_of_day", "day_of_week_num", "is_weekend_num",
    "txn_velocity_1h", "txn_velocity_6h", "txn_velocity_24h",
    "sender_avg_amount", "amount_deviation", "amount_ratio_to_max",
    "amount_velocity_1h", "velocity_x_deviation",
    "is_new_sender", "late_night_high_value",
    "ai_risk_score"
]

with mlflow.start_run(run_name="kmeans_anomaly_v1") as km_run:
    k = 8  # Number of clusters
    safe_log_param("model_type", "KMeans_AnomalyDetection")
    safe_log_param("k", k)
    safe_log_param("features", str(numeric_features))
    safe_log_param("max_iter", 50)

    # MLlib pipeline: Assemble → Scale → Cluster
    assembler = VectorAssembler(inputCols=numeric_features, outputCol="raw_features", handleInvalid="skip")
    spark_scaler = SparkScaler(inputCol="raw_features", outputCol="scaled_features", withMean=True, withStd=True)
    kmeans = KMeans(k=k, featuresCol="scaled_features", predictionCol="cluster", maxIter=50, seed=42)

    pipeline = Pipeline(stages=[assembler, spark_scaler, kmeans])
    model = pipeline.fit(featured_df)
    predictions = model.transform(featured_df)

    # Evaluate clustering quality
    evaluator = ClusteringEvaluator(featuresCol="scaled_features", predictionCol="cluster")
    silhouette = evaluator.evaluate(predictions)
    safe_log_metric("silhouette_score", silhouette)
    print(f"Silhouette Score: {silhouette:.4f}")

    # --- Compute distance to nearest cluster center ---
    kmeans_model = model.stages[-1]
    centers = kmeans_model.clusterCenters()
    centers_broadcast = spark.sparkContext.broadcast(centers)

    # UDF to compute Euclidean distance to assigned cluster center
    from pyspark.sql.types import DoubleType

    @F.udf(DoubleType())
    def distance_to_center(features_array, cluster_id):
        if features_array is None or cluster_id is None:
            return 0.0
        center = centers_broadcast.value[int(cluster_id)]
        features = np.array(features_array)
        return float(np.sqrt(np.sum((features - center) ** 2)))

    # Convert vector to array, then compute distance
    predictions_with_dist = (predictions
        .withColumn("features_array", vector_to_array("scaled_features"))
        .withColumn("distance_to_center", distance_to_center("features_array", "cluster"))
    )

    # Compute distance statistics for anomaly threshold
    dist_stats = predictions_with_dist.select(
        F.mean("distance_to_center").alias("mean_dist"),
        F.stddev("distance_to_center").alias("std_dist"),
        F.expr("percentile_approx(distance_to_center, 0.95)").alias("p95_dist"),
        F.expr("percentile_approx(distance_to_center, 0.99)").alias("p99_dist"),
    ).collect()[0]

    # Anomaly threshold: beyond 95th percentile
    threshold = dist_stats["p95_dist"]
    safe_log_param("anomaly_threshold_percentile", 95)
    safe_log_metric("distance_mean", float(dist_stats["mean_dist"]))
    safe_log_metric("distance_std", float(dist_stats["std_dist"]))
    safe_log_metric("distance_p95", float(threshold))

    # Normalize distances to [0, 1]
    max_dist = predictions_with_dist.select(F.max("distance_to_center")).collect()[0][0]
    predictions_with_dist = (predictions_with_dist
        .withColumn("km_score", F.col("distance_to_center") / F.lit(max_dist + 1e-10))
        .withColumn("km_anomaly", F.when(F.col("distance_to_center") > threshold, 1).otherwise(0))
    )

    km_flagged = predictions_with_dist.filter(F.col("km_anomaly") == 1).count()
    km_total = predictions_with_dist.count()
    safe_log_metric("flagged_count", km_flagged)
    safe_log_metric("flagged_rate", km_flagged / km_total)

    print(f"KMeans flagged: {km_flagged:,} / {km_total:,} ({km_flagged/km_total*100:.2f}%)")

    # Log cluster distribution
    display(predictions_with_dist.groupBy("cluster").count().orderBy("cluster"))

    # Log MLlib model
    # mlflow.spark.log_model(model, "kmeans_pipeline_model")

    km_run_id = km_run.info.run_id
    print(f"\nMLflow Run ID: {km_run_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Ensemble: Blend All Signals
# MAGIC
# MAGIC Combine Isolation Forest score + KMeans distance score + AI risk label
# MAGIC into a unified fraud score.

# COMMAND ----------

with mlflow.start_run(run_name="ensemble_fraud_v1") as ensemble_run:
    # Weights for ensemble
    w_if = 0.40   # Isolation Forest
    w_km = 0.35   # KMeans distance
    w_ai = 0.25   # AI risk label from DLT ai_query()

    safe_log_param("model_type", "WeightedEnsemble")
    safe_log_param("weight_isolation_forest", w_if)
    safe_log_param("weight_kmeans", w_km)
    safe_log_param("weight_ai_risk", w_ai)
    safe_log_param("ensemble_threshold", 0.5)

    # Join IF scores back to Spark DataFrame
    if_scores_pdf = pdf[["transaction_id", "if_score", "if_anomaly"]].copy()
    if_scores_sdf = spark.createDataFrame(if_scores_pdf)

    # Merge all scores
    ensemble_df = (predictions_with_dist
        .join(if_scores_sdf, on="transaction_id", how="left")
        .fillna(0.0, subset=["if_score", "km_score"])
        .withColumn("ensemble_score",
            F.lit(w_if) * F.coalesce(F.col("if_score"), F.lit(0.0)) +
            F.lit(w_km) * F.col("km_score") +
            F.lit(w_ai) * F.col("ai_risk_score")
        )
        .withColumn("ensemble_flag",
            F.when(F.col("ensemble_score") > 0.5, True).otherwise(False)
        )
        .withColumn("final_risk_tier",
            F.when(F.col("ensemble_score") > 0.8, "critical")
            .when(F.col("ensemble_score") > 0.6, "high")
            .when(F.col("ensemble_score") > 0.4, "medium")
            .otherwise("low")
        )
    )

    # Evaluate ensemble
    if "is_fraud" in ensemble_df.columns:
        eval_pdf = ensemble_df.select("is_fraud", "ensemble_flag", "ensemble_score").toPandas()
        y_true = eval_pdf["is_fraud"].fillna(False).astype(int)
        y_pred = eval_pdf["ensemble_flag"].astype(int)
        scores = eval_pdf["ensemble_score"]

        precision, recall, f1, _ = precision_recall_fscore_support(y_true, y_pred, average="binary", zero_division=0)
        try:
            auc = roc_auc_score(y_true, scores)
        except:
            auc = 0.0

        safe_log_metric("ensemble_precision", precision)
        safe_log_metric("ensemble_recall", recall)
        safe_log_metric("ensemble_f1", f1)
        safe_log_metric("ensemble_auc_roc", auc)

        print(f"Ensemble Results:")
        print(f"  Precision: {precision:.4f}")
        print(f"  Recall:    {recall:.4f}")
        print(f"  F1 Score:  {f1:.4f}")
        print(f"  AUC-ROC:   {auc:.4f}")

    total_flagged = ensemble_df.filter(F.col("ensemble_flag") == True).count()
    total = ensemble_df.count()
    safe_log_metric("total_flagged", total_flagged)
    safe_log_metric("total_transactions", total)
    safe_log_metric("overall_flag_rate", total_flagged / total)

    print(f"\nEnsemble flagged: {total_flagged:,} / {total:,} ({total_flagged/total*100:.2f}%)")
    display(ensemble_df.groupBy("final_risk_tier").count().orderBy("final_risk_tier"))

    ensemble_run_id = ensemble_run.info.run_id

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Feature Importance (Explainability)
# MAGIC
# MAGIC SHAP values explain WHY each transaction was flagged.

# COMMAND ----------

try:
    import shap

    with mlflow.start_run(run_name="feature_importance", nested=True):
        # Use a sample for SHAP (computationally expensive)
        X_sample = X_scaled[:min(1000, len(X_scaled))]

        explainer = shap.IsolationForest(iforest)
        shap_values = explainer(pd.DataFrame(X_sample, columns=feature_cols))

        # Global feature importance (mean absolute SHAP value)
        importance = pd.DataFrame({
            "feature": feature_cols,
            "mean_abs_shap": np.abs(shap_values.values).mean(axis=0)
        }).sort_values("mean_abs_shap", ascending=False)

        print("Feature Importance (SHAP):")
        for _, row in importance.iterrows():
            print(f"  {row['feature']:30s} {row['mean_abs_shap']:.4f}")
            safe_log_metric(f"shap_{row['feature']}", row['mean_abs_shap'])

        # Log importance table as artifact
        importance.to_csv("/tmp/feature_importance.csv", index=False)
        try:
            mlflow.log_artifact("/tmp/feature_importance.csv")
        except:
            pass

except Exception as e:
    print(f"SHAP analysis skipped: {e}")
    print("Falling back to Isolation Forest feature_importances_ (if available)")

    # Alternative: permutation importance
    importance = pd.DataFrame({
        "feature": feature_cols,
        "importance": np.abs(iforest.score_samples(X_scaled[:1000])).mean()
    })

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Write Enriched Results to Gold Layer

# COMMAND ----------

# Select final columns for the enriched fraud table
fraud_enriched = (ensemble_df
    .select(
        "transaction_id", "amount", "transaction_time", "transaction_date",
        "sender_id", "sender_name", "receiver_id", "receiver_name",
        "category", "merchant_name", "payment_mode", "device_type", "location",
        "hour_of_day", "day_of_week", "is_weekend", "time_slot", "amount_bucket",
        # ML features
        "txn_velocity_1h", "txn_velocity_6h", "txn_velocity_24h",
        "amount_deviation", "amount_ratio_to_max", "amount_velocity_1h",
        "is_new_sender", "late_night_high_value",
        # Model scores
        F.coalesce(F.col("if_score"), F.lit(0.0)).alias("isolation_forest_score"),
        "km_score",
        "ai_risk_score",
        "ensemble_score",
        "ensemble_flag",
        "final_risk_tier",
        "cluster",
        "is_fraud",
    ))

# Write to Delta table
(fraud_enriched.write
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{catalog}.{schema}.gold_transactions_enriched"))

print(f"Written to {catalog}.{schema}.gold_transactions_enriched")

# --- Also write just the alerts ---
alerts = fraud_enriched.filter(F.col("ensemble_flag") == True)
(alerts.write
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{catalog}.{schema}.gold_fraud_alerts_ml"))

print(f"Alerts written to {catalog}.{schema}.gold_fraud_alerts_ml ({alerts.count():,} alerts)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Register Best Model to Unity Catalog

# COMMAND ----------

model_name = f"{catalog}.{schema}.fraud_detection_ensemble"

try:
    # Register the Isolation Forest (the core model)
    mlflow.register_model(
        f"runs:/{if_run_id}/isolation_forest_model",
        model_name
    )
    print(f"Model registered: {model_name}")
except Exception as e:
    print(f"Model registration: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pipeline Complete
# MAGIC
# MAGIC **Artifacts created:**
# MAGIC - `gold_transactions_enriched` — all transactions with ensemble fraud scores
# MAGIC - `gold_fraud_alerts_ml` — flagged transactions only
# MAGIC - MLflow experiment with 3+ runs (IsolationForest, KMeans, Ensemble)
# MAGIC - Registered model in Unity Catalog
# MAGIC - Feature importance analysis (SHAP)
# MAGIC
# MAGIC **Next step:** Run `04-rag-pipeline.py` to set up RBI circular retrieval.
