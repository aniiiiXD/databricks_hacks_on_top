# Databricks notebook source
# MAGIC %md
# MAGIC # Digital-Artha: Fraud Detection ML Pipeline
# MAGIC
# MAGIC **Ensemble anomaly detection** (pure sklearn — serverless compatible):
# MAGIC 1. **Isolation Forest** — unsupervised outlier detection
# MAGIC 2. **K-Means + Distance** — cluster-based anomaly scoring
# MAGIC 3. **Weighted ensemble** — blends both scores + rule-based risk
# MAGIC
# MAGIC All runs tracked in **MLflow** (best-effort on Free Edition).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

dbutils.widgets.text("catalog", "digital_artha", "Catalog Name")
dbutils.widgets.text("schema", "main", "Schema Name")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

import os
os.environ["MLFLOW_TRACKING_URI"] = "databricks"

import numpy as np
import pandas as pd
from datetime import datetime
from sklearn.ensemble import IsolationForest
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler, LabelEncoder
from sklearn.metrics import precision_recall_fscore_support, roc_auc_score
import warnings
warnings.filterwarnings('ignore')

# MLflow — best effort, won't crash if unavailable
try:
    import mlflow
    import mlflow.sklearn
    username = spark.sql("SELECT current_user()").collect()[0][0]
    mlflow.set_experiment(f"/Users/{username}/digital-artha-fraud-detection")
    MLFLOW_OK = True
    print(f"MLflow experiment set for {username}")
except Exception as e:
    MLFLOW_OK = False
    print(f"MLflow setup warning (training will still work): {e}")

def safe_log_param(k, v):
    if MLFLOW_OK:
        try: mlflow.log_param(k, v)
        except: pass

def safe_log_metric(k, v):
    if MLFLOW_OK:
        try: mlflow.log_metric(k, v)
        except: pass

print(f"Data source: {catalog}.{schema}.gold_transactions")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Load Data & Explore

# COMMAND ----------

from pyspark.sql import functions as F

gold_df = spark.table(f"{catalog}.{schema}.gold_transactions")
print(f"Total transactions: {gold_df.count():,}")

display(gold_df.groupBy("is_fraud").count().orderBy("is_fraud"))
display(gold_df.groupBy("ai_risk_label").count().orderBy("ai_risk_label"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Feature Engineering

# COMMAND ----------

# Compute window features in Spark SQL (works on serverless)
featured_df = spark.sql(f"""
WITH sender_stats AS (
    SELECT
        sender_id,
        AVG(amount) AS sender_avg_amount,
        STDDEV(amount) AS sender_std_amount,
        MAX(amount) AS sender_max_amount,
        COUNT(*) AS sender_txn_count
    FROM {catalog}.{schema}.gold_transactions
    GROUP BY sender_id
)
SELECT
    t.*,
    s.sender_avg_amount,
    COALESCE(s.sender_std_amount, 0) AS sender_std_amount,
    s.sender_max_amount,
    s.sender_txn_count,
    -- Amount deviation (z-score)
    CASE WHEN s.sender_std_amount > 0
        THEN (t.amount - s.sender_avg_amount) / s.sender_std_amount
        ELSE 0.0
    END AS amount_deviation,
    -- Amount ratio to max
    CASE WHEN s.sender_max_amount > 0
        THEN t.amount / s.sender_max_amount
        ELSE 1.0
    END AS amount_ratio_to_max,
    -- Is new sender
    CASE WHEN s.sender_txn_count <= 2 THEN 1 ELSE 0 END AS is_new_sender,
    -- Late night high value
    CASE WHEN t.hour_of_day BETWEEN 0 AND 5 AND t.amount > 5000 THEN 1 ELSE 0 END AS late_night_high_value,
    -- Day of week numeric
    CASE
        WHEN t.day_of_week = 'Monday' THEN 1
        WHEN t.day_of_week = 'Tuesday' THEN 2
        WHEN t.day_of_week = 'Wednesday' THEN 3
        WHEN t.day_of_week = 'Thursday' THEN 4
        WHEN t.day_of_week = 'Friday' THEN 5
        WHEN t.day_of_week = 'Saturday' THEN 6
        WHEN t.day_of_week = 'Sunday' THEN 7
        ELSE 0
    END AS day_of_week_num,
    CAST(COALESCE(t.is_weekend, false) AS INT) AS is_weekend_num
FROM {catalog}.{schema}.gold_transactions t
LEFT JOIN sender_stats s ON t.sender_id = s.sender_id
""")

print(f"Feature-engineered: {featured_df.count():,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Convert to Pandas for ML

# COMMAND ----------

feature_cols = [
    "amount", "hour_of_day", "day_of_week_num", "is_weekend_num",
    "sender_avg_amount", "sender_std_amount",
    "amount_deviation", "amount_ratio_to_max",
    "is_new_sender", "late_night_high_value",
    "ai_risk_score"
]

# Sample if too large
row_count = featured_df.count()
if row_count > 200000:
    sample_fraction = 200000 / row_count
    pdf = featured_df.select(feature_cols + ["transaction_id", "is_fraud"]).sample(fraction=sample_fraction, seed=42).toPandas()
else:
    pdf = featured_df.select(feature_cols + ["transaction_id", "is_fraud"]).toPandas()

# Fill NaN
pdf[feature_cols] = pdf[feature_cols].fillna(0)

print(f"Training on {len(pdf):,} transactions")
print(f"Fraud rate: {pdf['is_fraud'].mean()*100:.2f}%")

X = pdf[feature_cols].values
scaler = StandardScaler()
X_scaled = scaler.fit_transform(X)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Model A: Isolation Forest

# COMMAND ----------

if MLFLOW_OK:
    try: mlflow.start_run(run_name="isolation_forest_v1")
    except: pass

n_estimators = 300
contamination = 0.02

iforest = IsolationForest(
    n_estimators=n_estimators,
    contamination=contamination,
    max_samples=min(len(X_scaled), 10000),
    max_features=0.8,
    random_state=42,
    n_jobs=-1
)
iforest.fit(X_scaled)
print("Isolation Forest trained.")

if_predictions = iforest.predict(X_scaled)
if_scores = iforest.decision_function(X_scaled)
if_scores_normalized = 1 - (if_scores - if_scores.min()) / (if_scores.max() - if_scores.min() + 1e-10)

pdf["if_anomaly"] = (if_predictions == -1).astype(int)
pdf["if_score"] = if_scores_normalized

safe_log_param("if_n_estimators", n_estimators)
safe_log_param("if_contamination", contamination)

# Evaluate
y_true = pdf["is_fraud"].fillna(False).astype(int)
y_pred = pdf["if_anomaly"]

precision, recall, f1, _ = precision_recall_fscore_support(y_true, y_pred, average="binary", zero_division=0)
try:
    auc = roc_auc_score(y_true, if_scores_normalized)
except:
    auc = 0.0

safe_log_metric("if_precision", precision)
safe_log_metric("if_recall", recall)
safe_log_metric("if_f1", f1)
safe_log_metric("if_auc", auc)

print(f"Isolation Forest Results:")
print(f"  Precision: {precision:.4f}")
print(f"  Recall:    {recall:.4f}")
print(f"  F1 Score:  {f1:.4f}")
print(f"  AUC-ROC:   {auc:.4f}")
print(f"  Flagged:   {pdf['if_anomaly'].sum():,} / {len(pdf):,} ({pdf['if_anomaly'].mean()*100:.2f}%)")

if MLFLOW_OK:
    try:
        mlflow.sklearn.log_model(iforest, "isolation_forest")
        mlflow.end_run()
    except: pass

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Model B: K-Means Anomaly (sklearn)

# COMMAND ----------

if MLFLOW_OK:
    try: mlflow.start_run(run_name="kmeans_anomaly_v1")
    except: pass

k = 8
km = KMeans(n_clusters=k, random_state=42, n_init=10)
km.fit(X_scaled)
print(f"KMeans trained with k={k}")

clusters = km.predict(X_scaled)
centers = km.cluster_centers_

# Distance to assigned cluster center
distances = np.array([
    np.sqrt(np.sum((X_scaled[i] - centers[clusters[i]]) ** 2))
    for i in range(len(X_scaled))
])

# Normalize to [0, 1]
km_scores = distances / (distances.max() + 1e-10)

# Threshold at 95th percentile
threshold = np.percentile(distances, 95)
km_anomaly = (distances > threshold).astype(int)

pdf["km_score"] = km_scores
pdf["km_anomaly"] = km_anomaly
pdf["cluster"] = clusters

safe_log_param("km_k", k)
safe_log_metric("km_threshold_p95", float(threshold))
safe_log_metric("km_flagged", int(km_anomaly.sum()))

print(f"KMeans flagged: {km_anomaly.sum():,} / {len(pdf):,} ({km_anomaly.mean()*100:.2f}%)")
print(f"Cluster distribution:")
for c in range(k):
    count = (clusters == c).sum()
    print(f"  Cluster {c}: {count:,}")

if MLFLOW_OK:
    try:
        mlflow.sklearn.log_model(km, "kmeans")
        mlflow.end_run()
    except: pass

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Ensemble: Blend All Signals

# COMMAND ----------

if MLFLOW_OK:
    try: mlflow.start_run(run_name="ensemble_v1")
    except: pass

w_if = 0.45
w_km = 0.30
w_ai = 0.25

pdf["ensemble_score"] = (
    w_if * pdf["if_score"] +
    w_km * pdf["km_score"] +
    w_ai * pdf["ai_risk_score"].fillna(0)
)

pdf["ensemble_flag"] = pdf["ensemble_score"] > 0.5
pdf["final_risk_tier"] = pd.cut(
    pdf["ensemble_score"],
    bins=[-0.01, 0.3, 0.5, 0.7, 1.01],
    labels=["low", "medium", "high", "critical"]
)

# Evaluate
y_true = pdf["is_fraud"].fillna(False).astype(int)
y_pred = pdf["ensemble_flag"].astype(int)

precision, recall, f1, _ = precision_recall_fscore_support(y_true, y_pred, average="binary", zero_division=0)
try:
    auc = roc_auc_score(y_true, pdf["ensemble_score"])
except:
    auc = 0.0

safe_log_metric("ensemble_precision", precision)
safe_log_metric("ensemble_recall", recall)
safe_log_metric("ensemble_f1", f1)
safe_log_metric("ensemble_auc", auc)
safe_log_param("ensemble_weights", f"IF={w_if},KM={w_km},AI={w_ai}")

print(f"Ensemble Results:")
print(f"  Precision: {precision:.4f}")
print(f"  Recall:    {recall:.4f}")
print(f"  F1 Score:  {f1:.4f}")
print(f"  AUC-ROC:   {auc:.4f}")

total_flagged = pdf["ensemble_flag"].sum()
print(f"\nFlagged: {total_flagged:,} / {len(pdf):,} ({total_flagged/len(pdf)*100:.2f}%)")
print(f"\nRisk Tier Distribution:")
print(pdf["final_risk_tier"].value_counts().sort_index())

if MLFLOW_OK:
    try: mlflow.end_run()
    except: pass

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Feature Importance

# COMMAND ----------

from sklearn.inspection import permutation_importance

try:
    perm_result = permutation_importance(
        iforest, X_scaled[:min(2000, len(X_scaled))],
        y_true[:min(2000, len(y_true))],
        n_repeats=5, random_state=42, scoring="f1"
    )
    importance = pd.DataFrame({
        "feature": feature_cols,
        "importance": perm_result.importances_mean
    }).sort_values("importance", ascending=False)

    print("Feature Importance:")
    for _, row in importance.iterrows():
        print(f"  {row['feature']:30s} {row['importance']:.4f}")
except Exception as e:
    print(f"Permutation importance skipped: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Write Results to Delta Tables

# COMMAND ----------

# Join scores back to the full Spark DataFrame
scores_sdf = spark.createDataFrame(
    pdf[["transaction_id", "if_score", "km_score", "ensemble_score", "ensemble_flag", "final_risk_tier", "cluster"]]
)

fraud_enriched = (featured_df
    .join(scores_sdf, on="transaction_id", how="left")
    .fillna(0.0, subset=["if_score", "km_score", "ensemble_score"])
    .fillna(False, subset=["ensemble_flag"])
    .fillna("low", subset=["final_risk_tier"])
    .withColumn("isolation_forest_score", F.col("if_score"))
)

# Write enriched table
(fraud_enriched.write
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{catalog}.{schema}.gold_transactions_enriched"))

print(f"Written: {catalog}.{schema}.gold_transactions_enriched")

# Write alerts only
alerts = fraud_enriched.filter(F.col("ensemble_flag") == True)
(alerts.write
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{catalog}.{schema}.gold_fraud_alerts_ml"))

print(f"Alerts: {catalog}.{schema}.gold_fraud_alerts_ml ({alerts.count():,} rows)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Verify

# COMMAND ----------

display(spark.sql(f"""
SELECT final_risk_tier, COUNT(*) as count,
       ROUND(AVG(ensemble_score), 3) as avg_score,
       ROUND(AVG(amount), 2) as avg_amount
FROM {catalog}.{schema}.gold_transactions_enriched
GROUP BY final_risk_tier
ORDER BY avg_score DESC
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Complete
# MAGIC
# MAGIC **Created:** `gold_transactions_enriched` + `gold_fraud_alerts_ml`
# MAGIC
# MAGIC **Next:** Run `04-rag-pipeline.py`
