# Databricks notebook source
# MAGIC %md
# MAGIC # Digital-Artha: Fraud Detection ML Pipeline
# MAGIC
# MAGIC **Ensemble anomaly detection** (pure sklearn — serverless compatible):
# MAGIC 1. **Isolation Forest** — unsupervised outlier detection
# MAGIC 2. **K-Means + Distance** — cluster-based anomaly scoring
# MAGIC 3. **Weighted ensemble** — blends both scores + rule-based risk

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

dbutils.widgets.text("catalog", "digital_artha", "Catalog Name")
dbutils.widgets.text("schema", "main", "Schema Name")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

import numpy as np
import pandas as pd
from datetime import datetime
from sklearn.ensemble import IsolationForest
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import precision_recall_fscore_support, roc_auc_score
import warnings
warnings.filterwarnings('ignore')

print(f"Data source: {catalog}.{schema}.gold_transactions")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Load Data & Explore

# COMMAND ----------

from pyspark.sql import functions as F

gold_df = spark.table(f"{catalog}.{schema}.gold_transactions")
print(f"Total transactions: {gold_df.count():,}")

display(gold_df.groupBy("is_fraud").count().orderBy("is_fraud"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Feature Engineering (Spark SQL)

# COMMAND ----------

featured_df = spark.sql(f"""
WITH sender_stats AS (
    SELECT
        sender_id,
        CAST(AVG(amount) AS DOUBLE) AS sender_avg_amount,
        CAST(COALESCE(STDDEV(amount), 0) AS DOUBLE) AS sender_std_amount,
        CAST(MAX(amount) AS DOUBLE) AS sender_max_amount,
        CAST(COUNT(*) AS DOUBLE) AS sender_txn_count
    FROM {catalog}.{schema}.gold_transactions
    GROUP BY sender_id
)
-- deduplicate: one row per transaction_id
SELECT
    t.*,
    CAST(s.sender_avg_amount AS DOUBLE) AS sender_avg_amount,
    CAST(COALESCE(s.sender_std_amount, 0) AS DOUBLE) AS sender_std_amount,
    CAST(s.sender_max_amount AS DOUBLE) AS sender_max_amount,
    CAST(s.sender_txn_count AS DOUBLE) AS sender_txn_count,
    CAST(CASE WHEN s.sender_std_amount > 0
        THEN (t.amount - s.sender_avg_amount) / s.sender_std_amount
        ELSE 0.0
    END AS DOUBLE) AS amount_deviation,
    CAST(CASE WHEN s.sender_max_amount > 0
        THEN t.amount / s.sender_max_amount
        ELSE 1.0
    END AS DOUBLE) AS amount_ratio_to_max,
    CAST(CASE WHEN s.sender_txn_count <= 2 THEN 1 ELSE 0 END AS DOUBLE) AS is_new_sender,
    CAST(CASE WHEN t.hour_of_day BETWEEN 0 AND 5 AND t.amount > 5000 THEN 1 ELSE 0 END AS DOUBLE) AS late_night_high_value,
    CAST(CASE
        WHEN t.day_of_week = 'Monday' THEN 1
        WHEN t.day_of_week = 'Tuesday' THEN 2
        WHEN t.day_of_week = 'Wednesday' THEN 3
        WHEN t.day_of_week = 'Thursday' THEN 4
        WHEN t.day_of_week = 'Friday' THEN 5
        WHEN t.day_of_week = 'Saturday' THEN 6
        WHEN t.day_of_week = 'Sunday' THEN 7
        ELSE 0
    END AS DOUBLE) AS day_of_week_num,
    CAST(COALESCE(t.is_weekend, false) AS DOUBLE) AS is_weekend_num,
    CAST(COALESCE(t.ai_risk_score, 0) AS DOUBLE) AS ai_risk_score_num
FROM {catalog}.{schema}.gold_transactions t
LEFT JOIN sender_stats s ON t.sender_id = s.sender_id
""")

print(f"Feature-engineered: {featured_df.count():,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Convert to Pandas

# COMMAND ----------

feature_cols = [
    "amount", "hour_of_day", "day_of_week_num", "is_weekend_num",
    "sender_avg_amount", "sender_std_amount",
    "amount_deviation", "amount_ratio_to_max",
    "is_new_sender", "late_night_high_value",
    "ai_risk_score_num"
]

# Stratified sampling — preserve ALL fraud rows + sample normal rows
row_count = featured_df.count()
fraud_df = featured_df.filter(F.col("is_fraud") == True).select(feature_cols + ["transaction_id", "is_fraud"])
normal_df = featured_df.filter((F.col("is_fraud") == False) | (F.col("is_fraud").isNull())).select(feature_cols + ["transaction_id", "is_fraud"])

fraud_count = fraud_df.count()
normal_count = normal_df.count()
print(f"Total: {row_count:,} | Fraud: {fraud_count:,} | Normal: {normal_count:,}")

# Keep all fraud, sample normal to max 200K total
max_normal = min(200000 - fraud_count, normal_count)
if normal_count > max_normal:
    normal_sampled = normal_df.sample(fraction=max_normal / normal_count, seed=42)
else:
    normal_sampled = normal_df

pdf = fraud_df.unionByName(normal_sampled).toPandas()

for col in feature_cols:
    pdf[col] = pd.to_numeric(pdf[col], errors="coerce").fillna(0)

pdf["is_fraud"] = pdf["is_fraud"].fillna(False).astype(bool)

print(f"Training on {len(pdf):,} transactions")
print(f"Fraud count: {pdf['is_fraud'].sum():,} ({pdf['is_fraud'].mean()*100:.2f}%)")

X = pdf[feature_cols].values.astype(np.float64)
scaler = StandardScaler()
X_scaled = scaler.fit_transform(X)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Model A: Isolation Forest

# COMMAND ----------

iforest = IsolationForest(
    n_estimators=300,
    contamination=0.02,
    max_samples=min(len(X_scaled), 10000),
    max_features=0.8,
    random_state=42,
    n_jobs=-1
)
iforest.fit(X_scaled)
print("Isolation Forest trained.")

if_predictions = iforest.predict(X_scaled)
if_scores = iforest.decision_function(X_scaled)
if_scores_norm = 1 - (if_scores - if_scores.min()) / (if_scores.max() - if_scores.min() + 1e-10)

pdf["if_anomaly"] = (if_predictions == -1).astype(int)
pdf["if_score"] = if_scores_norm

y_true = pdf["is_fraud"].astype(int)
y_pred = pdf["if_anomaly"]
precision, recall, f1, _ = precision_recall_fscore_support(y_true, y_pred, average="binary", zero_division=0)
try:
    auc = roc_auc_score(y_true, if_scores_norm)
except:
    auc = 0.0

print(f"\nIsolation Forest Results:")
print(f"  Precision: {precision:.4f}")
print(f"  Recall:    {recall:.4f}")
print(f"  F1 Score:  {f1:.4f}")
print(f"  AUC-ROC:   {auc:.4f}")
print(f"  Flagged:   {pdf['if_anomaly'].sum():,} / {len(pdf):,} ({pdf['if_anomaly'].mean()*100:.2f}%)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Model B: K-Means Anomaly Detection

# COMMAND ----------

k = 8
km = KMeans(n_clusters=k, random_state=42, n_init=10)
km.fit(X_scaled)
print(f"KMeans trained with k={k}")

clusters = km.predict(X_scaled)
centers = km.cluster_centers_

distances = np.sqrt(np.sum((X_scaled - centers[clusters]) ** 2, axis=1))
km_scores = distances / (distances.max() + 1e-10)
threshold = np.percentile(distances, 95)
km_anomaly = (distances > threshold).astype(int)

pdf["km_score"] = km_scores
pdf["km_anomaly"] = km_anomaly
pdf["cluster"] = clusters

print(f"KMeans flagged: {km_anomaly.sum():,} / {len(pdf):,} ({km_anomaly.mean()*100:.2f}%)")
print(f"\nCluster distribution:")
for c in range(k):
    print(f"  Cluster {c}: {(clusters == c).sum():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Ensemble: Blend All Signals

# COMMAND ----------

w_if = 0.45
w_km = 0.30
w_ai = 0.25

pdf["ensemble_score"] = (
    w_if * pdf["if_score"] +
    w_km * pdf["km_score"] +
    w_ai * pdf["ai_risk_score_num"]
)

pdf["ensemble_flag"] = pdf["ensemble_score"] > 0.5
pdf["final_risk_tier"] = pd.cut(
    pdf["ensemble_score"],
    bins=[-0.01, 0.3, 0.5, 0.7, 1.01],
    labels=["low", "medium", "high", "critical"]
)

y_true = pdf["is_fraud"].astype(int)
y_pred = pdf["ensemble_flag"].astype(int)
precision, recall, f1, _ = precision_recall_fscore_support(y_true, y_pred, average="binary", zero_division=0)
try:
    auc = roc_auc_score(y_true, pdf["ensemble_score"])
except:
    auc = 0.0

print(f"Ensemble Results:")
print(f"  Precision: {precision:.4f}")
print(f"  Recall:    {recall:.4f}")
print(f"  F1 Score:  {f1:.4f}")
print(f"  AUC-ROC:   {auc:.4f}")
print(f"  Flagged:   {pdf['ensemble_flag'].sum():,} / {len(pdf):,}")
print(f"\nRisk Tier Distribution:")
print(pdf["final_risk_tier"].value_counts().sort_index().to_string())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Feature Importance

# COMMAND ----------

from sklearn.inspection import permutation_importance

try:
    perm = permutation_importance(
        iforest, X_scaled[:min(2000, len(X_scaled))],
        y_true.values[:min(2000, len(y_true))],
        n_repeats=5, random_state=42, scoring="f1"
    )
    imp = pd.DataFrame({"feature": feature_cols, "importance": perm.importances_mean})
    imp = imp.sort_values("importance", ascending=False)
    print("Feature Importance:")
    for _, row in imp.iterrows():
        print(f"  {row['feature']:30s} {row['importance']:.4f}")
except Exception as e:
    print(f"Permutation importance skipped: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Write Results to Delta Tables

# COMMAND ----------

scores_sdf = spark.createDataFrame(
    pdf[["transaction_id", "if_score", "km_score", "ensemble_score", "ensemble_flag", "final_risk_tier", "cluster"]].astype({
        "if_score": float, "km_score": float, "ensemble_score": float,
        "ensemble_flag": bool, "cluster": int, "final_risk_tier": str
    })
)

fraud_enriched = (featured_df
    .join(scores_sdf, on="transaction_id", how="left")
    .fillna(0.0, subset=["if_score", "km_score", "ensemble_score"])
    .fillna(False, subset=["ensemble_flag"])
    .fillna("low", subset=["final_risk_tier"])
    .withColumn("isolation_forest_score", F.col("if_score"))
)

(fraud_enriched.write
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{catalog}.{schema}.gold_transactions_enriched"))

print(f"Written: {catalog}.{schema}.gold_transactions_enriched")

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
       ROUND(AVG(CAST(ensemble_score AS DOUBLE)), 3) as avg_score,
       ROUND(AVG(amount), 2) as avg_amount
FROM {catalog}.{schema}.gold_transactions_enriched
GROUP BY final_risk_tier
ORDER BY avg_score DESC
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Log to MLflow (best-effort)

# COMMAND ----------

# MLflow works on Free Edition IF you skip set_experiment()!
# start_run() uses the default experiment automatically.
import mlflow
import mlflow.sklearn

try:
    with mlflow.start_run(run_name="fraud_ensemble_final"):
        # Log parameters
        mlflow.log_param("model_a", "IsolationForest")
        mlflow.log_param("model_b", "KMeans")
        mlflow.log_param("if_n_estimators", 300)
        mlflow.log_param("if_contamination", 0.02)
        mlflow.log_param("km_k", 8)
        mlflow.log_param("ensemble_weights", "IF=0.45,KM=0.30,AI=0.25")
        mlflow.log_param("stratified_sampling", "all_fraud_preserved")
        mlflow.log_param("feature_count", len(feature_cols))
        mlflow.log_param("training_rows", len(pdf))

        # Log metrics
        mlflow.log_metric("precision", precision)
        mlflow.log_metric("recall", recall)
        mlflow.log_metric("f1", f1)
        mlflow.log_metric("auc_roc", auc)
        mlflow.log_metric("total_flagged", int(pdf["ensemble_flag"].sum()))
        mlflow.log_metric("total_scored", len(pdf))
        mlflow.log_metric("flag_rate", float(pdf["ensemble_flag"].mean()))
        mlflow.log_metric("anomaly_patterns", 8)

        # Log models
        mlflow.sklearn.log_model(iforest, "isolation_forest")
        mlflow.sklearn.log_model(km, "kmeans")
        mlflow.sklearn.log_model(scaler, "scaler")

        run_id = mlflow.active_run().info.run_id

    print(f"✅ MLflow logging complete! Run ID: {run_id}")
    print(f"   View at: Experiments → Default → {run_id}")
except Exception as e:
    print(f"MLflow logging failed: {e}")
    print("Training results are saved in Delta tables.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Complete
# MAGIC
# MAGIC **Created:** `gold_transactions_enriched` + `gold_fraud_alerts_ml`
# MAGIC
# MAGIC **Next:** Run `04-rag-pipeline.py`
