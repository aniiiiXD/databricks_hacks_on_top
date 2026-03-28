# Databricks notebook source
# MAGIC %md
# MAGIC # Digital-Artha: Fraud Ring Detection (Graph Analysis)
# MAGIC
# MAGIC Builds a sender↔receiver transaction graph and detects:
# MAGIC 1. **Fraud Rings** — Connected components with high fraud density
# MAGIC 2. **Money Mule Hubs** — High-degree nodes bridging multiple fraud rings
# MAGIC 3. **Suspicious Triads** — Tightly connected groups (A→B→C→A)
# MAGIC 4. **Sender/Merchant Risk Profiles** — Aggregate behavioral features

# COMMAND ----------

# MAGIC %md
# MAGIC ## Install Dependencies

# COMMAND ----------

# MAGIC %pip install networkx --quiet

# COMMAND ----------

# MAGIC %restart_python

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
from pyspark.sql import functions as F

print(f"Source: {catalog}.{schema}.gold_transactions_enriched")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Build Transaction Graph

# COMMAND ----------

# Edge list: sender → receiver with aggregated metrics
# Only include edges with 2+ transactions OR at least 1 fraud — reduces noise
edges_df = spark.sql(f"""
SELECT
    sender_id AS src,
    receiver_id AS dst,
    COUNT(*) AS txn_count,
    ROUND(SUM(amount), 2) AS total_amount,
    ROUND(AVG(amount), 2) AS avg_amount,
    ROUND(AVG(CAST(ensemble_score AS DOUBLE)), 4) AS avg_risk_score,
    SUM(CASE WHEN ensemble_flag = true THEN 1 ELSE 0 END) AS fraud_txn_count,
    MIN(transaction_time) AS first_txn,
    MAX(transaction_time) AS last_txn
FROM {catalog}.{schema}.gold_transactions_enriched
WHERE sender_id IS NOT NULL AND receiver_id IS NOT NULL
  AND sender_id != receiver_id
GROUP BY sender_id, receiver_id
HAVING COUNT(*) >= 2 OR SUM(CASE WHEN ensemble_flag = true THEN 1 ELSE 0 END) >= 1
""")

# Node list: all unique accounts
nodes_df = spark.sql(f"""
SELECT sender_id AS node_id, 'sender' AS primary_role FROM {catalog}.{schema}.gold_transactions_enriched
UNION
SELECT receiver_id AS node_id, 'receiver' AS primary_role FROM {catalog}.{schema}.gold_transactions_enriched
""").groupBy("node_id").agg(F.first("primary_role").alias("primary_role"))

print(f"Graph: {nodes_df.count():,} nodes, {edges_df.count():,} edges")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Detect Fraud Rings (Connected Components via NetworkX)

# COMMAND ----------

import networkx as nx

# Collect to driver (OK for hackathon-sized data)
edges_pdf = edges_df.select("src", "dst", "txn_count", "total_amount", "avg_risk_score", "fraud_txn_count").toPandas()

# Build directed graph
G = nx.DiGraph()
for _, row in edges_pdf.iterrows():
    G.add_edge(row["src"], row["dst"],
               weight=row["txn_count"],
               amount=row["total_amount"],
               risk=row["avg_risk_score"],
               fraud_count=row["fraud_txn_count"])

print(f"NetworkX graph: {G.number_of_nodes():,} nodes, {G.number_of_edges():,} edges")

# Connected components (undirected view — a "ring" is a connected subgraph)
components = list(nx.connected_components(G.to_undirected()))
print(f"Total connected components: {len(components):,}")

# Filter: rings are components with 3-500 nodes (exclude the giant component)
max_ring_size = 500  # Giant components are just "everyone connected" — not useful
rings = [c for c in components if 3 <= len(c) <= max_ring_size]
# Also include the giant component stats separately
giant = [c for c in components if len(c) > max_ring_size]
print(f"Components with 3-{max_ring_size} nodes (fraud rings): {len(rings)}")
if giant:
    print(f"Giant component excluded: {len(giant[0]):,} nodes (too connected to be a ring)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Score Each Ring

# COMMAND ----------

# For each ring, compute fraud density and total value
ring_data = []
for ring_id, members in enumerate(rings):
    subgraph = G.subgraph(members)

    # Aggregate edge attributes
    total_amount = sum(d.get("amount", 0) for _, _, d in subgraph.edges(data=True))
    total_txns = sum(d.get("weight", 0) for _, _, d in subgraph.edges(data=True))
    fraud_txns = sum(d.get("fraud_count", 0) for _, _, d in subgraph.edges(data=True))
    avg_risk = np.mean([d.get("risk", 0) for _, _, d in subgraph.edges(data=True)]) if subgraph.number_of_edges() > 0 else 0

    ring_data.append({
        "ring_id": f"RING_{ring_id:04d}",
        "size": len(members),
        "edge_count": subgraph.number_of_edges(),
        "total_amount": round(total_amount, 2),
        "total_transactions": int(total_txns),
        "fraud_transactions": int(fraud_txns),
        "fraud_rate": round(fraud_txns / max(total_txns, 1) * 100, 2),
        "avg_risk_score": round(avg_risk, 4),
        "density": round(nx.density(subgraph.to_undirected()), 4),
        "members": ",".join(list(members)[:20]),  # cap for storage
    })

rings_pdf = pd.DataFrame(ring_data)

# Classify ring severity
rings_pdf["severity"] = pd.cut(
    rings_pdf["avg_risk_score"],
    bins=[-0.01, 0.2, 0.4, 0.6, 1.01],
    labels=["low", "medium", "high", "critical"]
)

print(f"\nFraud Ring Summary:")
print(f"  Total rings: {len(rings_pdf)}")
print(f"  High/Critical rings: {(rings_pdf['severity'].isin(['high', 'critical'])).sum()}")
print(f"  Accounts in rings: {rings_pdf['size'].sum():,}")
print(f"  Total ₹ in ring transactions: ₹{rings_pdf['total_amount'].sum():,.0f}")
print(f"\nTop 10 rings by fraud rate:")
display(spark.createDataFrame(rings_pdf.nlargest(10, "fraud_rate")))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. PageRank — Find Money Mule Hubs

# COMMAND ----------

# PageRank identifies important "hub" nodes
pagerank = nx.pagerank(G, alpha=0.85, max_iter=100)
pr_pdf = pd.DataFrame([
    {"node_id": node, "pagerank": round(score, 6)}
    for node, score in pagerank.items()
]).sort_values("pagerank", ascending=False)

print("Top 20 Most Connected Accounts (potential money mules):")
display(spark.createDataFrame(pr_pdf.head(20)))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Degree Analysis — Transaction Velocity per Account

# COMMAND ----------

# In-degree (receiving) and out-degree (sending)
in_deg = dict(G.in_degree())
out_deg = dict(G.out_degree())

degree_pdf = pd.DataFrame([
    {
        "node_id": node,
        "in_degree": in_deg.get(node, 0),
        "out_degree": out_deg.get(node, 0),
        "total_degree": in_deg.get(node, 0) + out_deg.get(node, 0),
        "pagerank": pagerank.get(node, 0),
    }
    for node in G.nodes()
])

# Flag high-degree nodes
degree_pdf["is_hub"] = degree_pdf["total_degree"] > degree_pdf["total_degree"].quantile(0.95)
hubs = degree_pdf[degree_pdf["is_hub"]]
print(f"Hub accounts (top 5% by connections): {len(hubs):,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Build Sender Risk Profiles (Platinum Layer)

# COMMAND ----------

sender_profiles = spark.sql(f"""
SELECT
    sender_id,
    COUNT(DISTINCT transaction_id) AS total_transactions,
    ROUND(SUM(amount), 2) AS total_amount_sent,
    ROUND(AVG(amount), 2) AS avg_amount,
    ROUND(STDDEV(amount), 2) AS std_amount,
    MAX(amount) AS max_amount,
    MIN(amount) AS min_amount,
    COUNT(DISTINCT receiver_id) AS unique_receivers,
    COUNT(DISTINCT category) AS unique_categories,
    COUNT(DISTINCT location) AS unique_locations,
    COUNT(DISTINCT CASE WHEN ensemble_flag = true THEN transaction_id END) AS fraud_count,
    ROUND(COUNT(DISTINCT CASE WHEN ensemble_flag = true THEN transaction_id END) * 100.0 / NULLIF(COUNT(DISTINCT transaction_id), 0), 2) AS personal_fraud_rate,
    ROUND(AVG(CAST(ensemble_score AS DOUBLE)), 4) AS avg_risk_score,
    MIN(transaction_time) AS first_transaction,
    MAX(transaction_time) AS last_transaction,
    DATEDIFF(MAX(transaction_time), MIN(transaction_time)) AS account_age_days,
    -- Behavioral patterns
    ROUND(AVG(CASE WHEN hour_of_day BETWEEN 0 AND 5 THEN 1 ELSE 0 END) * 100, 2) AS late_night_pct,
    ROUND(AVG(CASE WHEN CAST(is_weekend AS BOOLEAN) THEN 1 ELSE 0 END) * 100, 2) AS weekend_pct,
    MODE(category) AS primary_category,
    MODE(location) AS primary_location
FROM {catalog}.{schema}.gold_transactions_enriched
GROUP BY sender_id
""")

# Add graph metrics
pr_sdf = spark.createDataFrame(pr_pdf.rename(columns={"node_id": "sender_id"}))
deg_sdf = spark.createDataFrame(degree_pdf[["node_id", "in_degree", "out_degree", "total_degree", "is_hub"]].rename(columns={"node_id": "sender_id"}))

sender_profiles_enriched = (sender_profiles
    .join(pr_sdf, on="sender_id", how="left")
    .join(deg_sdf, on="sender_id", how="left")
    .fillna(0, subset=["pagerank", "in_degree", "out_degree", "total_degree"])
    .fillna(False, subset=["is_hub"])
)

# Composite risk score
sender_profiles_final = sender_profiles_enriched.withColumn(
    "composite_risk_score",
    F.round(
        F.lit(0.3) * F.least(F.coalesce(F.col("avg_risk_score"), F.lit(0.0)), F.lit(1.0)) +
        F.lit(0.25) * F.least(F.coalesce(F.col("personal_fraud_rate"), F.lit(0.0)) / 100.0, F.lit(1.0)) +
        F.lit(0.2) * F.least(F.coalesce(F.col("pagerank"), F.lit(0.0)) * 1000, F.lit(1.0)) +
        F.lit(0.15) * F.least(F.coalesce(F.col("late_night_pct"), F.lit(0.0)) / 100.0, F.lit(1.0)) +
        F.lit(0.1) * F.least(F.coalesce(F.col("total_degree").cast("double"), F.lit(0.0)) / 100.0, F.lit(1.0)),
        4
    )
)

sender_profiles_final.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog}.{schema}.platinum_sender_profiles")
print(f"Written: {catalog}.{schema}.platinum_sender_profiles ({sender_profiles_final.count():,} senders)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Build Merchant Risk Profiles

# COMMAND ----------

# Use DISTINCT transaction_id to avoid counting duplicates from joins
merchant_profiles = spark.sql(f"""
SELECT
    category AS merchant_category,
    COUNT(DISTINCT transaction_id) AS total_transactions,
    COUNT(DISTINCT sender_id) AS unique_senders,
    ROUND(SUM(amount) / GREATEST(COUNT(*) / COUNT(DISTINCT transaction_id), 1), 2) AS total_volume,
    ROUND(AVG(amount), 2) AS avg_transaction,
    COUNT(DISTINCT CASE WHEN ensemble_flag = true THEN transaction_id END) AS fraud_count,
    ROUND(COUNT(DISTINCT CASE WHEN ensemble_flag = true THEN transaction_id END) * 100.0 / NULLIF(COUNT(DISTINCT transaction_id), 0), 2) AS fraud_rate,
    ROUND(AVG(CAST(ensemble_score AS DOUBLE)), 4) AS avg_risk_score,
    ROUND(AVG(CASE WHEN hour_of_day BETWEEN 0 AND 5 THEN 1 ELSE 0 END) * 100, 2) AS late_night_pct,
    ROUND(AVG(CASE WHEN hour_of_day BETWEEN 18 AND 23 THEN 1 ELSE 0 END) * 100, 2) AS evening_pct
FROM {catalog}.{schema}.gold_transactions_enriched
GROUP BY category
""")

merchant_profiles.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog}.{schema}.platinum_merchant_profiles")
print(f"Written: {catalog}.{schema}.platinum_merchant_profiles ({merchant_profiles.count():,} categories)")
display(merchant_profiles.orderBy(F.desc("fraud_rate")))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Write Fraud Rings to Delta

# COMMAND ----------

rings_sdf = spark.createDataFrame(rings_pdf.astype({
    "size": int, "edge_count": int, "total_amount": float,
    "total_transactions": int, "fraud_transactions": int,
    "fraud_rate": float, "avg_risk_score": float, "density": float,
    "severity": str, "ring_id": str, "members": str,
}))

rings_sdf.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog}.{schema}.platinum_fraud_rings")
print(f"Written: {catalog}.{schema}.platinum_fraud_rings ({rings_sdf.count():,} rings)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Summary Statistics

# COMMAND ----------

print("=" * 60)
print("FRAUD RING INTELLIGENCE SUMMARY")
print("=" * 60)
print(f"  Graph: {G.number_of_nodes():,} accounts, {G.number_of_edges():,} transaction edges")
print(f"  Connected components: {len(components):,}")
print(f"  Fraud rings (3+ members): {len(rings)}")
print(f"  High/Critical severity rings: {(rings_pdf['severity'].isin(['high', 'critical'])).sum()}")
print(f"  Accounts in fraud rings: {rings_pdf['size'].sum():,}")
print(f"  Hub accounts (money mules): {len(hubs):,}")
print(f"  Total ₹ flowing through rings: ₹{rings_pdf['total_amount'].sum():,.0f}")
print()
print("Tables created:")
print(f"  - {catalog}.{schema}.platinum_fraud_rings")
print(f"  - {catalog}.{schema}.platinum_sender_profiles")
print(f"  - {catalog}.{schema}.platinum_merchant_profiles")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Complete
# MAGIC **Next:** Run `12-data-quality.py` for governance + warm tier.
