# Databricks notebook source
# MAGIC %md
# MAGIC # BlackIce: Fraud Ring Detection (Graph Intelligence)
# MAGIC
# MAGIC Analyzes synthetic fraud ring transaction data to detect:
# MAGIC 1. **Circular money flows** — A→B→C→A patterns
# MAGIC 2. **Hub accounts** — money mule nodes bridging rings
# MAGIC 3. **Ring statistics** — size, volume, density
# MAGIC
# MAGIC Data: 2,981 synthetic transactions with real sender→receiver topology.

# COMMAND ----------

# MAGIC %pip install networkx --quiet

# COMMAND ----------

# MAGIC %restart_python

# COMMAND ----------

dbutils.widgets.text("catalog", "digital_artha", "Catalog Name")
dbutils.widgets.text("schema", "main", "Schema Name")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

import numpy as np
import pandas as pd
from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Ingest Fraud Ring Data

# COMMAND ----------

source_path = f"/Volumes/{catalog}/{schema}/raw_data/"

ring_df = spark.read.json(f"{source_path}synthetic_fraud_rings.json")

# Map columns to match our schema
ring_bronze = (ring_df
    .select(
        F.col("`transaction id`").cast("string").alias("transaction_id"),
        F.col("`amount (INR)`").cast("double").alias("amount"),
        F.to_timestamp("timestamp").alias("transaction_time"),
        F.col("explicit_sender_id").cast("string").alias("sender_id"),
        F.col("explicit_receiver_id").cast("string").alias("receiver_id"),
        F.col("merchant_category").cast("string").alias("category"),
        F.col("`transaction type`").cast("string").alias("payment_mode"),
        F.col("device_type").cast("string"),
        F.col("sender_state").cast("string").alias("location"),
        F.col("fraud_flag").cast("boolean").alias("is_fraud"),
        F.col("sender_bank").cast("string"),
        F.col("receiver_bank").cast("string"),
        F.col("hour_of_day").cast("integer"),
        F.col("day_of_week").cast("string"),
    )
)

ring_bronze.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog}.{schema}.fraud_ring_transactions")
print(f"Fraud ring transactions: {ring_bronze.count():,}")
display(ring_bronze.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Build Transaction Graph

# COMMAND ----------

import networkx as nx

# Aggregate edges
edges_df = spark.sql(f"""
SELECT
    sender_id AS src,
    receiver_id AS dst,
    COUNT(*) AS txn_count,
    ROUND(SUM(amount), 2) AS total_amount,
    ROUND(AVG(amount), 2) AS avg_amount
FROM {catalog}.{schema}.fraud_ring_transactions
WHERE sender_id IS NOT NULL AND receiver_id IS NOT NULL
  AND sender_id != receiver_id
GROUP BY sender_id, receiver_id
""").toPandas()

# Build graph
G = nx.DiGraph()
for _, row in edges_df.iterrows():
    G.add_edge(row["src"], row["dst"],
               weight=int(row["txn_count"]),
               amount=float(row["total_amount"]))

print(f"Graph: {G.number_of_nodes():,} nodes, {G.number_of_edges():,} edges")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Detect Fraud Rings (Connected Components)

# COMMAND ----------

components = list(nx.connected_components(G.to_undirected()))
rings = [c for c in components if len(c) >= 3]

print(f"Connected components: {len(components)}")
print(f"Fraud rings (3+ nodes): {len(rings)}")

# Score each ring
ring_data = []
for i, members in enumerate(rings):
    sub = G.subgraph(members)
    total_amt = sum(d.get("amount", 0) for _, _, d in sub.edges(data=True))
    total_txns = sum(d.get("weight", 1) for _, _, d in sub.edges(data=True))

    # Check for cycles (circular flows)
    try:
        cycles = list(nx.simple_cycles(sub))
        cycle_count = len(cycles)
    except:
        cycle_count = 0

    ring_data.append({
        "ring_id": f"RING_{i:04d}",
        "accounts": len(members),
        "edges": sub.number_of_edges(),
        "total_transactions": int(total_txns),
        "total_amount": round(total_amt, 2),
        "density": round(nx.density(sub.to_undirected()), 4),
        "has_cycles": cycle_count > 0,
        "cycle_count": min(cycle_count, 100),  # cap for large graphs
        "members": ",".join(sorted(members)[:10]),
    })

ring_pdf = pd.DataFrame(ring_data)
ring_sdf = spark.createDataFrame(ring_pdf)
ring_sdf.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog}.{schema}.detected_fraud_rings")

print(f"\nFraud Rings Detected: {len(ring_data)}")
print(f"Rings with circular flows: {sum(1 for r in ring_data if r['has_cycles'])}")
print(f"Total accounts in rings: {sum(r['accounts'] for r in ring_data)}")
print(f"Total ₹ flowing through rings: ₹{sum(r['total_amount'] for r in ring_data):,.0f}")
display(ring_sdf.orderBy(F.desc("total_amount")))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. PageRank — Find Money Mule Hubs

# COMMAND ----------

pagerank = nx.pagerank(G, alpha=0.85, max_iter=100, weight="weight")

# Degree analysis
hub_data = []
for node in G.nodes():
    hub_data.append({
        "account_id": node,
        "in_connections": G.in_degree(node),
        "out_connections": G.out_degree(node),
        "total_connections": G.in_degree(node) + G.out_degree(node),
        "pagerank": round(pagerank.get(node, 0), 6),
        "is_hub": G.in_degree(node) + G.out_degree(node) > 4,
        "role": "sender" if G.out_degree(node) > G.in_degree(node) else "receiver",
    })

hub_pdf = pd.DataFrame(hub_data)
hub_sdf = spark.createDataFrame(hub_pdf)
hub_sdf.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog}.{schema}.fraud_ring_hubs")

hub_count = sum(1 for h in hub_data if h["is_hub"])
print(f"Hub accounts (4+ connections): {hub_count}")
print(f"\nTop 15 by PageRank:")
display(hub_sdf.orderBy(F.desc("pagerank")).limit(15))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Money Flow Analysis

# COMMAND ----------

flow = spark.sql(f"""
WITH outflow AS (
    SELECT sender_id AS account_id, SUM(amount) AS sent, COUNT(*) AS txns_out
    FROM {catalog}.{schema}.fraud_ring_transactions
    GROUP BY sender_id
),
inflow AS (
    SELECT receiver_id AS account_id, SUM(amount) AS received, COUNT(*) AS txns_in
    FROM {catalog}.{schema}.fraud_ring_transactions
    GROUP BY receiver_id
)
SELECT
    COALESCE(o.account_id, i.account_id) AS account_id,
    ROUND(COALESCE(o.sent, 0), 2) AS total_sent,
    ROUND(COALESCE(i.received, 0), 2) AS total_received,
    ROUND(COALESCE(i.received, 0) - COALESCE(o.sent, 0), 2) AS net_flow,
    COALESCE(o.txns_out, 0) AS txns_out,
    COALESCE(i.txns_in, 0) AS txns_in,
    CASE
        WHEN COALESCE(i.received, 0) > COALESCE(o.sent, 0) * 1.5 THEN 'NET_ACCUMULATOR'
        WHEN COALESCE(o.sent, 0) > COALESCE(i.received, 0) * 1.5 THEN 'NET_DISPERSER'
        ELSE 'PASS_THROUGH'
    END AS flow_role
FROM outflow o
FULL OUTER JOIN inflow i ON o.account_id = i.account_id
ORDER BY ABS(COALESCE(i.received, 0) - COALESCE(o.sent, 0)) DESC
""")

flow.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog}.{schema}.fraud_ring_flows")

print("Money Flow Roles:")
display(flow.groupBy("flow_role").agg(
    F.count("*").alias("accounts"),
    F.round(F.sum("total_sent"), 0).alias("total_sent"),
    F.round(F.sum("total_received"), 0).alias("total_received"),
))
print("\nTop Accumulators (money mules receiving most):")
display(flow.filter("flow_role = 'NET_ACCUMULATOR'").orderBy(F.desc("net_flow")).limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Ring Visualization Data (for Dashboard)

# COMMAND ----------

# Create edge list for visualization
viz_edges = spark.sql(f"""
SELECT
    sender_id AS source,
    receiver_id AS target,
    COUNT(*) AS weight,
    ROUND(SUM(amount), 0) AS volume
FROM {catalog}.{schema}.fraud_ring_transactions
GROUP BY sender_id, receiver_id
ORDER BY weight DESC
""")
viz_edges.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog}.{schema}.viz_fraud_ring_edges")

print(f"Edge list for visualization: {viz_edges.count()} edges")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Log to MLflow

# COMMAND ----------

import mlflow
mlflow.set_tracking_uri("databricks")
mlflow.set_registry_uri("databricks")

with mlflow.start_run(run_name="fraud_ring_detection"):
    mlflow.log_param("method", "NetworkX_connected_components")
    mlflow.log_param("data_source", "synthetic_fraud_rings")
    mlflow.log_param("graph_type", "directed")
    mlflow.log_metric("nodes", G.number_of_nodes())
    mlflow.log_metric("edges", G.number_of_edges())
    mlflow.log_metric("rings_detected", len(rings))
    mlflow.log_metric("rings_with_cycles", sum(1 for r in ring_data if r["has_cycles"]))
    mlflow.log_metric("hub_accounts", hub_count)
    mlflow.log_metric("total_ring_transactions", sum(r["total_transactions"] for r in ring_data))
    mlflow.log_metric("total_ring_amount", sum(r["total_amount"] for r in ring_data))
print("✅ MLflow logged: fraud_ring_detection")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Summary

# COMMAND ----------

print("=" * 60)
print("FRAUD RING INTELLIGENCE SUMMARY")
print("=" * 60)
print(f"  Transactions analyzed: {ring_bronze.count():,}")
print(f"  Graph: {G.number_of_nodes()} accounts, {G.number_of_edges()} edges")
print(f"  Fraud rings detected: {len(rings)}")
print(f"  Rings with circular flows: {sum(1 for r in ring_data if r['has_cycles'])}")
print(f"  Hub accounts: {hub_count}")
print(f"  Total ₹ in rings: ₹{sum(r['total_amount'] for r in ring_data):,.0f}")
print()
print("Tables created:")
print(f"  {catalog}.{schema}.fraud_ring_transactions (raw)")
print(f"  {catalog}.{schema}.detected_fraud_rings (ring metadata)")
print(f"  {catalog}.{schema}.fraud_ring_hubs (hub accounts + PageRank)")
print(f"  {catalog}.{schema}.fraud_ring_flows (money flow analysis)")
print(f"  {catalog}.{schema}.viz_fraud_ring_edges (edge list for viz)")
