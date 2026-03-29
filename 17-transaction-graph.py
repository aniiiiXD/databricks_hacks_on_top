# Databricks notebook source
# MAGIC %md
# MAGIC # BlackIce: Transaction Relationship Graph
# MAGIC
# MAGIC Builds sender↔receiver relationship graph to discover:
# MAGIC 1. **High-volume corridors** — who sends money to whom most frequently
# MAGIC 2. **Hub accounts** — senders/receivers with unusually many connections
# MAGIC 3. **Suspicious clusters** — groups with high fraud concentration
# MAGIC 4. **Money flow patterns** — how funds move through the network

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

# Detect available columns
enriched_cols = [c.name for c in spark.table(f"{catalog}.{schema}.gold_transactions_enriched").schema]
flag_expr = "ensemble_flag = true" if "ensemble_flag" in enriched_cols else "is_fraud = true"
score_col = "ensemble_score" if "ensemble_score" in enriched_cols else "ai_risk_score"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Build Transaction Relationship Summary

# COMMAND ----------

# Aggregate sender → receiver relationships
relationships = spark.sql(f"""
SELECT
    sender_id,
    receiver_id,
    COUNT(DISTINCT transaction_id) AS txn_count,
    ROUND(SUM(amount), 2) AS total_amount,
    ROUND(AVG(amount), 2) AS avg_amount,
    MAX(amount) AS max_single_txn,
    COUNT(DISTINCT CASE WHEN {flag_expr} THEN transaction_id END) AS fraud_txn_count,
    ROUND(AVG(CAST({score_col} AS DOUBLE)), 4) AS avg_risk_score,
    COUNT(DISTINCT category) AS categories_used,
    MIN(transaction_time) AS first_txn,
    MAX(transaction_time) AS last_txn,
    DATEDIFF(MAX(transaction_time), MIN(transaction_time)) AS relationship_days,
    -- Relationship risk indicators
    ROUND(COUNT(DISTINCT CASE WHEN {flag_expr} THEN transaction_id END) * 100.0 /
        NULLIF(COUNT(DISTINCT transaction_id), 0), 2) AS relationship_fraud_rate,
    CASE
        WHEN COUNT(DISTINCT transaction_id) >= 10 THEN 'frequent'
        WHEN COUNT(DISTINCT transaction_id) >= 5 THEN 'regular'
        WHEN COUNT(DISTINCT transaction_id) >= 2 THEN 'occasional'
        ELSE 'one_time'
    END AS relationship_type
FROM {catalog}.{schema}.gold_transactions_enriched
WHERE sender_id IS NOT NULL AND receiver_id IS NOT NULL
  AND sender_id != receiver_id
GROUP BY sender_id, receiver_id
""")

relationships.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog}.{schema}.transaction_relationships")
print(f"Transaction relationships: {relationships.count():,}")
display(relationships.orderBy(F.desc("txn_count")).limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Network Analysis with NetworkX

# COMMAND ----------

import networkx as nx

# Get top relationships (most active + most risky)
top_edges = spark.sql(f"""
SELECT sender_id, receiver_id, txn_count, total_amount,
       fraud_txn_count, avg_risk_score, relationship_type, relationship_fraud_rate
FROM {catalog}.{schema}.transaction_relationships
WHERE txn_count >= 2 OR fraud_txn_count >= 1
ORDER BY txn_count DESC
LIMIT 5000
""").toPandas()

# Build graph
G = nx.DiGraph()
for _, row in top_edges.iterrows():
    G.add_edge(
        row["sender_id"], row["receiver_id"],
        weight=int(row["txn_count"]),
        amount=float(row["total_amount"]),
        fraud=int(row["fraud_txn_count"]),
        risk=float(row["avg_risk_score"]),
    )

print(f"Graph: {G.number_of_nodes():,} accounts, {G.number_of_edges():,} relationships")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Identify Hub Accounts (High Connectivity)

# COMMAND ----------

# Degree analysis
in_degree = dict(G.in_degree(weight="weight"))
out_degree = dict(G.out_degree(weight="weight"))

# PageRank for influence scoring
try:
    pagerank = nx.pagerank(G, alpha=0.85, max_iter=50, weight="weight")
except:
    pagerank = {n: 0.0 for n in G.nodes()}

# Build hub analysis
hub_data = []
for node in G.nodes():
    in_d = in_degree.get(node, 0)
    out_d = out_degree.get(node, 0)

    # Get fraud stats for this node's edges
    node_edges = list(G.edges(node, data=True)) + list(G.in_edges(node, data=True))
    fraud_edges = sum(1 for _, _, d in node_edges if d.get("fraud", 0) > 0)
    total_amount = sum(d.get("amount", 0) for _, _, d in node_edges)

    hub_data.append({
        "account_id": node,
        "in_connections": int(G.in_degree(node)),
        "out_connections": int(G.out_degree(node)),
        "total_connections": int(G.in_degree(node) + G.out_degree(node)),
        "weighted_in_txns": in_d,
        "weighted_out_txns": out_d,
        "total_volume": round(total_amount, 2),
        "fraud_edges": fraud_edges,
        "pagerank": round(pagerank.get(node, 0), 6),
        "account_type": "sender" if out_d > in_d else "receiver",
        "is_hub": int(G.in_degree(node) + G.out_degree(node)) > np.percentile(
            [G.in_degree(n) + G.out_degree(n) for n in G.nodes()], 95
        ),
    })

hub_df = pd.DataFrame(hub_data)
hub_sdf = spark.createDataFrame(hub_df)
hub_sdf.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog}.{schema}.network_hub_accounts")

print(f"Hub accounts (top 5% connectivity): {hub_df['is_hub'].sum()}")
print(f"\nTop 10 Most Connected Accounts:")
display(hub_sdf.orderBy(F.desc("total_connections")).limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Detect Suspicious Clusters

# COMMAND ----------

# Find connected components
undirected = G.to_undirected()
components = list(nx.connected_components(undirected))

# Analyze each cluster
cluster_data = []
for i, members in enumerate(components):
    if len(members) < 3:
        continue

    subgraph = G.subgraph(members)
    total_fraud = sum(d.get("fraud", 0) for _, _, d in subgraph.edges(data=True))
    total_txns = sum(d.get("weight", 1) for _, _, d in subgraph.edges(data=True))
    total_amount = sum(d.get("amount", 0) for _, _, d in subgraph.edges(data=True))
    avg_risk = np.mean([d.get("risk", 0) for _, _, d in subgraph.edges(data=True)]) if subgraph.number_of_edges() > 0 else 0

    cluster_data.append({
        "cluster_id": f"CLUSTER_{i:04d}",
        "accounts": len(members),
        "edges": subgraph.number_of_edges(),
        "total_transactions": int(total_txns),
        "total_amount": round(total_amount, 2),
        "fraud_transactions": int(total_fraud),
        "fraud_rate": round(total_fraud / max(total_txns, 1) * 100, 2),
        "avg_risk_score": round(avg_risk, 4),
        "density": round(nx.density(undirected.subgraph(members)), 4),
        "risk_level": "HIGH" if avg_risk > 0.3 or total_fraud > 5 else "MEDIUM" if total_fraud > 0 else "LOW",
    })

cluster_df = pd.DataFrame(cluster_data) if cluster_data else pd.DataFrame(
    columns=["cluster_id", "accounts", "edges", "total_transactions", "total_amount",
             "fraud_transactions", "fraud_rate", "avg_risk_score", "density", "risk_level"]
)

cluster_sdf = spark.createDataFrame(cluster_df)
cluster_sdf.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog}.{schema}.transaction_clusters")

print(f"Transaction clusters found: {len(cluster_data)}")
if len(cluster_data) > 0:
    high_risk = sum(1 for c in cluster_data if c["risk_level"] == "HIGH")
    print(f"High risk clusters: {high_risk}")
    display(cluster_sdf.orderBy(F.desc("fraud_rate")).limit(10))
else:
    print("No multi-account clusters found (may need more diverse sender-receiver pairs)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Relationship Pattern Summary

# COMMAND ----------

# What types of relationships exist?
rel_summary = spark.sql(f"""
SELECT
    relationship_type,
    COUNT(*) AS relationship_count,
    ROUND(AVG(txn_count), 1) AS avg_txns_per_pair,
    ROUND(AVG(total_amount), 2) AS avg_volume,
    SUM(fraud_txn_count) AS total_fraud_txns,
    ROUND(AVG(relationship_fraud_rate), 2) AS avg_fraud_rate,
    ROUND(AVG(relationship_days), 0) AS avg_relationship_days
FROM {catalog}.{schema}.transaction_relationships
GROUP BY relationship_type
ORDER BY relationship_count DESC
""")

print("Relationship Pattern Summary:")
display(rel_summary)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Money Flow Direction Analysis

# COMMAND ----------

# Which accounts are net senders vs net receivers?
flow_analysis = spark.sql(f"""
WITH outflow AS (
    SELECT sender_id AS account_id, SUM(total_amount) AS amount_sent, SUM(txn_count) AS txns_sent
    FROM {catalog}.{schema}.transaction_relationships
    GROUP BY sender_id
),
inflow AS (
    SELECT receiver_id AS account_id, SUM(total_amount) AS amount_received, SUM(txn_count) AS txns_received
    FROM {catalog}.{schema}.transaction_relationships
    GROUP BY receiver_id
)
SELECT
    COALESCE(o.account_id, i.account_id) AS account_id,
    COALESCE(o.amount_sent, 0) AS total_sent,
    COALESCE(i.amount_received, 0) AS total_received,
    COALESCE(i.amount_received, 0) - COALESCE(o.amount_sent, 0) AS net_flow,
    CASE
        WHEN COALESCE(i.amount_received, 0) > COALESCE(o.amount_sent, 0) * 2 THEN 'NET_RECEIVER (2x+)'
        WHEN COALESCE(o.amount_sent, 0) > COALESCE(i.amount_received, 0) * 2 THEN 'NET_SENDER (2x+)'
        ELSE 'BALANCED'
    END AS flow_pattern
FROM outflow o
FULL OUTER JOIN inflow i ON o.account_id = i.account_id
ORDER BY ABS(COALESCE(i.amount_received, 0) - COALESCE(o.amount_sent, 0)) DESC
LIMIT 20
""")

print("Top 20 Accounts by Net Money Flow:")
display(flow_analysis)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Summary

# COMMAND ----------

print("=" * 60)
print("TRANSACTION RELATIONSHIP GRAPH SUMMARY")
print("=" * 60)
print(f"  Graph: {G.number_of_nodes():,} accounts, {G.number_of_edges():,} relationships")
print(f"  Hub accounts: {hub_df['is_hub'].sum()}")
print(f"  Transaction clusters: {len(cluster_data)}")
if cluster_data:
    print(f"  High-risk clusters: {sum(1 for c in cluster_data if c['risk_level'] == 'HIGH')}")
print()
print("Tables created:")
print(f"  - {catalog}.{schema}.transaction_relationships")
print(f"  - {catalog}.{schema}.network_hub_accounts")
print(f"  - {catalog}.{schema}.transaction_clusters")
