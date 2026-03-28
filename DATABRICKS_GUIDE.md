# Databricks Free Edition — Hackathon Survival Guide

> Everything your team needs to build on Databricks Free Edition in 24 hours.
> Patterns sourced from the `bharatbricksiitb` starter kit and official Databricks docs.

---

## 1. Platform Basics

### What You Get
- **1 Workspace**, **1 Metastore** (Unity Catalog)
- **Serverless compute** only (Python + SQL — no R, no Scala)
- **1 SQL Warehouse** (2X-Small)
- **1 Databricks App** (auto-stops after 24h, restartable)
- **1 Vector Search endpoint** (1 unit, no Direct Vector Access)
- **1 active pipeline** per type (Lakeflow Declarative Pipelines)
- **5 concurrent job tasks** max
- **MLflow** fully available
- **Foundation Model APIs** (LLMs hosted by Databricks — no GPU needed on your end)
- **Genie Code** AI assistant in notebooks and SQL editor

### What You Don't Get
- GPU compute / dedicated endpoints / provisioned throughput
- Multiple workspaces or warehouses
- R / Scala / custom clusters
- Online tables, Clean Rooms, Agent Bricks, Lakebase
- Private networking, SSO, SCIM
- Outbound internet access is **restricted to trusted domains only**

### Fair Usage
- Exceed quota → compute shuts down for the rest of the day (or month in extreme cases)
- Data and settings preserved; you resume when limits reset
- **Strategy:** Don't leave notebooks/warehouses idle. Kill what you're not using. Sample data for dev, full data for final run.

---

## 2. Unity Catalog (Data Governance)

### Hierarchy
```
Metastore (auto-created)
  └── Catalog (e.g., `digital_artha`)
        └── Schema (e.g., `main`)
              ├── Tables (Delta)
              ├── Views / Materialized Views / Streaming Tables
              ├── Volumes (file storage for raw data)
              ├── Functions
              └── ML Models (via MLflow registry)
```

### Key Commands
```sql
-- Setup (run once)
USE CATALOG digital_artha;
CREATE SCHEMA IF NOT EXISTS digital_artha.main;
CREATE VOLUME IF NOT EXISTS digital_artha.main.raw_data;

-- Grant access to teammates
GRANT ALL PRIVILEGES ON CATALOG digital_artha TO `teammate@email.com`;
```

### Volumes (File Storage)
Upload raw data (JSON, CSV, PDF) to Volumes via the UI or programmatically.
Files live at: `/Volumes/{catalog}/{schema}/{volume}/{filename}`

---

## 3. Data Ingestion

### Option A: Auto Loader (Recommended — from starter kit)

The starter kit uses this exact pattern. Auto Loader provides streaming ingestion with exactly-once semantics, schema inference, and schema evolution.

```python
# Parameterize with widgets (from starter kit pattern)
dbutils.widgets.text("catalog", "digital_artha")
dbutils.widgets.text("schema", "main")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

source_path = f"/Volumes/{catalog}/{schema}/raw_data/"
checkpoint_path = f"{source_path}/_checkpoints"

spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")

# Ingest JSON with Auto Loader
df = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("multiLine", "true")                    # for pretty-printed JSON
    .option("cloudFiles.inferColumnTypes", "true")   # auto-detect types
    .option("cloudFiles.schemaLocation", f"{checkpoint_path}/transactions_schema")
    .option("pathGlobFilter", "*_transactions.json") # filter specific files
    .load(source_path))

# Transform during ingestion (flatten, rename, derive columns)
from pyspark.sql import functions as F

ingested = (df
    .select(
        F.col("txn_id").alias("transaction_id"),
        F.col("amount").cast("double"),
        F.from_unixtime("timestamp").cast("timestamp").alias("transaction_time"),
        F.col("sender_id"),
        F.col("receiver_id"),
        F.col("category"),
        # Derive time features during ingestion
        F.hour(F.from_unixtime("timestamp")).alias("hour_of_day"),
        F.dayofweek(F.from_unixtime("timestamp")).alias("day_of_week"),
    ))

# Write to Delta table with trigger(availableNow=True) for batch-style streaming
(ingested.writeStream
    .option("checkpointLocation", f"{checkpoint_path}/transactions")
    .option("mergeSchema", "true")
    .trigger(availableNow=True)
    .toTable(f"{catalog}.{schema}.bronze_transactions"))
```

### Option B: Direct Spark Read (Simplest, for small files)
```python
df = spark.read.json(f"/Volumes/{catalog}/{schema}/raw_data/schemes.json")
df.write.mode("overwrite").saveAsTable(f"{catalog}.{schema}.bronze_schemes")
```

### Option C: Explode Nested JSON (from starter kit — comments pattern)
```python
# When JSON has nested arrays
raw = (spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("multiLine", "true")
    .option("cloudFiles.inferColumnTypes", "true")
    .option("cloudFiles.schemaLocation", f"{checkpoint_path}/nested_schema")
    .load(source_path))

# Explode nested array into rows
flattened = (raw
    .select(F.explode("items").alias("item"))
    .select(
        F.col("item.id").alias("item_id"),
        F.col("item.text").alias("text"),
        F.col("item.parent_id"),
    ))
```

### After Ingestion: Governance Setup (from starter kit)
```sql
-- Primary/Foreign key constraints (judges love this)
ALTER TABLE bronze_transactions ALTER COLUMN transaction_id SET NOT NULL;
ALTER TABLE bronze_transactions ADD CONSTRAINT txn_pk PRIMARY KEY (transaction_id);

-- Enable Change Data Feed (for incremental downstream processing)
ALTER TABLE bronze_transactions SET TBLPROPERTIES (delta.enableChangeDataFeed = true);

-- Add column-level documentation (shows in Unity Catalog UI)
ALTER TABLE bronze_transactions ALTER COLUMN amount COMMENT 'Transaction amount in INR';
ALTER TABLE bronze_transactions ALTER COLUMN hour_of_day COMMENT 'Hour of transaction (0-23), derived from timestamp';
COMMENT ON TABLE bronze_transactions IS 'Raw UPI transaction data ingested from synthetic dataset via Auto Loader';
```

---

## 4. Delta Lake

Every table on Databricks is Delta by default. You get ACID transactions, schema enforcement, time travel, and efficient reads for free.

### Essential Commands
```sql
-- Upsert (merge) — idempotent reprocessing
MERGE INTO target USING source ON target.id = source.id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;

-- Time travel (rollback mistakes)
SELECT * FROM my_table VERSION AS OF 3;
SELECT * FROM my_table TIMESTAMP AS OF '2026-03-28';
DESCRIBE HISTORY my_table;

-- Maintenance
OPTIMIZE my_table;  -- compact small files for faster reads
```

---

## 5. Lakeflow Declarative Pipelines (DLT) — The Real Transform Pattern

**This is what the starter kit actually uses.** Not plain `CREATE TABLE AS SELECT` — it uses **DLT Streaming Tables and Materialized Views** with data quality constraints. These run inside a **DLT Pipeline**, not the SQL editor.

### Bronze → Silver: Streaming Tables with Data Quality

```sql
-- silver_transactions.sql (run inside a DLT pipeline)
CREATE OR REFRESH STREAMING TABLE silver_transactions (
  -- Data quality constraints — bad rows get dropped automatically
  CONSTRAINT valid_txn_id EXPECT (transaction_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_amount EXPECT (amount > 0) ON VIOLATION DROP ROW,
  CONSTRAINT valid_timestamp EXPECT (transaction_time IS NOT NULL) ON VIOLATION DROP ROW
)
TBLPROPERTIES('pipelines.channel' = 'PREVIEW')
COMMENT 'Cleaned UPI transactions — deduplicated, typed, quality-checked'
AS
SELECT DISTINCT
    transaction_id,
    amount,
    transaction_time,
    sender_id,
    receiver_id,
    category,
    hour_of_day,
    day_of_week,
    CASE WHEN dayofweek(transaction_time) IN (1, 7) THEN true ELSE false END AS is_weekend,
    -- Inline LLM classification using ai_query() !
    CASE
      WHEN amount > 0 THEN
        LOWER(TRIM(ai_query('databricks-meta-llama-3-1-70b-instruct',
          CONCAT('Classify this UPI transaction risk as exactly one of: low, medium, high, critical. Output ONLY the label.\nAmount: ', CAST(amount AS STRING), ' Category: ', COALESCE(category, 'unknown'), ' Hour: ', CAST(hour_of_day AS STRING)),
          'STRING')))
      ELSE 'low'
    END AS risk_label
FROM STREAM(bronze_transactions)
WHERE transaction_id IS NOT NULL
  AND amount > 0;
```

### Silver → Gold: Materialized Views

```sql
-- gold_transactions.sql (run inside a DLT pipeline)
CREATE OR REFRESH MATERIALIZED VIEW gold_transactions (
  CONSTRAINT valid_risk EXPECT (risk_label IN ('low', 'medium', 'high', 'critical')) ON VIOLATION DROP ROW
)
COMMENT 'Business-ready UPI transactions with risk classification and anomaly features'
AS
SELECT
    *,
    CASE WHEN risk_label IN ('high', 'critical') THEN true ELSE false END AS anomaly_flag
FROM silver_transactions;
```

### `ai_query()` — Inline LLM in SQL (Game Changer)

The starter kit uses `ai_query()` directly in SQL streaming tables to classify content. This means you can do LLM-powered classification **inside your ETL pipeline** without Python.

```sql
-- Classify RBI circular topics inline
ai_query('databricks-meta-llama-3-1-70b-instruct',
  CONCAT('Classify this RBI circular as one of: fraud_prevention, monetary_policy, lending_regulations, digital_payments, consumer_protection. Output ONLY the label.\nText: ',
    SUBSTRING(chunk_text, 1, 500)),
  'STRING')
```

### Text Chunking via SQL AGGREGATE() (from starter kit)

The starter kit has a sophisticated chunking pattern using SQL's `AGGREGATE()` with a named struct accumulator. Reuse this for chunking RBI circulars:

```sql
-- gold_circular_chunks.sql — chunk documents at ~4000 char boundaries
WITH combined AS (
    SELECT circular_id, title, full_text
    FROM silver_circulars
),
chunked AS (
    SELECT
        circular_id,
        title,
        -- AGGREGATE accumulator splits text at paragraph boundaries targeting ~4000 chars
        AGGREGATE(
            SPLIT(full_text, '\n\n'),
            named_struct('chunks', ARRAY(CAST('' AS STRING)), 'cur', CAST('' AS STRING)),
            (acc, paragraph) ->
                CASE
                    WHEN LENGTH(acc.cur) + LENGTH(paragraph) > 4000
                    THEN named_struct(
                        'chunks', CONCAT(acc.chunks, ARRAY(acc.cur)),
                        'cur', paragraph)
                    ELSE named_struct(
                        'chunks', acc.chunks,
                        'cur', CONCAT(acc.cur, '\n\n', paragraph))
                END,
            acc -> CONCAT(acc.chunks, ARRAY(acc.cur))
        ) AS chunks_array
    FROM combined
)
SELECT
    CONCAT(circular_id, '_', CAST(chunk_index AS STRING)) AS chunk_id,
    circular_id,
    title,
    chunk AS chunk_text,
    chunk_index
FROM chunked
LATERAL VIEW POSEXPLODE(chunks_array) AS chunk_index, chunk;
```

### How to Run DLT Pipelines

1. Go to **Workflows** → **Delta Live Tables** → **Create Pipeline**
2. Set source to your SQL files (or notebook)
3. Set target schema: `digital_artha.main`
4. Set `pipelines.channel = PREVIEW` in advanced settings
5. Click **Start** → pipeline runs Bronze → Silver → Gold automatically
6. **Free Edition limit:** 1 active pipeline per type

---

## 6. Notebooks

### Key Features
- **Multi-language cells:** Use `%sql` in a Python notebook, `%python` in a SQL notebook
- **Widgets:** Interactive parameters (`dbutils.widgets.text("catalog", "default")`)
- **`display(df)`:** Rich DataFrame visualization with built-in charts
- **Genie Code:** AI coding assistant — use it aggressively, it writes SQL/Python for you

### Patterns from Starter Kit
```python
# Parameterized notebooks (reusable across environments)
dbutils.widgets.text("catalog", "digital_artha")
dbutils.widgets.text("schema", "main")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

spark.sql(f"USE CATALOG {catalog}")

# Run SQL from Python
result_df = spark.sql(f"SELECT * FROM {catalog}.{schema}.gold_transactions")
display(result_df)

# Verify data
print(f"Transactions: {spark.table(f'{catalog}.{schema}.bronze_transactions').count():,}")
```

### Tips
- Use **markdown cells** to document your pipeline — judges read notebooks
- Use `display()` instead of matplotlib for quick charts
- Use the **Data Science Agent** (AI button) for rapid EDA

---

## 7. Metric Views — YAML Semantic Layer (from starter kit)

The starter kit defines metric views using `CREATE OR REPLACE VIEW ... WITH METRICS LANGUAGE YAML`. This creates a semantic layer that Genie Space and dashboards understand.

```sql
CREATE OR REPLACE VIEW digital_artha.main.financial_metrics
WITH METRICS LANGUAGE YAML
$$
source:
  table: digital_artha.main.gold_transactions_enriched

dimensions:
  - name: transaction_date
    display_name: "Transaction Date"
    expr: "DATE(transaction_time)"
    comment: "Date of the UPI transaction"

  - name: category
    display_name: "Merchant Category"
    expr: "COALESCE(category, 'Unknown')"
    comment: "Merchant category of the transaction"
    synonyms: ["merchant type", "txn type", "payment category"]

  - name: risk_category
    display_name: "Risk Category"
    expr: "risk_label"
    comment: "AI-classified risk level: low, medium, high, critical"
    synonyms: ["risk level", "fraud risk", "threat level"]

  - name: is_weekend
    display_name: "Weekend Flag"
    expr: "CASE WHEN is_weekend THEN 'Weekend' ELSE 'Weekday' END"

measures:
  - name: total_transactions
    display_name: "Total Transactions"
    expr: "COUNT(1)"
    comment: "Total number of UPI transactions"
    format: "#,##0"
    synonyms: ["transaction count", "txn count", "number of transactions"]

  - name: total_amount
    display_name: "Total Transaction Value"
    expr: "SUM(amount)"
    comment: "Sum of all transaction amounts in INR"
    format: "₹#,##0.00"
    synonyms: ["total value", "transaction volume"]

  - name: avg_amount
    display_name: "Average Transaction Amount"
    expr: "AVG(amount)"
    format: "₹#,##0.00"

  - name: flagged_transactions
    display_name: "Flagged Transactions"
    expr: "SUM(CASE WHEN anomaly_flag = true THEN 1 ELSE 0 END)"
    comment: "Transactions flagged as potentially fraudulent"
    format: "#,##0"
    synonyms: ["fraud count", "suspicious transactions", "anomalies"]

  - name: fraud_rate
    display_name: "Fraud Rate"
    expr: "SUM(CASE WHEN anomaly_flag = true THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(1), 0)"
    comment: "Percentage of transactions flagged as fraudulent"
    format: "#0.00'%'"
    synonyms: ["fraud percentage", "anomaly rate"]

  - name: high_risk_rate
    display_name: "High Risk Rate"
    expr: "SUM(CASE WHEN risk_label IN ('high', 'critical') THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(1), 0)"
    format: "#0.00'%'"
$$;
```

**Why this matters:** `synonyms` and `comment` fields help Genie understand your data, so users can ask "show me fraud percentage by merchant type" and Genie maps it correctly.

---

## 8. Genie Space — Programmatic Deployment (from starter kit)

Don't create Genie Spaces manually. Deploy via REST API with a config JSON.

```python
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
host = w.config.host

# Build space config (based on starter kit's serialized_space.json)
space_config = {
    "title": "Digital-Artha Financial Intelligence",
    "description": "Ask questions about UPI transactions, fraud patterns, and financial schemes",
    "warehouse_id": dbutils.widgets.get("warehouse_id"),
    "version": 2,
    "data_sources": {
        "tables": [
            {
                "catalog": catalog,
                "schema": schema,
                "table": "gold_transactions",
                "columns": [
                    {"name": "transaction_id", "enable_entity_matching": True},
                    {"name": "amount", "enable_format_assistance": True},
                    {"name": "risk_label", "enable_entity_matching": True},
                    {"name": "category", "enable_entity_matching": True},
                ]
            }
        ],
        "metric_views": [
            {
                "catalog": catalog,
                "schema": schema,
                "metric_view": "financial_metrics",
                "columns": [
                    {"name": "fraud_rate", "enable_format_assistance": True},
                    {"name": "total_transactions", "enable_format_assistance": True},
                ]
            }
        ]
    },
    "instructions": {
        "text_instructions": """You are a financial analytics assistant for Digital-Artha.
        - When asked about fraud, query the anomaly_flag and risk_label columns
        - Use the financial_metrics metric view for aggregate queries
        - Amount is in Indian Rupees (INR)
        - risk_label values: low, medium, high, critical""",
        "example_question_sqls": [
            {
                "question": "What is the fraud rate by merchant category?",
                "sql": f"SELECT category, COUNT(*) as total, SUM(CASE WHEN anomaly_flag THEN 1 ELSE 0 END) as flagged FROM {catalog}.{schema}.gold_transactions GROUP BY category ORDER BY flagged DESC"
            }
        ]
    }
}

# Deploy via REST API
response = w.api_client.do(
    method="POST",
    path="/api/2.0/genie/spaces",
    body=space_config
)
space_id = response["space_id"]
print(f"Genie Space: {host}/genie/rooms/{space_id}")
```

---

## 9. Machine Learning & AI

### Foundation Model APIs (LLM Access — No GPU Needed)
```python
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
response = w.serving_endpoints.query(
    name="databricks-meta-llama-3-1-70b-instruct",
    messages=[{"role": "user", "content": "Explain UPI fraud patterns in India"}],
    max_tokens=500
)
print(response.choices[0].message.content)
```

### `ai_query()` in SQL (No Python needed!)
```sql
-- Use LLMs directly in SQL transforms
SELECT
    circular_id,
    ai_query('databricks-meta-llama-3-1-70b-instruct',
      CONCAT('Summarize this RBI circular in 2 sentences:\n', full_text),
      'STRING') AS summary
FROM bronze_circulars;
```

### AutoML (Quick Classical ML)
```python
from databricks import automl
summary = automl.classify(
    dataset=spark.table(f"{catalog}.{schema}.gold_features"),
    target_col="is_fraud",
    timeout_minutes=30
)
# Generates notebooks with best model, feature importance, metrics
```

### Spark MLlib (Distributed ML — Shows Databricks Depth)
```python
from pyspark.ml.feature import VectorAssembler, StandardScaler, StringIndexer
from pyspark.ml.clustering import KMeans
from pyspark.ml import Pipeline

# Feature pipeline
indexer = StringIndexer(inputCol="category", outputCol="category_idx")
assembler = VectorAssembler(
    inputCols=["amount", "hour_of_day", "day_of_week", "category_idx", "txn_velocity"],
    outputCol="features")
scaler = StandardScaler(inputCol="features", outputCol="scaled_features")
kmeans = KMeans(k=5, featuresCol="scaled_features", predictionCol="cluster")

pipeline = Pipeline(stages=[indexer, assembler, scaler, kmeans])
model = pipeline.fit(gold_transactions_df)
predictions = model.transform(gold_transactions_df)
```

### Scikit-Learn (Driver-Side ML)
```python
from sklearn.ensemble import IsolationForest
import mlflow

mlflow.autolog()
with mlflow.start_run(run_name="isolation_forest_v1"):
    model = IsolationForest(n_estimators=200, contamination=0.05, random_state=42)
    model.fit(feature_df)
    predictions = model.predict(feature_df)  # -1 = anomaly, 1 = normal
```

---

## 10. MLflow (Experiment Tracking)

### Auto-Logging (Do This First!)
```python
import mlflow

mlflow.autolog()  # Automatically logs params, metrics, model for sklearn/spark/etc.
mlflow.set_experiment(f"/Users/{username}/digital-artha-fraud")
```

### Manual Logging
```python
with mlflow.start_run(run_name="ensemble_fraud_v2"):
    mlflow.log_param("isolation_forest_estimators", 200)
    mlflow.log_param("kmeans_k", 5)
    mlflow.log_param("ensemble_weight_if", 0.6)
    mlflow.log_metric("precision", precision_score)
    mlflow.log_metric("recall", recall_score)
    mlflow.log_metric("f1", f1_score)
    mlflow.sklearn.log_model(model, "fraud_model")
```

### Model Registry (Unity Catalog)
```python
mlflow.register_model(f"runs:/{run_id}/fraud_model", f"{catalog}.{schema}.fraud_detection_model")
```

### GenAI Tracing (from starter kit agent pattern)
```python
mlflow.langchain.autolog()  # Automatic tracing for LangChain/LangGraph agents
# Traces show up in MLflow UI with full prompt/response/tool call details
```

---

## 11. Vector Search & RAG Pipeline

### Option A: Databricks Vector Search (Managed — Recommended)

Free Edition gives you **1 Vector Search endpoint**. Use it.

```python
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

# Create vector search endpoint (once)
w.vector_search_endpoints.create_endpoint(name="digital_artha_vs")

# Create index from Delta table (auto-syncs when table updates!)
w.vector_search_indexes.create_index(
    name=f"{catalog}.{schema}.circular_chunks_vs_index",
    endpoint_name="digital_artha_vs",
    primary_key="chunk_id",
    index_type="DELTA_SYNC",
    delta_sync_index_spec={
        "source_table": f"{catalog}.{schema}.gold_circular_chunks",
        "embedding_source_columns": [{"name": "chunk_text", "embedding_model_endpoint_name": "databricks-bge-large-en"}],
        "pipeline_type": "TRIGGERED"
    }
)
```

### Option B: FAISS on DBFS (Manual but Full Control)
```python
%pip install sentence-transformers faiss-cpu

from sentence_transformers import SentenceTransformer
import faiss, numpy as np

# multilingual-e5-small: 384 dims, ~470MB, supports Hindi/Tamil/Telugu
embed_model = SentenceTransformer("intfloat/multilingual-e5-small")

# Embed documents (prefix required for e5 models)
texts = ["passage: " + t for t in documents]
embeddings = embed_model.encode(texts, batch_size=32, show_progress_bar=True)

# Build + save FAISS index
index = faiss.IndexFlatL2(embeddings.shape[1])
index.add(np.array(embeddings).astype('float32'))
faiss.write_index(index, "/dbfs/FileStore/faiss_index/rbi_circulars.faiss")

# Query
query_emb = embed_model.encode(["query: यूपीआई फ्रॉड के नियम"])  # Hindi query works!
distances, indices = index.search(np.array(query_emb).astype('float32'), k=5)
retrieved = [documents[i] for i in indices[0]]
```

### RAG with Foundation Model API
```python
context = "\n\n".join(retrieved)
response = w.serving_endpoints.query(
    name="databricks-meta-llama-3-1-70b-instruct",
    messages=[{"role": "user", "content": f"Based on these RBI circulars, answer in Hindi:\n\nContext:\n{context}\n\nQuestion: {user_question}"}],
    max_tokens=500
)
```

---

## 12. MCP-Based Agent (from starter kit — the production pattern)

The starter kit uses **Model Context Protocol (MCP)** to connect the agent to Vector Search and Genie Space as tools. This is the cleanest pattern.

### Architecture
```
User → MLflow AgentServer → LangGraph Agent → Tools:
                                                ├─ MCP: Vector Search (semantic retrieval)
                                                ├─ MCP: Genie Space (analytics queries)
                                                └─ Custom: get_current_time(), etc.
```

### Core Agent Code (adapted from starter kit)
```python
from langchain.agents import create_agent
from databricks_langchain import ChatDatabricks, DatabricksMCPServer, DatabricksMultiServerMCPClient
from langchain_core.tools import tool
import mlflow
from mlflow.genai.agent_server import invoke, stream

# LLM
llm = ChatDatabricks(endpoint="databricks-meta-llama-3-1-70b-instruct")

# Custom tools
@tool
def get_current_time() -> str:
    """Returns the current date and time."""
    from datetime import datetime
    return datetime.now().isoformat()

# MCP tools (Vector Search + Genie)
vector_search_server = DatabricksMCPServer(
    name="circular-search",
    url=f"{host}/api/2.0/mcp/vector-search/{catalog}/{schema}/circular_chunks_vs_index",
    workspace_client=w
)
genie_server = DatabricksMCPServer(
    name="financial-analytics",
    url=f"{host}/api/2.0/mcp/genie/{genie_space_id}",
    workspace_client=w
)

mcp_client = DatabricksMultiServerMCPClient([vector_search_server, genie_server])
mcp_tools = mcp_client.get_tools()

# Create agent
all_tools = [get_current_time] + mcp_tools
agent = create_agent(tools=all_tools, model=llm)

# MLflow handlers (gives you free chat UI at port 8000)
mlflow.langchain.autolog()

@invoke()
async def handle_invoke(request):
    response = await agent.ainvoke(request)
    return response

@stream()
async def handle_stream(request):
    async for event in agent.astream(request):
        yield event
```

### Running the Agent
```python
from mlflow.genai.agent_server import AgentServer

server = AgentServer("ResponsesAgent", enable_chat_proxy=True)
app = server.app  # ASGI app — run with uvicorn

# Built-in chat UI at http://localhost:8000
# API at http://localhost:8000/invocations
```

### Databricks Asset Bundle Deployment (databricks.yml)
```yaml
bundle:
  name: digital-artha-agent

variables:
  catalog: digital_artha
  schema: main
  genie_space_id: ""

resources:
  apps:
    digital-artha:
      name: digital-artha
      source_code_path: ./agent_server
      config:
        command: ["uv", "run", "start-app"]
        env:
          - name: MLFLOW_TRACKING_URI
            value: "databricks"
          - name: CATALOG
            value: ${var.catalog}
          - name: SCHEMA
            value: ${var.schema}
      permissions:
        - level: CAN_MANAGE
          user_name: ${workspace.current_user.userName}
      resources:
        - name: vector-search-index
          sql_name: ${var.catalog}.${var.schema}.circular_chunks_vs_index
          permission: SELECT
        - name: genie-space
          genie_space_id: ${var.genie_space_id}
          permission: CAN_RUN

targets:
  dev:
    default: true
    workspace:
      host: ${workspace.host}
```

---

## 13. Databricks Apps

You get **ONE app**. Make it count.

### Streamlit App (Recommended)
```python
import streamlit as st
from databricks import sql
from databricks.sdk import WorkspaceClient

# Auth is automatic in Databricks Apps — user info from headers
user = st.context.headers.get("X-Forwarded-Preferred-Username", "anonymous")

# Connect to Delta tables via SQL connector
conn = sql.connect(
    server_hostname=host,
    http_path="/sql/1.0/warehouses/your_warehouse_id",
    credentials_provider=lambda: {"Authorization": f"Bearer {access_token}"}
)
```

### Or: Use the Agent's Built-in Chat UI

The MLflow `AgentServer` with `enable_chat_proxy=True` gives you a chat UI for free at port 8000. The starter kit uses this with `e2e-chatbot-app-next` (a Next.js template cloned from `databricks/app-templates`). No need to build a separate Streamlit app if your primary interface is a chatbot.

---

## 14. AI/BI Dashboards (Lakeview)

### Create via UI
1. **Create** → Dashboard → Add data source (your gold tables + metric view)
2. Use `MEASURE()` syntax in queries to reference metric view measures
3. Widget types: `counter`, `line`, `bar`, `pie`, `heatmap`, `table`
4. **AI Assistant:** Describe what you want → it builds the chart
5. **Publish** with "Shared cache" for hackathon

### Dashboard JSON (exportable — from starter kit)
Dashboards can be exported as `.lvdash.json` files and committed to Git. The starter kit's dashboard uses this pattern with 3+ pages, global filters, and metric view queries.

---

## 15. Indian Language Models (CPU-Compatible)

| Model | HuggingFace ID | Size | CPU OK? | Best For |
|-------|---------------|------|---------|----------|
| **IndicTrans2** | `ai4bharat/indictrans2-en-indic-1B` | ~2-4GB | **Yes** | Translation (22 languages) |
| **Sarvam-m** | `sarvamai/sarvam-m` | ~4-5GB | **Yes** | Hindi/Indic Q&A |
| **Airavata** | `ai4bharat/Airavata` (GGUF) | ~4GB quantized | Slow | Hindi instruction following |
| **Param-1** | C-DAC (limited access) | ~3GB | Yes | Skip — availability uncertain |
| **multilingual-e5-small** | `intfloat/multilingual-e5-small` | ~470MB | **Fast** | Embeddings for Indian languages |

### IndicTrans2 Setup
```python
%pip install git+https://github.com/AI4Bharat/IndicTrans2.git
# Language codes: hin_Deva (Hindi), tam_Taml (Tamil), mar_Deva (Marathi), ben_Beng (Bengali)
```

---

## 16. Quick Reference — What to Use When

| Task | Tool | Pattern |
|------|------|---------|
| Upload data | Volume UI | Workspace → Add Data |
| Stream ingest | Auto Loader | `spark.readStream.format("cloudFiles")` |
| Transform (DLT) | Streaming Tables | `CREATE OR REFRESH STREAMING TABLE` with `EXPECT` |
| Transform (Gold) | Materialized Views | `CREATE OR REFRESH MATERIALIZED VIEW` |
| Inline LLM in SQL | `ai_query()` | `ai_query('model-name', prompt, 'STRING')` |
| Text chunking in SQL | `AGGREGATE()` | Accumulator pattern from starter kit |
| Semantic layer | Metric View YAML | `CREATE VIEW ... WITH METRICS LANGUAGE YAML` |
| NL queries | Genie Space | Deploy via REST API `/api/2.0/genie/spaces` |
| Quick ML | AutoML | `automl.classify(dataset, target_col)` |
| Custom ML | sklearn / MLlib | IsolationForest / KMeans pipeline |
| Track experiments | MLflow | `mlflow.autolog()` from the start |
| LLM in Python | Foundation Model API | `w.serving_endpoints.query()` |
| Vector search | Databricks VS or FAISS | 1 managed endpoint or FAISS on DBFS |
| Agent | MCP + LangGraph | `DatabricksMCPServer` + `AgentServer` |
| Dashboard | Lakeview | AI assistant + `MEASURE()` queries |
| Demo app | Databricks App | Streamlit or AgentServer chat UI |

---

## 17. Time-Saving Tips

1. **Pre-upload all data** to Volumes before hacking starts
2. **Use Genie Code** aggressively — it writes SQL/Python for you in notebooks
3. **`mlflow.autolog()`** at the top of every ML notebook — don't add tracking later
4. **`display(df)`** for quick visualizations — skip matplotlib
5. **`ai_query()` in SQL** for classification — no Python notebook needed
6. **Keep datasets small** — sample for dev, full data for final run
7. **Document with markdown cells** — judges read your notebooks
8. **Don't leave warehouses idle** — quota resets daily but downtime kills you
9. **Use the AI assistant** in dashboards — describe what you want, it builds the chart
10. **DLT pipeline dry run** before full refresh — catches errors without burning compute
11. **Clone the starter kit pattern** — swap data sources, reuse the pipeline shape
12. **Export dashboard JSON** to Git — reproducible for judges
