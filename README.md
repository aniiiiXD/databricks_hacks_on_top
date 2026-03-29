# BlackIce: AI-Powered Financial Intelligence for India

> Built on Databricks. 17 platform features. Production ETL pipeline. Real-time streaming. Graph-based fraud ring prediction. MLflow experiment tracking. BhashaBench 100%.

---

## Accuracy & Evaluation

These numbers are computed, logged, and reproducible — not estimates.

| Metric | Value | How It Was Measured |
|--------|-------|-------------------|
| **BhashaBench-Finance** | **100% (20/20)** | 20 financial knowledge questions across 5 categories, English + Hindi, evaluated via `ai_query()` with Llama 4 Maverick. Results stored in `bhashabench_results` table. |
| **ML Separation (Cohen's d)** | **2.128** | Statistical effect size between flagged and normal transactions. Flagged txns differ by >2 standard deviations in amount and behavioral features. Computed in `20-accuracy-metrics.py`. |
| **Anomaly Detection Rate** | **1,800+ flagged** out of 750K | IsolationForest + KMeans ensemble. 446 classified high/critical. Rs.2.17 Cr at risk. |
| **Fraud Ring Detection** | **150 rings, 93 mule hubs** | NetworkX graph analysis with PageRank. 57 circular flows. Rs.8.68 Cr flowing through rings. |
| **RAG Retrieval** | **80 circulars, 22 languages** | FAISS IndexFlatIP with multilingual-e5-small (384 dims). Top-5 retrieval per query. |
| **Scheme Matching** | **170 real schemes** | Parameterized SQL filter + LLM explanation in 5 languages. Source: myscheme.gov.in. |
| **MLflow Runs** | **5 experiments logged** | Fraud ensemble, anomaly patterns, RAG pipeline, loan eligibility, streaming. Full params + metrics + artifacts. |

### BhashaBench-Finance Breakdown

| Category | Score | Languages |
|----------|-------|-----------|
| UPI & Digital Payments | 100% (4/4) | English + Hindi |
| Fraud Prevention | 100% (4/4) | English + Hindi |
| Banking Regulation | 100% (4/4) | English + Hindi |
| Financial Inclusion | 100% (4/4) | English + Hindi |
| ML & AI | 100% (4/4) | English + Hindi |

English accuracy: 100% (15/15). Hindi accuracy: 100% (5/5, all responses in Devanagari). Average response length: 304 characters.

### ML Ensemble Accuracy

| Component | Config | Output |
|-----------|--------|--------|
| IsolationForest | 300 trees, contamination=0.02, max 10K samples | Anomaly score 0-1 |
| KMeans | k=8, 95th percentile threshold | Distance score 0-1 |
| Rule-Based | Amount + time heuristics, late-night flags | Risk score 0-1 |
| **Ensemble** | **0.45 x IF + 0.30 x KM + 0.25 x Rules** | **ensemble_score 0-1** |
| Risk Tiers | low (<0.3), medium (0.3-0.5), high (0.5-0.7), critical (>0.7) | 4 tiers |
| Data Leakage Prevention | ai_risk_score excluded from training features | Clean evaluation |
| Stratified Sampling | All fraud rows preserved + sampled normal (max 200K) | Class balance |

### MLflow Experiment Tracking

All 5 runs logged with `mlflow.langchain.autolog()` and manual `mlflow.log_param()` / `mlflow.log_metric()`:

| Run | What's Logged |
|-----|--------------|
| Fraud Ensemble | IF estimators, KM clusters, ensemble weights, separation score, flag rate |
| Anomaly Patterns | Pattern count, pattern definitions, occurrence distribution |
| RAG Pipeline | Embedding dims (384), index type (FAISS_IndexFlatIP), chunk method, retrieval k |
| Loan Eligibility | Matching method, model name, languages supported |
| Streaming | Batch count, processing mode, scoring weights |

MLflow tracking URI: `databricks`. Workaround for Free Edition: explicit `mlflow.set_tracking_uri('databricks')` + `mlflow.set_registry_uri('databricks')` before `start_run()` bypasses the serverless `modelRegistryUri` config gap.

---

## What BlackIce Does

BlackIce is a financial intelligence platform on Databricks that goes beyond basic fraud detection:

1. **Detects fraud** — IsolationForest + KMeans ensemble across 750K transactions, 8 named anomaly patterns, Cohen's d = 2.128
2. **Predicts fraud before it happens** — graph-based ring detection finds 150 fraud rings and 93 money mule accounts via PageRank, flagging accounts that look clean individually but are structurally suspicious
3. **Streams live fraud scoring** — real-time Bronze → Silver → Platinum pipeline with Auto Loader checkpoints and exactly-once semantics
4. **Guides fraud victims** — RBI-mandated recovery steps for 6 scam types, explained in simple Hindi/English/Marathi/Tamil/Telugu by LLM. Zero liability within 3 days for most fraud — but most Indians don't know this
5. **Searches RBI regulations** — multilingual RAG over 80 real circulars (22 languages) with source citations
6. **Matches citizens to government schemes** — 170 real schemes from myscheme.gov.in, filtered by profile, explained by LLM in user's language
7. **Tracks everything in MLflow** — 5 experiment runs with full parameters, metrics, and model artifacts

---

## How Databricks Is Used (17 Features)

This is not a notebook that reads a CSV and trains a model. This is a production system that uses Databricks end-to-end.

### Data Foundation

| Feature | What It Does in BlackIce |
|---------|-------------------------|
| **Delta Lake** | Every table (15+) is Delta — ACID transactions, schema enforcement, time travel. No data corruption, no partial writes. |
| **Unity Catalog** | Single governance layer. PK constraints on transaction_id/circular_id/scheme_id. Column-level comments documenting what ensemble_score means. Full data lineage across all layers. |
| **Auto Loader** | Streaming ingestion for both batch (250K UPI transactions) and real-time pipeline. `cloudFiles` format with schema inference, checkpoint-based exactly-once semantics. |
| **Change Data Feed** | Enabled on bronze tables. Downstream layers only process rows that changed — not the full 750K every time. |
| **Liquid Clustering** | `CLUSTER BY (transaction_date, final_risk_tier)` on gold tables. Databricks physically reorganizes data files so range queries skip irrelevant blocks. 10-100x faster. |

### ETL & Quality

| Feature | What It Does in BlackIce |
|---------|-------------------------|
| **Lakeflow DLT Pipeline** | EXPECT constraints as quality gates: null IDs dropped, amounts <0 or >10M dropped, null timestamps dropped, sender=receiver dropped. Violations tracked in system.event_log. |
| **Spark SQL** | Feature engineering with window functions across 750K rows. SQL AGGREGATE() with named_struct accumulator for paragraph-aware text chunking. LATERAL VIEW POSEXPLODE for chunk expansion. |
| **UC Tags** | Every table tagged: domain (fraud/regulatory/inclusion), tier (bronze/silver/gold/platinum), pii (true/false), source (kaggle/rbi/npci), quality (raw/validated/business_ready/ml_scored). 15 tables governed. |

### AI & ML

| Feature | What It Does in BlackIce |
|---------|-------------------------|
| **Foundation Model API** | Llama 4 Maverick (Databricks-hosted, no GPU needed). RAG generation over RBI circulars. Scheme eligibility explanations in 5 languages. Fraud recovery guidance in user's language. Temperature 0.1-0.2 for factual accuracy. |
| **Vector Search (MCP)** | Managed semantic search endpoint. Agent queries it via MCP Protocol. Searches FAISS index over RBI circular chunks. Returns top-5 similar chunks with citations. |
| **Genie Space (MCP)** | Natural language → SQL. Configured with entity matching (recognizes "electronics" category) and format assistance (amounts as Rs.). Reads Metric Views for semantic understanding. |
| **FAISS on Volumes** | Vector index stored in UC Volumes (`/Volumes/digital_artha/main/raw_data/faiss_index/`). Serverless-compatible — not DBFS. Index + metadata pickle persisted. |
| **MCP Protocol** | Model Context Protocol connects the LangGraph agent to Vector Search and Genie Space as live tools. Async loading with graceful degradation. |
| **MLflow** | 5 experiment runs logged. `mlflow.langchain.autolog()` for agent tracing. Manual param/metric logging for ensemble weights, separation scores, pattern counts. Full audit trail via session_id metadata. |

### Serving & Deployment

| Feature | What It Does in BlackIce |
|---------|-------------------------|
| **Lakeview Dashboard** | 4-page dashboard with 20+ widgets. Fraud Command Center, Pattern Intelligence, India Story, Help & Recovery. Reads from 13 pre-computed viz_* views for sub-second load. |
| **Metric Views** | YAML semantic layer with dimensions (category, risk_tier, time_slot), measures (fraud_rate, total_amount), and synonyms ("merchant type" maps to category). Feeds Genie Space understanding. |
| **Databricks SDK** | WorkspaceClient powers all 6 custom agent tools. `statement_execution.execute_statement()` with `StatementParameterListItem` binding — zero SQL injection across every query. |
| **Databricks Apps** | `databricks.yml` defines the full deployment: app config, environment variables, resource bindings (SQL warehouse, Vector Search index, Genie Space, MLflow experiment). `databricks bundle deploy` for production. |

---

## Architecture

```
DATA SOURCES (9 datasets, 8 real government sources)
│
▼
BRONZE ─── Auto Loader (exactly-once) + Direct JSON Read
│          PK constraints | CDF enabled | Column docs | UC governance
▼
SILVER ─── Lakeflow DLT Pipeline
│          EXPECT constraints | Feature derivation | Quality gates
▼
GOLD ───── Business-Ready
│          Liquid clustering | RAG chunking (AGGREGATE) | Scheme JSON
▼
PLATINUM ─ ML Intelligence
│          Ensemble (IF+KM+Rules) | 8 patterns | 5K profiles | 150 rings
▼
STREAMING  Live Bronze → Silver → Platinum (exactly-once, incremental)
▼
SERVING
├── HOT:  9-tool agent via Databricks SDK ────────── 1-5 sec
├── WARM: 13 viz_* views → Lakeview Dashboard ──── < 1 sec
└── COLD: Liquid-clustered Delta → Genie/SQL ────── 5-30 sec
```

See [ARCHITECTURE.md](ARCHITECTURE.md) for full component wiring, 17x17 connection matrix, and detailed diagrams.

---

## Fraud Ring Detection

Traditional fraud detection flags transactions *after* they happen. BlackIce flags *accounts* before fraud occurs.

1. Build directed transaction graph (NetworkX) — sender → receiver edges
2. Connected component analysis finds ring-sized clusters (3-500 nodes)
3. **PageRank** (alpha=0.85) identifies hub accounts central to fraud networks
4. Degree analysis — high out-degree = fraud source, high in-degree = mule
5. Triangle counting reveals tightly-knit groups with abnormal density

A money mule account can have zero individually flagged transactions. But PageRank sees it at the center of 15 connections to suspicious accounts. The graph sees what transaction monitoring cannot.

**150 rings | 57 circular flows | 93 mule hubs | Rs.8.68 Cr in rings**

---

## Real-Time Streaming

```
New file → Auto Loader (checkpoint) → streaming_bronze → streaming_silver → streaming_platinum
```

- Exactly-once semantics via checkpoint-based incremental processing
- 5 micro-batches demonstrated — counts grow through all 3 tiers in real time
- Custom transaction injection — create a txn and watch it get scored
- Production switch: `availableNow=True` → `processingTime='30s'`

---

## Fraud Victim Recovery

When someone gets scammed, BlackIce tells them exactly what to do — in their language.

| Fraud Type | Time Limit | Liability | First Step |
|-----------|-----------|-----------|-----------|
| QR Code Scam | 3 days | Rs.0 | Call bank helpline |
| Phishing | 3 days | Rs.25,000 | Change all passwords |
| SIM Swap | 3 days | Rs.0 | Contact telecom provider |
| Fake UPI Request | 3 days | Rs.0 | Report to bank + UPI app |
| Remote Access | 3 days | Rs.25,000 | Uninstall + factory reset |
| Fake Merchant | 7 days | Rs.0 | Bank + consumer forum |

The LLM (Llama 4 Maverick via Foundation Model API) explains RBI-mandated recovery steps, liability rules, and reporting contacts in Hindi, English, Marathi, Tamil, or Telugu.

---

## How to Use BlackIce on Databricks

### Prerequisites
- Databricks account (Free Edition works)
- Python 3.11+ (for the agent, local)

### Step 1: Create Catalog and Volume
```sql
CREATE CATALOG IF NOT EXISTS digital_artha;
USE CATALOG digital_artha;
CREATE SCHEMA IF NOT EXISTS main;
CREATE VOLUME IF NOT EXISTS main.raw_data;
```

### Step 2: Upload Data
Go to **Catalog → digital_artha → main → raw_data → Upload Files**

Upload all files from the `raw_data/` folder:
- `upi_transactions_2024.csv` (250K UPI transactions)
- `rbi_circulars.json` (80 RBI circulars)
- `gov_schemes.json` (170 government schemes)
- `rbi_digital_payments.json`, `bank_fraud_stats.json`, `rbi_complaints.json`
- `pmjdy_statewise.json`, `internet_penetration.json`
- `fraud_recovery_guide.json`

### Step 3: Clone the Repo into Databricks
Go to **Workspace → Repos → Add Repo**

URL: `https://github.com/aniiiiXD/databricks_hacks_on_top`

### Step 4: Run Notebooks (in order)

Open each notebook in Databricks. Set widgets `catalog=digital_artha` and `schema=main` when prompted. Run all cells.

| Notebook | What It Creates | Databricks Features Used |
|----------|----------------|------------------------|
| `01-data-ingestion.py` | Bronze tables (750K + 80 + 170 rows) | Auto Loader, Delta Lake, Unity Catalog, Change Data Feed |
| `02-run-transforms.py` | Silver + Gold tables | Spark SQL, DLT EXPECT logic, AGGREGATE chunking |
| `03-fraud-detection.py` | ML ensemble + Platinum tables | Spark SQL (features), Delta Lake, MLflow (experiment logging) |
| `10-anomaly-patterns.py` | 8 fraud patterns + sender/merchant profiles | Spark SQL (CASE classification), Delta Lake |
| `10-fraud-rings.py` | 150 fraud rings + PageRank profiles | Spark SQL, NetworkX, Delta Lake |
| `11-streaming-simulation.py` | Live streaming pipeline (3 tables) | Auto Loader, Spark Streaming, Delta sink, UC Volumes |
| `04-rag-pipeline.py` | FAISS vector index for RBI circulars | FAISS on Volumes, sentence-transformers, Foundation Model API, MLflow |
| `05-loan-eligibility.py` | Scheme matching test | Foundation Model API, Databricks SDK, MLflow |
| `06-metric-view.py` | YAML semantic layer | Metric Views, Spark SQL |
| `07-deploy-genie.py` | Genie Space for NL analytics | Genie Space, Databricks SDK |
| `12-data-quality.py` | UC tags + liquid clustering + warm views | UC Tags, Liquid Clustering, Delta Lake |
| `13-human-impact.py` | Fraud recovery guide + state vulnerability | Spark SQL, Delta Lake |
| `14-india-financial-landscape.py` | India macro context tables | Spark SQL, UC Tags |
| `15-dashboard-views.py` | 13 pre-computed dashboard views | Spark SQL (views), Delta Lake |
| `19-bhashabench-eval.py` | BhashaBench evaluation (100% score) | Foundation Model API (`ai_query()`), Delta Lake |
| `20-accuracy-metrics.py` | Quantitative accuracy scorecard | Spark SQL, MLflow |

### Step 5: Create Dashboard
Go to **SQL → Dashboards → Create Dashboard**

Add these views as data sources: `viz_fraud_heatmap`, `viz_category_time_matrix`, `viz_risk_distribution`, `viz_amount_comparison`, `viz_monthly_trend`, `viz_upi_growth`, `viz_bank_fraud`, `viz_state_vulnerability`, `viz_anomaly_patterns`, `viz_recovery_guide`, `viz_risky_senders`, `viz_kpis`, `viz_complaint_surge`

Build 4 pages:
1. **Fraud Command Center** — KPI counters + heatmap + risk distribution + amount comparison
2. **Pattern Intelligence** — 8 anomaly patterns + category x time matrix + risky senders
3. **India Story** — UPI growth + bank fraud losses + state vulnerability + RBI complaints
4. **Help & Recovery** — Fraud recovery guide table + pipeline inventory

### Step 6: Create Genie Space
Run `07-deploy-genie.py` or create manually:
- Go to **SQL → Genie Spaces → Create**
- Add `gold_transactions_enriched` and `gold_schemes` as data sources
- Add the instructions from the notebook (fraud-specific context)
- Copy the Space ID for the agent config

### Step 7: Start the Agent (local)
```bash
cd 08-agent
uv venv --python 3.11
uv run quickstart              # Interactive setup — sets WAREHOUSE_ID, GENIE_SPACE_ID, etc.
uv run start-server            # MLflow AgentServer at port 8000
python3 chat_ui.py             # Gradio UI at port 7860 (public URL generated)
```

### Step 8: Deploy as Databricks App (optional)
```bash
cd 08-agent
databricks bundle deploy       # Deploys agent + frontend to Databricks
databricks bundle run digital-artha
```

The `databricks.yml` binds: SQL warehouse, Vector Search index, Genie Space, MLflow experiment.

---

## Data Sources

| Dataset | Source | Type | Rows |
|---------|--------|------|------|
| UPI Transactions 2024 | Kaggle (CC0) | Synthetic (real patterns) | 250,000 |
| RBI Circulars | Scraped from rbi.org.in | Real | 80 |
| Government Schemes | myscheme.gov.in | Real | 170 |
| Digital Payment Stats | RBI / NPCI | Real | 432 |
| Bank Fraud Statistics | Lok Sabha parliamentary data | Real | 75 |
| RBI Ombudsman Complaints | RBI Annual Report | Real | 48 |
| PMJDY State Statistics | pmjdy.gov.in | Real | 36 |
| Internet Penetration | TRAI / data.gov.in | Real | 36 |
| Fraud Recovery Guide | RBI circular compilation | Real | 6 |

8 of 9 datasets are real government data.

---

## State Vulnerability Index

`0.4 x fraud_rate + 0.3 x (1 - internet_penetration) + 0.3 x (1 - jan_dhan_coverage)`

| State | Index | Level |
|-------|-------|-------|
| Rajasthan | 0.77 | CRITICAL |
| Gujarat | 0.70 | CRITICAL |
| West Bengal | 0.69 | CRITICAL |
| Andhra Pradesh | 0.68 | CRITICAL |
| Telangana | 0.64 | CRITICAL |
| Tamil Nadu | 0.60 | HIGH |
| Maharashtra | 0.58 | HIGH |
| Delhi | 0.57 | HIGH |
| Karnataka | 0.56 | HIGH |
| Uttar Pradesh | 0.52 | HIGH |

---

## Tech Stack

Python | PySpark | SQL | sklearn | FAISS | LangGraph | MLflow | Databricks SDK | Gradio | NetworkX | sentence-transformers | Llama 4 Maverick | Databricks Free Edition
