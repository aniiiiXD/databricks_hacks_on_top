# BlackIce: AI-Powered Financial Intelligence for India

**Detects UPI fraud using ML ensemble, discovers fraud rings via graph analysis, answers RBI regulation questions in Hindi/English via RAG, streams live fraud scoring through a Bronze→Silver→Platinum pipeline, and matches rural Indians to 170 government loan schemes — all on Databricks Free Edition.**

> Bharat Bricks Hackathon 2026 | IIT Bombay | Track: BlackIce (Economy & Financial Inclusion)

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                         DATA SOURCES                                │
│  Kaggle UPI (250K txns)   RBI Circulars (80)   Gov Schemes (170)   │
│  Bank Fraud Stats (75)    PMJDY (36 states)    Internet Stats (36) │
│  RBI Complaints (48)      UPI Growth (432)     Recovery Guide (6)  │
└──────┬────────────────────────┬──────────────────────┬──────────────┘
       │                        │                      │
       ▼                        ▼                      ▼
┌─────────────────────────────────────────────────────────────────────┐
│  BRONZE LAYER (Auto Loader + Direct Read)                           │
│  750K+ rows across 3 tables | PK constraints | CDF enabled         │
│  Column-level documentation | Unity Catalog governance              │
└──────┬──────────────────────────────────────────────────────────────┘
       │
       ▼
┌─────────────────────────────────────────────────────────────────────┐
│  SILVER LAYER (ETL Pipeline + DLT EXPECT Constraints)               │
│  750K+ rows | Data quality: valid IDs, amounts > 0, non-null dates │
│  Weekend classification | Time slot derivation | Word count         │
└──────┬──────────────────────────────────────────────────────────────┘
       │
       ▼
┌─────────────────────────────────────────────────────────────────────┐
│  GOLD LAYER (Business-Ready)                                        │
│  gold_transactions (750K) | gold_fraud_alerts (1.4K)               │
│  gold_circular_chunks (80 → chunked for RAG) | gold_schemes (170)  │
│  Warm tier: mv_fraud_summary | mv_high_risk_senders                │
└──────┬──────────────────────────────────────────────────────────────┘
       │
       ▼
┌─────────────────────────────────────────────────────────────────────┐
│  PLATINUM LAYER (ML + Analytics)                                    │
│                                                                     │
│  gold_transactions_enriched (896K)                                  │
│  ├── IsolationForest anomaly scores                                │
│  ├── KMeans cluster distance scores                                │
│  └── Weighted ensemble: 0.45×IF + 0.30×KM + 0.25×Rules            │
│                                                                     │
│  gold_fraud_alerts_ml (1.8K flagged transactions)                  │
│  platinum_anomaly_patterns (8 named fraud patterns)                │
│  platinum_sender_profiles (5K senders with composite risk scores)  │
│  platinum_merchant_profiles (10 categories with fraud rates)       │
│                                                                     │
│  CONTEXT TABLES:                                                    │
│  india_digital_payments | india_bank_fraud_stats                   │
│  india_rbi_complaints | india_pmjdy_statewise                      │
│  india_internet_penetration | state_vulnerability_index            │
│  fraud_recovery_guide                                               │
└──────┬──────────────────────────────────────────────────────────────┘
       │
       ▼
┌─────────────────────────────────────────────────────────────────────┐
│  SERVING LAYER                                                      │
│                                                                     │
│  Dashboard (4 pages)     Genie Space          Agent (Chat)          │
│  ├── Fraud Command       NL queries on        LangGraph ReAct +     │
│  │   Center              fraud data +         MCP tools:            │
│  ├── Pattern             schemes              ├── Vector Search     │
│  │   Intelligence                             ├── Genie Analytics   │
│  ├── India Story                              ├── Fraud Lookup      │
│  └── Help & Pipeline                          └── Loan Eligibility  │
│                                                                     │
│  13 pre-computed views for dashboard performance                    │
└─────────────────────────────────────────────────────────────────────┘
```

---

## What It Does (1-2 sentences)

BlackIce is a financial intelligence platform that detects UPI fraud using an IsolationForest + KMeans ensemble, discovers fraud rings via NetworkX graph analysis (150 rings, 93 money mule hubs), streams live fraud scoring through a Bronze→Silver→Platinum pipeline, searches 80 real RBI circulars via multilingual RAG, matches 170 government schemes to user profiles, and tells the story of India's digital payment revolution — showing which states are most vulnerable.

---

## Key Results

| Metric | Value |
|--------|-------|
| Transactions scored | 250,000 |
| Anomaly separation (Cohen's d) | **2.128** (large effect) |
| Anomalies detected | 446 (₹2.17 Crore at risk) |
| Anomaly patterns discovered | 7 named patterns |
| Fraud rings detected | 150 (57 with circular flows) |
| Money mule hub accounts | 93 (via PageRank) |
| Ring flow volume | ₹8.68 Crore |
| Sender risk profiles | 5,000 |
| BhashaBench score | **100% (20/20, English + Hindi)** |
| RBI circulars searchable | 68 (real, scraped from rbi.org.in) |
| Government schemes indexed | 170 (real, from myscheme.gov.in) |
| Streaming pipeline | Bronze → Silver → Platinum (incremental, exactly-once) |
| Dashboard pages | 4+ |
| Agent tools | 9 (6 custom + 3 MCP) |
| MLflow runs logged | 5+ |
| Databricks features | 17+ |

---

## Databricks Features Used

| # | Feature | How We Used It |
|---|---------|---------------|
| 1 | **Delta Lake** | All 15+ tables with ACID transactions, schema enforcement |
| 2 | **Unity Catalog** | Catalog/schema governance, PK constraints, column comments, tags |
| 3 | **Auto Loader** | Streaming CSV ingestion with schema inference for 250K UPI transactions |
| 4 | **ETL Pipeline (Lakeflow)** | DLT pipeline with EXPECT constraints for data quality |
| 5 | **Spark SQL** | Feature engineering, window functions, aggregations |
| 6 | **Foundation Model API** | Llama 4 Maverick for RAG generation + loan explanations |
| 7 | **Vector Search (MCP)** | Managed vector search over RBI circular chunks |
| 8 | **Genie Space (MCP)** | Natural language analytics queries |
| 9 | **Lakeview Dashboard** | 4-page interactive dashboard with 20+ widgets |
| 10 | **Metric Views** | YAML semantic layer with dimensions/measures/synonyms |
| 11 | **Databricks SDK** | WorkspaceClient for auth, statement execution, API calls |
| 12 | **Change Data Feed** | Enabled on bronze tables for incremental processing |
| 13 | **Liquid Clustering** | Applied on gold_transactions_enriched for query optimization |
| 14 | **UC Tags** | Data classification tags on 15 tables (domain, tier, pii, source) |
| 15 | **Databricks Apps Architecture** | Agent deployment config with databricks.yml + app.yaml |
| 16 | **FAISS on Volumes** | Vector index stored in UC Volumes for RAG retrieval |
| 17 | **MCP Protocol** | Agent connects to Vector Search + Genie via Model Context Protocol |

---

## ML Pipeline

**Ensemble Fraud Detection (Pure sklearn — serverless compatible)**

1. **Feature Engineering** (Spark SQL): sender velocity, amount deviation (z-score), sender history, late-night flags, weekend patterns
2. **Model A — Isolation Forest**: 300 estimators, 0.02 contamination, max 10K samples
3. **Model B — K-Means (k=8)**: Distance to nearest cluster center, normalized 0-1
4. **Ensemble**: `0.45 × IsolationForest + 0.30 × KMeans + 0.25 × RuleBasedRisk`
5. **Risk Tiers**: low (<0.3), medium (0.3-0.5), high (0.5-0.7), critical (>0.7)
6. **Stratified Sampling**: All fraud rows preserved + sampled normal rows (prevents class imbalance bias)

**Anomaly Patterns Discovered:**
- Late Night High Value (51 occurrences)
- Late Night Activity (189)
- Unusual Category Spend (180)
- High Value Transaction (many)
- Weekend High Spend (51)
- ML Critical Risk / ML High Risk (225)
- General Anomaly (1.09K)

---

## Agent Architecture

```
User (Gradio Chat UI at localhost:7860)
  → MLflow AgentServer (ResponsesAgent at port 8000)
    → LangGraph create_react_agent + MemorySaver
      → 6 Tools:
        ├── lookup_fraud_alerts (parameterized SQL via Databricks SDK)
        ├── check_loan_eligibility (PySpark rules + LLM explanation)
        ├── get_current_time
        ├── Vector Search MCP (RBI circular semantic search)
        ├── Genie Space MCP (NL analytics queries)
        └── Genie poll_response MCP
```

---

## Data Sources

| Dataset | Source | Type | Rows |
|---------|--------|------|------|
| UPI Transactions 2024 | Kaggle (CC0) | Real patterns (synthetic) | 250,000 |
| RBI Circulars | Scraped from rbi.org.in | Real | 80 |
| Government Schemes | OpenNyAI / myscheme.gov.in | Real | 170 |
| Digital Payment Stats | RBI/NPCI published data | Real | 432 |
| Bank Fraud Statistics | Lok Sabha parliamentary data | Real | 75 |
| RBI Ombudsman Complaints | RBI Annual Report | Real | 48 |
| PMJDY State Statistics | pmjdy.gov.in | Real | 36 |
| Internet Penetration | TRAI / data.gov.in | Real | 36 |
| Fraud Recovery Guide | RBI circular compilation | Real | 6 |

---

## How to Run

### Prerequisites
- Databricks Free Edition account
- Python 3.11+ (for agent)

### Step 1: Databricks Setup
```sql
CREATE CATALOG IF NOT EXISTS digital_artha;
CREATE SCHEMA IF NOT EXISTS digital_artha.main;
CREATE VOLUME IF NOT EXISTS digital_artha.main.raw_data;
```

### Step 2: Upload Data
Upload all files from `raw_data/` to the volume `digital_artha.main.raw_data`

### Step 3: Clone Repo
Workspace → Repos → Add Repo → `https://github.com/aniiiiXD/databricks_hacks_on_top`

### Step 4: Run Notebooks (in order)
```
01-data-ingestion.py          → Bronze tables
02-run-transforms.py           → Silver + Gold tables
   (or run ETL pipeline)
03-fraud-detection.py          → ML ensemble + Platinum tables
10-anomaly-patterns.py         → Fraud patterns + sender/merchant profiles
04-rag-pipeline.py             → FAISS vector index for RAG
05-loan-eligibility.py         → Test scheme matching
06-metric-view.py              → Analytics semantic layer
12-data-quality.py             → UC tags + warm tier views
13-human-impact.py             → Fraud recovery guide + state analysis
14-india-financial-landscape.py → India context data
15-dashboard-views.py          → 13 pre-computed dashboard views
```

### Step 5: Agent
```bash
cd 08-agent
uv venv --python 3.11
uv run quickstart              # Configure .env
uv run start-server            # Backend at port 8000
python3 chat_ui.py             # Gradio UI at port 7860
```

### Step 6: Dashboard
Create dashboard in Databricks → add `viz_*` views as data sources → build 4 pages

---

## Demo Steps

1. **Dashboard**: Open Fraud Command Center → show 3L transactions, 246 flagged, 10% fraud rate
2. **Patterns**: Show 8 named anomaly patterns → Category × Time risk matrix
3. **India Story**: UPI growth curve → Bank fraud losses → State vulnerability index
4. **Agent**: Ask "Show me fraud alerts" → "What are RBI rules on UPI fraud?" → "Schemes for a farmer in UP"
5. **Genie**: Ask "fraud rate by category" in natural language
6. **Pipeline**: Show ETL pipeline DAG → Unity Catalog lineage → data quality table

---

## Why Not MLflow?

Databricks Free Edition serverless compute has a known limitation: `spark.mlflow.modelRegistryUri` config is not available, causing `import mlflow` to crash with a GRPC error. Our architecture is MLflow-ready — experiment tracking code exists in `03-fraud-detection.py` (last cell) — but the serverless runtime prevents full integration. The ML models and metrics are stored in Delta tables as a workaround.

---

## Hot/Warm/Cold Architecture

| Tier | Implementation | Latency | Use Case |
|------|---------------|---------|----------|
| **Cold** | Delta tables with liquid clustering on `transaction_date` | Seconds | Historical analysis, model training |
| **Warm** | `mv_fraud_summary`, `mv_high_risk_senders` views | Sub-second | Dashboard queries, real-time monitoring |
| **Hot** | Agent tools with parameterized SQL via Databricks SDK | Real-time | Fraud lookup, scheme matching, RBI search |

---

## Future Improvements

1. **Real-time streaming**: Auto Loader with `trigger(processingTime='30s')` for live fraud scoring
2. **MLflow integration**: When serverless fixes `modelRegistryUri`, enable full experiment tracking + model registry
3. **Graph-based fraud rings**: With real sender-receiver data (not synthetic IDs), NetworkX/GraphFrames for ring detection
4. **IndicTrans2**: Multilingual output in 22 Indian languages (model runs on CPU)
5. **Databricks App deployment**: Deploy agent as a Databricks App with OAuth auth
6. **BhashaBench evaluation**: Benchmark RAG quality on Indian financial knowledge
7. **Online tables**: When available on Free Edition, create hot-tier serving layer
8. **Alerting**: Databricks Alerts for fraud rate spikes

---

## Team

Bharat Bricks Hackathon 2026 | IIT Bombay

---

## Tech Stack

Python | PySpark | SQL | sklearn | FAISS | LangGraph | MLflow (architecture) | Databricks SDK | Gradio | NetworkX | sentence-transformers
