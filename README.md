# BlackIce: AI-Powered Financial Intelligence for India

**Detects UPI fraud before it happens using ML ensembles and graph-based ring detection, streams live fraud scoring end-to-end, guides scam victims through RBI-mandated recovery in their language, searches 80 RBI circulars via multilingual RAG, and matches rural Indians to 170 government schemes — all on Databricks Free Edition.**

> Bharat Bricks Hackathon 2026 | IIT Bombay

---

## Architecture

```
DATA SOURCES (9 datasets, 8 real government sources)
│
▼
BRONZE ─── Auto Loader (exactly-once) + Direct JSON Read
│          PK constraints | CDF enabled | Column docs | UC governance
│
▼
SILVER ─── Lakeflow DLT Pipeline
│          EXPECT constraints: valid IDs, amounts >0 <10M, non-null timestamps
│          sender != receiver enforced | Feature derivation
│
▼
GOLD ───── Business-Ready
│          Liquid clustering on transaction_date
│          RBI circulars chunked (AGGREGATE + POSEXPLODE → FAISS index)
│          Scheme eligibility as structured JSON
│
▼
PLATINUM ─ ML Intelligence
│          IsolationForest + KMeans ensemble (Cohen's d = 2.128)
│          8 anomaly patterns | 5K sender profiles | 150 fraud rings
│
▼
STREAMING  Live Bronze → Silver → Platinum
│          Auto Loader checkpoints | Exactly-once | Incremental scoring
│
▼
SERVING
├── HOT:  9-tool agent (parameterized SQL, zero injection) ─── 1-5 sec
├── WARM: 13 viz_* views + 2 materialized views ──────────── < 1 sec
└── COLD: Full tables + liquid clustering ─────────────────── 5-30 sec

INTERFACES
├── Gradio App (5 tabs: Home, Command Center, Ask BlackIce, India Story, Scheme Finder)
├── Lakeview Dashboard (4 pages, 20+ widgets)
└── Genie Space (NL → SQL analytics)
```

---

## What It Does

BlackIce is a financial intelligence platform built on Databricks that:

1. **Detects fraud** using an IsolationForest + KMeans ensemble across 750K transactions, flagging 1,800+ anomalies with a separation score of 2.128 (Cohen's d)
2. **Predicts fraud** via graph-based ring detection — 150 rings found, 93 money mule hub accounts identified through PageRank before fraud occurs
3. **Streams live scoring** through a Bronze → Silver → Platinum pipeline with exactly-once semantics — drop a new file and watch it get scored in real time
4. **Guides fraud victims** through RBI-mandated recovery steps for 6 scam types, explained in simple language via LLM (zero liability within 3 days for most fraud types)
5. **Searches RBI regulations** via multilingual RAG over 80 real circulars (Hindi, English, Tamil, Telugu, Marathi + 17 more languages)
6. **Matches citizens to schemes** — 170 real government financial inclusion schemes filtered by age/income/occupation/state/gender, with LLM explanations in 5 languages
7. **Tells India's story** — UPI growth, bank fraud losses, RBI complaint trends, and a State Vulnerability Index cross-referencing fraud risk, internet penetration, and Jan Dhan coverage

---

## Key Results

| Metric | Value |
|--------|-------|
| Transactions scored | 750,000+ |
| Total rows processed | 3.15 million across 15 tables |
| Anomaly separation (Cohen's d) | **2.128** |
| Fraud alerts flagged | 1,800+ (446 high/critical, Rs.2.17 Cr at risk) |
| Anomaly patterns discovered | 8 named patterns |
| Fraud rings detected | 150 (57 circular flows, Rs.8.68 Cr volume) |
| Money mule hub accounts | 93 (PageRank, top 5% by degree) |
| Sender risk profiles | 5,000 (composite: ML + fraud rate + PageRank + behavior) |
| RBI circulars searchable | 80 (real, scraped from rbi.org.in) |
| Government schemes indexed | 170 (real, from myscheme.gov.in) |
| Fraud recovery types covered | 6 (QR scam, phishing, SIM swap, fake UPI, remote access, fake merchant) |
| BhashaBench score | **100% (20/20, English + Hindi)** |
| Streaming pipeline | Bronze → Silver → Platinum (incremental, exactly-once) |
| Agent tools | 9 (6 custom + 3 MCP) |
| MLflow experiment runs | 5 |
| Databricks features used | 17 |

---

## Fraud Ring Detection (Graph Intelligence)

Traditional fraud detection flags transactions *after* they happen. BlackIce flags *accounts* before fraud occurs.

**How it works:**
1. Build directed transaction graph (NetworkX) — sender → receiver edges filtered to 2+ interactions or 1+ fraud flag
2. Connected component analysis finds ring-sized clusters (3-500 nodes)
3. PageRank (alpha=0.85) identifies hub accounts — nodes central to fraud networks
4. Degree analysis — high out-degree = fraud source, high in-degree = mule collecting funds
5. Triangle counting reveals tightly-knit groups with abnormal connection density

**Why it's predictive:** A money mule account can have zero individually flagged transactions. But PageRank sees it sitting at the center of 15 connections to suspicious accounts. The graph structure reveals what transaction-level monitoring cannot.

**Results:** 150 rings | 57 circular flows | 93 mule hubs | Rs.8.68 Cr flowing through rings

**Composite risk score per sender:**
`30% ML_score + 25% fraud_rate + 20% PageRank + 15% late_night_pct + 10% degree_centrality`

---

## Real-Time Streaming Pipeline

Not a batch job. A live system.

```
New file dropped → Auto Loader (checkpoint) → Bronze (typed) → Silver (validated) → Platinum (scored + patterned)
```

- **Exactly-once semantics** via checkpoint-based incremental processing
- **5 micro-batches** demonstrated — drop files and watch counts grow through all 3 tiers
- **Custom transaction injection** — anyone can create a transaction and watch it flow end-to-end
- **Production-ready** — switch `trigger(availableNow=True)` to `trigger(processingTime='30s')` for continuous streaming
- **4-signal ensemble** in streaming: 25% ai_risk_score + 35% amount_threshold + 25% time_of_day + 15% risk_tier

---

## Fraud Victim Recovery (LLM-Powered Legal Guidance)

When someone gets scammed, they're scared and don't know their rights. BlackIce provides RBI-mandated recovery guidance, explained by an LLM in simple language.

| Fraud Type | Time Limit | Liability | Recovery Steps |
|-----------|-----------|-----------|---------------|
| QR Code Scam | 3 days | Rs.0 | Bank helpline → cybercrime.gov.in → RBI Ombudsman (30 days) |
| Phishing | 3 days | Rs.25,000 | Change passwords → Bank → cybercrime.gov.in → FIR |
| SIM Swap | 3 days | Rs.0 | Telecom provider → Bank → cybercrime.gov.in |
| Fake UPI Request | 3 days | Rs.0 | Never approve unknown → Report to bank + UPI app |
| Remote Access | 3 days | Rs.25,000 | Uninstall remote apps → Factory reset → Bank |
| Fake Merchant | 7 days | Rs.0 | Bank → Consumer forum → cybercrime.gov.in |

The agent's `fraud_recovery_guide` tool retrieves these steps and the LLM explains them in Hindi, Marathi, Tamil, or English — so a first-generation bank user can understand exactly what to do and within what timeframe.

---

## Financial Inclusion (Scheme Matching)

170 real government schemes from myscheme.gov.in. A street vendor in Maharashtra, age 25, earning Rs.1.5 lakh/year, can instantly discover:
- **PM Mudra Yojana** — Rs.50,000 collateral-free loans
- **PM-JAY** — Rs.5 lakh health insurance
- **State-specific schemes** for their profile

**How it works:**
1. Parameterized SQL filters `gold_schemes` by age, income, occupation, state, gender
2. Foundation Model API (Llama 4 Maverick) generates explanation in user's language
3. Temperature 0.2 for factual accuracy | 5 languages supported

---

## ML Pipeline

**Ensemble Fraud Detection (Pure sklearn — serverless compatible)**

1. **Feature Engineering** (Spark SQL): sender velocity, amount deviation (z-score), sender history, late-night flags, weekend patterns. `ai_risk_score` excluded to prevent data leakage.
2. **Model A — Isolation Forest**: 300 estimators, 0.02 contamination, max 10K samples, 80% features
3. **Model B — K-Means (k=8)**: Distance to nearest cluster center, normalized 0-1, 95th percentile threshold
4. **Ensemble**: `0.45 x IF + 0.30 x KM + 0.25 x Rules`
5. **Risk Tiers**: low (<0.3) | medium (0.3-0.5) | high (0.5-0.7) | critical (>0.7)
6. **Stratified Sampling**: All fraud rows preserved + sampled normal rows (max 200K), prevents class imbalance bias
7. **Quality**: Cohen's d = 2.128 — flagged transactions differ from normal by >2 standard deviations

**8 Anomaly Patterns Discovered:**

| Pattern | Count | Trigger |
|---------|-------|---------|
| Late Night High Value | 51 | 0-5am + >Rs.10,000 |
| Late Night Activity | 189 | Any transaction 0-5am |
| Unusual Category Spend | 180 | >Rs.20K Education/Healthcare, >Rs.15K Grocery |
| High Value Transaction | varies | >Rs.25,000 |
| Weekend High Spend | 51 | Weekend + >Rs.10,000 |
| ML Critical Risk | varies | Ensemble score >0.8 |
| ML High Risk | 225 | Ensemble score 0.6-0.8 |
| General Anomaly | 1,090 | Multiple minor signals |

---

## 9-Tool Agent

```
Gradio UI (localhost:7860)
  → MLflow AgentServer (port 8000)
    → LangGraph create_react_agent + MemorySaver (per-session memory)
      → 9 Tools:
          CUSTOM (6):
          ├── lookup_fraud_alerts      Parameterized SQL → gold_fraud_alerts_ml
          ├── check_loan_eligibility   SQL filter → gold_schemes → LLM explanation (5 langs)
          ├── fraud_recovery_guide     SQL → fraud_recovery_guide → LLM explanation
          ├── lookup_fraud_rings       SQL → platinum_fraud_rings (size, severity, members)
          ├── lookup_sender_profile    SQL → platinum_sender_profiles (composite risk)
          ├── get_current_time         Utility
          MCP (3):
          ├── Vector Search            Semantic RAG over 80 RBI circular chunks
          ├── Genie query              NL → SQL on fraud data
          └── Genie poll               Async result retrieval
```

**Security:** All 6 custom tools use `StatementParameterListItem` binding — zero SQL injection risk.

**RAG Pipeline:** 80 circulars → chunked at 4000-char paragraph boundaries (SQL AGGREGATE) → embedded with `multilingual-e5-small` (384 dims, 22 languages) → FAISS IndexFlatIP on UC Volumes → top-5 retrieval → Llama 4 Maverick generation with citations.

---

## Hot / Warm / Cold Serving

| Tier | Implementation | Latency | Access |
|------|---------------|---------|--------|
| **Hot** | 9 agent tools with parameterized SQL via Databricks SDK | 1-5 sec | Agent chat |
| **Warm** | 13 `viz_*` pre-computed views + `mv_fraud_summary` + `mv_high_risk_senders` | < 1 sec | Lakeview Dashboard |
| **Cold** | Delta tables with `CLUSTER BY (transaction_date, final_risk_tier)` | 5-30 sec | SQL Editor, Genie |

Three different optimization strategies for three different access patterns. Warm tier aggregates at write time. Cold tier uses liquid clustering so range queries skip irrelevant data blocks.

---

## Databricks Features (17)

| # | Feature | How It's Used |
|---|---------|--------------|
| 1 | **Delta Lake** | All 15+ tables — ACID, schema enforcement, time travel |
| 2 | **Unity Catalog** | PK constraints, column comments, data lineage, governance |
| 3 | **Auto Loader** | Streaming ingestion (bronze + real-time pipeline) with schema inference |
| 4 | **DLT Pipeline** | EXPECT constraints drop invalid rows: null IDs, bad amounts, sender=receiver |
| 5 | **Spark SQL** | Feature engineering, AGGREGATE accumulator, LATERAL VIEW POSEXPLODE |
| 6 | **Foundation Model API** | Llama 4 Maverick — RAG generation, scheme explanations, recovery guidance |
| 7 | **Vector Search (MCP)** | Managed semantic search over RBI circular FAISS index |
| 8 | **Genie Space (MCP)** | NL → SQL analytics with entity matching + format assistance |
| 9 | **Lakeview Dashboard** | 4-page dashboard with 20+ widgets across fraud/patterns/India/recovery |
| 10 | **Metric Views** | YAML semantic layer — dimensions, measures, synonyms for Genie |
| 11 | **Databricks SDK** | WorkspaceClient for agent SQL execution, auth, API calls |
| 12 | **Change Data Feed** | Incremental downstream processing from bronze tables |
| 13 | **Liquid Clustering** | `CLUSTER BY (transaction_date, final_risk_tier)` — 10-100x faster range queries |
| 14 | **UC Tags** | domain/tier/pii/source/quality tags on 15 tables |
| 15 | **Databricks Apps** | Production deployment via databricks.yml + app.yaml |
| 16 | **FAISS on Volumes** | Vector index in UC Volumes (serverless-compatible) |
| 17 | **MCP Protocol** | Agent connects to Vector Search + Genie via Model Context Protocol |

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

8 of 9 datasets are real government data. Only UPI transactions are synthetic (no bank releases real transaction data).

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
01-data-ingestion.py           → Bronze tables
02-run-transforms.py           → Silver + Gold tables
03-fraud-detection.py          → ML ensemble + Platinum tables
10-anomaly-patterns.py         → Fraud patterns + sender/merchant profiles
10-fraud-rings.py              → Graph-based fraud ring detection
11-streaming-simulation.py     → Real-time streaming pipeline
04-rag-pipeline.py             → FAISS vector index for RAG
05-loan-eligibility.py         → Scheme matching engine
06-metric-view.py              → Analytics semantic layer
12-data-quality.py             → UC tags + liquid clustering + warm tier views
13-human-impact.py             → Fraud recovery guide + state vulnerability
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

## State Vulnerability Index

Composite score: `0.4 x fraud_rate + 0.3 x (1 - internet_penetration) + 0.3 x (1 - jan_dhan_coverage)`

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

Python | PySpark | SQL | sklearn | FAISS | LangGraph | MLflow | Databricks SDK | Gradio | NetworkX | sentence-transformers | Llama 4 Maverick
