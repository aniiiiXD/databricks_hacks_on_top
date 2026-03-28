# Digital-Artha: AI-Powered Financial Intelligence for India

> UPI fraud detection + RBI circular intelligence + multilingual financial inclusion — all on Databricks Free Edition.

---

## The Pitch (30 seconds)

*"India processes 16 billion UPI transactions a month, but fraud detection and financial literacy haven't kept pace. Digital-Artha catches suspicious transactions using ML, explains RBI regulations in your language, and helps rural users find loans they qualify for — all running on Databricks Free Edition, CPU-only, using Indian-built models."*

---

## Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────────────┐
│                          DATA SOURCES (Volumes)                          │
│   Synthetic UPI Txns     RBI Circulars (text)     Gov Scheme Data        │
│   (JSON/CSV)             (scraped PDFs)           (data.gov.in CSV)      │
└─────┬────────────────────────┬────────────────────────┬──────────────────┘
      │                        │                        │
      ▼                        ▼                        ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                  BRONZE LAYER (Auto Loader → Delta Tables)               │
│                                                                          │
│  bronze_transactions          bronze_circulars        bronze_schemes      │
│  (cloudFiles + explode        (spark.read.json        (spark.read.csv     │
│   + trigger(availableNow))     or direct load)         or direct load)    │
│                                                                          │
│  + PK constraints, CDF enabled, column-level comments                    │
└─────┬────────────────────────┬────────────────────────┬──────────────────┘
      │                        │                        │
      ▼                        ▼                        ▼
┌─────────────────────────────────────────────────────────────────────────┐
│            SILVER LAYER (DLT Streaming Tables + EXPECT constraints)       │
│                                                                          │
│  silver_transactions          silver_circulars        silver_schemes      │
│  ├─ EXPECT valid_txn_id       ├─ EXPECT valid_id      ├─ EXPECT valid_id │
│  ├─ EXPECT amount > 0         ├─ chunked via          ├─ parsed criteria │
│  ├─ deduplicated               │  AGGREGATE()          │  into JSON       │
│  ├─ time features derived      │  at ~4000 chars       └─ normalized      │
│  └─ ai_query() risk_label     └─ ai_query() topic                        │
│     (inline LLM classification    classification                         │
│      directly in SQL!)                                                    │
└─────┬────────────────────────┬────────────────────────┬──────────────────┘
      │                        │                        │
      ▼                        ▼                        ▼
┌─────────────────────────────────────────────────────────────────────────┐
│              GOLD LAYER (DLT Materialized Views + clean constraint)       │
│                                                                          │
│  gold_transactions            gold_circular_chunks    gold_schemes        │
│  ├─ anomaly_flag derived      ├─ chunk_id             ├─ eligibility      │
│  ├─ risk_category             ├─ chunk_text              criteria JSON    │
│  └─ feature columns           ├─ topic_label          └─ ministry,        │
│                               └─ circular metadata       benefits text    │
│                                                                          │
│  gold_fraud_alerts            Vector Search Index                         │
│  (filtered: anomaly=true)     (Databricks managed                        │
│                                or FAISS on DBFS)                         │
└─────┬────────────────────────┬────────────────────────┬──────────────────┘
      │                        │                        │
      ▼                        ▼                        ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                           AI / ML LAYER                                  │
│                                                                          │
│  ┌──────────────────┐  ┌────────────────────┐  ┌─────────────────────┐  │
│  │ FRAUD DETECTION   │  │ RAG PIPELINE        │  │ LOAN ELIGIBILITY    │  │
│  │                   │  │                     │  │                     │  │
│  │ IsolationForest   │  │ MCP Vector Search   │  │ Rule engine over    │  │
│  │ (sklearn) +       │  │ + Foundation Model  │  │ scheme criteria +   │  │
│  │ KMeans (MLlib)    │  │   API generation    │  │ LLM explanation +   │  │
│  │ + MLflow tracking │  │ + IndicTrans2       │  │ IndicTrans2         │  │
│  │ + Model Registry  │  │   (multilingual)    │  │   (multilingual)    │  │
│  └────────┬──────────┘  └────────┬────────────┘  └────────┬────────────┘  │
│           │                      │                         │              │
│           ▼                      ▼                         ▼              │
│  ┌──────────────────────────────────────────────────────────────────┐    │
│  │              MLflow Experiment Hub (autolog + tracing)            │    │
│  │   Tracks: models, params, metrics, artifacts, LLM traces         │    │
│  └──────────────────────────────────────────────────────────────────┘    │
└─────┬────────────────────────┬────────────────────────┬──────────────────┘
      │                        │                        │
      ▼                        ▼                        ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                        PRESENTATION LAYER                                │
│                                                                          │
│  ┌───────────────┐  ┌──────────────────┐  ┌──────────────────────────┐  │
│  │ LAKEVIEW       │  │ GENIE SPACE       │  │ DATABRICKS APP            │  │
│  │ DASHBOARD      │  │ (REST API deploy) │  │ (MCP Agent + Chat UI)     │  │
│  │                │  │                   │  │                           │  │
│  │ 3 pages:       │  │ NL queries on     │  │ LangGraph + MCP:          │  │
│  │ - Fraud stats  │  │ financial_metrics  │  │ - Vector Search tool      │  │
│  │ - Model perf   │  │ + gold tables     │  │ - Genie analytics tool    │  │
│  │ - Inclusion    │  │                   │  │ - Loan eligibility tool   │  │
│  │                │  │ Deployed via       │  │                           │  │
│  │ MEASURE()      │  │ /api/2.0/genie/   │  │ MLflow AgentServer        │  │
│  │ queries on     │  │ spaces API        │  │ + e2e-chatbot-app-next    │  │
│  │ metric view    │  │                   │  │ (built-in chat UI)        │  │
│  └───────────────┘  └──────────────────┘  └──────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## Implementation Details (Using Real Starter Kit Patterns)

### Component 1: Data Ingestion Notebook

**File: `01-data-ingestion.py`**

Follows the exact starter kit pattern: parameterized widgets, Auto Loader with `cloudFiles`, schema inference, `trigger(availableNow=True)`, PK/FK constraints, CDF enable, column comments.

**3 ingestion streams:**

| Stream | Source | Pattern | Key Transforms |
|--------|--------|---------|----------------|
| UPI Transactions | `*_transactions.json` | Auto Loader + flatten | `from_unixtime()`, derive `hour_of_day`, `day_of_week`, `content_type` |
| RBI Circulars | `*_circulars.json` | Auto Loader or direct read | Extract `circular_id`, `title`, `date`, `full_text`, `department` |
| Gov Schemes | `*_schemes.csv` | `spark.read.csv` | Parse eligibility fields, normalize ministry names |

**Post-ingestion governance:**
```sql
-- PKs (from starter kit pattern)
ALTER TABLE bronze_transactions ALTER COLUMN transaction_id SET NOT NULL;
ALTER TABLE bronze_transactions ADD CONSTRAINT txn_pk PRIMARY KEY (transaction_id);

-- CDF (enables incremental downstream)
ALTER TABLE bronze_transactions SET TBLPROPERTIES (delta.enableChangeDataFeed = true);

-- Column docs (shows depth to judges in Unity Catalog UI)
ALTER TABLE bronze_transactions ALTER COLUMN amount COMMENT 'Transaction amount in INR';
```

---

### Component 2: DLT Transforms (Silver + Gold)

**Run inside a Lakeflow Declarative Pipeline, not the SQL editor.**

**File: `02-transforms/silver_transactions.sql`**
```sql
CREATE OR REFRESH STREAMING TABLE silver_transactions (
  CONSTRAINT valid_txn_id EXPECT (transaction_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_amount EXPECT (amount > 0 AND amount < 10000000) ON VIOLATION DROP ROW,
  CONSTRAINT valid_time EXPECT (transaction_time IS NOT NULL) ON VIOLATION DROP ROW
)
TBLPROPERTIES('pipelines.channel' = 'PREVIEW')
COMMENT 'Cleaned UPI transactions with AI risk classification'
AS
SELECT DISTINCT
    transaction_id, amount, transaction_time,
    sender_id, receiver_id, category, merchant_id,
    hour_of_day, day_of_week,
    CASE WHEN dayofweek(transaction_time) IN (1, 7) THEN true ELSE false END AS is_weekend,
    -- INLINE LLM CLASSIFICATION — this is the killer feature from the starter kit
    LOWER(TRIM(ai_query('databricks-meta-llama-3-1-70b-instruct',
      CONCAT('Classify this UPI transaction risk as exactly one of: low, medium, high, critical. Output ONLY the label, nothing else.\nAmount: ₹', CAST(amount AS STRING),
        ' Category: ', COALESCE(category, 'unknown'),
        ' Hour: ', CAST(hour_of_day AS STRING),
        ' Weekend: ', CAST(CASE WHEN dayofweek(transaction_time) IN (1,7) THEN 'yes' ELSE 'no' END)),
      'STRING'))) AS risk_label
FROM STREAM(bronze_transactions);
```

**File: `02-transforms/silver_circulars.sql`**
```sql
CREATE OR REFRESH STREAMING TABLE silver_circulars (
  CONSTRAINT valid_id EXPECT (circular_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT has_text EXPECT (full_text IS NOT NULL AND LENGTH(TRIM(full_text)) > 10) ON VIOLATION DROP ROW
)
TBLPROPERTIES('pipelines.channel' = 'PREVIEW')
COMMENT 'Cleaned RBI circulars with topic classification'
AS
SELECT DISTINCT
    circular_id, title, date, department, full_text,
    LOWER(TRIM(ai_query('databricks-meta-llama-3-1-70b-instruct',
      CONCAT('Classify this RBI circular as exactly one of: fraud_prevention, monetary_policy, lending, digital_payments, consumer_protection, banking_regulation. Output ONLY the label.\nTitle: ',
        COALESCE(title, ''), '\nText: ', SUBSTRING(COALESCE(full_text, ''), 1, 500)),
      'STRING'))) AS topic_label
FROM STREAM(bronze_circulars);
```

**File: `02-transforms/gold_circular_chunks.sql`** — Uses the starter kit's `AGGREGATE()` chunking pattern:
```sql
CREATE OR REFRESH MATERIALIZED VIEW gold_circular_chunks
COMMENT 'RBI circulars chunked at ~4000 char boundaries for vector search'
AS
WITH chunked AS (
    SELECT circular_id, title, topic_label,
        AGGREGATE(
            SPLIT(full_text, '\n\n'),
            named_struct('chunks', ARRAY(CAST('' AS STRING)), 'cur', CAST('' AS STRING)),
            (acc, para) ->
                CASE WHEN LENGTH(acc.cur) + LENGTH(para) > 4000
                    THEN named_struct('chunks', CONCAT(acc.chunks, ARRAY(acc.cur)), 'cur', para)
                    ELSE named_struct('chunks', acc.chunks, 'cur', CONCAT(acc.cur, '\n\n', para))
                END,
            acc -> CONCAT(acc.chunks, ARRAY(acc.cur))
        ) AS chunks_array
    FROM silver_circulars
)
SELECT
    CONCAT(circular_id, '_', CAST(idx AS STRING)) AS chunk_id,
    circular_id, title, topic_label,
    chunk AS chunk_text, idx AS chunk_index
FROM chunked
LATERAL VIEW POSEXPLODE(chunks_array) AS idx, chunk
WHERE LENGTH(TRIM(chunk)) > 0;
```

**File: `02-transforms/gold_transactions.sql`**
```sql
CREATE OR REFRESH MATERIALIZED VIEW gold_transactions (
  CONSTRAINT valid_risk EXPECT (risk_label IN ('low','medium','high','critical')) ON VIOLATION DROP ROW
)
COMMENT 'Business-ready UPI transactions with risk classification'
AS SELECT *, CASE WHEN risk_label IN ('high','critical') THEN true ELSE false END AS anomaly_flag
FROM silver_transactions;
```

**File: `02-transforms/gold_fraud_alerts.sql`**
```sql
CREATE OR REFRESH MATERIALIZED VIEW gold_fraud_alerts
COMMENT 'Flagged transactions for fraud review'
AS SELECT * FROM gold_transactions WHERE anomaly_flag = true;
```

---

### Component 3: Fraud Detection (ML Pipeline)

**File: `03-fraud-detection.py`** — Runs as a notebook, not DLT.

**Approach: Ensemble of two models (shows both sklearn AND MLlib to judges)**

```
gold_transactions (Delta table)
    │
    ├─► Feature Engineering (PySpark)
    │     ├─ txn_velocity_1h: COUNT per sender in last 1 hour window
    │     ├─ txn_velocity_24h: COUNT per sender in last 24 hours
    │     ├─ amount_deviation: (amount - sender_avg) / sender_stddev
    │     ├─ category_encoded: StringIndexer
    │     ├─ hour_of_day, day_of_week, is_weekend (already in silver)
    │     └─ cross_feature: velocity * amount_deviation
    │
    ├─► Model A: IsolationForest (sklearn)
    │     └─ anomaly_score: -1 to 1
    │
    ├─► Model B: KMeans (Spark MLlib)
    │     └─ distance_to_center: float
    │
    └─► Ensemble: weighted_risk = 0.6 * norm(if_score) + 0.4 * norm(distance)
          └─ threshold → fraud_flag
```

**MLflow tracking for everything:**
- `mlflow.autolog()` for sklearn and spark
- Manual logging: ensemble weights, threshold, precision/recall/F1
- Register best model to Unity Catalog
- Compare runs in MLflow UI (judges can see this)

---

### Component 4: RAG + Multilingual Pipeline

**File: `04-rag-pipeline.py`**

**Two options for vector search (choose based on time):**

| Option | Setup Time | Maintenance | Pattern |
|--------|-----------|-------------|---------|
| **Databricks Vector Search** (managed) | ~15 min | Auto-syncs with Delta | `w.vector_search_indexes.create_index()` with `DELTA_SYNC` |
| **FAISS on DBFS** (manual) | ~30 min | Manual rebuild | `faiss.IndexFlatL2` + `sentence-transformers` |

**Recommendation:** Try Databricks Vector Search first (it auto-syncs when `gold_circular_chunks` updates). Fall back to FAISS if VS endpoint doesn't work on Free Edition.

**RAG chain:**
1. User query (Hindi or English) → embed with multilingual-e5-small (or Databricks BGE for managed VS)
2. Retrieve top-5 chunks from vector index
3. Construct prompt with retrieved context
4. Generate answer via Foundation Model API (Llama 3.1 70B)
5. Optionally translate output via IndicTrans2

---

### Component 5: Loan Eligibility Engine

**File: `05-loan-eligibility.py`**

```
User Input (form):              Scheme Data (gold_schemes):
  age: 25                         criteria: {"age_min": 18, "income_max": 200000, ...}
  income: 150000
  occupation: street_vendor
  state: Maharashtra

    └──► PySpark filter/join on criteria
            └──► Matching schemes
                    └──► LLM explanation (Foundation Model API)
                            └──► IndicTrans2 translation (Hindi/Marathi)
```

---

### Component 6: Metric View (YAML Semantic Layer)

**File: `06-metric-view.py`**

Uses the starter kit's YAML metric view pattern with `synonyms` and `comment` fields for Genie compatibility.

**Dimensions:** Transaction Date, Month, Category, Risk Category, Weekend Flag, Sender
**Measures:** Total Transactions, Total Amount (₹), Avg Amount, Flagged Count, Fraud Rate (%), High Risk Rate (%), Unique Senders

---

### Component 7: Genie Space (REST API Deployment)

**File: `07-deploy-genie.py`**

Uses the starter kit pattern: `WorkspaceClient().api_client.do("POST", "/api/2.0/genie/spaces", body=config)` with:
- Table configs for `gold_transactions`, `gold_fraud_alerts`, `gold_schemes`
- Metric view config for `financial_metrics`
- Text instructions explaining the domain
- Example Q&A pairs with SQL
- Join specs
- Benchmark questions for evaluation

---

### Component 8: MCP Agent (Databricks App)

**File: `08-agent/agent.py`**

Uses the starter kit's exact architecture: LangGraph + MCP + MLflow Responses API.

**Tools:**
1. `DatabricksMCPServer` → Vector Search index (RBI circular retrieval)
2. `DatabricksMCPServer` → Genie Space (financial analytics queries)
3. `@tool check_loan_eligibility()` → Custom tool wrapping the eligibility engine
4. `@tool get_current_time()` → Utility

**LLM:** `ChatDatabricks(endpoint="databricks-meta-llama-3-1-70b-instruct")`

**System prompt:**
```
You are Digital-Artha, a financial intelligence assistant for Indian citizens.
You help with: UPI fraud awareness, RBI regulations, government loan schemes.
Always cite your sources. Respond in the user's language.
Use the circular-search tool for RBI regulation questions.
Use the financial-analytics tool for fraud statistics and trends.
Use check_loan_eligibility for scheme recommendations.
```

**Chat UI:** MLflow AgentServer's built-in `enable_chat_proxy=True` + `e2e-chatbot-app-next` template (same as starter kit's `start_app.py` pattern).

**Evaluation:** `ConversationSimulator` with test cases + MLflow scorers (Completeness, Safety, Fluency, ToolCallCorrectness).

---

## Databricks Features Used (Judging: 30%)

| # | Feature | How We Use It |
|---|---------|---------------|
| 1 | **Delta Lake** | All tables — ACID, schema enforcement, time travel |
| 2 | **Unity Catalog** | Catalog/schema/volume hierarchy, PK/FK constraints, column comments, lineage |
| 3 | **Auto Loader** | Streaming ingestion with `cloudFiles` + schema inference |
| 4 | **Lakeflow Declarative Pipelines (DLT)** | Streaming Tables (silver) + Materialized Views (gold) with EXPECT constraints |
| 5 | **`ai_query()`** | Inline LLM classification in SQL transforms (risk labeling, topic tagging) |
| 6 | **Spark MLlib** | KMeans clustering for anomaly detection, VectorAssembler, Pipeline |
| 7 | **Foundation Model APIs** | LLM inference for RAG, explanations, classification |
| 8 | **Vector Search** | Managed index on `gold_circular_chunks` for semantic retrieval |
| 9 | **MLflow** | autolog, experiment tracking, model registry, GenAI tracing |
| 10 | **Metric Views (YAML)** | Semantic layer with dimensions/measures/synonyms |
| 11 | **Genie Space** | Natural language analytics, deployed via REST API |
| 12 | **Lakeview Dashboard** | 3-page interactive analytics with MEASURE() queries |
| 13 | **Databricks App** | MCP Agent with LangGraph + built-in chat UI |
| 14 | **Change Data Feed** | Incremental processing downstream of bronze |
| 15 | **Databricks Asset Bundles** | Reproducible deployment via `databricks.yml` |

**Most teams will use 3-5 features. We use 15.** That's how you score 30%.

---

## Tech Stack Summary

| Layer | Technology | Purpose |
|-------|-----------|---------|
| Storage | Delta Lake + Unity Catalog | Tables, governance, lineage |
| Ingestion | Auto Loader (`cloudFiles`) | Streaming ingestion with exactly-once |
| Transforms | DLT Streaming Tables + Materialized Views | Bronze→Silver→Gold with EXPECT DQ |
| Inline AI | `ai_query()` | LLM classification in SQL (risk labels, topics) |
| Feature Eng | PySpark | Transaction velocity, deviation, time features |
| Fraud ML | sklearn IsolationForest + MLlib KMeans | Ensemble anomaly detection |
| Tracking | MLflow (autolog + tracing) | Models, params, metrics, LLM traces |
| Embeddings | multilingual-e5-small or Databricks BGE | Multilingual vector embeddings |
| Vector Search | Databricks VS (managed) or FAISS | Semantic retrieval for RAG |
| LLM | Foundation Model API (Llama 3.1 70B) | Generation, explanations |
| Translation | IndicTrans2 (1B) | Hindi/regional language I/O |
| Semantic Layer | Metric View (YAML) | Dimensions + measures for Genie |
| NL Queries | Genie Space (REST API deploy) | Natural language analytics |
| Dashboard | Lakeview | Interactive analytics, MEASURE() |
| Agent | LangGraph + MCP + MLflow AgentServer | Multi-tool conversational AI |
| Deployment | Databricks Asset Bundles | Reproducible `databricks.yml` config |

---

## Judging Score Maximization

| Criteria (Weight) | What We Hit | Score Strategy |
|---|---|---|
| **Databricks Usage (30%)** | 15 Databricks features including DLT, ai_query(), MCP, Vector Search, Genie, Lakeview, Asset Bundles | Most teams use 3-5. We use 15. Show the DLT pipeline DAG, Unity Catalog lineage graph, and MLflow experiment UI to judges. |
| **Accuracy & Effectiveness (25%)** | Ensemble fraud model with MLflow metrics, RAG with cited sources, EXPECT constraints for DQ | Show precision/recall/F1 in MLflow. Show DLT pipeline DQ tab with pass/fail rates. Run BhashaBench-Finance if time allows. |
| **Innovation (25%)** | 3 AI paradigms (classical ML + NLP/RAG + translation), `ai_query()` for inline classification, MCP agent with Genie + VS tools | No other team will combine fraud detection + regulatory NLP + financial inclusion in one system. The `ai_query()` inline classification is a novel pattern most teams won't know. |
| **Presentation (20%)** | Live demo: flag a fraud txn (explain why), ask about RBI in Hindi, find a loan scheme | Judges interact with the agent directly. Show Genie Space answering "fraud rate by category". Show dashboard. |

---

## Team Task Split (3-4 members)

### Member A: Data Engineer
**Owns:** Ingestion + DLT pipeline + metric view + Genie Space
- Hour 0-2: Create catalog, schema, volumes. Upload all raw data.
- Hour 2-4: Build `01-data-ingestion.py` (Auto Loader for all 3 sources, PK/FK, CDF, comments)
- Hour 4-8: Write all DLT SQL transforms (silver + gold). Create + run DLT pipeline.
- Hour 8-10: Build metric view (YAML). Deploy Genie Space via REST API.
- Hour 10-12: Build Lakeview dashboard (3 pages).
- Hour 12+: Polish, architecture diagram, README.

### Member B: ML Engineer
**Owns:** Fraud detection pipeline + MLflow
- Hour 0-2: Explore UPI data distributions, understand features.
- Hour 2-6: Feature engineering in PySpark (velocity windows, deviation, cross features).
- Hour 6-10: Train IsolationForest (sklearn) + KMeans (MLlib). Ensemble scoring. MLflow tracking.
- Hour 10-14: Tune thresholds, evaluate precision/recall, register model to Unity Catalog.
- Hour 14+: Feature importance per flagged txn (explainability), write results to gold_fraud_alerts.

### Member C: AI/NLP Engineer
**Owns:** RAG pipeline + loan eligibility + IndicTrans2 + agent
- Hour 0-3: Set up Vector Search index on gold_circular_chunks. Test retrieval.
- Hour 3-7: Build RAG chain (retrieval + Foundation Model API). Test in Hindi.
- Hour 7-10: Build loan eligibility engine (rule matching + LLM explanation).
- Hour 10-14: Build MCP agent (LangGraph + MCP servers + custom tools). Wire to AgentServer.
- Hour 14+: IndicTrans2 integration, agent evaluation with ConversationSimulator.

### Member D: Presenter + Polish
**Owns:** Presentation, demo script, README, video, submission
- Hour 0-6: Help with data collection, scheme data structuring, testing.
- Hour 6-10: Test all components end-to-end, find bugs, report to team.
- Hour 10-14: Build presentation (5-min pitch structure). Record demo video.
- Hour 14-16: Write README (what it does, architecture, how to run, demo steps). Final submission.
- Hour 16+: Rehearse pitch, prepare for Q&A.

---

## Timeline (24-hour hackathon)

### Day 1 (3:00 PM - 11:59 PM) — Build Core

| Time | Milestone | Who |
|------|-----------|-----|
| 3:00 PM | Catalog + schema + volumes created, raw data uploaded | A |
| 3:00 PM | Exploring UPI data, planning features | B |
| 3:00 PM | Processing circulars + scheme text | C |
| 3:00 PM | Helping with data prep | D |
| 5:00 PM | Bronze tables ingested via Auto Loader, governance set up | A |
| 5:00 PM | Feature engineering code working in PySpark | B |
| 5:00 PM | Vector Search index created or FAISS built | C |
| 8:00 PM | **DLT pipeline running: silver + gold tables exist** | A |
| 8:00 PM | First fraud model trained + logged to MLflow | B |
| 8:00 PM | RAG retrieves relevant chunks, generates answers | C |
| 11:00 PM | **CHECKPOINT: All gold tables exist, fraud model works, RAG returns answers** | ALL |

### Overnight (Remote)
- B: Tune ensemble, add explainability
- C: Build agent, wire MCP tools
- A: Metric view + Genie Space
- D: Test everything, document bugs

### Day 2 (9:00 AM - 3:00 PM) — Integrate & Polish

| Time | Milestone | Who |
|------|-----------|-----|
| 9:00 AM | Metric view created, Genie Space deployed | A |
| 9:00 AM | Ensemble model tuned, registered to UC | B |
| 9:00 AM | Agent working with MCP tools + AgentServer | C |
| 9:00 AM | Testing all components | D |
| 11:00 AM | Lakeview dashboard complete (3 pages) | A |
| 11:00 AM | Fraud explainability working (feature importance per txn) | B |
| 11:00 AM | Loan eligibility + IndicTrans2 in agent | C |
| 12:00 PM | **End-to-end demo working** | ALL |
| 12:00-2:00 PM | README, architecture diagram, demo video, submission prep | ALL |
| 2:00-2:30 PM | Final test, dry-run presentation | ALL |
| 2:30-3:00 PM | Submit | ALL |
| **3:00 PM** | **CODE FREEZE** | |
| 3:30 PM | Presentations begin | D leads |

---

## Data Sources to Collect BEFORE the Hackathon

| Dataset | Source | Format | Action |
|---------|--------|--------|--------|
| Synthetic UPI Transactions | Hackathon-provided or generate | JSON/CSV | Upload to Volume as `upi_transactions.json` |
| RBI Circulars | rbi.org.in (public PDFs) | PDF → JSON text | Scrape 50-100 recent circulars, convert to text, upload as `rbi_circulars.json` |
| Government Schemes | data.gov.in / myscheme.gov.in | CSV/JSON | Focus on financial inclusion schemes (PM-SVANidhi, MUDRA, etc.) |
| BhashaBench-Finance | Hackathon-provided | Various | Use for evaluation (bonus points) |

---

## File Structure (GitHub Repo)

```
digital-artha/
├── README.md                           # What, why, architecture, how to run, demo steps
├── architecture.png                    # Clean architecture diagram
├── 01-data-ingestion.py                # Auto Loader for all 3 sources
├── 02-transforms/
│   ├── silver_transactions.sql         # DLT streaming table + ai_query()
│   ├── silver_circulars.sql            # DLT streaming table + topic classification
│   ├── silver_schemes.sql              # DLT streaming table
│   ├── gold_transactions.sql           # DLT materialized view
│   ├── gold_circular_chunks.sql        # AGGREGATE() chunking for vector search
│   ├── gold_fraud_alerts.sql           # Filtered fraud view
│   └── gold_schemes.sql               # Business-ready schemes
├── 03-fraud-detection.py               # Feature eng + IsolationForest + KMeans + MLflow
├── 04-rag-pipeline.py                  # Vector search setup + RAG chain
├── 05-loan-eligibility.py              # Rule matching + LLM explanation
├── 06-metric-view.py                   # YAML metric view creation
├── 07-deploy-genie.py                  # Genie Space REST API deployment
├── 08-agent/
│   ├── agent.py                        # LangGraph + MCP + tools
│   ├── start_server.py                 # MLflow AgentServer
│   ├── utils.py                        # Session management, stream processing
│   ├── evaluate_agent.py               # ConversationSimulator tests
│   ├── app.yaml                        # Databricks App config
│   ├── databricks.yml                  # Asset Bundle deployment
│   ├── pyproject.toml                  # Dependencies
│   └── requirements.txt
├── 09-dashboard.lvdash.json            # Lakeview dashboard export
├── raw_data/                           # Source data files
│   ├── upi_transactions.json
│   ├── rbi_circulars.json
│   └── gov_schemes.csv
└── instructions/                       # Setup guides (optional)
```

---

## Submission Checklist

- [ ] Public GitHub repo (keep public for 30 days)
- [ ] README: what it does (1-2 sentences), architecture diagram, how to run (exact commands), demo steps
- [ ] Architecture diagram (clean version of the one above)
- [ ] 500-char project description
- [ ] List of Databricks technologies used (all 15!)
- [ ] List of open-source models used (IndicTrans2, multilingual-e5, IsolationForest, KMeans)
- [ ] 2-minute demo video
- [ ] Link to deployed prototype (Databricks App URL)
- [ ] **Bonus:** BhashaBench-Finance scores, MLflow experiment screenshots, precision/recall metrics

---

## Risk Mitigation

| Risk | Mitigation |
|------|-----------|
| Quota exceeded mid-hack | Sample data for dev (1K txns). Full data for final run only. Kill idle warehouses. |
| `ai_query()` too slow/expensive | Reduce to classifying only a sample. Or skip and use Python-based classification. |
| DLT pipeline fails | Fall back to plain `CREATE TABLE AS SELECT` in notebooks. Same result, less fancy. |
| Vector Search endpoint unavailable on Free Edition | Use FAISS on DBFS (manual but reliable). |
| Foundation Model API down | Use Sarvam-m or Airavata locally (slower). Or hardcode sample responses for demo. |
| IndicTrans2 install fails | Use Foundation Model API with Hindi prompts — Llama 3.1 handles Hindi reasonably. |
| App auto-stops (24h limit) | Restart manually. Keep code in Git for instant redeploy. |
| Team member stuck | Gold tables are the contract boundary. Anyone can pick up downstream work. |
| Model too slow on CPU | Cache embeddings to Delta table. Keep max_tokens low (256). Batch requests. |

---

## Demo Script (5 minutes)

**0:00 - 0:30 — Problem Statement**
"India's UPI processes 16 billion transactions monthly. Fraud is rising. RBI circulars are impenetrable English legalese. Rural Indians don't know what government loans they qualify for. We built Digital-Artha to solve all three."

**0:30 - 1:30 — Architecture Walkthrough**
Show architecture diagram. Walk through: 3 data sources → Auto Loader → DLT pipeline (show the pipeline DAG!) → 3 AI engines → unified agent. Name every Databricks feature. Show Unity Catalog lineage graph.

**1:30 - 3:00 — Live Demo**
1. **Agent chat:** Ask "Show me today's fraud alerts" → Agent uses Genie MCP → returns stats
2. **Hindi query:** "यूपीआई फ्रॉड के बारे में आरबीआई के नियम क्या हैं?" → Agent uses Vector Search MCP → returns sourced answer in Hindi
3. **Loan finder:** "I'm a 25-year-old street vendor in Maharashtra earning ₹1.5L" → Agent uses eligibility tool → returns matching schemes

**3:00 - 4:00 — Technical Depth**
Show MLflow: model comparison, precision/recall, LLM traces. Show Lakeview dashboard. Run a Genie Space query: "fraud rate by category this quarter". Show DLT pipeline quality metrics.

**4:00 - 5:00 — Impact & Wrap**
"Digital-Artha makes financial safety and inclusion accessible to every Indian, in their language. Built entirely on Databricks Free Edition with Indian models. 15 Databricks features. 3 AI paradigms. 1 unified agent."
