# BlackIce Architecture

## 1. How All 17 Databricks Components Connect

This is the master wiring diagram. Every arrow is a real data dependency in the system.

```
                        ┌─────────────────────────────────────┐
                        │         DATA SOURCES                 │
                        │  Kaggle CSV | RBI JSON | Schemes     │
                        │  NPCI | Lok Sabha | TRAI | PMJDY     │
                        └──────────────┬──────────────────────┘
                                       │
                                       ▼
┌──────────────────────────────────────────────────────────────────────────┐
│  [2] UNITY CATALOG                                                        │
│  Catalog: digital_artha | Schema: main                                   │
│  ┌──────────────────────────────────────────────────────────────────┐    │
│  │  [UC VOLUMES] /Volumes/digital_artha/main/raw_data/              │    │
│  │  Raw files land here. Auto Loader reads from here.               │    │
│  │  FAISS index + metadata.pkl also stored here (serverless-safe).  │    │
│  └──────────────────────────────────────────────────────────────────┘    │
│                                                                          │
│  Governance applied to EVERY table below:                                │
│  ├── PK constraints (transaction_id, circular_id, scheme_id)            │
│  ├── Column comments ("ensemble_score: weighted fraud risk 0-1...")      │
│  ├── [14] UC TAGS on 15 tables: domain, tier, pii, source, quality      │
│  └── Data lineage tracked automatically across all layers               │
└──────────────────────────────────────────────────────────────────────────┘
         │                                    │
         │ CSV files                          │ JSON files
         ▼                                    ▼
┌─────────────────────┐            ┌─────────────────────┐
│  [3] AUTO LOADER     │            │  [5] SPARK SQL       │
│                      │            │                      │
│  cloudFiles format   │            │  spark.read.json()   │
│  Schema inference    │            │  Direct read for     │
│  Checkpoint-based    │            │  circulars, schemes, │
│  Exactly-once        │            │  macro stats         │
│  [12] CHANGE DATA    │            │                      │
│  FEED enabled on     │            │                      │
│  output tables       │            │                      │
└─────────┬───────────┘            └──────────┬──────────┘
          │                                    │
          ▼                                    ▼
┌──────────────────────────────────────────────────────────────────────────┐
│  BRONZE LAYER  [1] DELTA LAKE                                             │
│                                                                          │
│  bronze_transactions (750K)  bronze_circulars (80)  bronze_schemes (170) │
│  All Delta format: ACID transactions, schema enforcement, time travel    │
│  [12] CDF enabled → downstream layers only process changed rows          │
│  [14] Tags: tier=bronze, domain=fraud/regulatory/inclusion               │
└──────────────────────────────────────────────────────────────────────────┘
          │
          │  [4] DLT PIPELINE reads from bronze
          ▼
┌──────────────────────────────────────────────────────────────────────────┐
│  [4] LAKEFLOW DECLARATIVE PIPELINE (DLT)                                  │
│                                                                          │
│  EXPECT constraints (data quality gates):                                │
│  ├── valid_txn_id: NOT NULL, LENGTH > 0         → ON VIOLATION DROP ROW │
│  ├── valid_amount: > 0 AND < 10,000,000         → ON VIOLATION DROP ROW │
│  ├── valid_timestamp: NOT NULL                   → ON VIOLATION DROP ROW │
│  └── sender_not_receiver: sender_id != receiver_id → ON VIOLATION DROP  │
│                                                                          │
│  [5] SPARK SQL feature derivation inside DLT:                            │
│  ├── is_weekend (DAYOFWEEK)                                              │
│  ├── time_slot (CASE on hour: late_night/morning/afternoon/evening)      │
│  ├── amount_bucket (micro < 100 / small / medium / large / very_large)  │
│  ├── risk_label (rule-based: amount + hour heuristics)                   │
│  ├── topic_label (keyword classification for circulars)                  │
│  └── eligibility_json (TO_JSON + NAMED_STRUCT for schemes)              │
│                                                                          │
│  Violations tracked in system.event_log (DLT metrics)                   │
└──────────────────────────────────────────────────────────────────────────┘
          │
          │  DLT outputs SILVER tables (streaming tables + materialized views)
          ▼
┌──────────────────────────────────────────────────────────────────────────┐
│  SILVER LAYER  [1] DELTA LAKE                                             │
│                                                                          │
│  silver_transactions (750K, 28 cols)                                     │
│  silver_circulars (80 rows, with topic_label + word_count)               │
│  silver_schemes (170 rows, with eligibility_json)                        │
│  [14] Tags: tier=silver, quality=validated                               │
└──────────────────────────────────────────────────────────────────────────┘
          │
          │  [5] SPARK SQL transforms silver → gold
          ▼
┌──────────────────────────────────────────────────────────────────────────┐
│  GOLD LAYER  [1] DELTA LAKE  [13] LIQUID CLUSTERING                       │
│                                                                          │
│  gold_transactions (750K)                                                │
│  ├── [13] CLUSTER BY (transaction_date, final_risk_tier)                │
│  ├── Liquid clustering physically reorganizes files for range queries    │
│  └── OPTIMIZE compacts small files after clustering                      │
│                                                                          │
│  gold_fraud_alerts (1,440 rows, anomaly_flag = true)                    │
│  ├── alert_summary: formatted text for LLM + human consumption          │
│                                                                          │
│  gold_circular_chunks                                                    │
│  ├── [5] SQL AGGREGATE() with named_struct accumulator                  │
│  │   Iterates paragraphs, splits at 4000-char boundaries                │
│  ├── [5] LATERAL VIEW POSEXPLODE expands chunks to rows                 │
│  ├── Each chunk gets: chunk_id, chunk_index, citation metadata          │
│  └── This table feeds → [16] FAISS INDEX (see RAG pipeline below)       │
│                                                                          │
│  gold_schemes (170 rows)                                                 │
│  ├── eligibility_json (structured matching for agent tool)               │
│  └── plain_summary (first 200 chars for quick display)                  │
│                                                                          │
│  [14] Tags: tier=gold, quality=business_ready                           │
└──────────────────────────────────────────────────────────────────────────┘
          │
          │  sklearn models trained on gold_transactions
          │  [5] Spark SQL for feature engineering (window functions)
          ▼
┌──────────────────────────────────────────────────────────────────────────┐
│  PLATINUM LAYER  [1] DELTA LAKE                                           │
│                                                                          │
│  gold_transactions_enriched (896K rows, 50 columns)                      │
│  ├── 10 engineered features via [5] Spark SQL window functions           │
│  ├── IsolationForest score (0-1) — 300 trees, 0.02 contamination        │
│  ├── KMeans distance score (0-1) — k=8, 95th percentile threshold       │
│  ├── Ensemble: 0.45×IF + 0.30×KM + 0.25×Rules                          │
│  ├── Risk tiers: low / medium / high / critical                         │
│  └── Cohen's d = 2.128 (separation between flagged and normal)          │
│                                                                          │
│  gold_fraud_alerts_ml (1,800 flagged transactions)                       │
│  ├── Queried by agent tool: lookup_fraud_alerts via [11] SDK            │
│                                                                          │
│  platinum_anomaly_patterns (8 named patterns)                            │
│  ├── Late Night High Value | Late Night Activity | Unusual Category      │
│  ├── High Value | Weekend High | ML Critical | ML High | General        │
│                                                                          │
│  platinum_sender_profiles (5,000 senders)                                │
│  ├── Composite: 30% ML + 25% fraud_rate + 20% PageRank + 15% late + 10% degree │
│  ├── Queried by agent tool: lookup_sender_profile via [11] SDK          │
│                                                                          │
│  platinum_fraud_rings (150 rings) ← NetworkX graph analysis              │
│  ├── 57 circular flows | 93 mule hubs | Rs.8.68 Cr                     │
│  ├── Queried by agent tool: lookup_fraud_rings via [11] SDK             │
│                                                                          │
│  platinum_merchant_profiles (10 categories)                              │
│                                                                          │
│  WARM VIEWS (pre-aggregated, zero storage overhead):                    │
│  ├── mv_fraud_summary (date × category × state × risk_tier)            │
│  ├── mv_high_risk_senders (composite_risk > 0.3)                        │
│  └── 13 viz_* views → feed [9] LAKEVIEW DASHBOARD                      │
│                                                                          │
│  [14] Tags: tier=platinum, quality=ml_scored                            │
└──────────────────────────────────────────────────────────────────────────┘
          │
          │  Tables are now queryable by 3 serving tiers
          ▼
```

### How the 17 features connect to the serving layer:

```
┌─────────────────────────────────────────────────────────────────────────┐
│                                                                         │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │  HOT TIER — [11] DATABRICKS SDK + [6] FOUNDATION MODEL API      │   │
│  │                                                                  │   │
│  │  Agent (LangGraph ReAct + MemorySaver)                          │   │
│  │  │                                                               │   │
│  │  ├── lookup_fraud_alerts ─── [11] SDK statement_execution ────┐ │   │
│  │  │   Parameterized SQL → gold_fraud_alerts_ml                 │ │   │
│  │  │                                                             │ │   │
│  │  ├── check_loan_eligibility ─── [11] SDK → gold_schemes       │ │   │
│  │  │   + [6] Foundation Model API (Llama 4 Maverick)            │ │   │
│  │  │   Generates explanation in 5 languages                      │ │   │
│  │  │                                                             │ │   │
│  │  ├── fraud_recovery_guide ─── [11] SDK → fraud_recovery_guide │ │   │
│  │  │   + [6] LLM explains RBI steps in user's language          │ │   │
│  │  │                                                             │ │   │
│  │  ├── lookup_fraud_rings ─── [11] SDK → platinum_fraud_rings   │ │   │
│  │  │                                                             │ │   │
│  │  ├── lookup_sender_profile ─ [11] SDK → platinum_sender_profiles   │
│  │  │                                                             │ │   │
│  │  ├── [7] VECTOR SEARCH (MCP) ←─ [17] MCP PROTOCOL             │ │   │
│  │  │   Agent sends query → VS endpoint searches FAISS index     │ │   │
│  │  │   [16] FAISS ON VOLUMES provides the index                 │ │   │
│  │  │   [6] Foundation Model API generates answer from chunks    │ │   │
│  │  │                                                             │ │   │
│  │  ├── [8] GENIE SPACE (MCP) ←─ [17] MCP PROTOCOL              │ │   │
│  │  │   Agent sends NL query → Genie translates to SQL           │ │   │
│  │  │   [10] METRIC VIEWS provide semantic layer                 │ │   │
│  │  │   Genie reads dimensions, measures, synonyms from YAML     │ │   │
│  │  │   Entity matching + format assistance configured            │ │   │
│  │  │                                                             │ │   │
│  │  └── All SQL queries use StatementParameterListItem binding   │ │   │
│  │      Zero SQL injection across all 6 custom tools              │ │   │
│  │                                                                  │   │
│  │  Latency: 1-5 seconds                                          │   │
│  └─────────────────────────────────────────────────────────────────┘   │
│                                                                         │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │  WARM TIER — [9] LAKEVIEW DASHBOARD                              │   │
│  │                                                                  │   │
│  │  Reads from 13 viz_* views (pre-aggregated SQL views):          │   │
│  │  ┌──────────────────┬──────────────────┬────────────────────┐   │   │
│  │  │ viz_fraud_heatmap│ viz_category_    │ viz_risk_          │   │   │
│  │  │ (hour × day)     │ time_matrix      │ distribution       │   │   │
│  │  ├──────────────────┼──────────────────┼────────────────────┤   │   │
│  │  │ viz_amount_      │ viz_monthly_     │ viz_anomaly_       │   │   │
│  │  │ comparison       │ trend            │ patterns           │   │   │
│  │  ├──────────────────┼──────────────────┼────────────────────┤   │   │
│  │  │ viz_upi_growth   │ viz_bank_fraud   │ viz_state_         │   │   │
│  │  │                  │                  │ vulnerability      │   │   │
│  │  ├──────────────────┼──────────────────┼────────────────────┤   │   │
│  │  │ viz_recovery_    │ viz_risky_       │ viz_kpis           │   │   │
│  │  │ guide            │ senders          │ (9 counters)       │   │   │
│  │  ├──────────────────┴──────────────────┴────────────────────┤   │   │
│  │  │ viz_complaint_surge                                      │   │   │
│  │  └──────────────────────────────────────────────────────────┘   │   │
│  │                                                                  │   │
│  │  Dashboard 4 pages:                                             │   │
│  │  1. Fraud Command Center (KPIs, heatmap, risk distribution)     │   │
│  │  2. Pattern Intelligence (8 patterns, category×time matrix)     │   │
│  │  3. India Story (UPI growth, bank losses, vulnerability index)  │   │
│  │  4. Help & Recovery (recovery guide, pipeline inventory)        │   │
│  │                                                                  │   │
│  │  Latency: < 1 second (aggregation at write time, not read)     │   │
│  └─────────────────────────────────────────────────────────────────┘   │
│                                                                         │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │  COLD TIER — [1] DELTA LAKE + [13] LIQUID CLUSTERING             │   │
│  │                                                                  │   │
│  │  Full historical tables queryable via:                          │   │
│  │  ├── [8] Genie Space (NL → SQL, uses [10] Metric Views)        │   │
│  │  ├── SQL Editor (direct queries)                                │   │
│  │  └── Notebooks (PySpark + SQL)                                  │   │
│  │                                                                  │   │
│  │  [13] Liquid Clustering:                                        │   │
│  │  CLUSTER BY (transaction_date, final_risk_tier)                 │   │
│  │  Databricks physically reorganizes data files so                │   │
│  │  WHERE transaction_date = '2025-03' AND risk_tier = 'high'     │   │
│  │  skips irrelevant data blocks → 10-100x faster range queries   │   │
│  │                                                                  │   │
│  │  Latency: 5-30 seconds                                         │   │
│  └─────────────────────────────────────────────────────────────────┘   │
│                                                                         │
│  DEPLOYMENT: [15] DATABRICKS APPS                                       │
│  databricks.yml defines: app config, env vars, resource permissions     │
│  app.yaml defines: runtime (Python/Node), startup command               │
│  Resources bound: SQL warehouse, Vector Search index, Genie Space,     │
│  MLflow experiment                                                      │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## 2. Component Connection Matrix

Every cell shows HOW component A (row) connects to component B (column).

```
                    Delta  UC    Auto   DLT   Spark  FMAPI  VS-MCP Genie  Dash   Metric SDK    CDF    Liquid Tags   Apps   FAISS  MCP
                    [1]    [2]   [3]    [4]   [5]    [6]    [7]    [8]    [9]    [10]   [11]   [12]   [13]   [14]   [15]   [16]   [17]
─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────
[1]  Delta Lake      ·    governs stores  reads  reads  ─      index  queries reads  source queries CDF    clust  tagged deploy store  ─
[2]  Unity Catalog  owns   ·     volume  schema  ─      ─      ─      ─      ─      ─      auth   ─      ─      tags   perms  volume ─
[3]  Auto Loader     sink  ─      ·      feeds  ─      ─      ─      ─      ─      ─      ─      ─      ─      ─      ─      ─      ─
[4]  DLT Pipeline   writes ─     reads    ·     inside ─      ─      ─      ─      ─      ─      reads  ─      ─      ─      ─      ─
[5]  Spark SQL      r/w    ─      ─      inside  ·     ─      ─      ─      ─      ─      ─      ─      ─      ─      ─      ─      ─
[6]  Foundation API  ─     ─      ─       ─      ─      ·     gen    ─      ─      ─      calls  ─      ─      ─      ─      ─      ─
[7]  Vector Search   ─     index  ─       ─      ─     gen     ·     ─      ─      ─      ─      ─      ─      ─      ─      reads  conn
[8]  Genie Space    queries ─     ─       ─      ─      ─      ─      ·     ─      reads  ─      ─      ─      ─      ─      ─      conn
[9]  Dashboard      reads  ─      ─       ─      ─      ─      ─      ─      ·     uses   ─      ─      ─      ─      ─      ─      ─
[10] Metric Views   source ─      ─       ─      ─      ─      ─     feeds  feeds   ·     ─      ─      ─      ─      ─      ─      ─
[11] Databricks SDK queries auth  ─       ─      ─     calls   ─      ─      ─      ─      ·     ─      ─      ─      ─      ─      ─
[12] Change Data    enables ─    source    ─      ─      ─      ─      ─      ─      ─      ─      ·     ─      ─      ─      ─      ─
[13] Liquid Clust   on     ─      ─       ─      ─      ─      ─      ─     fast   ─      ─      ─      ·     ─      ─      ─      ─
[14] UC Tags        on     in     ─       ─      ─      ─      ─      ─      ─      ─      ─      ─      ─      ·     ─      ─      ─
[15] Databricks Apps ─    perms   ─       ─      ─      ─     binds  binds  ─      ─     runs   ─      ─      ─      ·     ─      ─
[16] FAISS on Vol   ─     stored  ─       ─      ─      ─     serves ─      ─      ─      ─      ─      ─      ─      ─      ·     ─
[17] MCP Protocol   ─      ─      ─       ─      ─      ─     conn   conn   ─      ─      ─      ─      ─      ─      ─      ─      ·
```

**Reading the matrix:** Row = source, Column = target. "governs" means UC governs Delta tables. "feeds" means DLT reads from Auto Loader output. "conn" means MCP Protocol connects agent to Vector Search and Genie.

---

## 3. Data Flow — Every Table, Every Connection

```
RAW FILES (UC Volumes)
│
├── upi_transactions_2024.csv ──[3]Auto Loader──▶ bronze_transactions ──[4]DLT──▶ silver_transactions
│                                                                                        │
│                                                                            [5]Spark SQL (features)
│                                                                                        │
│                                                                                        ▼
│                                                                               gold_transactions
│                                                                          [13]Liquid Clustering
│                                                                                        │
│                                                                              sklearn (IF + KM)
│                                                                                        │
│                                                              ┌─────────────────────────┼──────────────────┐
│                                                              ▼                         ▼                  ▼
│                                                    gold_transactions      gold_fraud_alerts_ml   platinum_anomaly
│                                                    _enriched (896K)          (1,800)              _patterns (8)
│                                                              │                    │                       │
│                                                    NetworkX graph              [11]SDK              [11]SDK
│                                                              │               lookup_fraud          (dashboard)
│                                                              ▼               _alerts tool
│                                                    platinum_fraud_                  │
│                                                    rings (150)                     ▼
│                                                         │                   [6]FMAPI adds
│                                                    [11]SDK                  risk explanation
│                                                    lookup_fraud
│                                                    _rings tool
│
├── rbi_circulars.json ──[5]Spark──▶ bronze_circulars ──[4]DLT──▶ silver_circulars
│                                                                        │
│                                                        [5]AGGREGATE() chunking
│                                                                        │
│                                                                        ▼
│                                                            gold_circular_chunks
│                                                                        │
│                                                          multilingual-e5-small
│                                                            (384 dims, 22 langs)
│                                                                        │
│                                                                        ▼
│                                                        [16]FAISS index on [2]UC Volumes
│                                                                        │
│                                                            [7]Vector Search (MCP)
│                                                            ←─[17]MCP Protocol──
│                                                                        │
│                                                            [6]Foundation Model API
│                                                            Llama 4 Maverick (RAG)
│                                                                        │
│                                                                        ▼
│                                                           Answer + citations to user
│
├── gov_schemes.json ──[5]Spark──▶ bronze_schemes ──[4]DLT──▶ silver_schemes
│                                                                   │
│                                                          eligibility_json
│                                                                   │
│                                                                   ▼
│                                                             gold_schemes (170)
│                                                                   │
│                                                       [11]SDK check_loan_eligibility
│                                                           + [6]FMAPI (5 languages)
│                                                                   │
│                                                                   ▼
│                                                      Matched schemes + LLM explanation
│
├── macro stats (5 JSON) ──[5]Spark──▶ india_digital_payments (432)
│                                      india_bank_fraud_stats (75)
│                                      india_rbi_complaints (48)
│                                      india_pmjdy_statewise (36)
│                                      india_internet_penetration (36)
│                                              │
│                                    [5]Spark SQL cross-join
│                                              │
│                                              ▼
│                                    state_vulnerability_index (10)
│                                    0.4×fraud + 0.3×(1-internet) + 0.3×(1-jandhan)
│
└── fraud_recovery_guide.json ──[5]Spark──▶ fraud_recovery_guide (6 types)
                                                    │
                                        [11]SDK fraud_recovery_guide tool
                                            + [6]FMAPI explains in user's language
                                                    │
                                                    ▼
                                        RBI steps + liability + deadlines
```

---

## 4. Real-Time Streaming Pipeline

```
               Volume: streaming_incoming/
                         │
                    new JSON file
                         │
                         ▼
              ┌─────────────────────┐
              │  [3] AUTO LOADER     │
              │                      │
              │  cloudFiles format   │
              │  Checkpoint at:      │
              │  _checkpoints/       │
              │  streaming_bronze    │
              │                      │
              │  Knows which files   │
              │  already processed.  │
              │  Only reads NEW.     │
              └──────────┬──────────┘
                         │
                         ▼
              ┌─────────────────────┐
              │  streaming_bronze    │
              │  [1] DELTA TABLE     │
              │                      │
              │  10 columns cast     │
              │  ingested_at = now() │
              └──────────┬──────────┘
                         │
                    readStream from
                    streaming_bronze
                         │
                         ▼
              ┌─────────────────────┐
              │  streaming_silver    │
              │  [1] DELTA TABLE     │
              │                      │
              │  WHERE txn_id NOT    │
              │  NULL AND amount > 0 │
              │                      │
              │  + risk_tier (CASE)  │
              │  + processing_time   │
              └──────────┬──────────┘
                         │
                    readStream from
                    streaming_silver
                         │
                         ▼
              ┌─────────────────────┐
              │  streaming_platinum  │
              │  [1] DELTA TABLE     │
              │                      │
              │  4-signal ensemble:  │
              │  25% risk_score      │
              │  35% amount_thresh   │
              │  25% time_of_day     │
              │  15% risk_tier       │
              │                      │
              │  8 anomaly patterns  │
              │  matched via CASE    │
              │                      │
              │  scored_at = now()   │
              └─────────────────────┘

  Demonstration:
  ┌────┐  ┌────┐     ┌────┐  ┌────┐  ┌────┐
  │ B0 │  │ B1 │     │ B2 │  │ B3 │  │ B4 │
  └──┬─┘  └──┬─┘     └──┬─┘  └──┬─┘  └──┬─┘
     │       │          │       │       │
     ▼       ▼          ▼       ▼       ▼
   Initial load      Dropped one at a time.
   (2 batches)       Counts grow in real time.
                     Judges inject custom txn.
```

---

## 5. Fraud Ring Detection — Graph Intelligence

```
gold_transactions_enriched (896K)
         │
         │ Filter: sender-receiver pairs with
         │ ≥2 interactions OR ≥1 fraud flag
         │
         ▼
┌──────────────────────────────────────────────┐
│  EDGE LIST (Spark SQL aggregation)            │
│                                               │
│  Per edge: txn_count, total_amount,           │
│  avg_amount, avg_risk_score,                  │
│  fraud_txn_count, date_range                  │
└──────────────────┬───────────────────────────┘
                   │
                   ▼
┌──────────────────────────────────────────────┐
│  DIRECTED GRAPH (NetworkX)                    │
│                                               │
│  Nodes = all unique sender_ids + receiver_ids │
│  Edges = transaction relationships (directed) │
└──────────────────┬───────────────────────────┘
                   │
     ┌─────────────┼─────────────┬──────────────┐
     ▼             ▼             ▼              ▼
┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐
│ Connected│ │ PageRank │ │ Degree   │ │ Triangle │
│ Compo-   │ │          │ │ Analysis │ │ Count    │
│ nents    │ │ α = 0.85 │ │          │ │          │
│          │ │ max 100  │ │ in/out   │ │ Density  │
│ Undirect-│ │ iters    │ │ per node │ │ per sub- │
│ ed view  │ │          │ │          │ │ graph    │
│          │ │ Centrali-│ │ Top 5%   │ │          │
│ Keep:    │ │ ty score │ │ by total │ │ Tight-   │
│ 3-500    │ │ per node │ │ degree   │ │ knit     │
│ nodes    │ │          │ │ = HUB    │ │ groups   │
│          │ │ High PR  │ │          │ │          │
│ = RING   │ │ = MULE   │ │          │ │          │
└────┬─────┘ └────┬─────┘ └────┬─────┘ └────┬─────┘
     │            │            │             │
     ▼            ▼            ▼             ▼
┌──────────────────────────────────────────────────┐
│  RING SCORING & CLASSIFICATION                    │
│                                                   │
│  Per ring:                                        │
│  ├── size (node count)                            │
│  ├── edge_count                                   │
│  ├── fraud_density = fraud_txns / total_txns      │
│  ├── subgraph_density = edges / possible_edges    │
│  ├── total_amount flowing through ring            │
│  ├── avg_risk_score                               │
│  └── severity: low / medium / high / critical     │
│                                                   │
│  Per sender (composite risk, 5 signals):          │
│  ├── 30% ML ensemble score                        │
│  ├── 25% personal fraud rate                      │
│  ├── 20% PageRank centrality                      │
│  ├── 15% late-night transaction %                 │
│  └── 10% degree centrality                        │
│                                                   │
│  RESULTS:                                         │
│  ├── 150 fraud rings                              │
│  ├── 57 with circular money flows                 │
│  ├── 93 money mule hub accounts                   │
│  └── Rs.8.68 Crore flowing through rings          │
│                                                   │
│  WHY THIS IS PREDICTIVE:                          │
│  A mule account has ZERO flagged transactions.    │
│  But PageRank sees it at the center of 15         │
│  connections to suspicious accounts.              │
│  Flag it BEFORE the fraud event.                  │
└──────────────────────────────────────────────────┘
         │
         ▼
  platinum_fraud_rings   platinum_sender_profiles
  [1] Delta Lake         [1] Delta Lake
```

---

## 6. RAG Pipeline — RBI Circular Search

```
80 RBI Circulars (rbi.org.in)
         │
         ▼
┌─────────────────────────────────────────────┐
│  CHUNKING  [5] Spark SQL                     │
│                                              │
│  SQL AGGREGATE() with named_struct:          │
│  ┌────────────────────────────────────────┐  │
│  │ AGGREGATE(                             │  │
│  │   SPLIT(full_text, '\n\n'),            │  │
│  │   named_struct('chunks', ARRAY(''),    │  │
│  │                 'cur', ''),             │  │
│  │   (acc, paragraph) ->                  │  │
│  │     IF LENGTH(acc.cur) + LENGTH(para)  │  │
│  │        > 4000                          │  │
│  │     THEN flush chunk, start new        │  │
│  │     ELSE append to current             │  │
│  │ )                                      │  │
│  └────────────────────────────────────────┘  │
│                                              │
│  LATERAL VIEW POSEXPLODE(chunks) →           │
│  One row per chunk with chunk_index          │
│                                              │
│  Output: gold_circular_chunks                │
│  Each chunk: ~4000 chars, paragraph-aware    │
└──────────────────┬──────────────────────────┘
                   │
                   ▼
┌─────────────────────────────────────────────┐
│  EMBEDDING                                   │
│                                              │
│  Model: intfloat/multilingual-e5-small       │
│  Dimensions: 384                             │
│  Languages: 22 Indian + international        │
│  (Hindi, Tamil, Telugu, Marathi, Bengali...) │
│                                              │
│  Prefix convention:                          │
│  Documents: "passage: {text}"                │
│  Queries:   "query: {question}"              │
│                                              │
│  batch_size=32, show_progress_bar=True       │
└──────────────────┬──────────────────────────┘
                   │
                   ▼
┌─────────────────────────────────────────────┐
│  [16] FAISS INDEX on [2] UC VOLUMES          │
│                                              │
│  Index type: IndexFlatIP (inner product)     │
│  = cosine similarity on normalized vectors   │
│                                              │
│  Stored at:                                  │
│  /Volumes/digital_artha/main/raw_data/       │
│  └── faiss_index/                            │
│      ├── rbi_circulars.faiss                 │
│      └── chunk_metadata.pkl                  │
│                                              │
│  Why UC Volumes: serverless-compatible.      │
│  DBFS deprecated. Volumes = production path. │
└──────────────────┬──────────────────────────┘
                   │
          User asks question
          (Hindi, English, etc.)
                   │
                   ▼
┌─────────────────────────────────────────────┐
│  RETRIEVAL  [7] VECTOR SEARCH via [17] MCP   │
│                                              │
│  Agent → MCP Protocol → VS endpoint         │
│  Query embedded with "query: " prefix        │
│  Top-5 chunks retrieved by similarity        │
│  Each chunk carries: circular_id, title,     │
│  chunk_index → used for citation             │
└──────────────────┬──────────────────────────┘
                   │
                   ▼
┌─────────────────────────────────────────────┐
│  GENERATION  [6] FOUNDATION MODEL API        │
│                                              │
│  Model: Llama 4 Maverick (Databricks-hosted) │
│  Temperature: 0.1 (strictly factual)         │
│  Max tokens: 500                             │
│                                              │
│  System prompt:                              │
│  "You are BlackIce, an expert on RBI         │
│   regulations. Cite sources with circular    │
│   title and date."                           │
│                                              │
│  Language instruction:                       │
│  If Hindi → "Respond in Hindi (Devanagari)" │
│  If Tamil → "Respond in Tamil"              │
│                                              │
│  Output: answer + [Source 1], [Source 2]...  │
└─────────────────────────────────────────────┘
```

---

## 7. Fraud Victim Recovery — Component Wiring

```
fraud_recovery_guide table (6 rows)
│
│  Columns: fraud_type, description, rbi_rule,
│  recovery_steps, report_to, time_limit_days,
│  max_liability_inr, common_in_states
│
│  Created by: 13-human-impact.py
│  Source: RBI circular compilation
│
└──▶ Agent tool: fraud_recovery_guide
     │
     │  [11] DATABRICKS SDK
     │  statement_execution.execute_statement()
     │  Parameterized: WHERE LOWER(fraud_type) LIKE :fraud_type
     │
     ▼
     Retrieved: steps, liability, deadline, contacts
     │
     │  [6] FOUNDATION MODEL API
     │  Llama 4 Maverick explains in user's language:
     │  "Report to your bank within 3 days.
     │   Under RBI rules, you have zero liability
     │   for QR code scams if reported in time.
     │   Then file at cybercrime.gov.in..."
     │
     ▼
     User sees: simple, actionable recovery plan
     in Hindi / English / Marathi / Tamil / Telugu

     ┌──────────────────────────────────────┐
     │  WHAT THIS MEANS FOR REAL PEOPLE:    │
     │                                      │
     │  Most Indians don't know:            │
     │  • Zero liability exists (3 days)    │
     │  • cybercrime.gov.in is a resource   │
     │  • RBI Ombudsman can escalate        │
     │  • Time limits are strict            │
     │                                      │
     │  BlackIce surfaces this buried       │
     │  legal knowledge in their language.  │
     └──────────────────────────────────────┘
```

---

## 8. Metric Views — How Genie Understands the Data

```
┌─────────────────────────────────────────────────────────────┐
│  [10] METRIC VIEW (YAML Semantic Layer)                      │
│                                                              │
│  CREATE VIEW financial_metrics WITH METRICS LANGUAGE YAML    │
│                                                              │
│  Source table: gold_transactions_enriched                    │
│                                                              │
│  DIMENSIONS (how to group):                                  │
│  ┌───────────────┬─────────────────────────────────────────┐│
│  │ Name          │ Expression          │ Synonyms          ││
│  ├───────────────┼─────────────────────┼───────────────────┤│
│  │ transaction_  │ DATE(transaction_   │                   ││
│  │ date          │ time)               │                   ││
│  │ category      │ COALESCE(category,  │ "merchant type",  ││
│  │               │ 'Unknown')          │ "txn type"        ││
│  │ risk_tier     │ final_risk_tier     │ "risk level",     ││
│  │               │                     │ "fraud risk"      ││
│  │ time_slot     │ time_slot           │                   ││
│  │ location      │ location            │                   ││
│  └───────────────┴─────────────────────┴───────────────────┘│
│                                                              │
│  MEASURES (what to calculate):                               │
│  ┌───────────────┬─────────────────────────┬───────────────┐│
│  │ Name          │ Expression              │ Format        ││
│  ├───────────────┼─────────────────────────┼───────────────┤│
│  │ total_txns    │ COUNT(1)                │ #,##0         ││
│  │ flagged_txns  │ SUM(CASE ensemble_flag) │ #,##0         ││
│  │ fraud_rate    │ flagged / total * 100   │ #0.00'%'      ││
│  │ total_amount  │ SUM(amount)             │ ₹#,##0.00     ││
│  │ avg_amount    │ AVG(amount)             │ ₹#,##0.00     ││
│  │ avg_risk      │ AVG(ensemble_score)     │ #0.000        ││
│  └───────────────┴─────────────────────────┴───────────────┘│
│                                                              │
│  WHY SYNONYMS MATTER:                                        │
│  User asks [8] Genie: "fraud percentage by merchant type"   │
│  Genie maps: "fraud percentage" → fraud_rate measure         │
│              "merchant type" → category dimension             │
│  Generates: SELECT category, fraud_rate FROM ... GROUP BY    │
│                                                              │
│  Connection: [10] feeds → [8] Genie Space                   │
│              [10] feeds → [9] Lakeview Dashboard             │
└─────────────────────────────────────────────────────────────┘
```

---

## 9. ML Pipeline

```
gold_transactions (750K rows)
        │
        ▼
┌──────────────────────────────────────┐
│  FEATURE ENGINEERING  [5] SPARK SQL   │
│                                       │
│  Window functions per sender:         │
│  ├── sender_avg_amount                │
│  ├── sender_std_amount (z-score base) │
│  ├── sender_max_amount                │
│  ├── sender_txn_count                 │
│                                       │
│  Derived:                             │
│  ├── amount_deviation (z-score)       │
│  ├── amount_ratio_to_max              │
│  ├── is_new_sender (count ≤ 2)        │
│  ├── late_night_high_value            │
│  ├── day_of_week_num (encoded)        │
│  └── is_weekend_num                   │
│                                       │
│  EXCLUDED: ai_risk_score              │
│  (prevents data leakage — it's        │
│  derived from is_fraud label)         │
└──────────┬───────────────────────────┘
           │
           ▼ Stratified Sampling (max 200K)
           │ ALL fraud rows + sampled normal
           │
     ┌─────┴─────┐
     ▼           ▼
┌─────────┐ ┌─────────┐
│Isolation│ │ KMeans  │
│Forest   │ │ (k=8)   │
│         │ │         │
│300 trees│ │Distance │
│contam   │ │to center│
│= 0.02   │ │95th %   │
│max 10K  │ │threshold│
│samples  │ │         │
│80% feat │ │Normalize│
│         │ │ 0-1     │
│Score    │ │Score    │
│ 0-1     │ │ 0-1     │
└────┬────┘ └────┬────┘
     │           │
     ▼           ▼
┌─────────────────────────────────┐
│  ENSEMBLE  (logged to MLflow)    │
│                                   │
│  0.45 × IF + 0.30 × KM + 0.25 × Rules │
│  = ensemble_score (0-1)          │
│                                   │
│  Cohen's d = 2.128               │
│  (>2 std dev separation)         │
│                                   │
│  < 0.3 = low                     │
│  0.3-0.5 = medium                │
│  0.5-0.7 = high                  │
│  > 0.7 = critical                │
└──────────┬──────────────────────┘
           │
           ▼
┌─────────────────────────────────┐
│  8 ANOMALY PATTERNS (CASE SQL)   │
│                                   │
│  Late Night High Value (51)      │
│  Late Night Activity (189)       │
│  Unusual Category Spend (180)    │
│  High Value Transaction          │
│  Weekend High Spend (51)         │
│  ML Critical Risk                │
│  ML High Risk (225)              │
│  General Anomaly (1.09K)         │
└─────────────────────────────────┘
```

---

## 10. Deployment Architecture

```
┌──────────────────────────────────────────────────────────┐
│  [15] DATABRICKS APPS                                     │
│                                                           │
│  databricks.yml (Asset Bundle):                          │
│  ├── bundle: digital-artha-agent                         │
│  ├── variables: catalog, schema, genie_space_id,         │
│  │              warehouse_id                              │
│  ├── resources:                                           │
│  │   ├── experiments: MLflow experiment                   │
│  │   └── apps:                                            │
│  │       ├── name: digital-artha                         │
│  │       ├── command: uv run start-app                   │
│  │       ├── env: CATALOG, SCHEMA, LLM_ENDPOINT,        │
│  │       │        GENIE_SPACE_ID, WAREHOUSE_ID           │
│  │       └── resources:                                   │
│  │           ├── experiment (MLflow, CAN_MANAGE)         │
│  │           ├── vector-search-index (SELECT)            │
│  │           ├── genie-space (CAN_RUN)                   │
│  │           └── sql-warehouse (CAN_USE)                 │
│  └── targets: dev (default)                              │
│                                                           │
│  start-app (ProcessManager):                             │
│  ├── Backend: uv run start-server → MLflow AgentServer   │
│  │            port 8000 (ASGI via uvicorn)               │
│  └── Frontend: npm run start → e2e-chatbot-app-next      │
│                port 3000 (Express + React)                │
│                                                           │
│  Local development:                                      │
│  ├── uv run start-server     (agent backend)             │
│  ├── python3 chat_ui.py      (Gradio UI, share=True)    │
│  └── Gradio public URL: https://xxxxx.gradio.live        │
└──────────────────────────────────────────────────────────┘

  Deploy:
  $ cd 08-agent
  $ databricks bundle deploy
  $ databricks bundle run digital-artha
```
