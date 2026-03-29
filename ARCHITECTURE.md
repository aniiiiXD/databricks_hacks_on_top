# BlackIce Architecture

## 1. Data Flow Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                       DATA SOURCES                               │
│                                                                  │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌───────────────┐   │
│  │ Kaggle   │  │ RBI.org  │  │ MyScheme │  │ RBI/NPCI/TRAI │   │
│  │ UPI Txns │  │ Circulars│  │ Gov.in   │  │ Macro Stats   │   │
│  │ 250K CSV │  │ 80 JSON  │  │ 170 JSON │  │ 5 JSON files  │   │
│  └────┬─────┘  └────┬─────┘  └────┬─────┘  └──────┬────────┘   │
│       │              │             │               │             │
└───────┼──────────────┼─────────────┼───────────────┼─────────────┘
        │              │             │               │
        ▼              ▼             ▼               ▼
┌─────────────────────────────────────────────────────────────────┐
│                    UNITY CATALOG VOLUME                           │
│              /Volumes/digital_artha/main/raw_data/               │
│                                                                  │
│  upi_transactions_2024.csv | rbi_circulars.json                 │
│  gov_schemes.json | rbi_digital_payments.json                    │
│  bank_fraud_stats.json | rbi_complaints.json                    │
│  pmjdy_statewise.json | internet_penetration.json               │
└──────────────────────────┬──────────────────────────────────────┘
                           │
                           ▼
```

## 2. Medallion Architecture (4 Tiers)

```
┌─────────────────────────────────────────────────────────────────┐
│  BRONZE LAYER — Raw Ingestion                                    │
│  ┌────────────────────────────────────────────────────────────┐  │
│  │ Auto Loader (CSV)        Direct Read (JSON)                │  │
│  │ ┌──────────────────┐   ┌─────────────────┐ ┌───────────┐ │  │
│  │ │bronze_transactions│   │bronze_circulars │ │bronze_    │ │  │
│  │ │    750,000 rows   │   │    80 rows      │ │schemes    │ │  │
│  │ │    27 columns     │   │    10 columns   │ │170 rows   │ │  │
│  │ │ PK: transaction_id│   │ PK: circular_id │ │PK: scheme │ │  │
│  │ │ CDF: enabled      │   │ CDF: enabled    │ │_id        │ │  │
│  │ └──────────────────┘   └─────────────────┘ └───────────┘ │  │
│  └────────────────────────────────────────────────────────────┘  │
│  Tags: domain, tier=bronze, source, pii=false                    │
└──────────────────────────┬──────────────────────────────────────┘
                           │ ETL Pipeline (DLT EXPECT constraints)
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│  SILVER LAYER — Cleaned, Validated, Typed                        │
│  ┌────────────────────────────────────────────────────────────┐  │
│  │ DLT EXPECT Constraints:                                    │  │
│  │   valid_txn_id (NOT NULL) — ON VIOLATION DROP ROW          │  │
│  │   valid_amount (> 0, < 10M) — ON VIOLATION DROP ROW        │  │
│  │   valid_timestamp (NOT NULL)                                │  │
│  │                                                             │  │
│  │ Derived Features:                                           │  │
│  │   is_weekend (day_of_week IN Saturday/Sunday)               │  │
│  │   time_slot (late_night/morning/afternoon/evening/night)    │  │
│  │   amount_bucket (micro/small/medium/large/very_large)       │  │
│  │   risk_label (rule-based: amount + hour heuristics)         │  │
│  │   topic_label (keyword-based circular classification)       │  │
│  │   word_count (for RAG chunk validation)                     │  │
│  │                                                             │  │
│  │ ┌──────────────────┐ ┌──────────────┐ ┌──────────────┐    │  │
│  │ │silver_transactions│ │silver_       │ │silver_       │    │  │
│  │ │   750,000 rows    │ │circulars     │ │schemes       │    │  │
│  │ │   28 columns      │ │  80 rows     │ │ 170 rows     │    │  │
│  │ └──────────────────┘ └──────────────┘ └──────────────┘    │  │
│  └────────────────────────────────────────────────────────────┘  │
│  Tags: tier=silver, quality=validated                             │
└──────────────────────────┬──────────────────────────────────────┘
                           │ Materialized Views + Chunking
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│  GOLD LAYER — Business-Ready + RAG-Ready         [COLD TIER]    │
│  ┌────────────────────────────────────────────────────────────┐  │
│  │                                                             │  │
│  │ gold_transactions      gold_fraud_alerts                    │  │
│  │   750,000 rows           1,440 rows                         │  │
│  │   risk labels            filtered: anomaly=true             │  │
│  │   Liquid Clustering      alert_summary text                 │  │
│  │   on transaction_date                                       │  │
│  │                                                             │  │
│  │ gold_circular_chunks    gold_schemes                        │  │
│  │   80→chunked at 4000ch    170 rows                          │  │
│  │   AGGREGATE() accumulator plain_summary                     │  │
│  │   citation strings        eligibility_json                  │  │
│  │   → FAISS vector index                                      │  │
│  │                                                             │  │
│  └────────────────────────────────────────────────────────────┘  │
│  Tags: tier=gold, quality=business_ready                         │
└──────────────────────────┬──────────────────────────────────────┘
                           │ ML Ensemble + Pattern Discovery
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│  PLATINUM LAYER — ML Scored + Analytics          [WARM TIER]     │
│  ┌────────────────────────────────────────────────────────────┐  │
│  │                                                             │  │
│  │ gold_transactions_enriched (896K rows, 50 columns)         │  │
│  │ ┌─────────────────────────────────────────────────────┐    │  │
│  │ │ IsolationForest score (0-1)                         │    │  │
│  │ │ KMeans distance score (0-1)                         │    │  │
│  │ │ Ensemble: 0.45×IF + 0.30×KM + 0.25×Rules           │    │  │
│  │ │ Risk tiers: low / medium / high / critical          │    │  │
│  │ │ Sender features: velocity, deviation, new_sender    │    │  │
│  │ └─────────────────────────────────────────────────────┘    │  │
│  │                                                             │  │
│  │ gold_fraud_alerts_ml     1,800 flagged transactions        │  │
│  │ platinum_anomaly_patterns  8 named fraud patterns          │  │
│  │ platinum_sender_profiles   5,000 composite risk scores     │  │
│  │ platinum_merchant_profiles 10 category risk analysis       │  │
│  │                                                             │  │
│  │ WARM VIEWS (pre-aggregated for dashboard):                 │  │
│  │   mv_fraud_summary | mv_high_risk_senders                  │  │
│  │   + 13 viz_* views for dashboard widgets                   │  │
│  │                                                             │  │
│  └────────────────────────────────────────────────────────────┘  │
│  Tags: tier=platinum, quality=ml_scored                          │
└──────────────────────────┬──────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│  CONTEXT LAYER — India Financial Landscape                       │
│  ┌────────────────────────────────────────────────────────────┐  │
│  │ india_digital_payments    432 rows  UPI/NEFT growth 2020-25│  │
│  │ india_bank_fraud_stats     75 rows  15 banks × 5 years     │  │
│  │ india_rbi_complaints       48 rows  Complaint trends       │  │
│  │ india_pmjdy_statewise      36 rows  Jan Dhan by state      │  │
│  │ india_internet_penetration 36 rows  Digital divide          │  │
│  │ state_vulnerability_index  10 rows  Composite risk index   │  │
│  │ fraud_recovery_guide        6 rows  RBI recovery steps     │  │
│  └────────────────────────────────────────────────────────────┘  │
│  Tags: tier=context, source=rbi/npci/trai/lok_sabha              │
└─────────────────────────────────────────────────────────────────┘
```

## 3. Hot / Warm / Cold Serving Architecture

```
                    QUERY LATENCY
                    ◄────────────────────────────────►
                    ms          seconds          minutes

    ┌──────────────────────────────────────────────────────┐
    │  HOT TIER — Real-Time Agent Tools                     │
    │                                                       │
    │  Agent SQL queries via Databricks SDK                 │
    │  ├── lookup_fraud_alerts (parameterized SQL)          │
    │  ├── check_loan_eligibility (PySpark → LLM)           │
    │  ├── fraud_recovery_guide (direct table query)        │
    │  └── MCP: Vector Search + Genie Space                 │
    │                                                       │
    │  Latency: 1-5 seconds (includes LLM generation)      │
    │  Access: Agent chat UI at localhost:7860               │
    └───────────────────────────┬──────────────────────────┘
                                │
    ┌───────────────────────────┴──────────────────────────┐
    │  WARM TIER — Pre-Aggregated Views                     │
    │                                                       │
    │  13 viz_* views (pre-computed for dashboard)          │
    │  mv_fraud_summary (aggregated by date/category/state)│
    │  mv_high_risk_senders (filtered composite > 0.3)     │
    │                                                       │
    │  Latency: < 1 second (pre-computed, cached)          │
    │  Access: Lakeview Dashboard (4 pages)                │
    └───────────────────────────┬──────────────────────────┘
                                │
    ┌───────────────────────────┴──────────────────────────┐
    │  COLD TIER — Full Historical Tables                   │
    │                                                       │
    │  gold_transactions (750K) + Liquid Clustering        │
    │  gold_transactions_enriched (896K) + ML scores       │
    │  Full sender/merchant profiles                        │
    │  All bronze/silver intermediate tables                │
    │                                                       │
    │  Latency: 5-30 seconds (full table scan)             │
    │  Access: SQL Editor, Notebooks, Genie Space          │
    └──────────────────────────────────────────────────────┘
```

## 4. ML Pipeline Architecture

```
gold_transactions (750K rows)
        │
        ▼
┌──────────────────────────────────────┐
│  FEATURE ENGINEERING (Spark SQL)      │
│                                       │
│  Per-sender aggregates:               │
│  ├── sender_avg_amount                │
│  ├── sender_std_amount (z-score base) │
│  ├── sender_max_amount                │
│  ├── sender_txn_count                 │
│  │                                    │
│  Derived features:                    │
│  ├── amount_deviation (z-score)       │
│  ├── amount_ratio_to_max              │
│  ├── is_new_sender (count ≤ 2)        │
│  ├── late_night_high_value            │
│  ├── day_of_week_num (encoded)        │
│  └── is_weekend_num                   │
└──────────┬───────────────────────────┘
           │
           ▼ Stratified Sampling
           │ (ALL fraud + sampled normal)
           │
     ┌─────┴─────┐
     ▼           ▼
┌─────────┐ ┌─────────┐
│Isolation│ │ KMeans  │
│Forest   │ │ (k=8)   │
│300 trees│ │         │
│contam   │ │ Distance│
│= 0.02   │ │ to      │
│         │ │ center  │
│score    │ │ score   │
│(0-1)    │ │ (0-1)   │
└────┬────┘ └────┬────┘
     │           │
     ▼           ▼
┌─────────────────────────────────┐
│  ENSEMBLE BLENDING               │
│                                   │
│  0.45 × IsolationForest score    │
│  0.30 × KMeans distance score    │
│  0.25 × Rule-based risk score    │
│  ─────────────────────────────── │
│  = ensemble_score (0-1)          │
│                                   │
│  Threshold → Risk Tiers:         │
│  < 0.3 = low                     │
│  0.3-0.5 = medium                │
│  0.5-0.7 = high                  │
│  > 0.7 = critical                │
└──────────┬──────────────────────┘
           │
           ▼
┌─────────────────────────────────┐
│  ANOMALY PATTERN CLASSIFICATION  │
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

## 5. Agent Architecture

```
┌──────────────────────────────────────────────────────┐
│  GRADIO UI (localhost:7860)                            │
│  ┌────────────┬─────────────┬────────────┬─────────┐ │
│  │ Fraud      │ Ask Agent   │ Scheme     │ About   │ │
│  │ Monitor    │ (Chat)      │ Finder     │         │ │
│  │            │             │ (Form)     │         │ │
│  │ Direct SQL │ → Agent API │ → Agent    │ Static  │ │
│  └─────┬──────┴──────┬──────┴─────┬──────┴─────────┘ │
└────────┼─────────────┼────────────┼──────────────────┘
         │             │            │
         │        ┌────┴────┐       │
         │        ▼         │       │
         │  ┌───────────────┴───────┴──────────────┐
         │  │  MLflow AgentServer (port 8000)        │
         │  │  LangGraph create_react_agent          │
         │  │  + MemorySaver (conversation memory)   │
         │  │                                        │
         │  │  7 Tools:                              │
         │  │  ┌──────────────────────────────────┐  │
         │  │  │ lookup_fraud_alerts              │  │
         │  │  │ (SQL → gold_fraud_alerts_ml)     │  │
         │  │  ├──────────────────────────────────┤  │
         │  │  │ check_loan_eligibility           │  │
         │  │  │ (SQL → gold_schemes → LLM)       │  │
         │  │  ├──────────────────────────────────┤  │
         │  │  │ fraud_recovery_guide             │  │
         │  │  │ (SQL → fraud_recovery_guide)     │  │
         │  │  ├──────────────────────────────────┤  │
         │  │  │ get_current_time                 │  │
         │  │  ├──────────────────────────────────┤  │
         │  │  │ MCP: Vector Search               │  │
         │  │  │ (RBI circular semantic search)   │  │
         │  │  ├──────────────────────────────────┤  │
         │  │  │ MCP: Genie query                 │  │
         │  │  │ (NL analytics on fraud data)     │  │
         │  │  ├──────────────────────────────────┤  │
         │  │  │ MCP: Genie poll                  │  │
         │  │  │ (get Genie results)              │  │
         │  │  └──────────────────────────────────┘  │
         │  └────────────────────────────────────────┘
         │
         ▼ (Direct SQL for Fraud Monitor tab)
┌────────────────────────────────────────┐
│  DATABRICKS SQL WAREHOUSE              │
│  Serverless Starter (2X-Small)         │
│                                        │
│  All tables in digital_artha.main      │
│  15+ tables | 13 views | 2M+ rows     │
└────────────────────────────────────────┘
```

## 6. Key Differentiators

```
WHAT EVERY TEAM DOES          WHAT WE DID DIFFERENTLY
─────────────────────          ─────────────────────────
1 ML model                     Ensemble (IF + KMeans + Rules)
                               with stratified sampling

Generic "fraud/not fraud"      8 named anomaly patterns
                               (Late Night High Value, etc.)

Raw data → model               4-tier medallion with DLT
                               EXPECT constraints + UC tags

Simple chatbot                 7-tool agent with MCP protocol
                               (Vector Search + Genie live)

English only                   Hindi + English (multilingual
                               embeddings + LLM)

Model metrics only             State Vulnerability Index
                               (fraud × internet × Jan Dhan)

No human angle                 Fraud recovery guide with
                               RBI-mandated steps + liability

3-5 Databricks features        17 Databricks features

Dashboard with charts          4-page dashboard + 4-tab
                               Gradio app + Genie Space

No data governance             UC tags on 15 tables,
                               liquid clustering, warm views
```
