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
│  │   sender_not_receiver (sender_id != receiver_id)            │  │
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
│  │   → FAISS vector index    (TO_JSON + NAMED_STRUCT)          │  │
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
│  │ gold_fraud_alerts_ml      1,800 flagged transactions       │  │
│  │ platinum_anomaly_patterns   8 named fraud patterns         │  │
│  │ platinum_sender_profiles    5,000 composite risk scores    │  │
│  │ platinum_merchant_profiles  10 category risk analysis      │  │
│  │ platinum_fraud_rings        150 rings (graph analysis)     │  │
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

## 3. Real-Time Streaming Pipeline

```
┌──────────────┐     ┌──────────────┐     ┌──────────────┐     ┌──────────────┐
│  NEW FILE    │     │  STREAMING   │     │  STREAMING   │     │  STREAMING   │
│  DROPPED     │ ──▶ │  BRONZE      │ ──▶ │  SILVER      │ ──▶ │  PLATINUM    │
│              │     │              │     │              │     │              │
│  Auto Loader │     │  10 columns  │     │  Quality     │     │  4-signal    │
│  detects it  │     │  cast + typed│     │  filter:     │     │  ensemble    │
│  via         │     │  ingested_at │     │  NOT NULL    │     │  score       │
│  checkpoint  │     │  timestamp   │     │  amount > 0  │     │  8 patterns  │
│              │     │              │     │  risk_tier   │     │  scored_at   │
└──────────────┘     └──────────────┘     └──────────────┘     └──────────────┘

Properties:
  ├── Exactly-once semantics (checkpoint-based)
  ├── Incremental — only new files processed
  ├── 5 micro-batches demonstrated (2 initial + 3 live)
  ├── Judges can inject custom transactions
  └── Production: switch availableNow → processingTime for continuous
```

## 4. Hot / Warm / Cold Serving Architecture

```
                    QUERY LATENCY
                    ◄────────────────────────────────►
                    ms          seconds          minutes

    ┌──────────────────────────────────────────────────────┐
    │  HOT TIER — Real-Time Agent Tools                     │
    │                                                       │
    │  9 tools via Databricks SDK (parameterized SQL)       │
    │  ├── lookup_fraud_alerts                              │
    │  ├── check_loan_eligibility (SQL + LLM)               │
    │  ├── fraud_recovery_guide                             │
    │  ├── lookup_fraud_rings                               │
    │  ├── lookup_sender_profile                            │
    │  ├── get_current_time                                 │
    │  └── MCP: Vector Search + Genie Space (2 tools)       │
    │                                                       │
    │  Latency: 1-5 seconds (includes LLM generation)      │
    │  Security: StatementParameterListItem (no injection)  │
    └───────────────────────────┬──────────────────────────┘
                                │
    ┌───────────────────────────┴──────────────────────────┐
    │  WARM TIER — Pre-Aggregated Views                     │
    │                                                       │
    │  13 viz_* views (pre-computed for dashboard):         │
    │  viz_fraud_heatmap | viz_category_time_matrix         │
    │  viz_risk_distribution | viz_amount_comparison        │
    │  viz_monthly_trend | viz_upi_growth | viz_bank_fraud  │
    │  viz_state_vulnerability | viz_anomaly_patterns       │
    │  viz_recovery_guide | viz_risky_senders | viz_kpis    │
    │  viz_complaint_surge                                  │
    │                                                       │
    │  mv_fraud_summary (aggregated by date/category/state)│
    │  mv_high_risk_senders (filtered composite > 0.3)     │
    │                                                       │
    │  Latency: < 1 second (pre-computed at write time)    │
    │  Access: Lakeview Dashboard (4 pages, 20+ widgets)   │
    └───────────────────────────┬──────────────────────────┘
                                │
    ┌───────────────────────────┴──────────────────────────┐
    │  COLD TIER — Full Historical Tables                   │
    │                                                       │
    │  gold_transactions (750K) + Liquid Clustering        │
    │    CLUSTER BY (transaction_date, final_risk_tier)    │
    │  gold_transactions_enriched (896K) + ML scores       │
    │  Full sender/merchant profiles                        │
    │  All bronze/silver intermediate tables                │
    │                                                       │
    │  Latency: 5-30 seconds (optimized range scans)       │
    │  Access: SQL Editor, Notebooks, Genie Space          │
    └──────────────────────────────────────────────────────┘
```

## 5. ML Pipeline Architecture

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
│                                       │
│  NOTE: ai_risk_score excluded to      │
│  prevent data leakage                 │
└──────────┬───────────────────────────┘
           │
           ▼ Stratified Sampling
           │ (ALL fraud + sampled normal, max 200K)
           │
     ┌─────┴─────┐
     ▼           ▼
┌─────────┐ ┌─────────┐
│Isolation│ │ KMeans  │
│Forest   │ │ (k=8)   │
│300 trees│ │         │
│contam   │ │ Distance│
│= 0.02   │ │ to      │
│max 10K  │ │ center  │
│samples  │ │ 95th %  │
│         │ │ thresh  │
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
│  Separation: Cohen's d = 2.128   │
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

## 6. Fraud Ring Detection (Graph Intelligence)

```
gold_transactions_enriched
        │
        ▼ Filter: ≥2 interactions OR ≥1 fraud flag
┌──────────────────────────────────────┐
│  TRANSACTION GRAPH (NetworkX)         │
│                                       │
│  Nodes: all unique senders/receivers  │
│  Edges: sender → receiver (directed)  │
│  Edge attrs: txn_count, total_amount, │
│              avg_risk_score,           │
│              fraud_txn_count           │
└──────────┬───────────────────────────┘
           │
     ┌─────┼─────────┬──────────────┐
     ▼     ▼         ▼              ▼
┌────────┐┌────────┐┌─────────┐┌──────────┐
│Connect-││Page-   ││Degree   ││Triangle  │
│ed Comp-││Rank    ││Analysis ││Count     │
│onents  ││α=0.85  ││         ││          │
│        ││        ││in/out   ││Tight-knit│
│Rings:  ││Hub     ││degree   ││groups    │
│3-500   ││nodes = ││Top 5%   ││with high │
│nodes   ││money   ││= hubs   ││density   │
│        ││mules   ││         ││          │
└───┬────┘└───┬────┘└────┬────┘└─────┬────┘
    │         │          │           │
    ▼         ▼          ▼           ▼
┌─────────────────────────────────────────┐
│  RING SCORING                            │
│                                          │
│  150 fraud rings detected                │
│   57 with circular money flows           │
│   93 money mule hub accounts             │
│  ₹8.68 Crore flowing through rings      │
│                                          │
│  Severity: fraud_density + avg_risk +    │
│            subgraph_density              │
│  → low / medium / high / critical        │
│                                          │
│  Composite Risk (per sender):            │
│  30% ML_score + 25% fraud_rate +         │
│  20% PageRank + 15% late_night +         │
│  10% degree_centrality                   │
└─────────────────────────────────────────┘
```

## 7. Agent Architecture

```
┌──────────────────────────────────────────────────────────────┐
│  GRADIO UI (localhost:7860)                                    │
│  ┌──────────┬────────────┬────────────┬───────────┬────────┐ │
│  │ Home     │ Command    │ Ask        │ India     │ Scheme │ │
│  │          │ Center     │ BlackIce   │ Story     │ Finder │ │
│  │ KPIs     │ Direct SQL │ Agent Chat │ UPI/Fraud │ Form   │ │
│  │ auto-    │ Alerts +   │ 9 tools    │ Stats +   │ → LLM  │ │
│  │ load     │ Patterns + │ Memory     │ Vulnera-  │ match  │ │
│  │          │ Senders    │ Multilang  │ bility    │ 5 lang │ │
│  └─────┬────┴─────┬──────┴─────┬──────┴─────┬─────┴───┬────┘ │
└────────┼──────────┼────────────┼────────────┼─────────┼──────┘
         │          │            │            │         │
         │     ┌────┴────────────┴────────────┴─────────┘
         │     ▼
         │  ┌───────────────────────────────────────────────┐
         │  │  MLflow AgentServer (port 8000)                │
         │  │  LangGraph create_react_agent + MemorySaver    │
         │  │                                                │
         │  │  9 Tools:                                      │
         │  │  ┌─────────────────────────────────────────┐   │
         │  │  │ CUSTOM (6):                             │   │
         │  │  │  lookup_fraud_alerts     (param SQL)    │   │
         │  │  │  check_loan_eligibility  (SQL + LLM)    │   │
         │  │  │  fraud_recovery_guide    (param SQL)    │   │
         │  │  │  lookup_fraud_rings      (param SQL)    │   │
         │  │  │  lookup_sender_profile   (param SQL)    │   │
         │  │  │  get_current_time        (utility)      │   │
         │  │  ├─────────────────────────────────────────┤   │
         │  │  │ MCP (3):                                │   │
         │  │  │  Vector Search  (RBI circular RAG)      │   │
         │  │  │  Genie query    (NL → SQL analytics)    │   │
         │  │  │  Genie poll     (async result fetch)    │   │
         │  │  └─────────────────────────────────────────┘   │
         │  │                                                │
         │  │  Security: StatementParameterListItem binding  │
         │  │  on all 6 custom tools (zero SQL injection)    │
         │  └────────────────────────────────────────────────┘
         │
         ▼ (Direct SQL for Command Center tab)
┌────────────────────────────────────────┐
│  DATABRICKS SQL WAREHOUSE              │
│  Serverless Starter (2X-Small)         │
│                                        │
│  All tables in digital_artha.main      │
│  15+ tables | 13 views | 3.15M+ rows  │
└────────────────────────────────────────┘
```

## 8. RAG Pipeline (RBI Circular Search)

```
80 RBI Circulars (rbi.org.in)
        │
        ▼
┌──────────────────────────────────────┐
│  CHUNKING (SQL AGGREGATE pattern)     │
│                                       │
│  Split at paragraph boundaries        │
│  Target: ~4000 chars per chunk        │
│  AGGREGATE + named_struct accumulator │
│  LATERAL VIEW POSEXPLODE → rows       │
│  Output: gold_circular_chunks         │
└──────────┬───────────────────────────┘
           │
           ▼
┌──────────────────────────────────────┐
│  EMBEDDING                            │
│                                       │
│  Model: intfloat/multilingual-e5-small│
│  Dimensions: 384                      │
│  Languages: 22 (Hindi, Tamil, etc.)   │
│  Prefix: "passage: " / "query: "      │
└──────────┬───────────────────────────┘
           │
           ▼
┌──────────────────────────────────────┐
│  FAISS INDEX                          │
│                                       │
│  IndexFlatIP (cosine similarity)      │
│  Stored in UC Volumes (serverless OK) │
│  + chunk_metadata.pkl                 │
└──────────┬───────────────────────────┘
           │
           ▼ Query: top-5 chunks retrieved
┌──────────────────────────────────────┐
│  GENERATION                           │
│                                       │
│  Model: Llama 4 Maverick             │
│  via Foundation Model API             │
│  Temperature: 0.1 (factual)          │
│  System: "You are BlackIce, an       │
│           expert on RBI regulations"  │
│  Output: answer + source citations   │
└──────────────────────────────────────┘
```

## 9. Databricks Features Map (17 Total)

```
┌─────────────────────────────────────────────────────────────────┐
│  DATA LAYER                                                      │
│  ┌────────────────┐ ┌────────────────┐ ┌──────────────────────┐ │
│  │ 1. Delta Lake  │ │ 2. Unity       │ │ 3. Auto Loader       │ │
│  │ All 15+ tables │ │    Catalog     │ │ Streaming ingestion  │ │
│  │ ACID, schema   │ │ PK, comments,  │ │ Schema inference,    │ │
│  │ enforcement    │ │ lineage, tags  │ │ exactly-once         │ │
│  └────────────────┘ └────────────────┘ └──────────────────────┘ │
│  ┌────────────────┐ ┌────────────────┐                          │
│  │ 12. Change     │ │ 13. Liquid     │                          │
│  │  Data Feed     │ │  Clustering    │                          │
│  │ Incremental    │ │ CLUSTER BY     │                          │
│  │ processing     │ │ (date, tier)   │                          │
│  └────────────────┘ └────────────────┘                          │
├─────────────────────────────────────────────────────────────────┤
│  ETL + QUALITY LAYER                                             │
│  ┌────────────────┐ ┌────────────────┐ ┌──────────────────────┐ │
│  │ 4. DLT Pipeline│ │ 5. Spark SQL   │ │ 14. UC Tags          │ │
│  │ EXPECT         │ │ Window funcs,  │ │ domain, tier, pii,   │ │
│  │ constraints    │ │ AGGREGATE(),   │ │ source on 15 tables  │ │
│  │ DROP ROW       │ │ LATERAL VIEW   │ │                      │ │
│  └────────────────┘ └────────────────┘ └──────────────────────┘ │
├─────────────────────────────────────────────────────────────────┤
│  AI LAYER                                                        │
│  ┌────────────────┐ ┌────────────────┐ ┌──────────────────────┐ │
│  │ 6. Foundation  │ │ 7. Vector      │ │ 8. Genie Space       │ │
│  │  Model API     │ │  Search (MCP)  │ │    (MCP)             │ │
│  │ Llama 4        │ │ Semantic RAG   │ │ NL → SQL analytics   │ │
│  │ Maverick       │ │ over circulars │ │ Entity matching      │ │
│  └────────────────┘ └────────────────┘ └──────────────────────┘ │
│  ┌────────────────┐ ┌────────────────┐                          │
│  │ 16. FAISS on   │ │ 17. MCP        │                          │
│  │  Volumes       │ │  Protocol      │                          │
│  │ Vector index   │ │ Agent ↔ VS +   │                          │
│  │ in UC Volumes  │ │ Genie live     │                          │
│  └────────────────┘ └────────────────┘                          │
├─────────────────────────────────────────────────────────────────┤
│  SERVING LAYER                                                   │
│  ┌────────────────┐ ┌────────────────┐ ┌──────────────────────┐ │
│  │ 9. Lakeview    │ │ 10. Metric     │ │ 11. Databricks SDK   │ │
│  │  Dashboard     │ │  Views (YAML)  │ │ WorkspaceClient,     │ │
│  │ 4 pages, 20+   │ │ Dimensions +   │ │ statement execution, │ │
│  │ widgets        │ │ measures +     │ │ API calls            │ │
│  │                │ │ synonyms       │ │                      │ │
│  └────────────────┘ └────────────────┘ └──────────────────────┘ │
│  ┌────────────────┐                                              │
│  │ 15. Databricks │                                              │
│  │  Apps          │                                              │
│  │ databricks.yml │                                              │
│  │ + app.yaml     │                                              │
│  └────────────────┘                                              │
└─────────────────────────────────────────────────────────────────┘
```
