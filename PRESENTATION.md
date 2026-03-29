# BlackIce — Presentation Deck (10 Slides)

> This document is the presentation itself. Each slide has a title, content, and speaker notes. Hand this to anyone and they can present BlackIce.

---

## SLIDE 1: TITLE

### BlackIce
**AI-Powered Financial Intelligence for India**

Fraud detection | RBI regulation search | Government scheme matching | Fraud victim recovery

Built entirely on Databricks Free Edition.

Bharat Bricks Hackathon 2026 | IIT Bombay

**Speaker Notes:**
"India processes 16 billion UPI transactions every month. It's the largest real-time payment network on Earth. But fraud is rising fast --- SBI alone lost Rs.427 crore last year. RBI rules that protect citizens are buried in English legalese. And 170 government financial schemes exist that most rural Indians have no idea they qualify for. We built BlackIce to solve all of this --- on Databricks Free Edition."

---

## SLIDE 2: THE PRODUCTION PIPELINE

### From Raw Data to Intelligence --- 4-Tier Medallion Architecture

```
DATA SOURCES (9 real Indian datasets)
  |
  v
BRONZE --- Auto Loader with schema inference + exactly-once semantics
  |         PK constraints | Change Data Feed enabled | Column documentation
  |         750K UPI transactions + 80 RBI circulars + 170 government schemes
  |         + 5 macro datasets (bank fraud, PMJDY, internet penetration, RBI complaints, UPI growth)
  |
  v
SILVER --- Lakeflow Declarative Pipeline (DLT)
  |         EXPECT constraints: valid_transaction_id, valid_amount (>0, <10M), valid_timestamp
  |         ON VIOLATION DROP ROW --- bad data never reaches analytics
  |         Feature derivation: time_slot, is_weekend, amount_bucket, risk_label
  |         Business rules: sender != receiver constraint enforced in schema
  |
  v
GOLD --- Business-Ready Tables
  |       RBI circulars chunked at 4000-char paragraph boundaries
  |         using SQL AGGREGATE() with named_struct accumulator + LATERAL VIEW POSEXPLODE
  |       Liquid clustering on transaction_date for query optimization
  |       Scheme eligibility converted to structured JSON (TO_JSON + NAMED_STRUCT)
  |
  v
PLATINUM --- ML Intelligence Layer
            IsolationForest + KMeans ensemble scores every transaction
            8 named anomaly patterns discovered
            5,000 sender risk profiles with composite scores
            150 fraud rings detected via graph analysis
```

**Databricks features on this slide:**
Auto Loader | Delta Lake | Unity Catalog | DLT EXPECT constraints | Change Data Feed | Liquid Clustering | Spark SQL

**Speaker Notes:**
"This is not a notebook that reads a CSV and trains a model. This is a production ETL pipeline. Let me walk you through it.

Bronze layer: Auto Loader handles streaming ingestion of 250K UPI transactions with exactly-once semantics. We also ingest 80 real RBI circulars scraped from rbi.org.in, 170 government schemes from myscheme.gov.in, and 5 macroeconomic datasets from RBI, NPCI, Lok Sabha, PMJDY, and TRAI. Every table gets Primary Key constraints, Change Data Feed, and column-level documentation in Unity Catalog.

Silver layer: This is where data quality lives. Our Lakeflow Declarative Pipeline enforces DLT EXPECT constraints --- null transaction IDs are dropped. Negative amounts are dropped. Missing timestamps are dropped. We even enforce that sender and receiver cannot be the same account. The data that survives is validated, typed, and enriched with time slots, weekend flags, and amount buckets.

Gold layer: Business-ready. The RBI circulars get chunked using a SQL AGGREGATE accumulator --- this is a sophisticated functional programming pattern in SQL where we iterate through paragraphs and split at 4000-character boundaries. Not naive fixed-size splitting. The gold transactions table gets liquid clustering on transaction_date so range queries are 10-100x faster.

Platinum layer: This is where raw data becomes intelligence. Our ML ensemble scores every transaction. We discover 8 distinct fraud patterns. We compute risk profiles for 5,000 senders. And we detect 150 fraud rings through graph analysis. Every layer has a purpose. Every table is tagged and documented."

---

## SLIDE 3: ML ENSEMBLE --- UNSUPERVISED FRAUD DETECTION

### Three Models, One Score

| Model | Weight | What It Does |
|-------|--------|-------------|
| **IsolationForest** | 0.45 | 300 trees, 2% contamination. Learns what "normal" looks like, flags outliers. Score: 0-1. |
| **KMeans (k=8)** | 0.30 | Clusters transactions into 8 behavioral groups. Distance to nearest center = anomaly score. 95th percentile threshold. |
| **Rule-Based Risk** | 0.25 | Amount + time heuristics. Late-night (0-5am) + high value (>Rs.5000) = high risk. Weekend patterns. |

**Ensemble:** `score = 0.45 x IF + 0.30 x KM + 0.25 x Rules`

**Risk Tiers:** low (<0.3) | medium (0.3-0.5) | high (0.5-0.7) | critical (>0.7)

### Why Unsupervised?
The Kaggle UPI dataset has fraud labels randomly assigned at 0.19%. Training supervised models on random labels would be meaningless. Our ensemble learns behavioral norms and flags deviations --- exactly how real bank fraud detection works.

### Quality Metric
**Separation Score (Cohen's d) = 2.128** --- flagged transactions differ from normal by over 2 standard deviations. The model genuinely finds behavioral outliers.

### 8 Anomaly Patterns Discovered

| Pattern | Count | What It Means |
|---------|-------|--------------|
| Late Night High Value | 51 | Transactions 0-5am exceeding Rs.10,000 |
| Late Night Activity | 189 | Any transaction 0-5am |
| Unusual Category Spend | 180 | >Rs.20K in Education/Healthcare or >Rs.15K in Grocery |
| High Value Transaction | varies | Single transaction exceeding Rs.25,000 |
| Weekend High Spend | 51 | Weekend transactions exceeding Rs.10,000 |
| ML Critical Risk | varies | Ensemble score > 0.8 |
| ML High Risk | 225 | Ensemble score 0.6-0.8 |
| General Anomaly | 1,090 | Multiple minor signals combined |

**Key design choice:** We explicitly excluded the rule-based risk_label from ensemble training features to prevent data leakage. The ensemble uses only behavioral features: amount, time, sender velocity, deviation from sender's mean.

**Databricks features on this slide:**
Spark SQL (feature engineering with window functions) | Delta Lake | MLflow (5 experiment runs logged with params, metrics, model artifacts)

**Speaker Notes:**
"Why unsupervised? Because the fraud labels in the Kaggle data are randomly assigned --- 0.19% marked as fraud with no behavioral basis. Training a supervised classifier on random labels gives you a random classifier. So we did what real banks do: learn what 'normal' looks like and flag deviations.

Our ensemble combines three independent signals. IsolationForest at 45% weight learns the density structure of normal transactions --- outliers that don't fit any dense region get flagged. KMeans at 30% clusters transactions into 8 behavioral groups --- if your transaction is far from every cluster center, that's suspicious. Rule-based risk at 25% catches the obvious patterns: late night, high value, weekend.

The separation score is Cohen's d = 2.128. That's not a model accuracy number --- it's a statistical effect size. It means our flagged transactions differ from normal by over 2 standard deviations in behavioral features. For unsupervised detection without any labeled data, that's excellent. And we logged all 5 experiment runs in MLflow with full parameters and metrics."

---

## SLIDE 4: REAL-TIME STREAMING --- LIVE FRAUD SCORING

### Not a Batch Job. A Live System.

```
New Transaction File Dropped
         |
         v
  AUTO LOADER (cloudFiles)
  Schema inference | Checkpoint-based | Exactly-once
         |
         v
  STREAMING BRONZE
  10 columns cast and typed | ingested_at timestamp
         |
         v
  STREAMING SILVER
  Quality filters: NOT NULL + amount > 0
  Risk tier classification | processing_time stamp
         |
         v
  STREAMING PLATINUM
  4-signal ensemble score:
    25% ai_risk_score + 35% amount_threshold + 25% time_of_day + 15% risk_tier
  8 anomaly patterns matched | scored_at timestamp
         |
         v
  LIVE FRAUD ALERTS
```

### How It Works
1. Split 2,000 transactions into 5 micro-batches
2. Drop batches 0-1 into the incoming Volume path
3. Auto Loader ingests only new files (checkpoint tracks position)
4. Each batch flows Bronze -> Silver -> Platinum automatically
5. Drop batches 2, 3, 4 incrementally --- watch counts grow in real time
6. **Judges can inject a custom transaction** and watch it get scored

### Key Properties
- **Exactly-once semantics** --- checkpoints prevent duplicate processing
- **Incremental** --- each run processes only what's new since last checkpoint
- **End-to-end** --- ingestion + validation + ML scoring in one pipeline
- **Production-ready** --- switch `availableNow=True` to `processingTime='30s'` for continuous streaming

**Databricks features on this slide:**
Auto Loader (cloudFiles) | Spark Structured Streaming | Delta Lake sink | UC Volumes | Checkpoint-based processing

**Speaker Notes:**
"This is where BlackIce becomes a live system. We built a streaming pipeline that takes a new transaction from file drop to fraud alert in one pass.

Auto Loader watches the incoming Volume path. When a new file appears, the checkpoint tells it exactly which files have already been processed --- so it only picks up new data. Exactly-once. No duplicates. No missed records.

The data flows through three layers. Bronze: raw ingestion with type casting. Silver: quality validation --- null IDs and bad amounts get filtered. Platinum: the ensemble scores the transaction and assigns one of 8 anomaly patterns.

We demonstrate this live by splitting 2,000 transactions into 5 batches. We drop two initially, run the pipeline, then drop one more and run again --- the counts grow. Judges can edit a cell, inject a custom transaction with any amount and category, and watch it flow through all three tiers and come out the other end with a fraud score.

In production, you switch one parameter --- trigger from availableNow to processingTime --- and this becomes a continuously running streaming job."

---

## SLIDE 5: PREDICTIVE FRAUD RINGS --- GRAPH INTELLIGENCE

### Catching Fraud Before It Happens

**The insight:** Individual transactions can look completely clean. But the network structure gives them away.

### How We Built It
1. **Build transaction graph** (NetworkX) --- sender -> receiver edges, filtered to pairs with 2+ interactions or 1+ fraud flag
2. **Connected components** --- find all subgraphs with 3-500 nodes (ring-sized clusters)
3. **PageRank** (alpha=0.85) --- identify hub accounts: nodes that are central intermediaries
4. **Degree analysis** --- high out-degree = potential fraud source, high in-degree = potential mule collecting funds
5. **Triangle counting** --- tightly-knit groups with abnormal connection density

### Results

| Metric | Value |
|--------|-------|
| Fraud rings detected | 150 |
| Circular money flows | 57 |
| Money mule hub accounts | 93 (via PageRank, top 5% by degree) |
| Total Rs. flowing through rings | Rs.8.68 Crore |
| Ring sizes | 3-500 accounts |

### Why This Is Predictive
Traditional fraud detection flags transactions *after* they happen. Graph analysis flags *accounts* --- accounts that are being set up as intermediaries, accounts receiving from multiple flagged senders, accounts forming circular flows.

A money mule account might have zero individual flagged transactions. But PageRank sees it sitting at the center of 15 connections to known fraud accounts. That account can be monitored or frozen *before* the fraud event occurs.

### Composite Risk Score (5 signals)
`30% ML_score + 25% fraud_rate + 20% PageRank + 15% late_night_pct + 10% degree_centrality`

**Databricks features on this slide:**
Spark SQL (edge/node extraction) | Delta Lake | NetworkX (graph algorithms)

**Speaker Notes:**
"This is what we're most proud of. Fraud ring detection doesn't just find fraud --- it predicts it.

We build a directed transaction graph. Sender to receiver. Then we run connected component analysis to find clusters of 3 to 500 accounts that are tightly connected. 150 of these clusters are fraud rings.

But the real power is PageRank. The same algorithm Google uses to rank web pages --- we use it to rank account importance in fraud networks. A money mule account might look completely clean at the transaction level. But PageRank sees it sitting at the center of a web of suspicious connections. It's receiving from 15 different flagged senders. It's sending to accounts that are sending to accounts that are flagged.

We identified 93 money mule hub accounts this way. Rs.8.68 crore flowing through these ring structures. And the key point: these accounts can be flagged and monitored *before* the fraud event. The graph structure reveals what individual transaction monitoring cannot."

---

## SLIDE 6: HELPING REAL PEOPLE --- FRAUD RECOVERY + FINANCIAL INCLUSION

### When Someone Gets Scammed

BlackIce doesn't just detect fraud --- it tells victims exactly what to do.

**6 fraud types covered with RBI-mandated recovery steps:**

| Fraud Type | Time Limit | Max Liability | What to Do |
|-----------|-----------|--------------|------------|
| QR Code Scam | 3 days | Rs.0 (zero) | Bank helpline -> cybercrime.gov.in -> RBI Ombudsman if unresolved in 30 days |
| Phishing | 3 days | Rs.25,000 | Same + change all passwords immediately |
| SIM Swap | 3 days | Rs.0 | Contact telecom provider + bank + cybercrime.gov.in |
| Fake UPI Request | 3 days | Rs.0 | Never approve unknown requests. Report to bank + UPI app |
| Remote Access Fraud | 3 days | Rs.25,000 | Uninstall remote apps + factory reset + bank complaint |
| Fake Merchant | 7 days | Rs.0 | Report to bank + consumer forum + cybercrime.gov.in |

**The LLM makes it human:** When a user says "I was scammed via QR code", the agent calls the `fraud_recovery_guide` tool, retrieves the RBI-mandated steps, and explains them in simple language --- in Hindi, Marathi, Tamil, or English. A first-generation bank user can understand what to do.

### Matching Citizens to 170 Government Schemes

A street vendor in Maharashtra, age 25, earning Rs.1.5 lakh, doesn't know they qualify for:
- **PM Mudra Yojana** --- Rs.50,000 collateral-free loans for micro-enterprises
- **PM-JAY** --- Rs.5 lakh health insurance
- **State-specific schemes** for Maharashtra

**How it works:**
1. User provides: age, income, occupation, state, gender
2. Parameterized SQL filters `gold_schemes` (170 real schemes from myscheme.gov.in)
3. Foundation Model API (Llama 4 Maverick) generates explanation in the user's language
4. Temperature 0.2 for factual accuracy | Max 800 tokens | 5 languages supported

**Databricks features on this slide:**
Foundation Model API (Llama 4 Maverick) | Databricks SDK (statement execution) | Delta Lake

**Speaker Notes:**
"BlackIce isn't just about catching fraud. It's about helping real people.

When someone gets scammed, they're scared and confused. They don't know their rights. The RBI actually mandates zero liability for most scam types if reported within 3 days --- but that information is buried in English legalese that a rural Indian farmer cannot read.

Our agent has a fraud_recovery_guide tool. When someone says 'I was scammed via QR code', it retrieves the exact RBI-mandated recovery steps: call your bank helpline first, then file at cybercrime.gov.in, then escalate to RBI Ombudsman if unresolved in 30 days. Zero liability if reported in 3 days. And it explains all of this in Hindi, in simple terms.

For financial inclusion: 170 real government schemes are indexed from myscheme.gov.in. A street vendor in Maharashtra enters their profile and instantly sees which schemes they qualify for --- Mudra loans, PM-JAY health insurance, state-specific programs. The LLM explains each one in their language. In 5 minutes, they go from 'I don't know what I qualify for' to 'I have 3 options I can apply for today.'"

---

## SLIDE 7: HOT / WARM / COLD SERVING ARCHITECTURE

### Three Tiers, All Working

| Tier | What | Latency | How |
|------|------|---------|-----|
| **HOT** | Agent tools | 1-5 sec | 9 tools with parameterized SQL via Databricks SDK. Fraud lookup, scheme matching, RBI search, fraud rings, sender profiles --- all live queries. |
| **WARM** | Dashboard | < 1 sec | 13 pre-computed `viz_*` views + 2 materialized views (`mv_fraud_summary`, `mv_high_risk_senders`). Aggregation done at write time, not read time. |
| **COLD** | Historical | 5-30 sec | Full Delta tables with liquid clustering on `transaction_date + final_risk_tier`. 750K transactions queryable via Genie Space or SQL editor. |

### Why This Matters
This is how production systems work. Three different optimization strategies for three different access patterns:

- **Hot:** Purpose-built agent tools with parameterized SQL. The agent decides which tool to call, constructs the query, and returns results in seconds. No full table scans.
- **Warm:** The dashboard never waits. 13 views pre-aggregate fraud by hour/day, category/time, risk buckets, monthly trends, anomaly patterns, sender rankings, KPIs. The Lakeview dashboard reads from these views, not from 750K-row tables.
- **Cold:** For ad-hoc analysis, liquid clustering physically reorganizes Delta files so that `WHERE transaction_date = '2025-03' AND risk_tier = 'high'` skips irrelevant data blocks entirely.

**The 13 dashboard views:**
`viz_fraud_heatmap` | `viz_category_time_matrix` | `viz_risk_distribution` | `viz_amount_comparison` | `viz_monthly_trend` | `viz_upi_growth` | `viz_bank_fraud` | `viz_state_vulnerability` | `viz_anomaly_patterns` | `viz_recovery_guide` | `viz_risky_senders` | `viz_kpis` | `viz_complaint_surge`

**Databricks features on this slide:**
Lakeview Dashboard (4 pages, 20+ widgets) | Liquid Clustering | Databricks SDK | SQL Views

**Speaker Notes:**
"We didn't just train a model --- we built the infrastructure to serve it. Three tiers.

Hot tier: our 9-tool agent. When you ask 'show me fraud alerts', the agent calls lookup_fraud_alerts with parameterized SQL. It doesn't scan the whole table --- it builds a WHERE clause with your filters, sends it through the Databricks SDK, and gets results in 1-5 seconds. Same for scheme matching, fraud rings, sender profiles.

Warm tier: 13 pre-computed views shaped specifically for dashboard widgets. The fraud heatmap view already has hour-by-day aggregations computed. The anomaly patterns view already has occurrence counts and amounts at risk. The dashboard reads from these views, so it loads in under a second even with 750K underlying transactions.

Cold tier: for deep analysis, the full tables are available with liquid clustering. Databricks physically reorganizes the data files so that range queries on date and risk tier skip irrelevant blocks. If a fraud analyst wants to look at all critical transactions from March, the query only reads the relevant data partitions."

---

## SLIDE 8: THE 9-TOOL AGENT + RAG + GENIE

### LangGraph ReAct Agent with MCP Protocol

**Architecture:** `User -> Gradio UI -> MLflow AgentServer -> LangGraph create_react_agent + MemorySaver`

| # | Tool | Type | What It Does |
|---|------|------|-------------|
| 1 | `lookup_fraud_alerts` | Custom SQL | Parameterized query on `gold_fraud_alerts_ml`. Filters by risk score, category, sender. Returns alerts with human-readable risk explanations. |
| 2 | `check_loan_eligibility` | Custom SQL + LLM | Filters `gold_schemes` by age/income/occupation/state/gender. LLM explains matches in user's language. |
| 3 | `fraud_recovery_guide` | Custom SQL | Retrieves RBI-mandated recovery steps for 6 fraud types. Fallback: hardcoded steps if DB unavailable. |
| 4 | `lookup_fraud_rings` | Custom SQL | Queries `platinum_fraud_rings`. Filter by size, severity. Returns ring structure, fraud rate, members. |
| 5 | `lookup_sender_profile` | Custom SQL | Queries `platinum_sender_profiles`. Returns composite risk, behavioral patterns, fraud count. |
| 6 | `get_current_time` | Utility | Returns current ISO timestamp. |
| 7 | **Vector Search** | MCP | Semantic search over 80 RBI circular chunks. FAISS index with multilingual-e5-small embeddings (384 dims, 22 languages). |
| 8 | **Genie Query** | MCP | Natural language -> SQL on fraud data. Genie Space configured with entity matching + format assistance. |
| 9 | **Genie Poll** | MCP | Async result retrieval from Genie Space queries. |

### RAG Pipeline (How RBI Circular Search Works)
1. 80 real RBI circulars chunked at 4000-char paragraph boundaries (AGGREGATE pattern)
2. Embedded with `intfloat/multilingual-e5-small` (supports Hindi, Tamil, Telugu, Marathi + 18 more)
3. FAISS IndexFlatIP stored in UC Volumes (serverless-compatible)
4. Top-5 chunk retrieval per query
5. Foundation Model API (Llama 4 Maverick, temp=0.1) generates answer with source citations
6. System prompt: "You are BlackIce, an expert on RBI regulations"

### Security
Every SQL query uses `StatementParameterListItem` parameter binding --- zero SQL injection risk across all 6 custom tools.

**Databricks features on this slide:**
Foundation Model API | Vector Search (MCP) | Genie Space (MCP) | MCP Protocol | Databricks SDK | FAISS on Volumes | MLflow (autolog + tracing)

**Speaker Notes:**
"The agent has 9 tools. 6 custom SQL tools that we built, and 3 MCP connections to Databricks services.

Every custom tool uses parameterized SQL --- not string concatenation. When the agent builds a fraud alert query, it creates parameter objects with typed values. The Databricks SDK handles escaping. Zero SQL injection risk.

For RBI circular search, we built a full RAG pipeline. 80 real circulars chunked at paragraph boundaries, embedded with a multilingual model that supports 22 Indian languages, stored in FAISS on UC Volumes. When someone asks a question --- in Hindi, in English, in Tamil --- the vector search finds the most relevant chunks, and Llama 4 Maverick generates an answer with source citations.

The MCP connections are loaded asynchronously at startup. If Vector Search is unavailable, the agent still works with 6 custom tools. If Genie is unavailable, the agent still answers fraud and scheme questions. Graceful degradation throughout."

---

## SLIDE 9: DATABRICKS --- WE USED EVERYTHING

### 17 Features, Each With a Purpose

| # | Feature | Where We Used It | Why |
|---|---------|-----------------|-----|
| 1 | **Delta Lake** | Every table (15+) | ACID transactions, schema enforcement, time travel |
| 2 | **Unity Catalog** | All 15 tables | PK constraints, column comments, data lineage |
| 3 | **Auto Loader** | Bronze ingestion + streaming pipeline | Schema inference, exactly-once semantics, checkpoint-based |
| 4 | **DLT Pipeline** | Silver layer | EXPECT constraints drop invalid rows automatically |
| 5 | **Spark SQL** | Feature engineering | Window functions, AGGREGATE accumulator, LATERAL VIEW |
| 6 | **Foundation Model API** | RAG + scheme explanations | Llama 4 Maverick for generation in 5 languages |
| 7 | **Vector Search (MCP)** | RBI circular search | Managed semantic search over FAISS index |
| 8 | **Genie Space (MCP)** | Natural language analytics | NL -> SQL on fraud data with entity matching |
| 9 | **Lakeview Dashboard** | 4-page dashboard | 20+ widgets across fraud, patterns, India story, recovery |
| 10 | **Metric Views** | YAML semantic layer | Dimensions + measures + synonyms for Genie understanding |
| 11 | **Databricks SDK** | Agent SQL execution | WorkspaceClient, statement execution, API calls |
| 12 | **Change Data Feed** | Bronze tables | Enables incremental downstream processing |
| 13 | **Liquid Clustering** | Gold transactions | CLUSTER BY (transaction_date, risk_tier) for 10-100x faster queries |
| 14 | **UC Tags** | 15 tables | domain, tier, pii, source, quality classification |
| 15 | **Databricks Apps** | Agent deployment | databricks.yml + app.yaml for production deployment |
| 16 | **FAISS on Volumes** | RAG index storage | Serverless-compatible vector index in UC Volumes |
| 17 | **MCP Protocol** | Agent <-> Vector Search + Genie | Model Context Protocol for tool connectivity |

### Data Governance
- **15 tables tagged** with domain (fraud/regulatory/inclusion), tier (bronze/silver/gold/platinum), PII classification, source
- **5 key columns documented** with technical comments (e.g., "ensemble_score: Weighted fraud risk 0-1. Combines IsolationForest 45%, KMeans 30%, rules 25%")
- **OPTIMIZE + liquid clustering** applied to gold tables

**Speaker Notes:**
"We didn't just use Databricks --- we used all of it. 17 features, each with a specific purpose.

Data layer: Delta Lake with ACID on every table. Unity Catalog for governance --- PK constraints, column comments, lineage. Auto Loader for streaming. Change Data Feed for incremental processing. Liquid clustering for query optimization.

Quality layer: DLT EXPECT constraints that drop bad rows automatically. UC tags on all 15 tables classifying domain, tier, PII status, and source. Column-level documentation so anyone can understand what ensemble_score means.

AI layer: Foundation Model API for multilingual generation. Vector Search via MCP for semantic search over RBI circulars. Genie Space via MCP for natural language analytics. Metric Views with synonyms so Genie understands 'fraud rate' and 'anomaly percentage' mean the same thing.

Serving layer: Lakeview Dashboard with 4 pages. Agent deployed via Databricks Apps architecture. FAISS index stored in UC Volumes for serverless compatibility. And MCP Protocol connecting the agent to Vector Search and Genie live.

Every feature has a reason. Every feature is production-grade."

---

## SLIDE 10: IMPACT + KEY NUMBERS

### What BlackIce Achieved

| Metric | Value |
|--------|-------|
| **Transactions analyzed** | 750,000+ |
| **Total rows processed** | 3.15 million across 15 tables |
| **Fraud alerts flagged** | 1,800+ (446 high/critical) |
| **Amount at risk detected** | Rs.2.17 Crore |
| **Fraud rings detected** | 150 (57 with circular flows) |
| **Money mule hubs identified** | 93 (via PageRank) |
| **Ring volume** | Rs.8.68 Crore |
| **Separation score (Cohen's d)** | 2.128 |
| **RBI circulars searchable** | 80 (real, from rbi.org.in) |
| **Government schemes indexed** | 170 (real, from myscheme.gov.in) |
| **BhashaBench score** | 100% (20/20, English + Hindi) |
| **Databricks features used** | 17 |
| **MLflow experiment runs** | 5 |
| **Real-time streaming** | Bronze -> Silver -> Platinum, exactly-once |
| **Languages supported** | Hindi, English, Marathi, Tamil, Telugu |

### The Story in One Sentence

BlackIce processes 750K transactions through a 4-tier production pipeline, catches fraud rings before they strike using graph intelligence, helps scam victims recover with RBI-mandated guidance in their language, and connects rural Indians to 170 government schemes they didn't know existed --- all on Databricks Free Edition, using 17 platform features, with a live streaming pipeline that scores new transactions in real time.

**Speaker Notes:**
"Let me close with the numbers.

750,000 transactions scored through a 4-tier medallion architecture. 3.15 million rows processed across 15 tables, every one tagged and governed in Unity Catalog. Rs.2.17 crore at risk detected. 150 fraud rings caught through graph analysis --- 93 money mule accounts identified before they could be used. Rs.8.68 crore flowing through those ring structures.

But the numbers that matter most: 170 government schemes now accessible to people who didn't know they qualified. 6 fraud recovery guides that tell victims exactly what to do, in their language, with RBI-mandated steps. 80 real RBI circulars searchable in Hindi by anyone.

We scored 100% on BhashaBench. We used 17 Databricks features. We built a live streaming pipeline with exactly-once semantics. All on Free Edition.

BlackIce. Financial intelligence for every Indian."
