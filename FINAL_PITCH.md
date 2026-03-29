# BlackIce — Final Pitch Guide (5 Minutes)

## THE OPENING (30 seconds)

"India processes 16 billion UPI transactions every month — the largest real-time payment network in the world. But fraud is rising. Rs.427 crore lost by SBI alone last year. Rural Indians don't know what government schemes they qualify for. And if you get scammed, the RBI recovery process is buried in English legalese.

We built BlackIce — a financial intelligence platform that detects fraud patterns before they escalate, streams live fraud scoring in real time, searches RBI regulations in Hindi, and matches citizens to 170 government schemes. All on Databricks Free Edition — and we pushed every single feature to its limit."

---

## THE PIPELINE — HOW WE BUILT IT (90 seconds)

"Let us walk you through what we actually built — because the pipeline *is* the product.

### Data Ingestion
We start with Auto Loader — streaming ingestion of 250K UPI transactions with exactly-once semantics and schema inference. On top of that, 80 real RBI circulars scraped from rbi.org.in, 170 government schemes from myscheme.gov.in, and 5 macro datasets — bank fraud losses from Lok Sabha, PMJDY accounts, internet penetration by state, RBI complaints, and UPI growth data.

### ETL Pipeline — The Full Medallion
This is not a notebook that reads a CSV and trains a model. This is a production ETL pipeline:

**Bronze** — Raw ingestion with Primary Key constraints, Change Data Feed enabled, column-level documentation in Unity Catalog. Every table tagged with domain, tier, PII classification, and source.

**Silver** — Lakeflow Declarative Pipeline with DLT EXPECT constraints. Invalid transaction IDs? Dropped. Amounts less than zero? Dropped. Null timestamps? Dropped. The data that survives is validated, typed, and enriched with time slots, weekend flags, and risk classifications.

**Gold** — Business-ready tables. RBI circulars chunked at 4000-character boundaries using a SQL AGGREGATE accumulator — that feeds our RAG pipeline. Transactions get liquid clustering on date for query optimization. Schemes get eligibility JSON for instant matching.

**Platinum** — This is where raw data becomes intelligence. Our IsolationForest + KMeans ensemble scores every transaction. 8 distinct anomaly patterns named and classified. 5,000 sender risk profiles computed with composite scores.

We didn't skip a single layer. Every table has UC tags. Every column has documentation. Every constraint is enforced.

### Real-Time Streaming — Live Fraud Scoring
And here's what makes this a real system, not a demo: we built a streaming pipeline. Bronze to Silver to Platinum — Auto Loader with checkpoints, incremental processing, exactly-once semantics. You can drop a new transaction file and watch it flow through all three tiers and get scored in real time. This isn't batch. This is a live system."

---

## HOT / WARM / COLD SERVING (30 seconds)

"We implemented a three-tier serving architecture — and all three tiers are actually working:

**Hot tier** — Our 9-tool agent runs parameterized SQL queries through the Databricks SDK. Sub-5-second fraud lookups, scheme matching, RBI circular search — all live.

**Warm tier** — 13 pre-computed dashboard views plus 2 materialized views. The Lakeview dashboard loads in under a second because everything is pre-aggregated.

**Cold tier** — Full historical Delta tables with liquid clustering on transaction date. 750K transactions queryable through Genie Space or the SQL editor.

This is how production systems work. We didn't just train a model — we built the infrastructure to serve it."

---

## FRAUD RINGS — PREDICTIVE INTELLIGENCE (30 seconds)

"Now here's the part we're most proud of. Our fraud ring detection doesn't just find fraud after it happens — it finds it before it happens.

We built a transaction graph with NetworkX. 2,981 transactions analyzed. 150 fraud rings detected. 57 with circular money flows. And here's the key: we identified 93 money mule hub accounts using PageRank — accounts that look normal individually but become suspicious when you see the network around them.

Rs.8.68 crore flowing through these rings. The system catches patterns that are invisible at the transaction level — even if every individual transaction looks legitimate, the graph structure gives them away. You can't hide a ring from graph analysis."

---

## LIVE DEMO (60 seconds)

### Dashboard (20 seconds)
"Here's our Lakeview dashboard — 4 pages:
- Fraud Command Center: 250K transactions, 446 flagged, Rs.2.17 crore at risk
- Pattern Intelligence: 8 named patterns, Category x Time risk matrix
- India Story: UPI growth curve, bank fraud losses, State Vulnerability Index — Rajasthan is the most vulnerable
- Help & Recovery: RBI-mandated fraud recovery steps for 6 scam types"

### Agent Chat (20 seconds)
Open Gradio app. Type:
1. **"Show me fraud alerts"** — structured alert table with risk scores
2. **"I was scammed via QR code"** — RBI recovery steps, who to report to, 3-day liability window

### Scheme Finder + Genie (20 seconds)
Go to Scheme Finder tab. Set: Age 25, Farmer, Maharashtra, Rs.1.5 lakh.
Click "Find Schemes" — matching government programs appear.
Open Genie. Type: **"fraud rate by category"** — instant SQL + chart.

---

## DATABRICKS — WE USED EVERYTHING (60 seconds)

"We didn't just use Databricks — we used *all* of Databricks. 17 features:

**Data:** Delta Lake with ACID transactions. Unity Catalog for governance — PK constraints, column comments, UC tags on 15 tables. Auto Loader for streaming ingestion. Change Data Feed for incremental processing. Liquid clustering for query optimization.

**ETL:** Lakeflow Declarative Pipelines with DLT EXPECT constraints. Spark SQL for feature engineering with window functions across 750K rows.

**AI:** Foundation Model API — Llama 4 Maverick for RAG generation and multilingual explanations. Vector Search via MCP protocol for semantic search over RBI circulars. Genie Space via MCP for natural language analytics.

**Serving:** Lakeview Dashboard with 4 pages and 20+ widgets. Metric Views with YAML semantic layer — dimensions, measures, synonyms so Genie understands our data. Databricks SDK for real-time agent queries. FAISS on UC Volumes for vector storage.

**Agent:** 9 tools — 6 custom SQL tools plus 3 MCP connections to Vector Search and Genie Space. LangGraph ReAct agent with conversation memory. MLflow for experiment tracking — 5 logged runs.

Every feature has a reason. Every feature is production-grade."

---

## IMPACT + CLOSE (30 seconds)

"BlackIce detected Rs.2.17 crore at risk across 446 anomalous transactions. It found 150 fraud rings with Rs.8.68 crore flowing through them — catching patterns before they become losses. It indexes 170 government schemes for India's most vulnerable populations. And it tells scam victims exactly what to do — in their language.

We scored 100% on BhashaBench. We processed 3.15 million rows. We used 17 Databricks features — every single one with a purpose. And the streaming pipeline means this isn't a snapshot — it's a live system.

BlackIce. Financial intelligence for every Indian."

---

## TOUGH QUESTIONS — WHAT TO SAY

**"Why is the UPI data synthetic?"**
"No bank releases real UPI transaction data — that's a regulatory impossibility. The Kaggle dataset has realistic patterns — categories, amounts, timestamps, state distribution. But our macro data — bank fraud losses, PMJDY accounts, RBI complaints, UPI growth stats — is all real, scraped from government sources. And the platform works on any transaction dataset — swap in real bank data and it works immediately."

**"What's the separation score?"**
"Cohen's d = 2.128. It means our flagged transactions differ from normal by over 2 standard deviations in amount and behavioral patterns. For unsupervised anomaly detection without labeled training data, this is excellent. The model genuinely finds behavioral outliers."

**"Why not supervised learning?"**
"The fraud labels in the Kaggle data are randomly assigned at 0.19%. Training a supervised model on random labels would be meaningless. Our unsupervised ensemble learns what 'normal' looks like and flags deviations — which is how real bank fraud detection works in production."

**"How does the real-time streaming actually work?"**
"Auto Loader with trigger(availableNow=True) and checkpoint-based incremental processing. Each run processes only new files — exactly-once semantics. We demonstrate this by dropping files and watching counts grow through Bronze, Silver, Platinum. In production, the trigger switches to processingTime for continuous processing. The entire pipeline — ingestion, validation, ML scoring — runs end to end on new data."

**"How do fraud rings predict fraud before it happens?"**
"Individual transactions might look clean. But graph analysis reveals structural patterns — circular money flows, hub accounts with abnormal connectivity, tightly-knit groups with high density scores. PageRank identifies money mule accounts that are being set up as intermediaries. These accounts can be flagged and monitored *before* the fraud event occurs. The graph sees what individual transaction monitoring cannot."

**"Why did MLflow not work initially?"**
"Free Edition serverless doesn't whitelist spark.mlflow.modelRegistryUri for reading. We discovered that explicitly calling mlflow.set_tracking_uri('databricks') and mlflow.set_registry_uri('databricks') before start_run() bypasses this. We now have 5 logged MLflow runs with full params, metrics, and model artifacts."

**"What Indian models do you use?"**
"Our embeddings are multilingual-e5-small which supports 22 Indian languages including Hindi, Tamil, Telugu, Marathi. The LLM is Llama 4 Maverick via Databricks Foundation Model API which handles Hindi queries natively — verified by our BhashaBench 100% score. Our architecture supports plugging in Sarvam AI's dedicated Indian language API for production."

**"How is Hot/Warm/Cold different from just caching?"**
"Caching is passive. Our architecture is deliberate. Hot tier is purpose-built agent tools with parameterized SQL — designed for sub-5-second interactive queries. Warm tier is 13 pre-computed views specifically shaped for dashboard widgets — the aggregation is done at write time, not read time. Cold tier has liquid clustering on transaction date — Databricks physically reorganizes the data files for optimal range queries. Three different optimization strategies for three different access patterns."

---

## NUMBERS TO MEMORIZE

- 250K transactions scored, 3.15M total rows processed
- 446 anomalies, Rs.2.17 Cr at risk
- Separation score: 2.128 (Cohen's d)
- 150 fraud rings, 93 money mule hubs, Rs.8.68 Cr in rings
- 100% BhashaBench (20/20, English + Hindi)
- 170 schemes, 118 ministries
- 80 RBI circulars indexed for RAG
- 15 tables, 13 pre-computed views, 17 Databricks features
- 5 MLflow runs logged
- 9 agent tools (6 custom + 3 MCP)
- Real-time streaming: Bronze -> Silver -> Platinum with exactly-once semantics
