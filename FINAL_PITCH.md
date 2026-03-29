# BlackIce — Final Pitch Guide (5 Minutes)

## THE OPENING (30 seconds)

"India processes 16 billion UPI transactions every month — the largest real-time payment network in the world. But fraud is rising. ₹427 crore lost by SBI alone last year. Rural Indians don't know what government schemes they qualify for. And if you get scammed, the RBI recovery process is buried in English legalese.

We built BlackIce — a financial intelligence platform that detects fraud patterns, searches RBI regulations in Hindi, matches citizens to 170 government schemes, and tells you exactly what to do if you're scammed. All on Databricks Free Edition."

---

## ARCHITECTURE (60 seconds)

"Our pipeline has 4 tiers:

**Bronze** — Auto Loader ingests 250K UPI transactions from Kaggle, 80 real RBI circulars scraped from rbi.org.in, and 170 government schemes from myscheme.gov.in. Plus 5 macro datasets — bank fraud losses, PMJDY accounts, internet penetration by state.

**Silver** — ETL pipeline with DLT EXPECT constraints. Invalid IDs dropped. Amounts validated. Quality-checked.

**Gold** — Business-ready tables. RBI circulars chunked at 4000 characters using a SQL AGGREGATE accumulator for our RAG pipeline. Liquid clustering on transaction dates.

**Platinum** — This is where raw data becomes intelligence:
- IsolationForest + KMeans ensemble scores every transaction
- 7 anomaly patterns discovered: Late Night High Value, Unusual Category Spend, etc.
- 5,000 sender risk profiles with composite scores
- Separation score of 2.128 — flagged transactions are genuinely 2x standard deviations different from normal

Then we added graph intelligence — 2,981 synthetic fraud ring transactions analyzed with NetworkX. 150 rings detected. 57 with circular money flows. 93 money mule hub accounts identified via PageRank.

And a streaming pipeline — Bronze → Silver → Platinum with Auto Loader, incremental processing, exactly-once semantics. Judges can inject a custom transaction and watch it flow through all three tiers."

---

## LIVE DEMO (90 seconds)

### Dashboard (30 seconds)
"Here's our Lakeview dashboard — 4 pages:
- Fraud Command Center: 250K transactions, 446 flagged, ₹2.17 crore at risk
- Pattern Intelligence: 7 named patterns, Category × Time risk matrix
- India Story: UPI growth curve, bank fraud losses, State Vulnerability Index — Rajasthan is the most vulnerable
- Help & Recovery: RBI-mandated fraud recovery steps for 6 scam types"

### Agent Chat (30 seconds)
Open Gradio app. Type:
1. **"Show me fraud alerts"** → structured alert table with risk scores
2. **"I was scammed via QR code"** → RBI recovery steps, who to report to, 3-day liability window

### Scheme Finder (15 seconds)
Go to Scheme Finder tab. Set: Age 25, Farmer, Maharashtra, ₹1.5 lakh.
Click "Find Schemes" → matching government programs appear.

### Genie Space (15 seconds)
Open Genie. Type: **"fraud rate by category"** → instant SQL + chart.

---

## TECHNICAL DEPTH (60 seconds)

"Let me show you what's under the hood:

**MLflow** — we log 5 experiment runs: fraud ensemble, anomaly patterns, RAG pipeline, loan eligibility, streaming. Here are the params and metrics. [Show Experiments page]

**Data Quality** — 15 tables tagged in Unity Catalog with domain, tier, PII classification. Column-level documentation. Liquid clustering on gold tables. 13 pre-computed views as a warm serving tier.

**The ML** — Our ensemble uses pure behavioral features: amount, time, sender velocity, deviation from sender's average. We explicitly excluded the rule-based risk score to prevent data leakage. The separation score is 2.128 — that means our flagged transactions have genuinely anomalous patterns, not just random noise.

**BhashaBench** — We scored 100% on 20 financial knowledge questions across 5 categories, including 5 Hindi questions answered in Devanagari. Using ai_query() with Llama 4 Maverick.

**17 Databricks features** — Delta Lake, Unity Catalog, Auto Loader, ETL Pipeline, Spark SQL, Foundation Model API, Vector Search MCP, Genie Space MCP, Lakeview Dashboard, Metric Views, Databricks SDK, Change Data Feed, Liquid Clustering, UC Tags, FAISS on Volumes, MCP Protocol, MLflow."

---

## IMPACT + CLOSE (30 seconds)

"BlackIce detected ₹2.17 crore at risk across 446 anomalous transactions. It found 150 fraud rings with ₹8.68 crore flowing through them. It indexes 170 government schemes for India's most vulnerable populations. And it tells scam victims exactly what to do — in their language.

We scored 100% on BhashaBench. We used 17 Databricks features. We processed 3.15 million rows. All on Free Edition.

BlackIce. Financial intelligence for every Indian."

---

## TOUGH QUESTIONS — WHAT TO SAY

**"Why is the UPI data synthetic?"**
"No bank releases real UPI transaction data. The Kaggle dataset has realistic patterns — categories, amounts, timestamps, state distribution. Our macro data — bank fraud losses, PMJDY accounts, RBI complaints — is all real from government sources."

**"What's the separation score?"**
"Cohen's d = 2.128. It means our flagged transactions differ from normal by over 2 standard deviations in amount and behavioral patterns. For unsupervised anomaly detection without labeled training data, this is excellent. The model genuinely finds behavioral outliers."

**"Why not supervised learning?"**
"The fraud labels in the Kaggle data are randomly assigned at 0.19%. Training a supervised model on random labels would be meaningless. Our unsupervised ensemble learns what 'normal' looks like and flags deviations — which is how real bank fraud detection works."

**"How does streaming work on Free Edition?"**
"Auto Loader with trigger(availableNow=True). Each run processes only new files via checkpoints — exactly-once semantics. We demonstrate this by dropping files and watching counts grow through Bronze → Silver → Platinum. In production, the trigger switches to processingTime for continuous processing."

**"Why did MLflow not work initially?"**
"Free Edition serverless doesn't whitelist spark.mlflow.modelRegistryUri for reading. We discovered that explicitly calling mlflow.set_tracking_uri('databricks') and mlflow.set_registry_uri('databricks') before start_run() bypasses this. We now have 5 logged MLflow runs."

**"What Indian models do you use?"**
"Our embeddings are multilingual-e5-small which supports 22 Indian languages including Hindi, Tamil, Telugu, Marathi. The LLM is Llama 4 Maverick via Databricks Foundation Model API which handles Hindi queries natively — verified by our BhashaBench 100% score. Our architecture supports plugging in Sarvam AI's dedicated Indian language API for production."

**"How is this different from other teams?"**
"Three things no other team will have: (1) Graph-based fraud ring detection with 150 rings and PageRank hub identification, (2) State Vulnerability Index cross-referencing fraud risk × internet penetration × Jan Dhan banking access, (3) A production-grade agent with 9 tools including live MCP connections to Vector Search and Genie Space."

---

## NUMBERS TO MEMORIZE

- 250K transactions scored
- 446 anomalies, ₹2.17 Cr at risk
- Separation score: 2.128
- 150 fraud rings, 93 hubs, ₹8.68 Cr in rings
- 100% BhashaBench (20/20, English + Hindi)
- 170 schemes, 118 ministries
- 68 RBI circulars, 80 RAG chunks
- 3.15M total rows, 15 tables, 17+ features, 5 MLflow runs
- 9 agent tools (6 custom + 3 MCP)
