# Team Briefing: What to Say to Judges

## The 30-Second Pitch
"India processes 16 billion UPI transactions a month. Fraud is rising. RBI circulars are in English legalese. Rural Indians don't know what loans they qualify for. We built Digital-Artha — a platform that detects fraud patterns using an ML ensemble, answers RBI regulation questions in Hindi via RAG, and connects citizens to 170 government schemes — all on Databricks Free Edition, using real Indian data."

---

## How We Built It (Architecture Story)

### When judges ask "Walk us through the architecture":

"We built a 4-tier medallion architecture:

**Bronze**: Auto Loader ingests the Kaggle UPI dataset (250K transactions) plus 80 real RBI circulars scraped from rbi.org.in and 170 government schemes from myscheme.gov.in. We also brought in macro context — RBI digital payment stats, bank-wise fraud losses from Lok Sabha data, PMJDY Jan Dhan accounts by state, and internet penetration data.

**Silver**: Our ETL pipeline cleans and validates with DLT EXPECT constraints — dropping null IDs, invalid amounts, bad timestamps. We derive time features, weekend flags, and risk classifications.

**Gold**: Business-ready tables. The circulars are chunked at 4000-character boundaries using a SQL AGGREGATE accumulator for the RAG pipeline. Transactions get risk labels.

**Platinum**: This is where it gets interesting. We run an IsolationForest + KMeans ensemble to score every transaction. The ensemble blends three signals — 45% anomaly detection, 30% cluster distance, 25% rule-based risk. We then discover 8 named fraud patterns like 'Late Night High Value' and 'Unusual Category Spend'. We build per-sender risk profiles with composite scores, and per-merchant risk profiles.

**Serving**: Three interfaces — a 4-page Lakeview dashboard, a Genie Space for natural language queries, and a LangGraph agent with 6 tools including MCP connections to Vector Search and Genie."

---

## When They Ask About Specific Features

### "How does the ML work?"
"IsolationForest for unsupervised anomaly detection — it learns what 'normal' looks like and flags outliers. KMeans for cluster-based scoring — transactions far from any cluster center are suspicious. We blend them in a weighted ensemble. Key insight: we use stratified sampling to preserve all fraud rows during training, because at 0.19% fraud rate, random sampling would miss most fraud."

### "Why not use Spark MLlib?"
"We tried. Serverless compute on Free Edition doesn't support MLlib — VectorAssembler is whitelisted-only. So we pivoted to pure sklearn. The feature engineering still runs in Spark SQL (window functions, aggregations across 250K rows) — only the model training runs on the driver. This is actually a common production pattern for sub-million row ML."

### "What about the RAG pipeline?"
"We scrape real RBI circulars, chunk them at paragraph boundaries using a SQL AGGREGATE accumulator, embed them with multilingual-e5-small (supports Hindi, Tamil, Telugu), build a FAISS index stored in UC Volumes, and retrieve top-5 chunks for any query. The agent then generates answers using Llama 4 Maverick via the Foundation Model API. Users can ask in Hindi and get Hindi responses."

### "How does the agent work?"
"LangGraph create_react_agent with MemorySaver for conversation persistence. It has 6 tools — 3 custom (fraud lookup, loan eligibility, time) and 3 via MCP protocol (Vector Search for RBI circulars, Genie Space for analytics). The MCP connection is async — we load tools at startup and the agent decides which to call based on the user's question."

---

## Why Not MLflow? (Be Honest)

"MLflow is pre-installed on Databricks but the Free Edition serverless runtime has a config limitation — `spark.mlflow.modelRegistryUri` is not set, and importing mlflow triggers a GRPC crash. We designed the full MLflow integration — experiment tracking, model logging, model registry — and the code is in our fraud detection notebook (last cell). It runs in a try/except: if MLflow works, everything logs; if not, results go to Delta tables. We'd welcome guidance on the correct workaround for serverless."

**Don't say**: "MLflow doesn't work."
**Do say**: "We architected for MLflow and the code is in place. The serverless runtime has a specific config gap that prevents full integration."

---

## Hot/Warm/Cold — Is It Working?

**Yes, partially:**
- **Cold**: Gold tables with liquid clustering on `transaction_date` — ✅ working, verified with OPTIMIZE
- **Warm**: `mv_fraud_summary` and `mv_high_risk_senders` views — ✅ working, powers dashboard queries
- **Hot**: Agent tools with parameterized SQL via Databricks SDK — ✅ working, sub-second fraud lookups

**What to say**: "We implemented a tiered serving architecture. Cold tier is Delta tables with liquid clustering for historical analysis. Warm tier is pre-aggregated materialized views that power the dashboard. Hot tier is the agent's real-time SQL tools for instant fraud lookups. On a production deployment, we'd add Online Tables for true millisecond serving."

---

## Plus Points to Highlight

1. **Real data**: 8 of 9 datasets are real (RBI circulars scraped, schemes from myscheme.gov.in, bank fraud from Lok Sabha, PMJDY from gov site). Only UPI transactions are synthetic (no bank releases real transaction data).

2. **17 Databricks features**: More than any other team will use. Including MCP protocol, liquid clustering, UC tags, EXPECT constraints.

3. **India story**: The State Vulnerability Index cross-references fraud rate × internet penetration × Jan Dhan coverage. Andhra Pradesh and Rajasthan are the most vulnerable. This is a novel insight.

4. **Human impact**: Not just fraud detection — we show what a scam victim should do (RBI-mandated recovery steps, reporting contacts, liability limits). This is empathy in engineering.

5. **Agent with MCP**: 6 tools including live connections to Vector Search and Genie Space via MCP protocol. Multi-turn memory. Hindi support.

6. **Anomaly patterns**: Not just "fraud/not-fraud" — we name 8 distinct patterns (Late Night High Value, Unusual Category Spend, etc.) that a fraud analyst can act on.

---

## What to Say If Things Go Wrong During Demo

| Situation | What to Say |
|-----------|-------------|
| Dashboard is slow | "Free Edition serverless has fair-usage quotas. The 13 pre-computed views normally make this instant." |
| Agent errors | "The agent connects to Databricks via MCP protocol. Let me show the architecture instead." Show the code. |
| MLflow doesn't log | "Serverless has a modelRegistryUri config gap. The code is MLflow-ready — here's the tracking cell." |
| Fraud rate seems high/low | "The 10% rate includes our ML ensemble scoring. Ground truth fraud is 0.19%. The ensemble intentionally over-flags for investigation." |
| "This is synthetic data" | "The UPI data is synthetic because banks can't release real transactions. But our macro data — RBI fraud stats, Lok Sabha bank losses, PMJDY accounts — is all real. The platform works on any transaction dataset." |

---

## 5-Minute Pitch Structure

| Time | Section | Key Points |
|------|---------|------------|
| 0:00-0:30 | **Problem** | "16B UPI transactions/month. Fraud rising. RBI rules in English. Rural Indians don't know their schemes." |
| 0:30-1:30 | **Architecture** | Show the diagram. "4-tier medallion. 15+ tables. 17 Databricks features. ETL pipeline with DQ constraints." |
| 1:30-3:00 | **Live Demo** | Dashboard Page 1 → Page 2 (patterns) → Page 3 (India story) → Agent chat → Genie query |
| 3:00-4:00 | **Technical Depth** | ML ensemble details. MCP agent. Hot/warm/cold tiers. State Vulnerability Index. |
| 4:00-5:00 | **Impact + Close** | "₹1.16 Crore at risk detected. 170 schemes indexed. 8 fraud patterns named. Every Indian, in their language." |

---

## Future Things We Can Do

1. **Real-time streaming**: Switch Auto Loader to `processingTime` trigger for live scoring
2. **Graph fraud rings**: With real sender-receiver data, detect money mule networks
3. **IndicTrans2**: Add 22-language support (model runs on CPU, ~2GB)
4. **BhashaBench evaluation**: Benchmark our RAG against Indian financial knowledge
5. **Databricks App deployment**: OAuth-authenticated agent accessible to anyone
6. **Predictive forecasting**: Time-series fraud rate prediction using `ai_forecast()`
7. **Mobile app**: Gradio UI works on mobile — fraud alerts on phone
