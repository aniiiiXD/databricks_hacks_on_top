# Submission Materials

## Project Write-up (500 characters)

Digital-Artha detects UPI fraud using an IsolationForest+KMeans ensemble on 250K transactions, answers RBI regulation queries via multilingual RAG over 80 real circulars, and matches citizens to 170 government schemes. Built on Databricks with 17+ platform features: Auto Loader, DLT pipeline, Delta Lake, Unity Catalog, Foundation Model API, Vector Search (MCP), Genie Space, Lakeview Dashboard. Real Indian data from RBI, NPCI, PMJDY. State Vulnerability Index reveals which states need help most.

(498 characters)

---

## Databricks Technologies Used

1. Delta Lake — ACID tables, schema enforcement, time travel
2. Unity Catalog — Governance, PK constraints, tags, column comments, lineage
3. Auto Loader (cloudFiles) — Streaming CSV ingestion with schema inference
4. Lakeflow Declarative Pipelines (DLT) — ETL with EXPECT data quality constraints
5. Spark SQL — Feature engineering, aggregations, window functions
6. Foundation Model API — Llama 4 Maverick for RAG generation and translations
7. Vector Search (MCP) — Managed semantic search over RBI circular chunks
8. Genie Space (MCP) — Natural language analytics queries
9. Lakeview Dashboard — 4-page interactive analytics (20+ widgets)
10. Metric Views (YAML) — Semantic layer with dimensions/measures/synonyms
11. Databricks SDK — WorkspaceClient, statement execution, API calls
12. Change Data Feed — Incremental processing from bronze tables
13. Liquid Clustering — Query optimization on gold tables
14. UC Tags — Data classification (domain, tier, pii, source) on 15 tables
15. Databricks Apps Architecture — Agent deployment config (databricks.yml)
16. FAISS on Volumes — Vector index stored in Unity Catalog Volumes
17. MCP Protocol — Agent connects to Vector Search + Genie via Model Context Protocol

## Open-Source Models Used

1. **IsolationForest** (scikit-learn) — Unsupervised anomaly detection
2. **KMeans** (scikit-learn) — Cluster-based anomaly scoring
3. **multilingual-e5-small** (intfloat/HuggingFace) — Multilingual embeddings for RAG
4. **FAISS** (Meta) — CPU vector search index
5. **LangGraph** (LangChain) — ReAct agent framework
6. **NetworkX** — Transaction relationship graph analysis
7. **Llama 4 Maverick** (Meta, via Databricks) — LLM for generation, explanation, translation

---

## Demo Steps (for judges reproducing)

### Quick Demo (2 minutes)
1. Open the Databricks dashboard → Page 1 shows fraud stats
2. Page 2 → Anomaly patterns + risky senders
3. Page 3 → India's digital payment story + state vulnerability
4. Page 4 → Fraud recovery guide + pipeline inventory
5. Open Genie Space → Ask "fraud rate by category"
6. Open the Gradio app URL → Ask "Show me fraud alerts"
7. Ask in Hindi: "यूपीआई फ्रॉड से कैसे बचें?"

### Full Reproduction (20 minutes)
```
1. Create catalog: CREATE CATALOG digital_artha; CREATE SCHEMA main;
2. Upload 8 data files to Volume digital_artha.main.raw_data
3. Clone repo: github.com/aniiiiXD/databricks_hacks_on_top
4. Run notebooks in order: 01 → 02 → 03 → 10 → 04 → 05 → 06 → 12 → 13 → 14 → 15
5. Create dashboard from viz_* views
6. Agent: cd 08-agent && uv run quickstart && uv run start-server
7. Gradio: python3 chat_ui.py
```

---

## Demo Video Script (2 minutes)

**0:00-0:10** "Digital-Artha: AI financial intelligence for India. Detects UPI fraud, explains RBI rules, finds government schemes."

**0:10-0:30** Show dashboard Page 1: "250K transactions analyzed. 246 flagged. 10% fraud rate. Here's fraud by category and the hour-day heatmap."

**0:30-0:50** Show Page 2: "Our ML ensemble discovered 8 distinct fraud patterns. Late Night High Value, Unusual Category Spend. Here's the Category × Time risk matrix."

**0:50-1:10** Show Page 3: "UPI grew from near zero to 20K million transactions per month. Bank fraud losses hit ₹427 crore for SBI alone. Rajasthan is the most vulnerable state."

**1:10-1:30** Show agent chat: Type "I was scammed via QR code" → show recovery steps. Type "schemes for farmer in UP" → show matching schemes.

**1:30-1:50** Show Genie Space: "fraud rate by category" → instant SQL + chart.

**1:50-2:00** "Digital-Artha. 17 Databricks features. 9 real Indian datasets. Built on Free Edition. Bharat Bricks 2026."
