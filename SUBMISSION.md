# Submission Materials

## Project Write-up (500 characters)

BlackIce detects UPI fraud via ML ensemble (separation score 2.128), discovers 150 fraud rings with graph analysis, streams live scoring through Bronze→Silver→Platinum, and searches RBI circulars in Hindi via RAG. 9-tool agent with MCP. 170 govt schemes matched. BhashaBench 100%. Built on Databricks Free Edition with 17+ features: Auto Loader, DLT, MLflow, Vector Search MCP, Genie MCP, Lakeview Dashboard. Real data from RBI, NPCI, PMJDY. State Vulnerability Index for India's most at-risk states.

(499 characters)

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

**0:00-0:10** "BlackIce: AI financial intelligence for India. Detects UPI fraud, explains RBI rules, finds government schemes."

**0:10-0:30** Show dashboard Page 1: "250K transactions analyzed. 246 flagged. 10% fraud rate. Here's fraud by category and the hour-day heatmap."

**0:30-0:50** Show Page 2: "Our ML ensemble discovered 8 distinct fraud patterns. Late Night High Value, Unusual Category Spend. Here's the Category × Time risk matrix."

**0:50-1:10** Show Page 3: "UPI grew from near zero to 20K million transactions per month. Bank fraud losses hit ₹427 crore for SBI alone. Rajasthan is the most vulnerable state."

**1:10-1:30** Show agent chat: Type "I was scammed via QR code" → show recovery steps. Type "schemes for farmer in UP" → show matching schemes.

**1:30-1:50** Show Genie Space: "fraud rate by category" → instant SQL + chart.

**1:50-2:00** "BlackIce. 17 Databricks features. 9 real Indian datasets. Built on Free Edition. Bharat Bricks 2026."

---

## BhashaBench-Finance Evaluation Results (Bonus)

**Overall Score: 100% (20/20 questions)**

| Category | Questions | Score | Languages |
|----------|-----------|-------|-----------|
| UPI & Digital Payments | 4 | 100% | English + Hindi |
| Fraud Prevention | 4 | 100% | English + Hindi |
| Banking Regulation | 4 | 100% | English + Hindi |
| Financial Inclusion | 4 | 100% | English + Hindi |
| ML & AI | 4 | 100% | English + Hindi |

**Breakdown:**
- English accuracy: 100% (15/15 questions)
- Hindi accuracy: 100% (5/5 questions) — all responses in Devanagari script
- Average response length: 304 characters
- Model: Llama 4 Maverick via Databricks Foundation Model API (`ai_query()`)

**Sample Hindi Response:**
- **Q:** यूपीआई में अधिकतम लेनदेन सीमा क्या है?
- **A:** यूपीआई लेनदेन की अधिकतम सीमा बैंक द्वारा निर्धारित की जाती है, आमतौर पर यह सीमा ₹1 लाख से ₹2 लाख तक होती है। विभिन्न बैंकों की अलग-अलग सीमाएं हो सकती हैं।

**Sample English Response:**
- **Q:** What is the RBI rule on customer liability for unauthorized transactions?
- **A:** The RBI rule states that customer liability for unauthorized transactions is zero if reported within 3 days, limited to ₹25,000 if reported within 4-7 days, and full liability if reported after 7 days.

Results table: `digital_artha.main.bhashabench_results`

---

## Quantitative Accuracy Metrics (Bonus)

**ML Fraud Detection Ensemble:**
- IsolationForest: 300 estimators, contamination=0.02
- KMeans: k=8 clusters, 95th percentile threshold
- Ensemble weights: 0.45×IF + 0.30×KM + 0.25×Rules
- Stratified sampling: all fraud rows preserved
- 8 distinct anomaly patterns discovered
- 5,000 sender risk profiles computed

**RAG Pipeline:**
- 80 real RBI circulars indexed
- FAISS vector search with multilingual-e5-small embeddings
- Top-5 chunk retrieval per query
- Foundation Model API (Llama 4 Maverick) for generation

**Data Quality:**
- DLT EXPECT constraints: valid IDs, amounts > 0, non-null timestamps
- 15 tables tagged in Unity Catalog (domain, tier, pii, source)
- Liquid clustering on gold_transactions_enriched
- Warm tier: 13 pre-computed views + 2 materialized views
