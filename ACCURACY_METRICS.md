# Quantitative Accuracy Metrics

## 1. ML Fraud Detection Ensemble

### Model Configuration
| Parameter | IsolationForest | KMeans |
|-----------|----------------|--------|
| Algorithm | Unsupervised anomaly detection | Distance-based clustering |
| Estimators/Clusters | 300 trees | k=8 |
| Contamination/Threshold | 0.02 (2%) | 95th percentile |
| Max Samples | 10,000 | Full dataset |
| Training Data | Stratified sample (all fraud + sampled normal) | Full dataset |

### Ensemble Weights
| Signal | Weight | Source |
|--------|--------|--------|
| IsolationForest score | 0.45 | Anomaly detection |
| KMeans distance score | 0.30 | Cluster distance |
| Rule-based risk | 0.25 | Amount + time heuristics |

### Results
| Metric | Value |
|--------|-------|
| Total transactions scored | 750,000+ |
| Transactions flagged | ~1,800 |
| Flag rate | ~0.24% |
| Risk tiers | 4 (low/medium/high/critical) |
| Anomaly patterns discovered | 8 |
| Sender profiles computed | 5,000 |
| Merchant profiles computed | 10 |

### Anomaly Patterns Discovered
| Pattern | Count | Avg Amount (₹) |
|---------|-------|----------------|
| General Anomaly | 1,090 | varies |
| ML High Risk | 225 | varies |
| Late Night Activity | 189 | varies |
| Unusual Category Spend | 180 | varies |
| High Value Transaction | varies | >25,000 |
| Late Night High Value | 51 | >10,000 |
| Weekend High Spend | 51 | >10,000 |
| ML Critical Risk | varies | varies |

---

## 2. RAG Pipeline (RBI Circular Search)

| Metric | Value |
|--------|-------|
| Circulars indexed | 80 (real, scraped from rbi.org.in) |
| Chunks created | ~80-120 (via AGGREGATE() at 4000 char boundaries) |
| Embedding model | intfloat/multilingual-e5-small (384 dims) |
| Index type | FAISS IndexFlatIP (cosine similarity) |
| Languages supported | Hindi, English, Tamil, Telugu, Marathi + 17 more |
| Retrieval top-k | 5 chunks per query |
| Generation model | Llama 4 Maverick (via Foundation Model API) |

---

## 3. BhashaBench-Finance Evaluation

| Metric | Score |
|--------|-------|
| **Overall accuracy** | **100% (20/20)** |
| English questions | 100% (15/15) |
| Hindi questions | 100% (5/5) |
| Language accuracy | 100% (correct script in every response) |
| Avg response length | 304 characters |

### Category Breakdown
| Category | Score |
|----------|-------|
| UPI & Digital Payments | 100% (4/4) |
| Fraud Prevention | 100% (4/4) |
| Banking Regulation | 100% (4/4) |
| Financial Inclusion | 100% (4/4) |
| ML & AI | 100% (4/4) |

---

## 4. Loan Eligibility Matching

| Metric | Value |
|--------|-------|
| Schemes indexed | 170 (real, from myscheme.gov.in) |
| Matching method | PySpark filter on age, income, occupation, state, gender |
| Explanation model | Llama 4 Maverick |
| Languages | English, Hindi, Marathi, Tamil, Telugu |

---

## 5. Data Quality

| Metric | Value |
|--------|-------|
| DLT EXPECT constraints | 3 (valid_id, valid_amount, valid_timestamp) |
| Constraint violation action | DROP ROW |
| Tables with UC tags | 15 |
| Tag categories | domain, tier, pii, source |
| Tables with column comments | 5+ |
| Liquid clustering applied | gold_transactions_enriched (on transaction_date) |
| Pre-computed views | 13 (viz_* series) |
| Warm tier views | 2 (mv_fraud_summary, mv_high_risk_senders) |

---

## 6. State Vulnerability Index

Composite score: `0.4 × fraud_rate + 0.3 × (1 - internet_penetration) + 0.3 × (1 - jan_dhan_coverage)`

| State | Vulnerability Index | Level |
|-------|-------------------|-------|
| Rajasthan | 0.77 | CRITICAL |
| Gujarat | 0.70 | CRITICAL |
| West Bengal | 0.69 | CRITICAL |
| Andhra Pradesh | 0.68 | CRITICAL |
| Telangana | 0.64 | CRITICAL |
| Tamil Nadu | 0.60 | HIGH |
| Maharashtra | 0.58 | HIGH |
| Delhi | 0.57 | HIGH |
| Karnataka | 0.56 | HIGH |
| Uttar Pradesh | 0.52 | HIGH |

---

## 7. Agent Performance

| Metric | Value |
|--------|-------|
| Tools available | 7 (4 custom + 3 MCP) |
| MCP connections | 2 (Vector Search + Genie Space) |
| Conversation memory | MemorySaver (per-session) |
| Avg response time | 3-8 seconds (depends on tool) |
| Languages tested | English, Hindi |
| Framework | LangGraph create_react_agent |
| LLM | Llama 4 Maverick (Databricks Foundation Model API) |

---

## 8. Pipeline Scale

| Tier | Tables | Total Rows | Columns |
|------|--------|-----------|---------|
| Bronze | 3 | 750,250 | 52 |
| Silver | 3 | 750,250 | 55 |
| Gold | 6 | ~1.65M | 143 |
| Platinum | 3 | ~6,810 | 35 |
| Context | 7 | ~700 | 42 |
| Views | 13 | computed | - |
| **Total** | **35** | **~3.16M** | **327** |
