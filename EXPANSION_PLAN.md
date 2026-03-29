# BlackIce Expansion Plan — Final

## Vision
A financial intelligence platform that detects fraud rings, helps fraud victims navigate recovery, connects rural Indians to government schemes, and monitors the health of the entire UPI ecosystem — all powered by real Indian data on Databricks.

---

## Data Sources (What We Have + What to Add)

### Already Loaded
| Dataset | Rows | Source |
|---------|------|--------|
| UPI Transactions | 250K | Kaggle (real, with fraud labels) |
| RBI Circulars | 80 | Scraped from rbi.org.in |
| Gov Schemes | 170 | OpenNyAI / myscheme.gov.in |

### To Add (fetch tonight)
| Dataset | Source | Why |
|---------|--------|-----|
| Indian State GeoJSON | datameet GitHub | State-level fraud heatmap |
| UPI Monthly Statistics | NPCI (npci.org.in) | Show UPI growth context |
| Bank IFSC Data | RBI open data | Map transactions to bank branches |
| Consumer Complaints | data.gov.in (RBI ombudsman) | What do fraud victims complain about? |
| Financial Literacy Stats | data.gov.in | Show financial inclusion gaps by state |

---

## New Notebooks to Build

### 1. `10-fraud-rings.py` — Graph-Based Fraud Ring Detection
- Build sender↔receiver transaction graph (NetworkX)
- Connected components → identify fraud rings
- PageRank → find money mule hubs
- Triangle count → tightly-knit suspicious groups
- Write: `platinum_fraud_rings`, `platinum_sender_profiles`

### 2. `11-streaming-simulation.py` — Live Scoring Demo
- Split UPI CSV into 50 micro-batch files
- Auto Loader with `trigger(processingTime='30 seconds')`
- Each batch scored in real-time by the ensemble model
- Writes to `live_fraud_alerts` table
- Dashboard auto-refreshes showing new alerts

### 3. `12-data-quality.py` — DQ Observatory + UC Governance
- Add UC tags to every table (domain, tier, pii)
- Add liquid clustering to gold tables
- Create materialized view `mv_fraud_stats_24h` (warm tier)
- Create `platinum_sender_profiles` (per-sender aggregates)
- Create `platinum_merchant_profiles` (per-merchant aggregates)
- Query DLT event log for EXPECT pass/fail rates

### 4. `13-human-impact.py` — Fraud Victim Support + Financial Inclusion
- What happens when someone is defrauded? Build a "fraud recovery guide"
- Match fraud patterns to specific RBI complaint categories
- "If you were scammed via QR code, here's your RBI-mandated recovery process"
- Combine with loan eligibility: "After recovering, here are schemes to rebuild"
- Geospatial: which states have highest fraud + lowest scheme awareness?

---

## Dashboard Vision (4 Pages)

### Page 1: Fraud Command Center
- Counters: Total Txns | Flagged | Fraud Rate | Avg Risk Score
- Bar: Fraud by Category
- Heatmap: Fraud by Hour × Day of Week
- Line: Fraud trend over time
- Table: Top 20 highest-risk transactions

### Page 2: Fraud Ring Intelligence
- Network graph visualization (or table of rings)
- Counter: Rings Detected | Accounts Involved | Total ₹ at Risk
- Table: Top fraud rings with size, total amount, avg risk score
- Bar: Ring size distribution
- PageRank: Top 10 hub accounts (money mules)

### Page 3: Financial Inclusion & Victim Support
- Geo heatmap: Fraud rate by Indian state
- Bar: Schemes available by ministry
- Table: "If you're a victim of X fraud, do Y" recovery guide
- Counter: Schemes Available | States Covered | Avg Income Limit
- Funnel: Population → Bank Account → UPI User → Fraud Aware → Scheme Eligible

### Page 4: Data Quality & Pipeline Health
- DLT constraint pass rates (bar chart over time)
- Data freshness per table (when was each table last updated)
- Row volume trend (ingestion over time)
- Table lineage summary
- UC tags compliance (% of tables with tags)

---

## Agent Expansion (New Tools)

| Tool | What User Asks | What It Does |
|------|---------------|-------------|
| `lookup_fraud_alerts` | "Show me fraud alerts" | Queries gold_fraud_alerts_ml |
| `check_loan_eligibility` | "Schemes for a farmer in UP" | PySpark filter + LLM |
| `search_rbi_circulars` | "RBI rules on UPI refund" | FAISS RAG |
| `get_fraud_rings` ← NEW | "Show fraud rings" | Queries platinum_fraud_rings |
| `fraud_recovery_guide` ← NEW | "I was scammed, what do I do?" | RBI recovery process + relevant circulars |
| `sender_risk_profile` ← NEW | "Is sender_123@upi risky?" | Queries platinum_sender_profiles |

---

## Implementation Order

| # | What | Time | File |
|---|------|------|------|
| 1 | Fetch additional datasets | 30 min | Script |
| 2 | Fraud ring detection | 1.5 hr | `10-fraud-rings.py` |
| 3 | Platinum layer + DQ + UC governance | 1 hr | `12-data-quality.py` |
| 4 | Streaming simulation | 1 hr | `11-streaming-simulation.py` |
| 5 | Human impact / victim support | 1 hr | `13-human-impact.py` |
| 6 | Dashboard expansion (4 pages) | 1 hr | Databricks UI |
| 7 | Agent new tools | 1 hr | Update agent.py |
| 8 | README + architecture diagram | 30 min | README.md |
