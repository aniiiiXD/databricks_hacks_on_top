# UI Test Guide — Pre-Demo Checklist

Run through this before presenting to judges.

---

## 1. Dashboard (Databricks)

Open your published dashboard. Test each page:

### Page 1: Fraud Summary
- [ ] Counters show: Total Txns, Flagged, Fraud Rate % (should be non-zero)
- [ ] Bar chart: Fraud Rate by Category (should show multiple categories)
- [ ] Line chart: Monthly Fraud Rate Trend (should show variation)
- [ ] Heatmap: Hour × Day (should show darker cells in late night / weekends)
- [ ] Risk Score Distribution bar chart (should show most in 0.0-0.1 bucket)

### Page 2: Patterns
- [ ] Counters: Patterns Found (8), Amount at Risk
- [ ] Horizontal bar: patterns by frequency (General Anomaly highest)
- [ ] Heatmap: Category × Time (Education × late_night should be darkest)
- [ ] Table: Top 20 Risky Senders (composite_risk values 0.2-0.4)

### Page 3: India Story
- [ ] UPI Growth line (should curve up steeply)
- [ ] Bank Fraud Losses bar (SBI highest at ~₹427 Cr)
- [ ] RBI Complaint Surge stacked bar (Mobile Banking growing fastest)
- [ ] State Vulnerability horizontal bar (Rajasthan highest)

### Page 4: Help & Recovery
- [ ] Recovery guide table (6 fraud types)
- [ ] Pipeline inventory table (15+ tables with tiers)
- [ ] Counters: Schemes (170), RBI Circulars (68-80)

---

## 2. Genie Space

Open your Genie Space. Try these exact queries:

- [ ] `What is the fraud rate by category?` → should return table
- [ ] `Show me the top 10 highest risk transactions` → should return table
- [ ] `How many transactions are flagged as fraud?` → should return number
- [ ] `What is the total amount at risk?` → should return number
- [ ] `Show fraud count by time of day` → should return table/chart

---

## 3. Agent + Gradio UI

Start: `uv run start-server` (Terminal 1) + `python3 chat_ui.py` (Terminal 2)

### Tab 1: Command Center
- [ ] KPIs load automatically on page open
- [ ] Click "Search Alerts" with default settings → table appears
- [ ] Change min score to 0.5 → fewer results
- [ ] Click "Show Anomaly Patterns" → 8 patterns listed
- [ ] Click "Show Risky Senders" → table with risk scores

### Tab 2: Ask Agent
- [ ] Type: `Show me fraud alerts` → agent responds with alert data
- [ ] Type: `I was scammed via QR code` → recovery steps appear
- [ ] Type: `What are RBI rules on UPI fraud?` → cited RBI circular content
- [ ] Type: `Schemes for a farmer in UP earning 2 lakh` → scheme matches
- [ ] Type: `यूपीआई फ्रॉड से कैसे बचें?` → Hindi response
- [ ] Click an example query → works

### Tab 3: India Story
- [ ] Click "Load India's Digital Payment Story" → UPI stats + vulnerability
- [ ] Click "Bank Fraud Losses" → bank-wise table

### Tab 4: Scheme Finder
- [ ] Set: Age=25, Income=150000, Occupation=farmer, State=Maharashtra
- [ ] Click "Find Matching Schemes" → agent returns matches
- [ ] Change to gender=female → different/additional results
- [ ] Change language to Hindi → response in Hindi

### Tab 5: Fraud Recovery
- [ ] Select "QR Code Scam" → detailed recovery steps
- [ ] Select "All Types" → all 6 types listed

### Tab 6: Architecture
- [ ] Diagram renders correctly

---

## 4. Notebooks (if judges re-run)

These should work without errors on a fresh run:
- [ ] `01-data-ingestion.py` (set clear_checkpoints=yes)
- [ ] `02-run-transforms.py`
- [ ] `03-fraud-detection.py`
- [ ] `10-anomaly-patterns.py`

---

## 5. Pre-Demo Checklist

Before presenting:
- [ ] Dashboard is published and accessible
- [ ] Agent server running (`uv run start-server`)
- [ ] Gradio UI running (`python3 chat_ui.py`)
- [ ] Copy the public Gradio URL (https://xxxxx.gradio.live)
- [ ] Test one query in each tab
- [ ] Have backup screenshots ready (in case live demo fails)
