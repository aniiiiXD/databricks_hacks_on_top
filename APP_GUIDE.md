# BlackIce App — What It Is & How to Demo It

## Starting the App

Terminal 1:
```bash
cd 08-agent && uv run start-server
```

Terminal 2:
```bash
cd 08-agent && python3 chat_ui.py
```

App opens at: **http://localhost:7860**
Public URL: **https://xxxxx.gradio.live** (share with judges)

---

## 6 Tabs — What Each Does

### Tab 1: COMMAND CENTER
**What it does:** Queries Databricks SQL DIRECTLY (not through agent). Shows live fraud data.

**Demo flow:**
1. KPIs auto-load on page open (total txns, flagged, fraud rate)
2. Click "Search Alerts" → table of fraud alerts with risk scores
3. Slide min score to 0.5 → fewer, higher-risk results
4. Click "Show Anomaly Patterns" → 7 named patterns with ₹ at risk
5. Click "Show Risky Senders" → top senders by composite risk

**What to say:** "This tab queries our Platinum layer directly via Databricks SQL API. No agent, no LLM — pure data."

### Tab 2: ASK BLACKICE (Agent Chat)
**What it does:** LangGraph agent with 9 tools. Remembers conversation.

**Demo queries (in order):**
1. `Show me the top 5 fraud alerts` → uses lookup_fraud_alerts tool
2. `I was scammed via QR code, what should I do?` → uses fraud_recovery_guide tool
3. `What are RBI guidelines on UPI fraud?` → uses Vector Search MCP (searches circulars)
4. `I'm a 25 year old farmer from UP earning 2 lakh` → uses check_loan_eligibility tool
5. `Show me fraud rings` → uses lookup_fraud_rings tool
6. `यूपीआई फ्रॉड से कैसे बचें?` → Hindi response

**What to say:** "9 tools — 6 custom SQL tools plus 3 MCP connections to Vector Search and Genie Space. Conversation memory via LangGraph MemorySaver."

### Tab 3: INDIA STORY
**What it does:** Shows macro context — UPI growth, bank fraud, state vulnerability.

**Demo flow:**
1. Click "Load India's Digital Payment Story" → vulnerability index + stats
2. Click "Bank Fraud Losses" → bank-wise losses table

**What to say:** "Real data from RBI, NPCI, Lok Sabha, PMJDY. The vulnerability index combines fraud rate, internet penetration, and Jan Dhan coverage."

### Tab 4: SCHEME FINDER (Form-based)
**What it does:** Fill a form → get matching schemes. NOT chat — structured input.

**Demo flow:**
1. Set: Age=25, Income=150000, Occupation=farmer, State=Maharashtra
2. Click "Find Matching Schemes" → agent returns matches with explanation
3. Change language to Hindi → response in Hindi
4. Change gender to female → different schemes appear

**What to say:** "170 real schemes from myscheme.gov.in. PySpark rule matching + LLM explanation in 5 languages."

### Tab 5: FRAUD RECOVERY
**What it does:** Select a fraud type → get RBI-mandated recovery steps.

**Demo flow:**
1. Select "QR Code Scam" → detailed steps, who to report, 3-day limit
2. Select "SIM Swap Fraud" → different steps, telecom provider contact
3. Select "All Types" → full guide

**What to say:** "Based on actual RBI circulars. Time limits, liability rules, reporting contacts — all mandated by RBI."

### Tab 6: ARCHITECTURE
**What it does:** Shows system overview.

**When to show:** Only if judges ask "walk me through the architecture."

---

## Agent Tools (9 Total)

| Tool | Trigger Query | Response |
|------|--------------|----------|
| lookup_fraud_alerts | "show fraud alerts" | Table of flagged transactions |
| check_loan_eligibility | "schemes for a farmer" | Matching schemes + LLM explanation |
| fraud_recovery_guide | "I was scammed" | RBI recovery steps |
| lookup_fraud_rings | "show fraud rings" | Ring metadata from graph analysis |
| lookup_sender_profile | "risk profile for sender_123" | Sender behavioral analysis |
| get_current_time | "what time is it" | Current timestamp |
| Vector Search MCP | "RBI rules on UPI" | Semantic search over circulars |
| Genie query MCP | "fraud rate by category" | NL → SQL on fraud data |
| Genie poll MCP | (internal) | Gets async Genie results |

---

## If Something Breaks During Demo

| Problem | Fix |
|---------|-----|
| App won't start | Check .env has DATABRICKS_TOKEN, WAREHOUSE_ID, GENIE_SPACE_ID |
| Agent returns error | Show the Command Center tab instead (direct SQL, always works) |
| Hindi doesn't work | Say "multilingual embeddings support it, Maverick sometimes defaults to English" |
| Slow response | "Foundation Model API on Free Edition has fair-use throttling" |
| MCP tools not loading | Show the code, explain architecture. "MCP connects to Vector Search and Genie" |

---

## Pre-Demo Checklist (5 minutes before)

- [ ] `uv run start-server` running in Terminal 1
- [ ] `python3 chat_ui.py` running in Terminal 2
- [ ] Copy the gradio.live public URL
- [ ] Test one query: "show fraud alerts"
- [ ] Dashboard published in Databricks
- [ ] Genie Space accessible
- [ ] Have FINAL_PITCH.md open for reference
