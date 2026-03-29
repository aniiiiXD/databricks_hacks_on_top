const PptxGenJS = require("pptxgenjs");

const pptx = new PptxGenJS();

// -- Theme --
const BG_DARK = "0D1117";
const BG_ACCENT = "161B22";
const ACCENT = "58A6FF";
const ACCENT2 = "F78166";
const ACCENT3 = "3FB950";
const WHITE = "FFFFFF";
const GRAY = "8B949E";
const FONT = "Segoe UI";

pptx.author = "BlackIce Team";
pptx.title = "BlackIce: AI-Powered Financial Intelligence for India";
pptx.subject = "Bharat Bricks Hackathon 2026";
pptx.layout = "LAYOUT_WIDE";

// ============================================================
// SLIDE 1: TITLE
// ============================================================
let slide = pptx.addSlide();
slide.background = { color: BG_DARK };

slide.addText("BLACKICE", {
  x: 0.8, y: 1.0, w: 11, h: 1.2,
  fontSize: 48, fontFace: FONT, color: ACCENT, bold: true,
});
slide.addText("AI-Powered Financial Intelligence for India", {
  x: 0.8, y: 2.2, w: 11, h: 0.8,
  fontSize: 24, fontFace: FONT, color: WHITE,
});
slide.addText(
  "Detects UPI fraud using ML  |  Searches RBI regulations in Hindi  |  Matches citizens to 170 government schemes\nAll on Databricks Free Edition",
  {
    x: 0.8, y: 3.3, w: 11, h: 0.9,
    fontSize: 14, fontFace: FONT, color: GRAY, lineSpacingMultiple: 1.5,
  }
);
slide.addText("Bharat Bricks Hackathon 2026  |  IIT Bombay", {
  x: 0.8, y: 6.5, w: 11, h: 0.5,
  fontSize: 14, fontFace: FONT, color: ACCENT2, italic: true,
});

// ============================================================
// SLIDE 2: THE PROBLEM
// ============================================================
slide = pptx.addSlide();
slide.background = { color: BG_DARK };

slide.addText("THE PROBLEM", {
  x: 0.8, y: 0.4, w: 11, h: 0.7,
  fontSize: 32, fontFace: FONT, color: ACCENT, bold: true,
});

const problems = [
  { icon: "16B+", text: "UPI transactions every month \u2014 largest real-time payment network on Earth" },
  { icon: "\u20B9427Cr", text: "lost by SBI alone last year to fraud" },
  { icon: "80+", text: "RBI circulars buried in English legalese \u2014 inaccessible to rural Indians" },
  { icon: "170+", text: "government schemes most citizens don\u2019t know they qualify for" },
];

problems.forEach((p, i) => {
  const y = 1.5 + i * 1.15;
  slide.addText(p.icon, {
    x: 0.8, y, w: 2.2, h: 0.9,
    fontSize: 28, fontFace: FONT, color: ACCENT2, bold: true, valign: "middle",
  });
  slide.addText(p.text, {
    x: 3.0, y, w: 9, h: 0.9,
    fontSize: 18, fontFace: FONT, color: WHITE, valign: "middle",
  });
});

// ============================================================
// SLIDE 3: KEY RESULTS
// ============================================================
slide = pptx.addSlide();
slide.background = { color: BG_DARK };

slide.addText("KEY RESULTS", {
  x: 0.8, y: 0.4, w: 11, h: 0.7,
  fontSize: 32, fontFace: FONT, color: ACCENT, bold: true,
});

const metrics = [
  ["750K+", "Transactions Analyzed"],
  ["1,800+", "Fraud Alerts Flagged"],
  ["8", "Anomaly Patterns"],
  ["5,000", "Sender Risk Profiles"],
  ["80", "RBI Circulars (RAG)"],
  ["170", "Government Schemes"],
  ["150", "Fraud Rings Detected"],
  ["100%", "BhashaBench Score"],
];

metrics.forEach((m, i) => {
  const col = i % 4;
  const row = Math.floor(i / 4);
  const x = 0.5 + col * 3.1;
  const y = 1.6 + row * 2.2;

  slide.addShape(pptx.shapes.ROUNDED_RECTANGLE, {
    x, y, w: 2.8, h: 1.8,
    fill: { color: BG_ACCENT }, rectRadius: 0.15,
    line: { color: "30363D", width: 1 },
  });
  slide.addText(m[0], {
    x, y: y + 0.2, w: 2.8, h: 0.8,
    fontSize: 32, fontFace: FONT, color: ACCENT3, bold: true, align: "center",
  });
  slide.addText(m[1], {
    x, y: y + 1.0, w: 2.8, h: 0.6,
    fontSize: 12, fontFace: FONT, color: GRAY, align: "center",
  });
});

// ============================================================
// SLIDE 4: ARCHITECTURE
// ============================================================
slide = pptx.addSlide();
slide.background = { color: BG_DARK };

slide.addText("4-TIER MEDALLION ARCHITECTURE", {
  x: 0.8, y: 0.4, w: 11, h: 0.7,
  fontSize: 28, fontFace: FONT, color: ACCENT, bold: true,
});

const tiers = [
  { name: "BRONZE", desc: "Auto Loader \u2022 250K UPI txns \u2022 80 RBI circulars \u2022 170 schemes \u2022 PK constraints \u2022 CDF enabled", color: "CD7F32" },
  { name: "SILVER", desc: "DLT EXPECT constraints \u2022 Data quality validation \u2022 Feature derivation \u2022 Time slots \u2022 Risk labels", color: "C0C0C0" },
  { name: "GOLD", desc: "Business-ready tables \u2022 Liquid clustering \u2022 RAG chunking (4000 chars) \u2022 Scheme eligibility JSON", color: "FFD700" },
  { name: "PLATINUM", desc: "IsolationForest + KMeans ensemble \u2022 8 anomaly patterns \u2022 5K sender profiles \u2022 Fraud rings", color: "E5E4E2" },
];

tiers.forEach((t, i) => {
  const y = 1.4 + i * 1.2;
  slide.addShape(pptx.shapes.ROUNDED_RECTANGLE, {
    x: 0.5, y, w: 12.2, h: 1.0,
    fill: { color: BG_ACCENT }, rectRadius: 0.1,
    line: { color: t.color, width: 2 },
  });
  slide.addText(t.name, {
    x: 0.7, y, w: 2.2, h: 1.0,
    fontSize: 18, fontFace: FONT, color: t.color, bold: true, valign: "middle",
  });
  slide.addText(t.desc, {
    x: 3.0, y, w: 9.5, h: 1.0,
    fontSize: 13, fontFace: FONT, color: WHITE, valign: "middle",
  });

  if (i < tiers.length - 1) {
    slide.addText("\u25BC", {
      x: 6, y: y + 0.9, w: 1, h: 0.35,
      fontSize: 14, color: GRAY, align: "center",
    });
  }
});

// Serving layer
slide.addText("SERVING: Dashboard (4 pages)  \u2022  Genie Space (NL\u2192SQL)  \u2022  LangGraph Agent (9 tools + MCP)", {
  x: 0.5, y: 6.2, w: 12.2, h: 0.5,
  fontSize: 13, fontFace: FONT, color: ACCENT, align: "center", italic: true,
});

// ============================================================
// SLIDE 5: ML PIPELINE
// ============================================================
slide = pptx.addSlide();
slide.background = { color: BG_DARK };

slide.addText("ML ENSEMBLE FRAUD DETECTION", {
  x: 0.8, y: 0.4, w: 11, h: 0.7,
  fontSize: 28, fontFace: FONT, color: ACCENT, bold: true,
});

// Three model boxes
const models = [
  { name: "IsolationForest", weight: "0.45", detail: "300 trees\ncontamination=0.02\nAnomaly score (0\u20131)", color: ACCENT },
  { name: "KMeans (k=8)", weight: "0.30", detail: "Distance to center\nNormalized 0\u20131\n95th percentile threshold", color: ACCENT2 },
  { name: "Rule-Based Risk", weight: "0.25", detail: "Amount + time heuristics\nLate-night flags\nWeekend patterns", color: ACCENT3 },
];

models.forEach((m, i) => {
  const x = 0.5 + i * 4.2;
  slide.addShape(pptx.shapes.ROUNDED_RECTANGLE, {
    x, y: 1.4, w: 3.8, h: 2.6,
    fill: { color: BG_ACCENT }, rectRadius: 0.15,
    line: { color: m.color, width: 2 },
  });
  slide.addText(m.name, {
    x, y: 1.5, w: 3.8, h: 0.6,
    fontSize: 16, fontFace: FONT, color: m.color, bold: true, align: "center",
  });
  slide.addText("Weight: " + m.weight, {
    x, y: 2.1, w: 3.8, h: 0.5,
    fontSize: 14, fontFace: FONT, color: WHITE, align: "center",
  });
  slide.addText(m.detail, {
    x: x + 0.2, y: 2.6, w: 3.4, h: 1.2,
    fontSize: 11, fontFace: FONT, color: GRAY, lineSpacingMultiple: 1.4,
  });
});

// Ensemble formula
slide.addShape(pptx.shapes.ROUNDED_RECTANGLE, {
  x: 1.5, y: 4.4, w: 10.2, h: 0.7,
  fill: { color: "1A2332" }, rectRadius: 0.1,
  line: { color: ACCENT, width: 1 },
});
slide.addText("ensemble_score = 0.45 \u00D7 IF + 0.30 \u00D7 KM + 0.25 \u00D7 Rules  \u2192  Risk Tiers: low (<0.3) | medium | high | critical (>0.7)", {
  x: 1.5, y: 4.4, w: 10.2, h: 0.7,
  fontSize: 13, fontFace: FONT, color: WHITE, align: "center", valign: "middle",
});

// Patterns
slide.addText("8 Anomaly Patterns Discovered", {
  x: 0.8, y: 5.4, w: 5, h: 0.5,
  fontSize: 16, fontFace: FONT, color: ACCENT3, bold: true,
});
slide.addText(
  "Late Night High Value (51)  \u2022  Late Night Activity (189)  \u2022  Unusual Category Spend (180)\nHigh Value Transaction  \u2022  Weekend High Spend (51)  \u2022  ML Critical/High Risk (225)  \u2022  General Anomaly (1.09K)",
  {
    x: 0.8, y: 5.9, w: 12, h: 0.8,
    fontSize: 12, fontFace: FONT, color: GRAY, lineSpacingMultiple: 1.4,
  }
);

// ============================================================
// SLIDE 6: AGENT ARCHITECTURE
// ============================================================
slide = pptx.addSlide();
slide.background = { color: BG_DARK };

slide.addText("9-TOOL AI AGENT", {
  x: 0.8, y: 0.4, w: 11, h: 0.7,
  fontSize: 28, fontFace: FONT, color: ACCENT, bold: true,
});

slide.addText("LangGraph ReAct Agent  +  MemorySaver  +  MLflow Responses API", {
  x: 0.8, y: 1.0, w: 11, h: 0.5,
  fontSize: 14, fontFace: FONT, color: GRAY, italic: true,
});

const tools = [
  ["lookup_fraud_alerts", "Parameterized SQL on gold_fraud_alerts_ml", ACCENT],
  ["check_loan_eligibility", "PySpark filter + LLM explanation (5 languages)", ACCENT],
  ["fraud_recovery_guide", "RBI-mandated recovery steps for 6 scam types", ACCENT],
  ["lookup_fraud_rings", "Graph analysis: 150 rings, PageRank hubs", ACCENT],
  ["lookup_sender_profile", "Behavioral analysis + composite risk scores", ACCENT],
  ["get_current_time", "Utility", ACCENT],
  ["Vector Search MCP", "Semantic search over 80 RBI circular chunks", ACCENT2],
  ["Genie Query MCP", "Natural language \u2192 SQL on fraud data", ACCENT2],
  ["Genie Poll MCP", "Async result retrieval from Genie Space", ACCENT2],
];

tools.forEach((t, i) => {
  const y = 1.7 + i * 0.52;
  const isMCP = t[2] === ACCENT2;

  slide.addShape(pptx.shapes.ROUNDED_RECTANGLE, {
    x: 0.5, y, w: 12.2, h: 0.45,
    fill: { color: BG_ACCENT }, rectRadius: 0.05,
    line: { color: "30363D", width: 0.5 },
  });
  slide.addText(isMCP ? "MCP" : "CUSTOM", {
    x: 0.6, y, w: 1.2, h: 0.45,
    fontSize: 9, fontFace: FONT, color: BG_DARK, bold: true, align: "center", valign: "middle",
    fill: { color: t[2] },
  });
  slide.addText(t[0], {
    x: 2.0, y, w: 3.5, h: 0.45,
    fontSize: 13, fontFace: FONT, color: WHITE, bold: true, valign: "middle",
  });
  slide.addText(t[1], {
    x: 5.5, y, w: 7, h: 0.45,
    fontSize: 11, fontFace: FONT, color: GRAY, valign: "middle",
  });
});

// ============================================================
// SLIDE 7: DATABRICKS FEATURES
// ============================================================
slide = pptx.addSlide();
slide.background = { color: BG_DARK };

slide.addText("17 DATABRICKS FEATURES", {
  x: 0.8, y: 0.4, w: 11, h: 0.7,
  fontSize: 28, fontFace: FONT, color: ACCENT, bold: true,
});

const features = [
  "Delta Lake", "Unity Catalog", "Auto Loader", "ETL Pipeline (DLT)",
  "Spark SQL", "Foundation Model API", "Vector Search (MCP)", "Genie Space (MCP)",
  "Lakeview Dashboard", "Metric Views (YAML)", "Databricks SDK", "Change Data Feed",
  "Liquid Clustering", "UC Tags", "Databricks Apps", "FAISS on Volumes", "MCP Protocol",
];

features.forEach((f, i) => {
  const col = i % 4;
  const row = Math.floor(i / 4);
  const x = 0.4 + col * 3.2;
  const y = 1.4 + row * 1.0;

  slide.addShape(pptx.shapes.ROUNDED_RECTANGLE, {
    x, y, w: 3.0, h: 0.75,
    fill: { color: BG_ACCENT }, rectRadius: 0.1,
    line: { color: ACCENT, width: 1 },
  });
  slide.addText(`${i + 1}. ${f}`, {
    x, y, w: 3.0, h: 0.75,
    fontSize: 12, fontFace: FONT, color: WHITE, align: "center", valign: "middle",
  });
});

slide.addText("Every feature has a reason. Every feature is production-grade.", {
  x: 0.8, y: 6.3, w: 11, h: 0.4,
  fontSize: 14, fontFace: FONT, color: ACCENT2, italic: true, align: "center",
});

// ============================================================
// SLIDE 8: HOT/WARM/COLD
// ============================================================
slide = pptx.addSlide();
slide.background = { color: BG_DARK };

slide.addText("HOT / WARM / COLD SERVING", {
  x: 0.8, y: 0.4, w: 11, h: 0.7,
  fontSize: 28, fontFace: FONT, color: ACCENT, bold: true,
});

const servingTiers = [
  { tier: "HOT", latency: "1\u20135 sec", impl: "Agent tools with parameterized SQL via Databricks SDK", access: "Agent Chat UI", color: "F85149" },
  { tier: "WARM", latency: "< 1 sec", impl: "13 viz_* pre-computed views + mv_fraud_summary", access: "Lakeview Dashboard (4 pages)", color: "F0883E" },
  { tier: "COLD", latency: "5\u201330 sec", impl: "Delta tables with liquid clustering on transaction_date", access: "SQL Editor, Notebooks, Genie", color: ACCENT },
];

servingTiers.forEach((t, i) => {
  const y = 1.6 + i * 1.6;
  slide.addShape(pptx.shapes.ROUNDED_RECTANGLE, {
    x: 0.5, y, w: 12.2, h: 1.3,
    fill: { color: BG_ACCENT }, rectRadius: 0.1,
    line: { color: t.color, width: 2 },
  });
  slide.addText(t.tier, {
    x: 0.7, y, w: 1.8, h: 1.3,
    fontSize: 24, fontFace: FONT, color: t.color, bold: true, valign: "middle", align: "center",
  });
  slide.addText(t.latency, {
    x: 2.5, y, w: 1.8, h: 1.3,
    fontSize: 16, fontFace: FONT, color: WHITE, valign: "middle", align: "center",
  });
  slide.addText(t.impl, {
    x: 4.5, y: y + 0.1, w: 8, h: 0.6,
    fontSize: 13, fontFace: FONT, color: WHITE, valign: "middle",
  });
  slide.addText(t.access, {
    x: 4.5, y: y + 0.7, w: 8, h: 0.5,
    fontSize: 11, fontFace: FONT, color: GRAY, valign: "middle",
  });
});

// ============================================================
// SLIDE 9: REAL-TIME STREAMING
// ============================================================
slide = pptx.addSlide();
slide.background = { color: BG_DARK };

slide.addText("REAL-TIME STREAMING PIPELINE", {
  x: 0.8, y: 0.4, w: 11, h: 0.7,
  fontSize: 28, fontFace: FONT, color: ACCENT, bold: true,
});

slide.addText("Not a batch job. A live system.", {
  x: 0.8, y: 1.0, w: 11, h: 0.5,
  fontSize: 16, fontFace: FONT, color: ACCENT2, italic: true,
});

// Pipeline flow
const streamStages = [
  { name: "NEW DATA\nDROPPED", desc: "Auto Loader detects\nnew files automatically", color: ACCENT2 },
  { name: "BRONZE\nINGEST", desc: "Schema inference\nExactly-once semantics", color: "CD7F32" },
  { name: "SILVER\nVALIDATE", desc: "DLT EXPECT constraints\nFeature derivation", color: "C0C0C0" },
  { name: "PLATINUM\nSCORE", desc: "ML ensemble scores\nFraud alerts generated", color: "E5E4E2" },
];

streamStages.forEach((s, i) => {
  const x = 0.3 + i * 3.2;
  slide.addShape(pptx.shapes.ROUNDED_RECTANGLE, {
    x, y: 1.8, w: 2.8, h: 2.2,
    fill: { color: BG_ACCENT }, rectRadius: 0.15,
    line: { color: s.color, width: 2 },
  });
  slide.addText(s.name, {
    x, y: 1.9, w: 2.8, h: 0.9,
    fontSize: 14, fontFace: FONT, color: s.color, bold: true, align: "center", valign: "middle",
  });
  slide.addText(s.desc, {
    x: x + 0.15, y: 2.85, w: 2.5, h: 0.9,
    fontSize: 11, fontFace: FONT, color: GRAY, align: "center", lineSpacingMultiple: 1.3,
  });
  if (i < streamStages.length - 1) {
    slide.addText("\u25B6", {
      x: x + 2.8, y: 2.5, w: 0.4, h: 0.5,
      fontSize: 18, color: ACCENT, align: "center", valign: "middle",
    });
  }
});

// Key points
const streamPoints = [
  "Checkpoint-based incremental processing \u2014 only new data is processed",
  "Exactly-once semantics \u2014 no duplicates, no missed records",
  "Drop a file \u2192 watch it flow through all 3 tiers \u2192 fraud alert generated",
  "Production-ready: switch trigger to processingTime for continuous streaming",
];

streamPoints.forEach((p, i) => {
  const y = 4.5 + i * 0.5;
  slide.addText("\u2713", {
    x: 0.6, y, w: 0.5, h: 0.45,
    fontSize: 14, fontFace: FONT, color: ACCENT3, bold: true, valign: "middle",
  });
  slide.addText(p, {
    x: 1.1, y, w: 11.5, h: 0.45,
    fontSize: 13, fontFace: FONT, color: WHITE, valign: "middle",
  });
});

// ============================================================
// SLIDE 10: PREDICTIVE FRAUD RINGS
// ============================================================
slide = pptx.addSlide();
slide.background = { color: BG_DARK };

slide.addText("FRAUD RINGS \u2014 PREDICTIVE INTELLIGENCE", {
  x: 0.8, y: 0.4, w: 11, h: 0.7,
  fontSize: 28, fontFace: FONT, color: ACCENT, bold: true,
});

slide.addText("Catches fraud before it happens \u2014 even when every individual transaction looks clean.", {
  x: 0.8, y: 1.0, w: 11, h: 0.5,
  fontSize: 14, fontFace: FONT, color: ACCENT2, italic: true,
});

// Stats row
const ringStats = [
  { num: "150", label: "Fraud Rings\nDetected" },
  { num: "93", label: "Money Mule Hubs\n(PageRank)" },
  { num: "57", label: "Circular\nMoney Flows" },
  { num: "\u20B98.68 Cr", label: "Flowing Through\nRings" },
];

ringStats.forEach((s, i) => {
  const x = 0.3 + i * 3.2;
  slide.addShape(pptx.shapes.ROUNDED_RECTANGLE, {
    x, y: 1.7, w: 2.8, h: 1.5,
    fill: { color: BG_ACCENT }, rectRadius: 0.15,
    line: { color: "F85149", width: 1 },
  });
  slide.addText(s.num, {
    x, y: 1.75, w: 2.8, h: 0.75,
    fontSize: 28, fontFace: FONT, color: ACCENT3, bold: true, align: "center", valign: "middle",
  });
  slide.addText(s.label, {
    x, y: 2.5, w: 2.8, h: 0.6,
    fontSize: 10, fontFace: FONT, color: GRAY, align: "center", lineSpacingMultiple: 1.3,
  });
});

// How it works
slide.addText("HOW IT WORKS", {
  x: 0.8, y: 3.6, w: 5, h: 0.5,
  fontSize: 16, fontFace: FONT, color: WHITE, bold: true,
});

const ringPoints = [
  "Graph analysis with NetworkX on sender\u2194receiver transaction networks",
  "Connected components reveal ring structures invisible at transaction level",
  "PageRank identifies money mule hub accounts \u2014 normal individually, suspicious in context",
  "Triangle counting finds tightly-knit groups with abnormal density scores",
  "Accounts flagged and monitored BEFORE the fraud event \u2014 the graph sees what transaction monitoring cannot",
];

ringPoints.forEach((p, i) => {
  const y = 4.2 + i * 0.45;
  slide.addText("\u2022", {
    x: 0.8, y, w: 0.3, h: 0.4,
    fontSize: 12, fontFace: FONT, color: ACCENT2, valign: "middle",
  });
  slide.addText(p, {
    x: 1.2, y, w: 11.5, h: 0.4,
    fontSize: 12, fontFace: FONT, color: WHITE, valign: "middle",
  });
});

// ============================================================
// SLIDE 11: REAL DATA SOURCES
// ============================================================
slide = pptx.addSlide();
slide.background = { color: BG_DARK };

slide.addText("REAL INDIAN DATA", {
  x: 0.8, y: 0.4, w: 11, h: 0.7,
  fontSize: 28, fontFace: FONT, color: ACCENT, bold: true,
});

const dataSources = [
  ["UPI Transactions 2024", "Kaggle (CC0)", "250,000 rows"],
  ["RBI Circulars", "Scraped from rbi.org.in", "80 documents"],
  ["Government Schemes", "myscheme.gov.in", "170 schemes"],
  ["Digital Payment Stats", "RBI / NPCI", "432 rows"],
  ["Bank Fraud Statistics", "Lok Sabha data", "75 banks \u00D7 5 years"],
  ["RBI Complaints", "RBI Annual Report", "48 records"],
  ["PMJDY State Stats", "pmjdy.gov.in", "36 states"],
  ["Internet Penetration", "TRAI / data.gov.in", "36 states"],
  ["Fraud Recovery Guide", "RBI circulars", "6 scam types"],
];

// Header
const headerY = 1.3;
slide.addText("Dataset", { x: 0.5, y: headerY, w: 4.5, h: 0.45, fontSize: 12, fontFace: FONT, color: ACCENT, bold: true });
slide.addText("Source", { x: 5.2, y: headerY, w: 4.0, h: 0.45, fontSize: 12, fontFace: FONT, color: ACCENT, bold: true });
slide.addText("Scale", { x: 9.5, y: headerY, w: 3.0, h: 0.45, fontSize: 12, fontFace: FONT, color: ACCENT, bold: true });

slide.addShape(pptx.shapes.LINE, {
  x: 0.5, y: headerY + 0.45, w: 12.2, h: 0,
  line: { color: "30363D", width: 1 },
});

dataSources.forEach((d, i) => {
  const y = 1.85 + i * 0.5;
  const bg = i % 2 === 0 ? BG_ACCENT : BG_DARK;
  slide.addShape(pptx.shapes.RECTANGLE, { x: 0.5, y, w: 12.2, h: 0.45, fill: { color: bg } });
  slide.addText(d[0], { x: 0.6, y, w: 4.4, h: 0.45, fontSize: 11, fontFace: FONT, color: WHITE, valign: "middle" });
  slide.addText(d[1], { x: 5.2, y, w: 4.0, h: 0.45, fontSize: 11, fontFace: FONT, color: GRAY, valign: "middle" });
  slide.addText(d[2], { x: 9.5, y, w: 3.0, h: 0.45, fontSize: 11, fontFace: FONT, color: ACCENT3, valign: "middle" });
});

slide.addText("8 of 9 datasets are REAL government data. Only UPI transactions are synthetic (no bank releases real txn data).", {
  x: 0.8, y: 6.3, w: 11, h: 0.4,
  fontSize: 12, fontFace: FONT, color: ACCENT2, italic: true, align: "center",
});

// ============================================================
// SLIDE 12: IMPACT + CLOSE
// ============================================================
slide = pptx.addSlide();
slide.background = { color: BG_DARK };

slide.addText("IMPACT", {
  x: 0.8, y: 0.4, w: 11, h: 0.7,
  fontSize: 32, fontFace: FONT, color: ACCENT, bold: true,
});

const impactItems = [
  { num: "\u20B92.17 Cr", text: "at risk detected across 446 anomalous transactions" },
  { num: "150 rings", text: "detected \u2014 catching fraud before it happens" },
  { num: "LIVE", text: "real-time streaming pipeline \u2014 Bronze \u2192 Silver \u2192 Platinum" },
  { num: "170 schemes", text: "indexed for India\u2019s most vulnerable populations" },
  { num: "100%", text: "BhashaBench \u2022 17 features \u2022 3.15M rows \u2022 All on Free Edition" },
];

impactItems.forEach((item, i) => {
  const y = 1.5 + i * 0.95;
  slide.addText(item.num, {
    x: 0.8, y, w: 3.0, h: 0.8,
    fontSize: 28, fontFace: FONT, color: ACCENT3, bold: true, valign: "middle", align: "right",
  });
  slide.addText(item.text, {
    x: 4.2, y, w: 8.5, h: 0.8,
    fontSize: 18, fontFace: FONT, color: WHITE, valign: "middle",
  });
});

slide.addText("BlackIce. Financial intelligence for every Indian.", {
  x: 0.8, y: 6.2, w: 11, h: 0.5,
  fontSize: 20, fontFace: FONT, color: ACCENT, bold: true, align: "center",
});

// ============================================================
// SAVE
// ============================================================
const outPath = "BlackIce-Pitch-Deck.pptx";
pptx.writeFile({ fileName: outPath }).then(() => {
  console.log(`Presentation saved: ${outPath}`);
  console.log("12 slides generated.");
});
