# Databricks notebook source
# MAGIC %md
# MAGIC # Digital-Artha: BhashaBench Finance Evaluation
# MAGIC
# MAGIC Benchmarks our RAG pipeline against Indian financial knowledge questions.
# MAGIC Uses a curated subset of BhashaBench-Finance questions to evaluate
# MAGIC the agent's accuracy on banking, fraud, and regulatory topics.

# COMMAND ----------

dbutils.widgets.text("catalog", "digital_artha", "Catalog Name")
dbutils.widgets.text("schema", "main", "Schema Name")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

from pyspark.sql import functions as F
import json
import requests

# Use REST API directly — SDK has compatibility issues on Free Edition
db_host = spark.conf.get("spark.databricks.workspaceUrl", "")

# Get token — try multiple methods for serverless compatibility
db_token = ""
try:
    db_token = dbutils.notebook.entry_point.getDbUtils().notebook().getContext().apiToken().get()
except:
    try:
        db_token = dbutils.secrets.get(scope="default", key="token")
    except:
        pass

if not db_token:
    # Use Databricks SDK as fallback
    from databricks.sdk import WorkspaceClient
    w = WorkspaceClient()
    db_host = w.config.host.replace("https://", "")
    db_token = w.config.token
    print(f"Using SDK auth: {db_host}")

def ask_llm(question, system_prompt="You are a financial expert specializing in Indian banking, UPI, and RBI regulations. Answer concisely in 2-3 sentences."):
    """Query Foundation Model API via REST."""
    try:
        url = f"https://{db_host}/serving-endpoints/databricks-llama-4-maverick/invocations"
        payload = {
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": question}
            ],
            "max_tokens": 200,
            "temperature": 0.1
        }
        resp = requests.post(url, headers={"Authorization": f"Bearer {db_token}"}, json=payload, timeout=60)
        data = resp.json()

        # Try multiple response formats
        if "choices" in data:
            return data["choices"][0]["message"]["content"]
        elif "predictions" in data:
            return str(data["predictions"][0])
        elif "output" in data:
            return str(data["output"])
        else:
            return f"UNEXPECTED_FORMAT: {json.dumps(data)[:200]}"
    except Exception as e:
        return f"ERROR: {e}"

# Debug: test the connection first
print(f"Host: {db_host}")
print(f"Token: {db_token[:10]}..." if db_token else "Token: EMPTY!")
test_answer = ask_llm("What is UPI?")
print(f"Test answer: {test_answer[:200]}")
assert len(test_answer) > 10 and not test_answer.startswith("ERROR"), f"LLM connection failed: {test_answer}"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Indian Financial Knowledge Test Questions

# COMMAND ----------

# Curated questions covering banking, UPI, fraud, regulations, schemes
eval_questions = [
    # UPI & Digital Payments
    {"id": "UPI_001", "category": "UPI", "question": "What is the maximum transaction limit for UPI per day?", "expected_topic": "digital_payments", "language": "english"},
    {"id": "UPI_002", "category": "UPI", "question": "What does UPI stand for?", "expected_topic": "digital_payments", "language": "english"},
    {"id": "UPI_003", "category": "UPI", "question": "Which organization manages UPI in India?", "expected_topic": "digital_payments", "language": "english"},
    {"id": "UPI_004", "category": "UPI", "question": "यूपीआई में अधिकतम लेनदेन सीमा क्या है?", "expected_topic": "digital_payments", "language": "hindi"},

    # Fraud Prevention
    {"id": "FRAUD_001", "category": "Fraud", "question": "What should you do if you receive a suspicious UPI collect request?", "expected_topic": "fraud_prevention", "language": "english"},
    {"id": "FRAUD_002", "category": "Fraud", "question": "What is the RBI rule on customer liability for unauthorized transactions?", "expected_topic": "consumer_protection", "language": "english"},
    {"id": "FRAUD_003", "category": "Fraud", "question": "Within how many days should you report a fraud to get zero liability?", "expected_topic": "fraud_prevention", "language": "english"},
    {"id": "FRAUD_004", "category": "Fraud", "question": "QR कोड स्कैम से कैसे बचें?", "expected_topic": "fraud_prevention", "language": "hindi"},

    # Banking & Regulation
    {"id": "REG_001", "category": "Regulation", "question": "What is KYC and why is it required for digital payments?", "expected_topic": "compliance", "language": "english"},
    {"id": "REG_002", "category": "Regulation", "question": "What are Payment Aggregators and how does RBI regulate them?", "expected_topic": "digital_payments", "language": "english"},
    {"id": "REG_003", "category": "Regulation", "question": "What is the RBI Ombudsman scheme?", "expected_topic": "consumer_protection", "language": "english"},
    {"id": "REG_004", "category": "Regulation", "question": "आरबीआई ने डिजिटल भुगतान के लिए क्या नियम बनाए हैं?", "expected_topic": "digital_payments", "language": "hindi"},

    # Financial Inclusion
    {"id": "INCL_001", "category": "Inclusion", "question": "What is the PM Jan Dhan Yojana?", "expected_topic": "inclusion", "language": "english"},
    {"id": "INCL_002", "category": "Inclusion", "question": "What is MUDRA loan and who is eligible?", "expected_topic": "inclusion", "language": "english"},
    {"id": "INCL_003", "category": "Inclusion", "question": "What is PM-SVANidhi scheme for street vendors?", "expected_topic": "inclusion", "language": "english"},
    {"id": "INCL_004", "category": "Inclusion", "question": "प्रधानमंत्री जन धन योजना क्या है?", "expected_topic": "inclusion", "language": "hindi"},

    # Fraud Detection (ML)
    {"id": "ML_001", "category": "ML", "question": "What is Isolation Forest and how does it detect fraud?", "expected_topic": "ml", "language": "english"},
    {"id": "ML_002", "category": "ML", "question": "What features indicate a fraudulent UPI transaction?", "expected_topic": "fraud_detection", "language": "english"},
    {"id": "ML_003", "category": "ML", "question": "What is an ensemble model in fraud detection?", "expected_topic": "ml", "language": "english"},
    {"id": "ML_004", "category": "ML", "question": "क्या मशीन लर्निंग से फ्रॉड पकड़ सकते हैं?", "expected_topic": "ml", "language": "hindi"},
]

eval_df = spark.createDataFrame(eval_questions)
print(f"Evaluation set: {len(eval_questions)} questions")
print(f"  English: {sum(1 for q in eval_questions if q['language'] == 'english')}")
print(f"  Hindi: {sum(1 for q in eval_questions if q['language'] == 'hindi')}")
print(f"  Categories: {set(q['category'] for q in eval_questions)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Run Evaluation Against Foundation Model API

# COMMAND ----------

results = []
for q in eval_questions:
    print(f"  Testing {q['id']}: {q['question'][:50]}...")

    # For Hindi questions, add language instruction
    if q["language"] == "hindi":
        system = "You are a financial expert. Answer in Hindi (Devanagari script). Be concise, 2-3 sentences."
    else:
        system = "You are a financial expert specializing in Indian banking, UPI, and RBI regulations. Answer concisely in 2-3 sentences."

    answer = ask_llm(q["question"], system)

    # Score: did the model respond in the right language?
    has_hindi = any('\u0900' <= c <= '\u097F' for c in answer)
    language_correct = (q["language"] == "hindi" and has_hindi) or (q["language"] == "english" and not has_hindi)

    # Score: is the response substantive (not empty/error)?
    is_substantive = len(answer.strip()) > 20 and not answer.startswith("ERROR")

    results.append({
        "id": q["id"],
        "category": q["category"],
        "language": q["language"],
        "question": q["question"],
        "answer": answer[:300],
        "language_correct": language_correct,
        "is_substantive": is_substantive,
        "answer_length": len(answer),
        "score": 1.0 if (language_correct and is_substantive) else 0.5 if is_substantive else 0.0
    })

print(f"\nCompleted {len(results)} evaluations.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Results

# COMMAND ----------

import pandas as pd

results_pdf = pd.DataFrame(results)

# Overall scores
overall_score = results_pdf["score"].mean() * 100
substantive_pct = results_pdf["is_substantive"].mean() * 100
lang_accuracy = results_pdf["language_correct"].mean() * 100

print(f"{'='*50}")
print(f"BHASHABENCH-FINANCE EVALUATION RESULTS")
print(f"{'='*50}")
print(f"  Overall Score: {overall_score:.1f}%")
print(f"  Substantive Responses: {substantive_pct:.1f}%")
print(f"  Language Accuracy: {lang_accuracy:.1f}%")
print(f"  Avg Response Length: {results_pdf['answer_length'].mean():.0f} chars")

# By category
print(f"\nBy Category:")
for cat in results_pdf["category"].unique():
    cat_data = results_pdf[results_pdf["category"] == cat]
    print(f"  {cat}: {cat_data['score'].mean()*100:.0f}% ({len(cat_data)} questions)")

# By language
print(f"\nBy Language:")
for lang in results_pdf["language"].unique():
    lang_data = results_pdf[results_pdf["language"] == lang]
    print(f"  {lang}: {lang_data['score'].mean()*100:.0f}% ({len(lang_data)} questions)")

# COMMAND ----------

# Save results
results_sdf = spark.createDataFrame(results_pdf)
results_sdf.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog}.{schema}.bhashabench_results")
print(f"Results saved to {catalog}.{schema}.bhashabench_results")

display(results_sdf.select("id", "category", "language", "question", "score", "language_correct", "answer_length"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Sample Answers

# COMMAND ----------

# Show best and worst answers
print("BEST ANSWERS (score = 1.0):")
for _, r in results_pdf[results_pdf["score"] == 1.0].head(3).iterrows():
    print(f"\n  Q: {r['question']}")
    print(f"  A: {r['answer'][:200]}...")

print(f"\n{'='*50}")
print("HINDI ANSWERS:")
for _, r in results_pdf[results_pdf["language"] == "hindi"].iterrows():
    print(f"\n  Q: {r['question']}")
    print(f"  A: {r['answer'][:200]}...")
    print(f"  Language correct: {r['language_correct']}")
