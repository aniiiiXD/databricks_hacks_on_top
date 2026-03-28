# Databricks notebook source
# MAGIC %md
# MAGIC # Digital-Artha: Loan Eligibility Engine
# MAGIC
# MAGIC Matches users to government financial inclusion schemes:
# MAGIC 1. **Rule-based matching** — PySpark filter on structured eligibility criteria
# MAGIC 2. **LLM explanation** — Foundation Model API generates human-readable reasoning
# MAGIC 3. **Translation** — Output in user's preferred language

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

dbutils.widgets.text("catalog", "digital_artha", "Catalog Name")
dbutils.widgets.text("schema", "main", "Schema Name")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

from pyspark.sql import functions as F
from databricks.sdk import WorkspaceClient
import mlflow
import json

w = WorkspaceClient()
username = spark.sql("SELECT current_user()").collect()[0][0]
mlflow.set_experiment(f"/Users/{username}/digital-artha-eligibility")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Load Scheme Data

# COMMAND ----------

schemes_df = spark.table(f"{catalog}.{schema}.gold_schemes")
print(f"Total schemes: {schemes_df.count()}")
display(schemes_df.select("scheme_name", "ministry", "target_group", "income_limit", "age_min", "age_max", "occupation", "state").limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Eligibility Matching Engine

# COMMAND ----------

def find_eligible_schemes(
    age: int,
    income: float,
    occupation: str,
    state: str,
    gender: str = "all"
) -> list:
    """
    Find government schemes matching a user's profile.

    Uses PySpark filtering on gold_schemes for fast, scalable matching.
    Returns list of matching schemes with details.
    """
    schemes = spark.table(f"{catalog}.{schema}.gold_schemes")

    # Apply filters — 'all' or empty means no restriction
    matched = (schemes
        # Age filter
        .filter(
            (F.col("age_min") <= age) & (F.col("age_max") >= age)
        )
        # Income filter (0 means no limit)
        .filter(
            (F.col("income_limit") == 0) | (F.col("income_limit") >= income)
        )
        # Gender filter
        .filter(
            (F.col("gender") == "all") | (F.col("gender") == gender.lower())
        )
        # Occupation filter
        .filter(
            (F.col("occupation") == "all") |
            (F.col("occupation").contains(occupation.lower()))
        )
        # State filter
        .filter(
            (F.col("state") == "All India") |
            (F.lower(F.col("state")).contains(state.lower()))
        )
    )

    results = matched.select(
        "scheme_id", "scheme_name", "ministry", "description",
        "eligibility_criteria", "benefits", "target_group",
        "income_limit", "age_min", "age_max", "gender", "occupation", "state",
        "plain_summary", "url"
    ).collect()

    return [row.asDict() for row in results]


# Test
test_schemes = find_eligible_schemes(
    age=25, income=150000, occupation="street_vendor",
    state="Maharashtra", gender="male"
)
print(f"Found {len(test_schemes)} matching schemes")
for s in test_schemes[:5]:
    print(f"  - {s['scheme_name']} ({s['ministry']})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. LLM-Powered Explanation

# COMMAND ----------

def explain_eligibility(user_profile: dict, schemes: list, language: str = "english") -> str:
    """
    Generate a human-readable explanation of why the user qualifies
    for each matched scheme, using the Foundation Model API.
    """
    if not schemes:
        no_match_msg = "Based on your profile, we couldn't find matching schemes. This may be because:\n" \
                       "- Your income exceeds the scheme limits\n" \
                       "- No schemes are available for your state/occupation combination\n" \
                       "- Try adjusting your criteria or check data.gov.in for more schemes."
        return no_match_msg

    # Build scheme summaries for the prompt
    scheme_descriptions = []
    for i, s in enumerate(schemes[:10]):  # Limit to 10 to fit in context
        scheme_descriptions.append(
            f"{i+1}. **{s['scheme_name']}** ({s['ministry']})\n"
            f"   - Benefits: {s.get('benefits', 'N/A')}\n"
            f"   - Eligibility: {s.get('eligibility_criteria', 'N/A')}\n"
            f"   - Income limit: ₹{s.get('income_limit', 0):,.0f}\n"
            f"   - Age range: {s.get('age_min', 0)}-{s.get('age_max', 999)}"
        )

    language_instruction = ""
    if language.lower() in ["hindi", "hi"]:
        language_instruction = "IMPORTANT: Respond entirely in Hindi (Devanagari script). "
    elif language.lower() in ["marathi", "mr"]:
        language_instruction = "IMPORTANT: Respond entirely in Marathi. "
    elif language.lower() in ["tamil", "ta"]:
        language_instruction = "IMPORTANT: Respond entirely in Tamil. "

    prompt = f"""You are Digital-Artha, a financial inclusion advisor for Indian citizens.
{language_instruction}
A user with the following profile is looking for government financial schemes they are eligible for:

**User Profile:**
- Age: {user_profile.get('age', 'N/A')}
- Annual Income: ₹{user_profile.get('income', 0):,.0f}
- Occupation: {user_profile.get('occupation', 'N/A')}
- State: {user_profile.get('state', 'N/A')}
- Gender: {user_profile.get('gender', 'N/A')}

**Matching Schemes:**
{chr(10).join(scheme_descriptions)}

For each scheme:
1. Explain in simple language what the scheme offers
2. Explain specifically why THIS user qualifies based on their profile
3. Mention any key documents they might need to apply
4. Include the application process if known

Be warm, encouraging, and use simple language that a first-generation bank user would understand."""

    with mlflow.start_run(run_name="eligibility_explanation", nested=True):
        mlflow.log_param("user_age", user_profile.get("age"))
        mlflow.log_param("user_income", user_profile.get("income"))
        mlflow.log_param("user_occupation", user_profile.get("occupation"))
        mlflow.log_param("schemes_matched", len(schemes))
        mlflow.log_param("language", language)

        response = w.serving_endpoints.query(
            name="databricks-meta-llama-3-1-70b-instruct",
            messages=[{"role": "user", "content": prompt}],
            max_tokens=1200,
            temperature=0.2
        )

        answer = response.choices[0].message.content
        mlflow.log_metric("response_length", len(answer))

    return answer

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Full Eligibility Pipeline (Combined)

# COMMAND ----------

def check_eligibility(
    age: int,
    income: float,
    occupation: str,
    state: str,
    gender: str = "all",
    language: str = "english"
) -> dict:
    """
    End-to-end eligibility check:
    1. Match schemes via PySpark rules
    2. Generate LLM explanation
    3. Return structured result

    This is the function the agent will call as a tool.
    """
    user_profile = {
        "age": age,
        "income": income,
        "occupation": occupation,
        "state": state,
        "gender": gender,
    }

    # Step 1: Rule-based matching
    matched_schemes = find_eligible_schemes(age, income, occupation, state, gender)

    # Step 2: LLM explanation
    explanation = explain_eligibility(user_profile, matched_schemes, language)

    return {
        "user_profile": user_profile,
        "matched_schemes": [
            {
                "name": s["scheme_name"],
                "ministry": s["ministry"],
                "benefits": s.get("benefits", ""),
                "summary": s.get("plain_summary", ""),
                "url": s.get("url", ""),
            }
            for s in matched_schemes
        ],
        "total_matches": len(matched_schemes),
        "explanation": explanation,
        "language": language,
    }

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Test Full Pipeline

# COMMAND ----------

# Test: Street vendor in Maharashtra
result = check_eligibility(
    age=25,
    income=150000,
    occupation="street_vendor",
    state="Maharashtra",
    gender="male",
    language="english"
)

print(f"Matched {result['total_matches']} schemes:")
for s in result["matched_schemes"]:
    print(f"  - {s['name']} ({s['ministry']})")
print(f"\n--- Explanation ---\n{result['explanation']}")

# COMMAND ----------

# Hindi test
result_hi = check_eligibility(
    age=35,
    income=250000,
    occupation="farmer",
    state="Uttar Pradesh",
    gender="female",
    language="hindi"
)

print(f"Matched {result_hi['total_matches']} schemes")
print(f"\n--- Hindi Explanation ---\n{result_hi['explanation']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Eligibility Engine Complete
# MAGIC
# MAGIC **Functions available:**
# MAGIC - `find_eligible_schemes()` — PySpark rule matching
# MAGIC - `explain_eligibility()` — LLM explanation in any language
# MAGIC - `check_eligibility()` — Full pipeline (used as agent tool)
# MAGIC
# MAGIC **Next step:** Run `06-metric-view.py` for analytics semantic layer.
