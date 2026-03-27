# Databricks notebook source
#Install the required libraries
%pip install openai

# COMMAND ----------

# ============================================
# UC1: DQ EXPLAINER USING DATABRICKS LLM
# ============================================

# Install dependency (run once)
# %pip install openai
# dbutils.library.restartPython()

from openai import OpenAI
from pyspark.sql.functions import current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, LongType

# ============================================
# STEP 1: LLM CONNECTION
# ============================================
DATABRICKS_TOKEN = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
WORKSPACE_URL = spark.conf.get("spark.databricks.workspaceUrl")

client = OpenAI(
    api_key=DATABRICKS_TOKEN,
    base_url=f"https://{WORKSPACE_URL}/serving-endpoints"
)

MODEL_NAME = "databricks-gpt-oss-20b"

# ============================================
# STEP 2: READ DQ ISSUES
# ============================================
dq_df = spark.table("bricksquad.silver.dq_issues")

# Convert to Pandas (small dataset - OK)
dq_pd = dq_df.toPandas()

# FIX: Add issue_id (since not present in DLT)
dq_pd["issue_id"] = range(1, len(dq_pd) + 1)

# ============================================
# STEP 3: LLM FUNCTION
# ============================================
def generate_explanation(row):

    prompt = f"""
    You are a business data analyst.

    Explain the following data quality issue in plain English.

    Details:
    - Table: {row['table_name']}
    - Issue: {row['rule_name']}
    - Affected Records: {row['affected_records']}
    - Severity: {row['severity']}

    Provide:
    1. What was found
    2. Why it matters to the business
    3. What caused it
    4. What was done to fix it
    5. What should be done to prevent it

    Keep it simple and business-friendly.
    """

    try:
        response = client.chat.completions.create(
            model=MODEL_NAME,
            messages=[{"role": "user", "content": prompt}],
            temperature=0.2
        )

        explanation = response.choices[0].message.content
        status = "SUCCESS"

    except Exception as e:
        explanation = str(e)
        status = "FAILED"

    return explanation, status

# ============================================
# STEP 4: GENERATE EXPLANATIONS
# ============================================
results = []

for _, row in dq_pd.iterrows():
    explanation, status = generate_explanation(row)

    results.append((
        str(row["issue_id"]),
        row["table_name"],
        row["rule_name"],
        row["severity"],
        int(row["affected_records"]),
        explanation,
        MODEL_NAME,
        status
    ))

# ============================================
# STEP 5: DEFINE SCHEMA
# ============================================
schema = StructType([
    StructField("issue_id", StringType(), True),
    StructField("table_name", StringType(), True),
    StructField("rule_name", StringType(), True),
    StructField("severity", StringType(), True),
    StructField("affected_records", LongType(), True),
    StructField("ai_explanation", StringType(), True),
    StructField("model_name", StringType(), True),
    StructField("generation_status", StringType(), True)
])

# ============================================
# STEP 6: CREATE SPARK DF
# ============================================
result_df = spark.createDataFrame(results, schema=schema)

# Add timestamp
result_df = result_df.withColumn(
    "generated_timestamp",
    current_timestamp()
)

# ============================================
# STEP 7: WRITE TO GOLD TABLE
# ============================================
result_df.write.mode("overwrite") \
    .saveAsTable("bricksquad.gold.dq_explanation_report")

# COMMAND ----------

#Output data
spark.sql("SELECT * FROM bricksquad.gold.dq_explanation_report").show()

# COMMAND ----------


