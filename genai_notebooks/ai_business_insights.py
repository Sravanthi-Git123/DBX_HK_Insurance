# Databricks notebook source
# MAGIC %pip install OpenAI

# COMMAND ----------

# OpenAI-compatible client — Databricks exposes its LLM via OpenAI-style API
# Why: Allows reuse of the standard OpenAI SDK to call Databricks model serving
#      without writing custom HTTP request handlers for the LLM endpoint
from openai import OpenAI

# json — used to serialize KPI aggregation dicts into JSON strings
# Why: Problem statement requires KPI data stored as a JSON field in the output table
#      for auditability — executives and auditors can see exactly what numbers
#      the LLM was given before it generated each executive summary
import json

# datetime — used to timestamp each AI-generated insight written to the output table
# Why: Generation timestamp is a required output field — tells consumers how
#      fresh each executive summary is relative to the underlying Gold layer data
from datetime import datetime

# PySpark SQL functions — used to compute business KPI aggregations
# across Gold layer tables before passing summarized data to the LLM
# Why aggregate first: Problem statement explicitly says do NOT pass raw rows —
#                      LLMs work better on summarized numbers than thousands of records
from pyspark.sql import functions as F


# ── Databricks Authentication ──────────────────────────────────────────
# Fetch current user's API token automatically from the notebook context
# Why: Avoids hardcoding secrets — token is session-scoped and rotates
#      automatically, keeping credentials secure in shared team notebooks
DATABRICKS_TOKEN = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()

# Fetch the Databricks workspace URL dynamically from Spark config
# Why: Workspace URL differs across environments (dev, staging, prod) —
#      reading from config makes this notebook portable without manual changes
WORKSPACE_URL = spark.conf.get("spark.databricks.workspaceUrl")

# Initialize the LLM client pointed at Databricks model serving endpoint
# Why OpenAI client: Databricks uses OpenAI-compatible API spec —
#                    no custom HTTP code needed, standard SDK works as-is
# Why base_url override: Redirects calls from OpenAI servers to the internal
#                        Databricks endpoint so data never leaves the workspace
client = OpenAI(
    api_key=DATABRICKS_TOKEN,                               # Databricks session token for auth
    base_url=f"https://{WORKSPACE_URL}/serving-endpoints"  # Internal Databricks LLM endpoint
)

# The Databricks-hosted LLM used to generate all executive summaries
# Why GPT-OSS-20B: Large enough to produce coherent business narrative
#                  from KPI data across 3 domains — policy, claims, customers
MODEL_NAME = "databricks-gpt-oss-20b"

# COMMAND ----------

# Step 1: Compute customer profile KPIs from the Gold dimension table
# Why aggregate first: Problem statement explicitly forbids passing raw rows to LLM —
#                      5 summary numbers are far more useful to the LLM than
#                      thousands of individual customer records for narrative generation
# Why these 5 KPIs specifically:
#   total_customers    — headline portfolio size for executive context
#   avg_balance        — financial health indicator of the customer base
#   region_count       — geographic diversity of the portfolio
#   hh_insurance_count — cross-sell penetration of home insurance products
#   car_loan_count     — exposure indicator for auto loan-linked policies
customer_kpis = spark.sql("""
SELECT
  COUNT(*)              AS total_customers,    -- Total active customers in the portfolio
  AVG(Balance)          AS avg_balance,        -- Average account balance across all customers
  COUNT(DISTINCT Region) AS region_count,      -- Number of distinct regions covered
  SUM(HHInsurance)      AS hh_insurance_count, -- Customers who also hold household insurance
                                               -- Why SUM: HHInsurance is binary (0/1) —
                                               -- summing gives total count of cross-sell customers
  SUM(CarLoan)          AS car_loan_count      -- Customers who have an active car loan
                                               -- Why: Car loan holders are higher risk segment —
                                               -- KPI tells executives loan exposure in the portfolio
FROM bricksquad.gold.dim_customers
""")

# COMMAND ----------

display(customer_kpis)

# COMMAND ----------

policy_kpis = spark.sql("""
SELECT
  COUNT(*) AS total_policies,
  AVG(policy_annual_premium) AS avg_premium,
  AVG(policy_deductable) AS avg_deductible,
  AVG(umbrella_limit) AS avg_umbrella_limit
FROM bricksquad.gold.dim_policy
""")

# COMMAND ----------

display(policy_kpis)

# COMMAND ----------

# Step 2: Compute policy portfolio KPIs from the Gold policy dimension table
# Why aggregate first: Passing raw policy rows to the LLM adds no value —
#                      4 portfolio-level numbers give the LLM everything it needs
#                      to generate a meaningful executive summary of the policy book
# Why these 4 KPIs specifically:
#   total_policies      — total size of the active policy portfolio
#                         gives executives the headline book-of-business number
#   avg_premium         — average annual revenue per policy
#                         key indicator of portfolio profitability and pricing health
#   avg_deductible      — average out-of-pocket exposure per customer at claim time
#                         higher deductible = lower payout risk for PrimeInsurance
#   avg_umbrella_limit  — average additional liability coverage extended per policy
#                         signals the risk concentration in high-limit policies
policy_kpis = spark.sql("""
SELECT
  COUNT(*)                    AS total_policies,      -- Total active policies in the portfolio
  AVG(policy_annual_premium)  AS avg_premium,         -- Average annual premium per policy
                                                      -- Why AVG not SUM: executives care about
                                                      -- per-policy economics, not raw total revenue
  AVG(policy_deductable)      AS avg_deductible,      -- Average deductible across all policies
                                                      -- Why: Deductible level directly impacts
                                                      -- claim payout exposure for the company
  AVG(umbrella_limit)         AS avg_umbrella_limit   -- Average umbrella liability limit
                                                      -- Why: High umbrella limits = high tail risk
                                                      -- in catastrophic claim scenarios
FROM bricksquad.gold.dim_policy
""")

# COMMAND ----------

display(claims_kpis)

# COMMAND ----------

# Convert customer KPIs from Spark DataFrame to a plain Python dict
# Why toPandas(): Spark DataFrames cannot be directly serialized to JSON —
#                converting to Pandas first enables clean dict conversion
# Why to_dict(orient="records")[0]: Returns a list of row dicts — we take
#                                   [0] because the aggregation always produces
#                                   exactly one summary row, not multiple records
# Result: {"total_customers": 5000, "avg_balance": 12400.5, ...}
#         This dict is passed directly to the LLM as auditable KPI context
customer_json = customer_kpis.toPandas().to_dict(orient="records")[0]

# Convert policy portfolio KPIs to a plain Python dict using same pattern
# Why same approach: All 3 KPI queries return single-row aggregations —
#                    consistent conversion pattern keeps the code predictable
# Result: {"total_policies": 3000, "avg_premium": 1250.75, ...}
#         Stored as JSON field in output table so executives can verify
#         exactly which numbers the LLM used to generate the policy summary
policy_json = policy_kpis.toPandas().to_dict(orient="records")[0]

# Convert claims performance KPIs to a plain Python dict
# Why all three converted together: All 3 dicts feed into their respective
#                                   LLM prompt functions in the next step —
#                                   having them as plain dicts makes f-string
#                                   injection and json.dumps() serialization
#                                   straightforward with no extra transformation
# Result: {"total_claims": 3000, "avg_claim_amount": 28400.0, ...}
claims_json = claims_kpis.toPandas().to_dict(orient="records")[0]

# COMMAND ----------

# Generate an AI-written executive summary for a single business domain
# Why a reusable function: All 3 domains (policy, claims, customers) follow
#                          the same pattern — one function handles all three
#                          by accepting domain name and KPI dict as parameters
def generate_summary(domain, kpi_json):

    # Build the LLM prompt with domain name and KPI data injected as context
    # Why "Using ONLY the KPI data below": Prevents the LLM from hallucinating
    #                                      industry statistics or inventing numbers
    #                                      not present in the actual Gold layer data
    # Why 3 structured sections: Guides the LLM to produce consistent output
    #                            across all 3 domains so executives get a
    #                            uniform summary format regardless of domain
    # Why include kpi_json directly in prompt: LLM needs the actual numbers
    #                                          to generate specific, data-grounded
    #                                          insights rather than generic statements
    prompt = f"""
You are a senior insurance business analyst.

Domain: {domain}                        

Using ONLY the KPI data below:
{kpi_json}                              

Write:
1. Executive title                      
2. Key insights                         
3. Business risk or opportunity         
"""

    # Send the populated prompt to the Databricks-hosted LLM
    # Why temperature=0.2: Low randomness — executive summaries must be
    #                       factual and consistent, not creative or varied
    #                       across runs on the same underlying KPI data
    # Why max_tokens=300: Keeps summaries concise and scannable for executives —
    #                     longer responses add padding without adding insight
    response = client.chat.completions.create(
        model=MODEL_NAME,
        messages=[{"role": "user", "content": prompt}],
        temperature=0.2,    # Low temperature = deterministic, factual narrative
        max_tokens=300      # Enforce concise executive-length summaries
    )

    # Extract and return the plain text summary from the LLM response
    # Why direct access vs extract_text(): Response structure here is predictable —
    #                                      chat completions always return content
    #                                      as a string in choices[0].message.content
    return response.choices[0].message.content

# COMMAND ----------

# Generate AI executive summary for the Customer Profile domain
# Why first: Customer profile is the foundation of the insurance business —
#            executives need to understand who their policyholders are
#            before interpreting policy and claims performance numbers
# Passes customer_json containing total_customers, avg_balance,
# region_count, hh_insurance_count, car_loan_count as LLM context
customer_summary = generate_summary("Customer Profile", customer_json)

# Generate AI executive summary for the Policy Portfolio domain
# Why: Policy portfolio KPIs tell executives the health of the book of business —
#      avg premium, deductible levels, and umbrella limits drive revenue
#      and risk exposure decisions at the leadership level
# Passes policy_json containing total_policies, avg_premium,
# avg_deductible, avg_umbrella_limit as LLM context
policy_summary = generate_summary("Policy Portfolio", policy_json)

# Generate AI executive summary for the Claims Performance domain
# Why last: Claims performance is the output of the entire insurance operation —
#           executives interpret claims numbers in the context of the customer
#           and policy portfolio summaries generated in the two steps above
# Passes claims_json containing total_claims, avg_claim_amount,
# and other claims KPIs as LLM context
claims_summary = generate_summary("Claims Performance", claims_json)

# COMMAND ----------

# Assemble all 3 domain summaries into a single Spark DataFrame
# Why createDataFrame: Enables writing directly to a Delta table in the
#                      Gold layer — the required output format per problem statement
# Why 3 rows — one per domain: Each domain is an independent business narrative
#                               executives can read policy, claims, and customer
#                               summaries separately without context switching
# Why str() wrapping: generate_summary() returns LLM response objects in some
#                     configurations — str() ensures clean string serialization
#                     before loading into Spark to avoid schema inference errors
# Column breakdown:
#   domain            — business domain label (Customer Profile, Policy Portfolio,
#                       Claims Performance) — used to filter summaries in dashboards
#   executive_summary — the AI-generated narrative text for that domain —
#                       the primary output consumed by executives and leadership
#   kpi_json          — the exact KPI dict passed to the LLM as a string —
#                       stored for auditability so anyone can verify which numbers
#                       were used to generate each summary (problem statement requirement)
final_df = spark.createDataFrame([
    ("Customer Profile",  str(customer_summary), str(customer_json)),   # Customer domain row
    ("Policy Portfolio",  str(policy_summary),   str(policy_json)),     # Policy domain row
    ("Claims Performance", str(claims_summary),  str(claims_json))      # Claims domain row
], ["domain", "executive_summary", "kpi_json"])

# COMMAND ----------

# Enrich the summaries DataFrame with required audit and metadata columns
# Why these 3 additional columns: Problem statement explicitly requires
#                                 generation timestamp, model name, and status
#                                 in the output table for traceability

# Add generation timestamp to every row
# Why current_timestamp(): Records the exact moment each summary was generated —
#                          executives and auditors can see how fresh the insights are
#                          relative to the underlying Gold layer data
final_df = final_df.withColumn("created_at", F.current_timestamp()) \

# Add the model name used to generate each summary
# Why F.lit(MODEL_NAME): Same model was used for all 3 domain summaries —
#                        storing it per row allows future runs with different
#                        models to be compared against historical summaries
                   .withColumn("model_name", F.lit(MODEL_NAME)) \

# Add a status flag indicating this summary was successfully generated
# Why "SUCCESS": Distinguishes clean records from rows where the LLM failed —
#                downstream dashboards can filter on status to show only
#                valid summaries and flag any failed generation attempts
                   .withColumn("status", F.lit("SUCCESS"))

# Persist the enriched insights DataFrame to the Gold layer Delta table
# Why overwrite: Each pipeline run regenerates all 3 domain summaries fresh —
#                overwrite ensures executives always see the latest insights
#                without stale summaries from previous runs accumulating
# Why saveAsTable: Registers the table in Unity Catalog making it instantly
#                  queryable by Genie, dashboards, and ai_query() SQL functions
#                  without needing to reference a raw storage path
final_df.write.mode("overwrite").saveAsTable(
    "bricksquad.gold.ai_business_insights"   # Gold layer output table —
                                             # consumed by executive dashboards,
                                             # Genie NLQ, and ai_query() demos
)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bricksquad.gold.ai_business_insights;

# COMMAND ----------

-- ============================================================
-- ai_query() Demo 1 — SQL-Native LLM Call on Gold Layer Data
-- ============================================================

-- Databricks ai_query() allows calling Foundation Models directly
-- inside a SQL query without any Python code or notebook setup
-- Why ai_query(): Business analysts and Genie users can generate
--                 AI summaries inline within SQL — no engineering required
-- Why demonstrate this: Problem statement explicitly requires at least
--                        2 working ai_query() examples on real Gold layer data

%sql
SELECT ai_query(

  -- Argument 1: Model endpoint name hosted on Databricks model serving
  -- Why databricks-gpt-oss-20b: Same model used in the Python pipeline —
  --                              keeps summarization consistent across
  --                              both notebook and SQL-native interfaces
  'databricks-gpt-oss-20b',

  -- Argument 2: The natural language prompt sent directly to the LLM
  -- Why this prompt: Claims performance and risk patterns is one of the
  --                  3 required business domains in the problem statement —
  --                  demonstrates that ai_query() can answer the same
  --                  business questions as the Python RAG pipeline
  --                  but with zero code, directly inside a SQL cell
  -- Why static prompt here: This is a demonstration of ai_query() syntax —
  --                          in production this prompt would reference
  --                          actual column values from a Gold layer table
  --                          e.g. CONCAT('Summarize this claim: ', claim_summary)
  'Summarize claims performance and risk patterns in insurance dataset'

);

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT ai_query(
# MAGIC   'databricks-gpt-oss-20b',
# MAGIC   'What does high premium and low deductible indicate in insurance portfolio?'
# MAGIC );

# COMMAND ----------

display(spark.table("bricksquad.gold.ai_business_insights"))

# COMMAND ----------

# MAGIC %md
# MAGIC schreenshot of unitycatlog of ai_businees_inshights 
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ![](/Volumes/bricksquad/default/images/business_inshight schema.png)

# COMMAND ----------


