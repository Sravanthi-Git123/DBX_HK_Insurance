# Databricks notebook source
# MAGIC %pip install OpenAI

# COMMAND ----------

# PySpark SQL functions — used for column operations, filtering, and aggregations
# across Spark DataFrames in the pipeline (e.g., F.col, F.when, F.count)
from pyspark.sql import functions as F

# Window — enables row-level operations partitioned by a column
# Why: Used for ranking, deduplication, and running aggregations
#      across customer/policy groups (e.g., ROW_NUMBER per customer_id)
from pyspark.sql.window import Window

# OpenAI-compatible client — Databricks exposes its LLM via OpenAI-style API
# Why: Allows reuse of the standard OpenAI SDK to call Databricks model serving
#      without writing custom HTTP request handlers
from openai import OpenAI

# Suppress non-critical warnings during pipeline execution
# Why: Libraries like sentence-transformers and FAISS emit deprecation warnings
#      that clutter notebook output without affecting functionality
import warnings

# Used to add controlled delays between API calls
# Why: Prevents rate-limiting errors when making multiple LLM requests
#      in quick succession during batch processing or test loops
import time

# Used to introduce randomized jitter into retry delays
# Why: Avoids thundering herd problem — if multiple calls fail simultaneously,
#      random jitter prevents them all retrying at the exact same moment
import random

# COMMAND ----------

df = spark.table("bricksquad.silver.claims_silver")
display(df)

# COMMAND ----------

# Compute how many days elapsed between the incident and when the claim was logged
# Why: Report delay is a key risk signal — longer delays may indicate fraud,
#      late reporting, or operational inefficiency in claims processing
#      This derived column feeds downstream risk scoring and adjuster dashboards
df = df.withColumn(
    "report_delay_days",
    F.datediff("claim_logged_on", "incident_date")   # positive = logged after incident (expected)
                                                      # negative = logged before incident (data quality flag)
)

# Fill NULL values in critical categorical and numeric columns with safe defaults
# Why fillna instead of dropping: Dropping rows loses valid claim data —
#                                 NULLs here mean "not recorded", not "invalid"
df = df.fillna({
    "incident_type":         "UNKNOWN",   # Claim cannot be processed without type —
                                          # UNKNOWN flags it for adjuster review
    "incident_severity":     "UNKNOWN",   # Severity drives payout calculation —
                                          # UNKNOWN routes claim to manual assessment
    "police_report_available": "NO",      # Absence of value treated as no report filed —
                                          # conservative default protects against fraud
    "witnesses":             0            # NULL witness count treated as zero witnesses —
                                          # numeric default avoids aggregation errors downstream
})

# COMMAND ----------

# Step 1: Compute the mean and standard deviation of property claim amounts
# Why: These two statistics are required to calculate the Z-score for each claim
#      Z-score measures how many standard deviations a value is from the average
# Why .collect()[0]: Brings the single summary row to the driver as a Python object
#                    so we can reference mean and std as scalar values in the next step
stats = df.select(
    F.mean("property").alias("mean"),
    F.stddev("property").alias("std")
).collect()[0]

# Step 2: Compute the Z-score for each claim's property damage amount
# Formula: Z = |( value - mean ) / std|
# Why Z-score: Identifies statistically anomalous claims regardless of currency scale —
#              a Z-score > 2.0 means the claim is unusually high compared to the population
# Why abs(): We only care about magnitude of deviation, not direction —
#            both extremely high AND extremely low property claims are suspicious
# Why this column name r1_high_claim_amount: "r1" = Rule 1 in the fraud detection
#            rule set — downstream logic uses this score to flag potential fraud cases
df = df.withColumn(
    "r1_high_claim_amount",
    F.abs((F.col("property") - stats["mean"]) / stats["std"])
)

# COMMAND ----------

# Rule 2: Flag claims where incident severity contradicts the property damage amount
# Why: A "Trivial Damage" incident should never result in a property claim above $5,000
#      This mismatch is a strong indicator of claim inflation or fraudulent reporting —
#      adjuster marked severity low but claimed a high payout
# Why threshold $5,000: Industry benchmark — trivial damage (scratches, minor dents)
#                       rarely exceeds $5,000 in legitimate claims
# Output: Binary flag — 1 = mismatch detected (suspicious), 0 = consistent claim
df = df.withColumn(
    "r2_severity_mismatch",
    F.when(
        (F.col("incident_severity") == "Trivial Damage") &  # Severity reported as minor
        (F.col("property") > 5000),                         # But property claim is unusually high
        1                                                    # Flag as suspicious
    ).otherwise(0)                                          # No mismatch — claim looks consistent
)

# COMMAND ----------

# Define a window partitioned by policy_id
# Why Window: Allows us to count claims per policy without collapsing the DataFrame —
#             every row retains its own data while also seeing the group-level count
# Why partition by policy_id: Each policy is the unit of analysis —
#                             we want to know how many claims were filed under the same policy
window_policy = Window.partitionBy("policy_id")

# Count total number of claims filed under each policy
# Why over(window_policy): Computes count at the policy group level, not the whole dataset —
#                          every row gets the total claim count for its own policy_id
# Why count("claim_id"): claim_id is the unique identifier per claim —
#                        counting it gives exact number of claims per policy
df = df.withColumn(
    "policy_count",
    F.count("claim_id").over(window_policy)
)

# Rule 3: Flag policies with more than 2 claims as repeat claimants
# Why: Multiple claims under the same policy is a known fraud pattern —
#      legitimate policyholders rarely file more than 2 claims in a period
# Why threshold > 2: Industry standard — 3+ claims triggers enhanced review
#                    in most insurance fraud detection frameworks
# Output: Binary flag — 1 = repeat claimant (suspicious), 0 = normal claim frequency
df = df.withColumn(
    "r3_repeat_claimant",
    F.when(F.col("policy_count") > 2, 1)   # 3 or more claims on same policy = flagged
    .otherwise(0)                           # 1 or 2 claims = normal, no flag
)

# COMMAND ----------

# Rule 4: Flag claims where the incident was reported more than 3 days after it occurred
# Why: Legitimate claims are typically reported within 24–72 hours of the incident —
#      delays beyond 3 days suggest the claimant may have fabricated the incident,
#      gathered witnesses post-hoc, or is attempting to manipulate the timeline
# Why threshold 3 days: Industry standard — most insurance policies contractually
#                       require prompt reporting; 3 days is the accepted grace period
#                       before a delay becomes a red flag for fraud investigation
# Why use report_delay_days: This column was derived earlier as
#                            datediff(claim_logged_on, incident_date) —
#                            positive value = days between incident and filing
# Output: Binary flag — 1 = delayed report (suspicious), 0 = reported on time
df = df.withColumn(
    "r4_reporting_delay",
    F.when(F.col("report_delay_days") > 3, 1)   # Reported 4+ days after incident = flagged
    .otherwise(0)                                # Reported within 3 days = normal
)

# COMMAND ----------

# Rule 5: Flag claims that have no police report AND no witnesses
# Why: Unverifiable claims — ones with zero supporting evidence — are
#      significantly more likely to be fraudulent or exaggerated
#      A legitimate accident almost always produces at least one of:
#      a police report OR an independent witness
# Why both conditions together (AND): Either alone is acceptable —
#      minor fender-benders may not involve police but have witnesses,
#      or a solo incident may have a police report but no witnesses
#      Only when BOTH are missing does the claim become unverifiable
# Why police_report == "NO": We filled NULLs with "NO" earlier in fillna()
#                             so this condition safely catches both
#                             originally missing and explicitly reported as NO
# Why witnesses == 0: Zero witnesses means no independent party can
#                     corroborate the claimant's version of events
# Output: Binary flag — 1 = no supporting evidence (suspicious), 0 = verifiable claim
df = df.withColumn(
    "r5_missing_docs",
    F.when(
        (F.col("police_report_available") == "NO") &  # No police report filed
        (F.col("witnesses") == 0),                    # AND no witnesses present
        1                                             # Flag as unverifiable — needs investigation
    ).otherwise(0)                                    # At least one evidence source exists — okay
)

# COMMAND ----------

# Compute a composite fraud risk score by combining all 5 rule flags
# Why weighted sum: Not all rules carry equal fraud signal strength —
#                   weights reflect the relative importance of each rule
#                   based on insurance fraud domain knowledge
# Why multiply by 100: Converts the weighted sum to a 0–100 scale
#                      making it intuitive for adjusters to interpret
#                      (e.g., 75 = high risk, 30 = low risk)
#
# Weight breakdown and reasoning:
#   r1_high_claim_amount  × 0.25 — Inflated claim amount is the strongest
#                                   single indicator of fraud (25% weight)
#   r2_severity_mismatch  × 0.25 — Severity vs payout contradiction is equally
#                                   damning — hard to explain legitimately (25% weight)
#   r3_repeat_claimant    × 0.20 — Multiple claims signal pattern abuse
#                                   but alone could be bad luck (20% weight)
#   r4_reporting_delay    × 0.15 — Delay is suspicious but has legitimate
#                                   explanations (hospitalization etc.) (15% weight)
#   r5_missing_docs       × 0.15 — Missing evidence is concerning but minor
#                                   incidents often lack documentation (15% weight)
df = df.withColumn(
    "anomaly_score",
    (
        F.col("r1_high_claim_amount") * 0.25 +
        F.col("r2_severity_mismatch") * 0.25 +
        F.col("r3_repeat_claimant")   * 0.20 +
        F.col("r4_reporting_delay")   * 0.15 +
        F.col("r5_missing_docs")      * 0.15
    ) * 100
)

# Classify each claim into a priority tier based on its anomaly score
# Why tiered classification: Adjusters cannot investigate every flagged claim —
#                            tiers allow them to prioritize HIGH risk cases first
#                            while NORMAL claims are auto-approved without review
#
# Tier thresholds and reasoning:
#   HIGH   (≥ 70) — Multiple strong fraud signals present — immediate investigation
#                   required before any payout is approved
#   MEDIUM (≥ 45) — Notable red flags but not conclusive — senior adjuster review
#                   recommended before processing
#   LOW    (≥ 40) — Minor anomalies detected — standard adjuster review sufficient,
#                   no escalation needed
#   NORMAL (< 40) — No significant fraud signals — eligible for straight-through
#                   processing without manual intervention
df = df.withColumn(
    "priority_tier",
    F.when(F.col("anomaly_score") >= 70, "HIGH")    # Immediate investigation required
     .when(F.col("anomaly_score") >= 45, "MEDIUM")  # Senior adjuster review recommended
     .when(F.col("anomaly_score") >= 40, "LOW")     # Standard review sufficient
     .otherwise("NORMAL")                           # Auto-approve — no fraud signals
)

# COMMAND ----------

# Build a human-readable summary of exactly which fraud rules fired for each claim
# Why: Adjusters need to know specifically WHY a claim was flagged —
#      a single "anomaly_score" number is not actionable without knowing
#      which rules contributed to it
# Why concat_ws with comma: Produces a clean comma-separated string like "R1,R3,R5"
#                           that is easy to display in dashboards and pass to the LLM
#                           for generating plain English explanations per claim
#
# Rule label reasoning:
#   R1 — triggered when Z-score > 0 (any statistical anomaly in claim amount)
#         Why > 0 instead of == 1: R1 is a continuous Z-score, not a binary flag —
#         any non-zero value means the claim deviated from the population average
#   R2 — triggered when severity vs payout mismatch is detected (binary 1)
#   R3 — triggered when policy has 3+ claims filed (binary 1)
#   R4 — triggered when report was filed 4+ days after incident (binary 1)
#   R5 — triggered when no police report AND no witnesses present (binary 1)
df = df.withColumn(
    "triggered_rules",
    F.concat_ws(",",
        F.when(F.col("r1_high_claim_amount") > 0,  "R1"),  # Statistically anomalous claim amount
        F.when(F.col("r2_severity_mismatch") == 1, "R2"),  # Severity contradicts payout
        F.when(F.col("r3_repeat_claimant")   == 1, "R3"),  # Multiple claims on same policy
        F.when(F.col("r4_reporting_delay")   == 1, "R4"),  # Claim filed 4+ days after incident
        F.when(F.col("r5_missing_docs")      == 1, "R5")   # No police report and no witnesses
    )
)

# Re-filter flagged claims after triggered_rules column is added
# Why re-assign flagged_df: Previous flagged_df did not have triggered_rules column —
#                           this updated version now carries the full fraud context
#                           needed for LLM explanation generation and adjuster review
flagged_df = df.filter(F.col("priority_tier") != "NORMAL")

# COMMAND ----------

# Suppress all non-critical warnings from third-party libraries
# Why: Libraries like sentence-transformers, FAISS, and PySpark emit
#      deprecation and runtime warnings that clutter notebook output
#      without affecting pipeline functionality or results
warnings.filterwarnings("ignore")

# Fetch the current user's Databricks API token automatically from notebook context
# Why: Avoids hardcoding secrets or API keys directly in the notebook —
#      token is scoped to the current session and rotates automatically
#      making it safe for shared team notebooks and CI/CD pipelines
DATABRICKS_TOKEN = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()

# Fetch the Databricks workspace URL dynamically from Spark config
# Why: Workspace URL varies per environment (dev, staging, prod) —
#      reading it from config makes the notebook portable across environments
#      without any manual URL changes between deployments
WORKSPACE_URL = spark.conf.get("spark.databricks.workspaceUrl")

# Initialize the OpenAI-compatible client pointed at Databricks model serving
# Why OpenAI client: Databricks exposes its LLM endpoints using the OpenAI API spec —
#                    reusing the standard SDK avoids custom HTTP request handling
# Why base_url: Redirects all API calls from OpenAI's servers to the
#               Databricks workspace's own hosted model serving endpoints
#               ensuring data never leaves the secure Databricks environment
client = OpenAI(
    api_key=DATABRICKS_TOKEN,                                    # Databricks session token as auth
    base_url=f"https://{WORKSPACE_URL}/serving-endpoints"       # Internal Databricks LLM endpoint
)

# COMMAND ----------

# Parse the LLM API response and extract the generated explanation text
# Why a separate function: Databricks model serving returns content in two
#                          different formats depending on the model version —
#                          this function handles both safely without crashing
#                          the fraud explanation pipeline on format mismatches
def extract_text(response):

    # Convert the Pydantic response object into a plain Python dict
    # Why model_dump(): Direct attribute access on nested Pydantic objects
    #                   raises AttributeError — dict access is safer and
    #                   works consistently across different model response schemas
    data = response.model_dump()

    # Iterate through all candidate responses returned by the LLM
    # Why multiple choices: OpenAI-style APIs support returning N candidate
    #                       responses — we pick the first one with valid text
    for choice in data.get("choices", []):

        # Extract the message payload from this candidate response
        msg = choice.get("message", {})

        # Extract content — format varies by model (list of blocks OR plain string)
        content = msg.get("content", [])

        # Format 1: Structured content — list of typed content blocks
        # Why: Newer Databricks models return content as a list like
        #      [{"type": "text", "text": "fraud explanation here"}, ...]
        #      allowing mixed content types (text, tool_use, images) in one response
        if isinstance(content, list):
            for item in content:
                # Only extract text-type blocks — skip tool_use or image blocks
                # which are not relevant for fraud explanation generation
                if item.get("type") == "text":
                    return item.get("text")

        # Format 2: Plain string content
        # Why: Simpler models return the explanation directly as a string —
        #      no need to iterate, just return it immediately
        if isinstance(content, str):
            return content

    # Fallback: return empty string if no valid content found in any choice
    # Why not raise exception: Caller handles empty string gracefully —
    #                          prevents one failed LLM call from crashing
    #                          the entire batch of fraud explanations
    return ""

# COMMAND ----------

# Generate a structured plain English fraud explanation for a single flagged claim
# Why per-row function: Each claim needs a personalised explanation based on its
#                       own specific values — a generic summary would not give
#                       adjusters the actionable context they need to investigate
def generate_brief(row):

    # Build the LLM prompt with actual claim values injected as context
    # Why structured sections: Guides the LLM to produce consistent, scannable output
    #                          that adjusters can read quickly during high-volume reviews
    # Why "You are NOT a classifier": Prevents the LLM from returning a fraud verdict —
    #                                 only human adjusters are authorized to make that
    #                                 determination; the LLM's role is explanation only
    prompt = f"""
You are an insurance fraud investigator.

IMPORTANT:
You are NOT a classifier. You only EXPLAIN anomalies.

Write structured response:

1. Why this claim is suspicious:
- Claim amount: {row.get('property')}           # Actual property damage amount claimed
- Severity: {row.get('incident_severity')}      # Reported severity of the incident
- Type: {row.get('incident_type')}              # Type of incident (collision, theft etc.)
- Delay: {row.get('report_delay_days')}         # Days between incident and claim filing

2. Statistical / Rule-based detection used:
- Z-score anomaly detection (claim amount)      # How far this claim deviates from average
- Rule-based thresholds (severity mismatch, delay, missing docs)
- These are fast and reliable for structured data

3. Risk factors:
- Triggered Rules: {row.get('triggered_rules')} # Comma-separated list of rules that fired
                                                # e.g. "R1,R3,R5" — tells adjuster exactly
                                                # which red flags were detected on this claim

4. Next steps:
- Verify documents       # Check policy documents and claim submission paperwork
- Check claim history    # Review all prior claims under this policy_id
- Contact claimant       # Direct outreach to verify incident details

Claim ID: {row.get('claim_id')}                 # Unique identifier for adjuster to locate claim
"""

    # Send the populated prompt to the Databricks-hosted LLM
    # Why no temperature set: Defaults to model temperature — explanation tasks
    #                         benefit from slightly more natural language variation
    #                         unlike Q&A tasks where we want deterministic answers
    # Why databricks-gpt-oss-20b: Large enough to generate coherent multi-section
    #                              fraud explanations with proper reasoning structure
    response = client.chat.completions.create(
        model="databricks-gpt-oss-20b",
        messages=[{"role": "user", "content": prompt}]
    )

    # Extract and return the plain text explanation from the LLM response
    # Why extract_text(): Handles both list-of-blocks and plain string response
    #                     formats safely without crashing on format differences
    return extract_text(response)

# COMMAND ----------

# Pull all flagged claims (LOW, MEDIUM, HIGH) from Spark to Python driver
# Why collect(): generate_brief() calls the LLM via OpenAI client which runs
#               on the driver — Spark workers have no access to the LLM client
# Why flagged_df only: No point generating briefs for NORMAL claims —
#                      they are auto-approved with no adjuster review needed
rows = flagged_df.collect()

# Master list to accumulate each claim's original data + LLM explanation
results = []

# Process each flagged claim one by one through the LLM explanation pipeline
for row in rows:

    # Convert Spark Row object to a plain Python dict
    # Why asDict(): Spark Row objects don't support item assignment —
    #              we need a mutable dict to attach the LLM brief to the same record
    row_dict = row.asDict()

    # Retry configuration — attempt LLM call up to 3 times before giving up
    # Why retries: Databricks model serving occasionally returns transient errors
    #              under load — retrying recovers gracefully without losing the claim
    retries = 3
    success = False

    for i in range(retries):
        try:
            # Introduce a delay before each LLM call
            # Why 2 + random.random(): Base delay of 2 seconds prevents rate limiting —
            #                          random jitter (0.0–1.0s) staggers concurrent retries
            #                          so they don't all hit the endpoint at the same moment
            time.sleep(2 + random.random())

            # Generate the plain English fraud explanation for this claim
            # Result is attached directly to the claim's dict for later DataFrame creation
            row_dict["investigation_brief"] = generate_brief(row_dict)
            success = True
            break  # Exit retry loop immediately on success

        except Exception as e:
            # Log which retry attempt failed and why — helps debug rate limit vs model errors
            print("Retry:", i+1, e)

            # Wait 5 seconds before next retry attempt
            # Why longer than initial delay: Gives the LLM endpoint more recovery
            #                               time after a failure before hitting it again
            time.sleep(5)

    # All 3 retries exhausted — mark brief as failed instead of crashing pipeline
    # Why "LLM_FAILED" string: Allows downstream code and dashboards to detect
    #                          which claims need manual brief generation by an engineer
    if not success:
        row_dict["investigation_brief"] = "LLM_FAILED"

    # Append fully enriched claim dict (original data + LLM brief) to results list
    results.append(row_dict)

# COMMAND ----------

# Convert the enriched results list back into a Spark DataFrame
# Why createDataFrame: Results list contains plain Python dicts —
#                      converting back to Spark enables Delta table writes,
#                      SQL queries, and Databricks dashboard integration
# Why from results list: Each dict now carries both original claim fields
#                        AND the LLM-generated investigation_brief column
final_df = spark.createDataFrame(results)

# Select only the columns relevant for adjuster review and fraud reporting
# Why subset of columns: Original DataFrame contains 30+ raw fields —
#                        adjusters only need the fraud-specific columns
#                        to make investigation decisions quickly
# Why these 8 columns specifically:
#   claim_id           — unique identifier to locate the claim in the system
#   property           — actual claim amount that triggered the anomaly
#   incident_severity  — reported severity to cross-check against claim amount
#   incident_type      — type of incident for context during investigation
#   anomaly_score      — composite fraud risk score (0–100) for prioritization
#   priority_tier      — HIGH / MEDIUM / LOW label for adjuster queue routing
#   triggered_rules    — which specific rules fired (R1, R2 etc.) for this claim
#   investigation_brief — LLM-generated plain English explanation of why
#                         this claim was flagged and what steps to take next
final_df = final_df.select(
    "claim_id",
    "property",
    "incident_severity",
    "incident_type",
    "anomaly_score",
    "priority_tier",
    "triggered_rules",
    "investigation_brief"
)

# Render the final fraud investigation DataFrame in the notebook UI
# Why display() instead of show(): display() renders an interactive Databricks
#                                  table with sorting, filtering, and pagination —
#                                  adjusters and reviewers can explore results
#                                  without writing any additional queries
display(final_df)

# COMMAND ----------

# Persist the fraud investigation results to the Gold layer as a Delta table
# Why Gold layer: This is a fully enriched, business-ready dataset —
#                 it combines raw claim data, fraud rule flags, anomaly scores,
#                 priority tiers, and LLM-generated investigation briefs
#                 making it the final authoritative output of the fraud pipeline

# Why Delta format: Delta tables provide ACID transactions, schema enforcement,
#                   and time travel — critical for fraud audit trails where
#                   regulators may request historical versions of flagged claims

# Why overwrite mode: Each pipeline run produces a fresh complete set of
#                     flagged claims — overwrite ensures stale fraud flags
#                     from previous runs don't persist alongside new results
#                     and prevents duplicate investigation briefs accumulating

# Why saveAsTable vs write.parquet: saveAsTable registers the table in the
#                                   Databricks Unity Catalog making it instantly
#                                   queryable by Genie, dashboards, and SQL users
#                                   without needing to know the storage path
final_df.write \
    .mode("overwrite") \
    .format("delta") \
    .saveAsTable("bricksquad.gold.anomalyengine")   # Gold layer fraud results table —
                                                    # consumed by adjuster dashboards,
                                                    # Genie NLQ, and compliance reports

# COMMAND ----------

# MAGIC %md
# MAGIC Screenshot of bricksqaurd.gold.anamolyengine in Unity Catalog showing row count and schema

# COMMAND ----------

# MAGIC %md
# MAGIC ![](/Volumes/bricksquad/default/images/anamoly schema.png)

# COMMAND ----------

# MAGIC %md
# MAGIC Sample output: paste or screenshot 2–3 investigation briefs for HIGH-priority claims, showing the anomaly score, triggered rules, and AI-generated brief

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     claim_id,
# MAGIC     property AS claim_amount,
# MAGIC     incident_severity,
# MAGIC     incident_type,
# MAGIC     anomaly_score,
# MAGIC     priority_tier,
# MAGIC     triggered_rules,
# MAGIC     investigation_brief
# MAGIC FROM bricksquad.gold.anomalyengine
# MAGIC WHERE priority_tier = 'HIGH'
# MAGIC ORDER BY anomaly_score DESC;
