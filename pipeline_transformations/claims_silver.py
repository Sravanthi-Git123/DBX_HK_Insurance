"""
Silver Layer Transformation for Claims Data

Purpose:
    Transforms raw bronze claims into cleaned, validated, deduplicated silver records
    with referential integrity checks against policy data.

Data Flow:
    bronze.claims_bronze → clean → FK validation → deduplicate → silver.claims_silver
                                                                ↘ quarantine.claims_errors (invalid records)

Key Features:
    - Standardizes text fields and timestamp formats
    - Validates foreign key relationships with policy table
    - Watermark-based deduplication for streaming safety
    - Data quality validation with expectations
    - Quarantine table for rejected records (invalid data + orphaned claims)
"""

import dlt
from pyspark.sql.functions import *

# ================================
# HELPER FUNCTION: Timestamp Cleanup
# ================================
def clean_time(col_name):
    """
    Fixes malformed timestamp format in source data.
    
    Problem: Source timestamps have format like "2021-01-15 14:30:00.0"
    Solution: Remove the trailing ".0" and parse as proper timestamp
    
    Args:
        col_name: Name of the timestamp column to clean
        
    Returns:
        Cleaned timestamp column expression
        
    Example:
        "2021-01-15 14:30:00.0" → "2021-01-15 14:30:00" → timestamp
    """
    return to_timestamp(regexp_replace(col(col_name), r":00\.0$", ":00"))

# ================================
# SILVER TABLE: Cleaned and Validated Claims
# ================================
@dlt.table(
    name="bricksquad.silver.claims_silver",
    comment="Cleaned claims data",
    table_properties={"quality": "silver"}
)
@dlt.expect("valid_claim_id", "claim_id IS NOT NULL")
@dlt.expect("valid_policy_id", "policy_id IS NOT NULL")
@dlt.expect("valid_injury", "injury >= 0 OR injury IS NULL")
@dlt.expect("valid_property", "property >= 0 OR property IS NULL")
@dlt.expect("valid_vehicle", "vehicle >= 0 OR vehicle IS NULL")
@dlt.expect("valid_bodily_injuries", "bodily_injuries >= 0 OR bodily_injuries IS NULL")
@dlt.expect("valid_witnesses", "witnesses >= 0 OR witnesses IS NULL")
@dlt.expect("valid_claim_rejected", "claim_rejected IN ('YES','NO','UNKNOWN') OR claim_rejected IS NULL")
def claims():
    # Read streaming claims data from bronze layer
    df = spark.readStream.table("bricksquad.bronze.claims_bronze")

    # ============================
    # COLUMN CLEANING & STANDARDIZATION
    # ============================
    df = df.select(
        # Standardize ClaimID and PolicyID: remove leading/trailing spaces
        trim(col("ClaimID")).alias("claim_id"),
        trim(col("PolicyID")).alias("policy_id"),

        # Clean and parse incident_date to timestamp, removing malformed suffix
        clean_time("incident_date").alias("incident_date"),
        # Standardize incident_state to uppercase for consistency
        upper(trim(col("incident_state"))).alias("incident_state"),
        # Standardize incident_city to title case for readability
        initcap(trim(col("incident_city"))).alias("incident_city"),
        # Remove extra spaces from incident_location
        trim(col("incident_location")).alias("incident_location"),
        # Standardize incident_type, collision_type, incident_severity to title case
        initcap(trim(col("incident_type"))).alias("incident_type"),        
        initcap(trim(col("incident_severity"))).alias("incident_severity"),

        # Cast injury, property, vehicle to double for numeric consistency
        col("injury").cast("double"),
        col("property").cast("double"),
        col("vehicle").cast("double"),

        # Authorities contacted: standardize to title case, default to 'Unknown' if missing
        coalesce(initcap(trim(col("authorities_contacted"))), lit("Unknown")).alias("authorities_contacted"),
        # Cast number_of_vehicles_involved to integer
        col("number_of_vehicles_involved").cast("int"),

        # Property damage: replace '?' with 'UNKNOWN', otherwise uppercase
        when(trim(col("property_damage")) == "?", "UNKNOWN")
        .otherwise(upper(trim(col("property_damage"))))
        .alias("property_damage"),

        # Collision Type: replace '?' with 'UNKNOWN'
        when(trim(col("collision_type")) == "?", "UNKNOWN")
        .otherwise(initcap(trim(col("collision_type"))))
        .alias("collision_type"),    

        # Cast bodily_injuries and witnesses to integer for downstream analytics
        col("bodily_injuries").cast("int"),
        col("witnesses").cast("int"),

        # Police report available: replace '?' with 'UNKNOWN', otherwise uppercase
        when(trim(col("police_report_available")) == "?", "UNKNOWN")
        .otherwise(upper(trim(col("police_report_available"))))
        .alias("police_report_available"),

        # Claim status normalization: map 'Y'/'N' to 'YES'/'NO', else 'UNKNOWN'
        when(upper(trim(col("Claim_Rejected"))) == "Y", "YES")
        .when(upper(trim(col("Claim_Rejected"))) == "N", "NO")
        .otherwise("UNKNOWN")
        .alias("claim_rejected"),

        # Clean and parse claim_logged_on and claim_processed_on timestamps
        clean_time("Claim_Logged_On").alias("claim_logged_on"),
        clean_time("Claim_Processed_On").alias("claim_processed_on"),

        # Add insert timestamp for audit trail
        current_timestamp().alias("insert_timestamp"),
        # Pass through _load_ts for lineage tracking
        col("_load_ts")
    )

    # ============================
    # FOREIGN KEY VALIDATION
    # Ensures referential integrity:
    # ============================
    # Load policy table and alias policy_number for join
    policy_df = spark.read.table("bricksquad.silver.policy_silver") \
        .select(col("policy_number").alias("policy_number_ref"))

    # Join claims to policy table on policy_id, using left join to identify orphaned claims
    df = df.join(
        policy_df,
        df.policy_id == col("policy_number_ref"),
        "left"
    )

    # Filter out claims with invalid policy_id (not found in policy table)
    df = df.filter(col("policy_number_ref").isNotNull())

    # ============================
    # DERIVED COLUMNS
    # ============================
    # Derive claim_status based on claim_processed_on and claim_logged_on timestamps
    df = df.withColumn(
        "claim_status",
        when(col("claim_processed_on").isNotNull(), "PROCESSED")
        .when(col("claim_logged_on").isNotNull(), "OPEN")
        .otherwise("UNKNOWN")
    ).withColumn(
        # Calculate claim_processing_days as difference between processed and logged dates
        "claim_processing_days",
        datediff(col("claim_processed_on"), col("claim_logged_on"))
    )

    # ============================
    # DEDUPLICATION (STREAMING SAFE)
    # ============================
    # Drop duplicate claims based on all relevant columns, using watermark for streaming safety
    dedup_cols = [
        "policy_id", "incident_date", "incident_state", "incident_city",
        "incident_location", "incident_type", "collision_type",
        "incident_severity", "injury", "property", "vehicle",
        "authorities_contacted", "number_of_vehicles_involved",
        "property_damage", "bodily_injuries", "witnesses",
        "police_report_available", "claim_rejected",
        "claim_logged_on", "claim_processed_on"
    ]

    df = df.withWatermark("incident_date", "1 day") \
           .dropDuplicates(dedup_cols)

    # Drop join helper and lineage columns before returning
    return df.drop("policy_number_ref", "_load_ts")


# ================================
# QUARANTINE TABLE: Invalid Claims
# ================================
@dlt.table(
    name="bricksquad.quarantine.claims_error",
    comment="Invalid claims records",
    table_properties={"quality": "quarantine"}
)
def claims_quarantine():

    # Read streaming claims data from bronze layer
    df = spark.readStream.table("bricksquad.bronze.claims_bronze")

    # Select and standardize relevant columns for error analysis
    df = df.select(
        trim(col("ClaimID")).alias("claim_id"),
        trim(col("PolicyID")).alias("policy_id"),
        col("injury").cast("double").alias("injury"),
        col("property").cast("double").alias("property"),
        col("vehicle").cast("double").alias("vehicle"),
        col("bodily_injuries").cast("int").alias("bodily_injuries"),
        col("witnesses").cast("int").alias("witnesses"),
        upper(trim(col("Claim_Rejected"))).alias("claim_rejected"),
        col("_load_ts")
    )

    # Load policy table for referential integrity check
    policy_df = spark.read.table("bricksquad.silver.policy_silver") \
        .select(col("policy_number").alias("policy_number_ref"))

    # Join claims to policy table to identify orphaned claims
    df = df.join(
        policy_df,
        df.policy_id == col("policy_number_ref"),
        "left"
    )

    # Add error_reason column to describe why each record is quarantined
    df = df.withColumn(
        "error_reason",
        when(col("claim_id").isNull(), "Missing Claim ID")
        .when(col("policy_id").isNull(), "Missing Policy ID")
        .when(col("injury") < 0, "Invalid Injury Amount")
        .when(col("property") < 0, "Invalid Property Amount")
        .when(col("vehicle") < 0, "Invalid Vehicle Amount")
        .when(col("bodily_injuries") < 0, "Invalid Bodily Injuries")
        .when(col("witnesses") < 0, "Invalid Witness Count")
        .when(~col("claim_rejected").isin("YES", "NO","UNKNOWN"), "Invalid Claim Rejected Flag")
        .when(col("policy_number_ref").isNull(), "policy_id not found in policy table")
        .otherwise("Other Data Issue")
    )

    # Filter for records that fail any validation check (nulls, negative values, invalid flags, orphaned claims)
    df = df.filter(
        col("claim_id").isNull() |
        col("policy_id").isNull() |
        (col("injury") < 0) |
        (col("property") < 0) |
        (col("vehicle") < 0) |
        (col("bodily_injuries") < 0) |
        (col("witnesses") < 0) |
        (~col("claim_rejected").isin("YES", "NO","UNKNOWN")) |
        col("policy_number_ref").isNull()
    )

    # Drop join helper column, add insert timestamp for audit, and return quarantined records
    return df.drop("policy_number_ref") \
             .withColumn("insert_timestamp", current_timestamp())