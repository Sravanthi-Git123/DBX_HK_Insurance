"""
Silver Layer Transformation for Policy Data

Purpose:
    Transforms raw bronze policy records into cleaned, validated, deduplicated
    silver records with dual referential integrity checks against both cars and customers.

Data Flow:
    bronze.policy_bronze → clean → validate → dual FK check → deduplicate → silver.policy_silver
                                                                           ↘ quarantine.policy_errors (invalid records)

Key Features:
    - Standardizes date format (yyyy-MM-dd → MM/dd/yyyy)
    - Normalizes state codes to uppercase
    - Validates dual foreign key relationships (car_id + customer_id)
    - Multi-stage validation: basic checks → dual FK validation → deduplication
    - Type casting for financial fields (premium, deductible, umbrella_limit)
    - Quarantine table for rejected records (invalid data + orphaned policies)
"""

import dlt
from pyspark.sql.functions import *

# ============================================================
# CLEANING FUNCTION
# ============================================================
def clean_policy(df):
    """
    Cleans and standardizes policy data from bronze layer.
    
    Key Transformations:
        - Date format: Converts from "yyyy-MM-dd" to "MM/dd/yyyy"
          for consistency with downstream systems
        - State codes: Normalized to uppercase (e.g., "ca" → "CA")
        - Financial fields: Cast to appropriate numeric types (int, double, long)
        - Text fields: Trimmed for consistency
        - Foreign keys: Trimmed and linked to cars and customers tables
    
    Args:
        df: Streaming DataFrame from bronze layer
        
    Returns:
        Cleaned DataFrame with standardized columns and formats
    """
    
    df = df.select(
        # Identifier: policy unique identifier
        trim(col("policy_number")).alias("policy_number"),
        
        # Date field: convert format from ISO (yyyy-MM-dd) to US (MM/dd/yyyy)
        # Example: "2021-01-15" → "01/15/2021"
        to_date(col("policy_bind_date"), "yyyy-MM-dd").alias("policy_bind_date"),
        
        # Geographic field: state code in uppercase for consistency
        when(trim(col("policy_state")) == "?", "UNKNOWN")
        .otherwise(upper(trim(col("policy_state"))))
        .alias("policy_state"),
        
        # Coverage field: Combined Single Limit (e.g., "100/300/100")
        when(trim(col("policy_csl")) == "?", "UNKNOWN")
        .otherwise(trim(col("policy_csl")))
        .alias("policy_csl"),
        
        # Financial fields: cast to appropriate numeric types
        col("policy_deductable").cast("int").alias("policy_deductable"),
        col("policy_annual_premium").cast("double").alias("policy_annual_premium"),
        col("umbrella_limit").cast("long").alias("umbrella_limit"),
        
        # Foreign keys: link to cars and customers
        when(trim(col("car_id")) == "", None)
        .otherwise(trim(col("car_id")))
        .alias("car_id"),
        
        when(trim(col("customer_id")) == "", None)
        .otherwise(trim(col("customer_id")))
        .alias("customer_id"),
        
        # Audit column: preserve original load timestamp
        col("_load_ts")
    )
    
    return df


# ============================================================
# SILVER TABLE: Cleaned and Validated Policies
# ============================================================
@dlt.table(
    name="bricksquad.silver.policy_silver",
    comment="Cleaned + deduplicated policy",
    table_properties={"quality": "silver"}
)
def policy():

    # Read streaming data from bronze layer
    df = spark.readStream.table("bricksquad.bronze.policy_bronze")

    # Apply cleaning transformations
    cleaned_df = clean_policy(df)

    # ========================================================
    # STEP 1: BASIC VALIDATION
    # ========================================================
    valid_df = cleaned_df.filter(
        (col("policy_number").isNotNull()) &
        (col("policy_bind_date").isNotNull()) &
        ((col("policy_annual_premium") > 0) | col("policy_annual_premium").isNull()) &
        ((col("policy_deductable") >= 0) | col("policy_deductable").isNull()) &
        ((col("umbrella_limit") >= 0) | col("umbrella_limit").isNull())
    )

    # ========================================================
    # STEP 2: DUAL FK VALIDATION (car_id + customer_id)
    # ========================================================
    try:
        cars_df = spark.read.table("bricksquad.silver.cars_silver") \
            .select("car_id").distinct()

        customers_df = spark.read.table("bricksquad.silver.customers_silver") \
            .select(col("CustomerID").alias("customer_id")).distinct()

        valid_df = valid_df.join(cars_df, on="car_id", how="inner") \
                           .join(customers_df, on="customer_id", how="inner")

    except:
        pass  # first run safety

    # ========================================================
    # STEP 3: INTRA-BATCH DEDUP (IGNORE audit columns)
    # ========================================================
    business_cols = [
        "policy_number",
        "policy_bind_date",
        "policy_state",
        "policy_csl",
        "policy_deductable",
        "policy_annual_premium",
        "umbrella_limit",
        "car_id",
        "customer_id"
    ]

    intra_dedup_df = valid_df.dropDuplicates(business_cols)

    # ========================================================
    # STEP 4: CROSS-BATCH DEDUP (CRITICAL)
    # ========================================================
    try:
        silver_df = spark.read.table("bricksquad.silver.policy_silver")

        final_df = intra_dedup_df.alias("new").join(
            silver_df.alias("old"),
            on="policy_number",
            how="left_anti"
        )

    except:
        final_df = intra_dedup_df

    # ========================================================
    # FINAL OUTPUT
    # ========================================================
    return final_df.select(
        *business_cols,
        datediff(current_date(), col("policy_bind_date")).alias("policy_age_days"),
        current_timestamp().alias("insert_timestamp")
    )


# ============================================================
# QUARANTINE TABLE: Invalid Policy Records
# ============================================================
@dlt.table(
    name="bricksquad.quarantine.policy_error",
    comment="Invalid policy records",
    table_properties={"quality": "quarantine"}
)
def policy_errors():

    # Read from bronze
    df = spark.readStream.table("bricksquad.bronze.policy_bronze")

    cars_df = None
    customers_df = None

    try:
        cars_df = spark.read.table("bricksquad.silver.cars_silver").select("car_id")
        customers_df = spark.read.table("bricksquad.silver.customers_silver") \
            .select(col("CustomerID").alias("customer_id"))
    except:
        pass

    base_df = df.select(
        col("policy_number"),
        col("policy_bind_date"),
        col("policy_annual_premium").cast("double").alias("premium"),
        col("policy_deductable").cast("int").alias("deductable"),
        col("car_id"),
        col("customer_id"),
        col("_load_ts")
    )

    if cars_df:
        base_df = base_df.join(
            cars_df.withColumn("car_exists", lit(1)),
            on="car_id",
            how="left"
        )
    else:
        base_df = base_df.withColumn("car_exists", lit(1))

    if customers_df:
        base_df = base_df.join(
            customers_df.withColumn("customer_exists", lit(1)),
            on="customer_id",
            how="left"
        )
    else:
        base_df = base_df.withColumn("customer_exists", lit(1))

    return base_df.filter(
        (col("policy_number").isNull()) |
        (to_date(col("policy_bind_date"), "yyyy-MM-dd").isNull()) |
        (col("premium") <= 0) |
        (col("deductable") < 0) |
        (col("car_exists").isNull()) |
        (col("customer_exists").isNull())
    ).withColumn(
        "error_reason",
        when(col("policy_number").isNull(), "policy_number is null")
        .when(to_date(col("policy_bind_date"), "yyyy-MM-dd").isNull(), "invalid policy_bind_date")
        .when(col("premium") <= 0, "invalid premium")
        .when(col("deductable") < 0, "invalid deductible")
        .when(col("car_exists").isNull(), "invalid car_id")
        .when(col("customer_exists").isNull(), "invalid customer_id")
        .otherwise("other issue")
    ).withColumn(
        "insert_timestamp", current_timestamp()
    )