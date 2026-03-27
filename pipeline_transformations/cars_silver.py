"""
Silver Layer Transformation for Cars Data

Purpose:
    Transforms raw bronze car listings into cleaned, deduplicated silver records
    suitable for analytics and downstream processing.

Data Flow:
    bronze.cars_bronze → clean → deduplicate → silver.cars_silver
                                              ↘ quarantine.cars_error (invalid records)

Key Features:
    - Standardizes text fields (trim, title case)
    - Extracts numeric values from string fields (mileage, engine, power)
    - Two-stage deduplication (intra-batch + cross-batch)
    - Data quality validation with expectations
    - Quarantine table for rejected records
"""

import dlt
from pyspark.sql.functions import *

# ============================================================
# CLEANING FUNCTION
# ============================================================
def clean_cars(df):
    """
    Cleans and standardizes car data from bronze layer.
    
    Transformations:
        - Text fields: trimmed and title-cased for consistency
        - Numeric fields: cast to appropriate types (int/double)
        - Complex fields: regex extraction of numeric values from strings
        - Preserves original load timestamp for traceability
    
    Args:
        df: Streaming DataFrame from bronze layer
        
    Returns:
        Cleaned DataFrame with standardized columns
    """
    
    df = df.select(
        # Identifiers: trim whitespace
        trim(col("car_id")).alias("car_id"),
        initcap(trim(col("name"))).alias("name"),
        
        # Basic attributes: cast to proper types and standardize
        col("km_driven").cast("int").alias("km_driven"),
        initcap(trim(col("fuel"))).alias("fuel"),
        initcap(trim(col("transmission"))).alias("transmission"),
        
        # Performance metrics: extract numeric values from strings
        # e.g., "18.9 kmpl" → 18.9, "1197 CC" → 1197, "88.7 bhp" → 88.7
        regexp_extract(col("mileage"), "\\d+\\.?\\d*", 0).cast("double").alias("mileage"),
        regexp_extract(col("engine"), "\\d+", 0).cast("int").alias("engine"),
        regexp_extract(col("max_power"), "\\d+\\.?\\d*", 0).cast("double").alias("max_power"),
        
        # Keep torque as-is (complex format with multiple units)
        trim(col("torque")).alias("torque"),
        col("seats").cast("int").alias("seats"),
        
        # Model information
        initcap(trim(col("model"))).alias("model"),
        
        # Audit column: preserve original load timestamp
        col("_load_ts")
    )
    
    return df


# ============================================================
# SILVER TABLE: Cleaned and Deduplicated Cars
# ============================================================
@dlt.table(
    name="bricksquad.silver.cars_silver",
    comment="Cleaned + deduplicated cars",
    table_properties={"quality": "silver"}
)
@dlt.expect_all_or_drop({
    # Data quality rules: drop records that fail these expectations
    "valid_car_id": "car_id IS NOT NULL",                                           # car_id is required
    "valid_km": "km_driven > 0 OR km_driven IS NULL",                              # km must be positive or null
    "valid_fuel": "fuel IN ('Petrol','Diesel','Cng','Lpg') OR fuel IS NULL",       # only known fuel types
    "valid_transmission": "transmission IN ('Manual','Automatic') OR transmission IS NULL",  # only known transmissions
    "valid_seats": "seats > 0 OR seats IS NULL"                                    # seats must be positive or null
})
def cars():
    """
    Creates the silver cars table with cleaned, validated, and deduplicated records.
    
    Deduplication Strategy:
        1. Intra-batch: Removes duplicates within the current batch based on business
           attributes (excluding car_id to handle same car listed with different IDs)
        2. Cross-batch: Prevents duplicates across different batches/files by comparing
           against existing silver table records
    
    Returns:
        Streaming DataFrame with unique, valid car records
    """
    
    # Read streaming data from bronze layer
    df = spark.readStream.table("bricksquad.bronze.cars_bronze")
    
    # Apply cleaning transformations
    cleaned_df = clean_cars(df)
    
    # ========================================================
    # STEP 1: INTRA-BATCH DEDUP (IGNORE car_id)
    # Removes duplicates within the current batch based on business attributes only.
    # car_id is intentionally excluded because the same car might be listed with
    # different IDs (e.g., relisted, different platforms).
    # ========================================================
    business_cols_no_id = [
        "name", "km_driven", "fuel", "transmission",
        "mileage", "engine", "max_power",
        "torque", "seats", "model"
    ]
    
    intra_dedup_df = cleaned_df.dropDuplicates(business_cols_no_id)
    
    # ========================================================
    # STEP 2: CROSS-BATCH DEDUP (INCLUDE car_id)
    # Prevents duplicates across multiple batches/days/files.
    # Now includes car_id to ensure exact matches are filtered out.
    # Uses left_anti join: keeps only records from new batch that don't exist in silver.
    # ========================================================
    all_cols = ["car_id"] + business_cols_no_id
    
    try:
        # Read existing silver table (batch read for comparison)
        silver_df = spark.read.table("bricksquad.silver.cars_silver")
        
        # Keep only new records that don't match any existing records
        final_df = intra_dedup_df.alias("new").join(
            silver_df.alias("old"),
            on=[col(f"new.{c}") == col(f"old.{c}") for c in all_cols],
            how="left_anti"  # Returns records from 'new' that have no match in 'old'
        )
    except:
        # First run: table doesn't exist yet, use all records
        final_df = intra_dedup_df
    
    # ========================================================
    # FINAL OUTPUT
    # ========================================================
    return final_df.select(
        *all_cols,
        current_timestamp().alias("insert_timestamp")  # Track when record was inserted
    )


# ============================================================
# QUARANTINE TABLE: Invalid Records
# ============================================================
@dlt.table(
    name="bricksquad.quarantine.cars_error",
    comment="Invalid car records",
    table_properties={"quality": "quarantine"}
)
def cars_error():
    """
    Captures records that fail data quality validation for further investigation.
    
    Records are quarantined if they have:
        - Missing car_id (null)
        - Invalid or negative km_driven
        - Unknown fuel type
        - Unknown transmission type
        - Invalid or zero seats
    
    Returns:
        Streaming DataFrame with invalid records and error reasons
    """
    
    # Read from bronze (before cleaning to preserve original values)
    df = spark.readStream.table("bricksquad.bronze.cars_bronze")
    
    # Select relevant columns and filter for invalid records
    return df.select(
        col("car_id"),
        col("name"),
        col("km_driven"),
        col("fuel"),
        col("transmission"),
        col("seats"),
        col("_load_ts")  # Keep original load timestamp for audit trail
    ).filter(
        # Filter criteria: any of these conditions triggers quarantine
        (col("car_id").isNull()) |
        (col("km_driven").cast("int") <= 0) |
        (~col("fuel").isin("Petrol", "Diesel", "CNG", "LPG")) |
        (~col("transmission").isin("Manual", "Automatic")) |
        (col("seats").cast("int") <= 0)
    ).withColumn(
        # Add error reason (first matching condition wins due to .when() chain)
        "error_reason",
        when(col("car_id").isNull(), "car_id is null")
        .when(col("km_driven").cast("int") <= 0, "invalid km_driven")
        .when(~col("fuel").isin("Petrol", "Diesel", "CNG", "LPG"), "invalid fuel")
        .when(~col("transmission").isin("Manual", "Automatic"), "invalid transmission")
        .when(col("seats").cast("int") <= 0, "invalid seats")
    ).withColumn(
        "insert_timestamp", current_timestamp()  # Track when error was recorded
    )