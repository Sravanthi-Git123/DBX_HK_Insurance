"""
Silver Layer Transformation for Customers Data

Purpose:
    Transforms raw bronze customer records into cleaned, validated, deduplicated
    silver records with intelligent field correction for common data quality issues.

Data Flow:
    bronze.customers_bronze → clean → field swap correction → deduplicate → silver.customers_silver
                                                                          ↘ quarantine.customers_error (invalid records)

Key Features:
    - Handles multiple column name variations (coalesce strategy)
    - Standardizes region codes (W→West, C→Central, E→East, S→South)
    - Intelligent swap correction for mixed-up Marital/Education fields
    - Removes special characters from location and job fields
    - Two-stage deduplication (intra-batch + cross-batch)
    - Data quality validation with expectations
    - Quarantine table for rejected records
"""

import dlt
from pyspark.sql.functions import *

# ============================================================
# CLEANING FUNCTION
# ============================================================
def clean_customers(df):
    """
    Cleans and standardizes customer data from bronze layer.
    
    Key Transformations:
        - Column name variations: Uses coalesce to handle different naming conventions
          (e.g., CustomerID / Customer_ID / cust_id)
        - Region codes: Expands single-letter codes to full names (W→West)
        - Text cleaning: Removes special characters, applies title case
        - Type casting: Converts numeric fields to appropriate types
        - Field swap correction: Fixes common data entry errors where Marital
          and Education values are swapped
    
    Args:
        df: Streaming DataFrame from bronze layer
        
    Returns:
        Cleaned DataFrame with standardized columns and corrected fields
    """
    
    # Define valid values for cross-field validation and swap detection
    marital_valid = ["Single", "Married", "Divorced", "Na"]
    education_valid = ["Primary", "Secondary", "Tertiary", "Terto", "Na"]
    
    df = df.select(
        # Identifier: handle multiple possible column names
        # coalesce returns the first non-null value
        trim(coalesce(col("CustomerID"), col("Customer_ID"), col("cust_id"))).alias("CustomerID"),
        
        # Region: expand single-letter codes to full names, or keep full name if already provided
        # Handles both abbreviated (W/C/E/S) and full (West/Central/East/South) formats
        when(upper(trim(coalesce(col("Region"), col("Reg")))) == "W", "West")
        .when(upper(trim(coalesce(col("Region"), col("Reg")))) == "C", "Central")
        .when(upper(trim(coalesce(col("Region"), col("Reg")))) == "E", "East")
        .when(upper(trim(coalesce(col("Region"), col("Reg")))) == "S", "South")
        .otherwise(initcap(trim(coalesce(col("Region"), col("Reg"))))).alias("Region"),
        
        # Location fields: remove special characters (numbers, symbols) and apply title case
        # Ensures clean geographic names (e.g., "New-York123" → "New York")
        initcap(regexp_replace(trim(col("State")), "[^a-zA-Z ]", "")).alias("State"),
        initcap(regexp_replace(trim(coalesce(col("City"), col("City_in_state"))), "[^a-zA-Z ]", "")).alias("City"),
        
        # Demographic fields: standardize format (will be corrected in swap logic if needed)
        initcap(trim(coalesce(col("Marital_status"), col("Marital")))).alias("Marital"),
        initcap(trim(coalesce(col("Education"), col("Edu")))).alias("Education"),
        
        # Job field: remove special characters and apply title case
        initcap(regexp_replace(trim(col("Job")), "[^a-zA-Z ]", "")).alias("Job"),
        
        # Financial and insurance attributes: cast to appropriate numeric types
        col("Balance").cast("double").alias("Balance"),           # Account balance
        col("HHInsurance").cast("int").alias("HHInsurance"),      # Household insurance count
        col("CarLoan").cast("int").alias("CarLoan"),              # Car loan flag (0/1)
        col("Default").cast("int").alias("Default"),              # Default flag (0/1)
        
        # Audit column: preserve original load timestamp
        col("_load_ts")
    )
    
    # ============================
    # INTELLIGENT FIELD SWAP CORRECTION
    # Problem: Data entry errors sometimes cause Marital and Education values to be swapped
    # Solution: Detect when values are in wrong columns and swap them back
    # ============================
    # Example issue:
    #   Marital="Secondary", Education="Married" → should be Marital="Married", Education="Secondary"
    
    # Step 1: If Marital contains an education value, replace it with the Education column value
    df = df.withColumn(
        "Marital",
        when(col("Marital").isin(education_valid), col("Education")).otherwise(col("Marital"))
    )
    
    # Step 2: If Education contains a marital value, replace it with the Marital column value
    # Note: We must do this in a second step to avoid circular reference
    df = df.withColumn(
        "Education",
        when(col("Education").isin(marital_valid), col("Marital")).otherwise(col("Education"))
    )
    
    return df


# ============================================================
# SILVER TABLE: Cleaned and Deduplicated Customers
# ============================================================
@dlt.table(
    name="bricksquad.silver.customers_silver",
    comment="Cleaned + deduplicated customers",
    table_properties={"quality": "silver"}
)
@dlt.expect_all_or_drop({
    # Data quality rules: drop records that fail these expectations
    "valid_customer_id": "CustomerID IS NOT NULL",                                                  # CustomerID is required
    "valid_balance": "Balance >= 0 OR Balance IS NULL",                                             # Balance must be non-negative or null
    "distinct_marital_education": "(Marital != Education) OR (Marital IS NULL OR Education IS NULL)"  # Marital and Education must be different (or one is null)
})
def customers():
    """
    Creates the silver customers table with cleaned, validated, and deduplicated records.
    
    Deduplication Strategy:
        1. Intra-batch: Removes duplicates within the current batch based on business
           attributes (excluding CustomerID to handle same customer with different IDs)
        2. Cross-batch: Prevents duplicates across different batches/files by comparing
           against existing silver table records
    
    Returns:
        Streaming DataFrame with unique, valid customer records
    """
    
    # Read streaming data from bronze layer
    df = spark.readStream.table("bricksquad.bronze.customers_bronze")
    
    # Apply cleaning transformations (including intelligent field swap correction)
    cleaned_df = clean_customers(df)
    
    # ========================================================
    # STEP 1: INTRA-BATCH DEDUP (IGNORE CustomerID)
    # Removes duplicates within the current batch based on business attributes only.
    # CustomerID is intentionally excluded because the same customer might appear
    # with different IDs (e.g., data migration, system changes, mergers).
    # ========================================================
    business_cols_no_id = [
        "Region", "State", "City", "Marital",
        "Education", "Job", "Balance",
        "HHInsurance", "CarLoan", "Default"
    ]
    
    intra_dedup_df = cleaned_df.dropDuplicates(business_cols_no_id)
    
    # ========================================================
    # STEP 2: CROSS-BATCH DEDUP (INCLUDE CustomerID)
    # Prevents duplicates across multiple batches/days/files.
    # Now includes CustomerID to ensure exact matches are filtered out.
    # Uses left_anti join: keeps only records from new batch that don't exist in silver.
    # ========================================================
    all_cols = ["CustomerID"] + business_cols_no_id
    
    try:
        # Read existing silver table (batch read for comparison)
        silver_df = spark.read.table("bricksquad.silver.customers_silver")
        
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
    name="bricksquad.quarantine.customers_error",
    comment="Invalid records",
    table_properties={"quality": "quarantine"}
)
def customers_error():
    """
    Captures records that fail data quality validation for further investigation.
    
    Records are quarantined if they have:
        - Missing CustomerID (null)
        - Negative balance
        - Invalid state name (contains non-alphabetic characters except spaces)
        - Invalid city name (contains non-alphabetic characters except spaces)
        - Duplicate values in Marital and Education after swap correction (indicates corrupted data)
    
    Purpose:
        Allows investigation of data quality issues without blocking the pipeline.
        Invalid records can be corrected and reprocessed.
    
    Returns:
        Streaming DataFrame with invalid records and error reasons
    """
    
    # Read from bronze and apply cleaning to detect post-swap issues
    df = spark.readStream.table("bricksquad.bronze.customers_bronze")
    
    # Apply cleaning function to get corrected values
    cleaned_df = clean_customers(df)
    
    # Select relevant columns for validation
    return cleaned_df.select(
        col("CustomerID"),
        col("Balance"),
        col("State"),
        col("City"),
        col("Marital"),
        col("Education"),
        col("_load_ts")
    ).filter(
        # Filter criteria: any of these conditions triggers quarantine
        (col("CustomerID").isNull()) |
        (col("Balance") < 0) |
        (col("State").rlike("[^a-zA-Z ]")) |      # State contains invalid characters (not letter or space)
        (col("City").rlike("[^a-zA-Z ]")) |       # City contains invalid characters (not letter or space)
        ((col("Marital") == col("Education")) & col("Marital").isNotNull() & col("Education").isNotNull())  # Same value in both fields after swap
    ).withColumn(
        # Add error reason (first matching condition wins due to .when() chain)
        "error_reason",
        when(col("CustomerID").isNull(), "customer_id is null")
        .when(col("Balance") < 0, "negative balance")
        .when(col("State").rlike("[^a-zA-Z ]"), "invalid state")
        .when(col("City").rlike("[^a-zA-Z ]"), "invalid city")
        .when((col("Marital") == col("Education")) & col("Marital").isNotNull() & col("Education").isNotNull(), "duplicate marital_education values")
    ).withColumn(
        "insert_timestamp", current_timestamp()  # Track when error was recorded
    )
