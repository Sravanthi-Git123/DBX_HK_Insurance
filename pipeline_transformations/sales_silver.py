"""
Silver Layer Transformation for Sales Data

Purpose:
    Transforms raw bronze sales transactions into cleaned, validated, deduplicated
    silver records with referential integrity checks against car inventory data.

Data Flow:
    bronze.sales_bronze → clean → validate → FK check → deduplicate → silver.sales_silver
                                                                     ↘ quarantine.sales_error (invalid records)

Key Features:
    - Standardizes date format (dd-MM-yyyy → MM/dd/yyyy)
    - Normalizes region codes (W→West, C→Central, E→East, S→South)
    - Validates foreign key relationships with cars table
    - Multi-stage validation: basic checks → FK validation → deduplication
    - Removes special characters from location fields
    - Quarantine table for rejected records (invalid data + orphaned sales)
"""

import dlt
from pyspark.sql.functions import *

# ============================================================
# CLEANING FUNCTION
# ============================================================
def clean_sales(df):
    """
    Cleans and standardizes sales data from bronze layer.
    
    Key Transformations:
        - Date format: Converts from "dd-MM-yyyy HH:mm" to "MM/dd/yyyy HH:mm"
          for consistency with downstream systems
        - Region codes: Expands single-letter codes to full names (W→West)
        - Location fields: Removes special characters and applies title case
        - Price: Cast to double for calculations
        - Text fields: Standardized with title case
    
    Args:
        df: Streaming DataFrame from bronze layer
        
    Returns:
        Cleaned DataFrame with standardized columns and formats
    """
    
    print("Cleaning sales data...")  # Added print statement
    print(df)  # Print statement for DataFrame
    
    df = df.select(
        # Identifiers: trim whitespace
        trim(col("sales_id")).alias("sales_id"),
        
        # Date fields: convert format from European (dd-MM-yyyy) to US (MM/dd/yyyy)
        # Example: "15-01-2021 14:30" → "01/15/2021 14:30"
        date_format(
            to_timestamp(col("ad_placed_on"), "dd-MM-yyyy HH:mm"),
            "MM/dd/yyyy HH:mm"
        ).alias("ad_placed_on"),
        
        date_format(
            to_timestamp(col("sold_on"), "dd-MM-yyyy HH:mm"),
            "MM/dd/yyyy HH:mm"
        ).alias("sold_on"),
        
        # Financial field: cast to double for calculations
        col("original_selling_price").cast("double").alias("original_selling_price"),
        
        # Region normalization: expand single-letter codes to full names
        # Handles both abbreviated (W/C/E/S) and full (West/Central/East/South) formats
        when(upper(trim(col("Region"))) == "W", "West")
        .when(upper(trim(col("Region"))) == "C", "Central")
        .when(upper(trim(col("Region"))) == "E", "East")
        .when(upper(trim(col("Region"))) == "S", "South")
        .otherwise(initcap(trim(col("Region")))).alias("region"),
        
        # Location fields: remove special characters (numbers, symbols) and apply title case
        # Ensures clean geographic names (e.g., "New-York123" → "New York")
        initcap(regexp_replace(trim(col("State")), "[^a-zA-Z ]", "")).alias("state"),
        initcap(regexp_replace(trim(col("City")), "[^a-zA-Z ]", "")).alias("city"),
        
        # Sales details: standardize text
        initcap(trim(col("seller_type"))).alias("seller_type"),  # e.g., "Dealer", "Individual"
        initcap(trim(col("owner"))).alias("owner"),              # e.g., "First Owner", "Second Owner"
        
        # Foreign key: links to cars table
        trim(col("car_id")).alias("car_id"),
        
        # Audit column: preserve original load timestamp
        col("_load_ts")
    )
    
    return df


# ============================================================
# SILVER TABLE: Cleaned and Validated Sales
# ============================================================
@dlt.table(
    name="bricksquad.silver.sales_silver",
    comment="Cleaned + deduplicated sales",
    table_properties={"quality": "silver"}
)
def sales():
    """
    Creates the silver sales table with cleaned, validated, and deduplicated records.
    
    Multi-Stage Processing:
        1. Basic Validation: Check for required fields and valid values
        2. Foreign Key Validation: Ensure car_id exists in cars table
        3. Intra-batch Deduplication: Remove duplicates within current batch
        4. Cross-batch Deduplication: Prevent duplicates across different batches
    
    Referential Integrity:
        Only sales with valid car_id (existing in silver.cars_silver) are kept.
        Orphaned sales (invalid car_id) are filtered to quarantine table.
    
    Returns:
        Streaming DataFrame with valid, unique sales records
    """
    
    # Read streaming data from bronze layer
    df = spark.readStream.table("bricksquad.bronze.sales_bronze")
    
    # Apply cleaning transformations
    cleaned_df = clean_sales(df)
    
    # ========================================================
    # STEP 1: BASIC VALIDATION FILTER (LIKE EXPECT)
    # Applies fundamental data quality checks before FK validation
    # ========================================================
    valid_df = cleaned_df.filter(
        (col("sales_id").isNotNull()) &                                              # sales_id is required
        ((col("original_selling_price") >= 0) | col("original_selling_price").isNull()) &  # price must be non-negative
        (col("car_id").isNotNull())                                                  # car_id is required for FK check
    )
    
    # ========================================================
    # STEP 2: FK VALIDATION (car_id exists in cars)
    # Ensures referential integrity: only keep sales for valid cars
    # ========================================================
    try:
        # Read existing cars (batch read for validation)
        # Use distinct() to optimize join performance
        cars_df = spark.read.table("bricksquad.silver.cars_silver").select("car_id").distinct()
        
        # Inner join: keeps only sales where car_id exists in cars table
        # Orphaned sales (car_id not found) are excluded and go to quarantine
        valid_df = valid_df.join(
            cars_df,
            on="car_id",
            how="inner"   # keeps only matching car_id
        )
    except:
        # First run: cars table may not exist yet, skip FK validation
        # This allows sales pipeline to run independently during initial setup
        pass
    
    # ========================================================
    # STEP 3: INTRA-BATCH DEDUP (IGNORE audit columns)
    # Removes duplicates within the current batch based on business key columns
    # ========================================================
    business_cols = [
        "sales_id", "car_id", "original_selling_price",
        "ad_placed_on", "sold_on",
        "region", "state", "city",
        "seller_type", "owner"
    ]
    
    intra_dedup_df = valid_df.dropDuplicates(business_cols)
    
    # ========================================================
    # STEP 4: CROSS-BATCH DEDUP
    # Prevents duplicates across multiple batches/days/files.
    # Uses left_anti join: keeps only records from new batch that don't exist in silver.
    # ========================================================
    try:
        # Read existing silver table (batch read for comparison)
        silver_df = spark.read.table("bricksquad.silver.sales_silver")
        
        # Keep only new records that don't match any existing records
        final_df = intra_dedup_df.alias("new").join(
            silver_df.alias("old"),
            on=[col(f"new.{c}") == col(f"old.{c}") for c in business_cols],
            how="left_anti"  # Returns records from 'new' that have no match in 'old'
        )
    except:
        # First run: table doesn't exist yet, use all records
        final_df = intra_dedup_df
    
    # ========================================================
    # FINAL OUTPUT
    # ========================================================
    return final_df.select(
        *business_cols,
        current_timestamp().alias("insert_timestamp")  # Track when record was inserted
    )


# ============================================================
# QUARANTINE TABLE: Invalid Sales Records
# ============================================================
@dlt.table(
    name="bricksquad.quarantine.sales_error",
    comment="Invalid sales records",
    table_properties={"quality": "quarantine"}
)
def sales_error():
    """
    Captures sales records that fail data quality validation or referential integrity checks.
    
    Records are quarantined if they have:
        - Missing sales_id (null)
        - Negative price
        - Missing car_id (null)
        - Orphaned foreign key (car_id not found in cars table)
        - Invalid date format for ad_placed_on
        - Invalid date format for sold_on (when provided)
    
    Purpose:
        Allows investigation of data quality issues without blocking the pipeline.
        Invalid records can be fixed and reprocessed.
    
    Returns:
        Streaming DataFrame with invalid records and error reasons
    """
    
    # Read from bronze (before cleaning to preserve original problematic values)
    df = spark.readStream.table("bricksquad.bronze.sales_bronze")
    print(df)  # Print statement for DataFrame
    
    # ============================
    # FOREIGN KEY VALIDATION SETUP
    # Check if cars table exists and prepare for FK validation
    # ============================
    cars_df = None
    try:
        cars_df = spark.read.table("bricksquad.silver.cars_silver").select("car_id")
    except:
        # Cars table doesn't exist yet, skip FK validation
        pass
    
    # Select relevant columns for validation (minimal cleaning)
    base_df = df.select(
        col("sales_id"),
        col("original_selling_price").cast("double").alias("price"),
        col("car_id"),
        col("ad_placed_on"),   # Keep original to validate format
        col("sold_on"),        # Keep original to validate format
        col("_load_ts")        # Keep original load timestamp for audit trail
    )
    
    # ============================
    # FK CHECK FLAG CREATION
    # Add flag to identify orphaned sales (car_id not in cars table)
    # ============================
    if cars_df:
        # If cars table exists, perform left join to identify orphaned sales
        # car_exists will be null for sales without matching car_id
        base_df = base_df.join(
            cars_df.withColumn("car_exists", lit(1)),  # Add flag column
            on="car_id",
            how="left"  # Keep all sales, mark non-matches with null
        )
    else:
        # Cars table doesn't exist, assume all car_ids are valid
        # Set car_exists=1 for all records (no FK validation)
        base_df = base_df.withColumn("car_exists", lit(1))
    print(base_df)  # Print statement for DataFrame
    
    # ============================
    # FILTER INVALID RECORDS
    # Only keep records that fail at least one validation rule
    # ============================
    # Small harmless change: add a comment for clarity
    print("Filtering invalid sales records...")  # Added print statement
    return base_df.filter(
        (col("sales_id").isNull()) |                                              # Missing sales_id
        (col("price") < 0) |                                                      # Negative price
        (col("car_id").isNull()) |                                                # Missing car_id
        (col("car_exists").isNull()) |                                            # FK violation: car_id not in cars table
        (to_timestamp(col("ad_placed_on"), "dd-MM-yyyy HH:mm").isNull()) |      # Invalid date format
        (
            # sold_on is optional, but if provided must be valid format
            col("sold_on").isNotNull() &
            to_timestamp(col("sold_on"), "dd-MM-yyyy HH:mm").isNull()
        )
    ).withColumn(
        # Add error reason (first matching condition wins due to .when() chain)
        "error_reason",
        when(col("sales_id").isNull(), "sales_id is null")
        .when(col("price") < 0, "negative price")