import dlt
from pyspark.sql.functions import col, lit, count, current_timestamp, when

@dlt.table(
    name="bricksquad.silver.dq_issues",
    comment="Aggregated data quality issues from all quarantine tables",
    table_properties={"quality": "silver"}
)
def dq_issues():

    # ================================
    # Read as BATCH (NOT streaming)
    # ================================
    claims = spark.read.table("bricksquad.quarantine.claims_error") \
        .select(lit("claims").alias("table_name"), col("error_reason"))

    customers = spark.read.table("bricksquad.quarantine.customers_error") \
        .select(lit("customers").alias("table_name"), col("error_reason"))

    policy = spark.read.table("bricksquad.quarantine.policy_error") \
        .select(lit("policy").alias("table_name"), col("error_reason"))

    cars = spark.read.table("bricksquad.quarantine.cars_error") \
        .select(lit("cars").alias("table_name"), col("error_reason"))

    sales = spark.read.table("bricksquad.quarantine.sales_error") \
        .select(lit("sales").alias("table_name"), col("error_reason"))

    # ================================
    # Union
    # ================================
    dq_union = claims.unionByName(customers) \
        .unionByName(policy) \
        .unionByName(cars) \
        .unionByName(sales)

    # ================================
    # Aggregate
    # ================================
    dq_agg = dq_union.groupBy(
        "table_name",
        "error_reason"
    ).agg(
        count("*").alias("affected_records")
    )

    # ================================
    # Add metadata
    # ================================
    dq_final = dq_agg \
        .withColumnRenamed("error_reason", "rule_name") \
        .withColumn(
            "severity",
            when(col("affected_records") > 100, "HIGH")
            .when(col("affected_records") > 10, "MEDIUM")
            .otherwise("LOW")
        ) \
        .withColumn("generated_timestamp", current_timestamp())

    return dq_final