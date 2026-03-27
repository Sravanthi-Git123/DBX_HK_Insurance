# Databricks Pipeline-Level Auditing (Medallion Architecture)
# Queries the DLT event log to extract pipeline metrics and auditing information

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import *

# ------------------------------
# Pipeline Audit Metrics
# ------------------------------
# This materialized view queries the DLT event log to track:
# - Pipeline update execution details
# - Dataset-level metrics (row counts, timing)
# - Data quality metrics (expectations)
# - Flow status and duration

@dp.materialized_view(
    comment="Pipeline audit metrics from DLT event log - tracks execution, timing, and data quality"
)
def pipeline_audit_metrics():
    """
    Extracts comprehensive audit metrics from the pipeline event log.
    Includes dataset-level execution details, row counts, and data quality metrics.
    """
    
    # Read the event log for this pipeline using the event_log() table-valued function
    # Pipeline ID: 5d14b20a-a571-49c6-8a9b-228e4f0050a7
    event_log_df = spark.sql("SELECT * FROM event_log('5d14b20a-a571-49c6-8a9b-228e4f0050a7')")
    
    # Filter for flow_progress events (contains dataset execution metrics)
    flow_events = event_log_df.filter(
        (F.col("event_type") == "flow_progress") &
        (F.col("origin.flow_name").isNotNull()) &
        (F.col("origin.flow_name") != "pipelines.flowTimeMetrics.missingFlowName")
    )
    
    # Extract metrics from the details JSON field using get_json_object
    # The details field is stored as a JSON string, not a struct
    metrics_df = flow_events.select(
        F.col("origin.pipeline_id").alias("pipeline_id"),
        F.col("origin.pipeline_name").alias("pipeline_name"),
        F.col("origin.update_id").alias("update_id"),
        F.col("origin.flow_name").alias("dataset_name"),
        F.col("timestamp"),
        F.get_json_object("details", "$.flow_progress.status").alias("status"),
        
        # Extract row count metrics using get_json_object
        F.get_json_object("details", "$.flow_progress.metrics.num_output_rows").cast("bigint").alias("num_output_rows"),
        F.get_json_object("details", "$.flow_progress.metrics.num_upserted_rows").cast("bigint").alias("num_upserted_rows"),
        F.get_json_object("details", "$.flow_progress.metrics.num_deleted_rows").cast("bigint").alias("num_deleted_rows"),
        
        # Extract data quality metrics
        F.get_json_object("details", "$.flow_progress.data_quality.dropped_records").cast("bigint").alias("num_dropped_records"),
        
        # Extract expectations array as JSON string
        F.get_json_object("details", "$.flow_progress.data_quality.expectations").alias("expectations_json")
    )
    
    # Parse the expectations JSON array
    metrics_with_expectations = metrics_df.withColumn(
        "expectations",
        F.from_json(
            F.col("expectations_json"),
            ArrayType(StructType([
                StructField("name", StringType()),
                StructField("dataset", StringType()),
                StructField("passed_records", IntegerType()),
                StructField("failed_records", IntegerType())
            ]))
        )
    ).drop("expectations_json")
    
    # Aggregate metrics per dataset per update
    # Calculate timing, status, and total row counts
    aggregated = metrics_with_expectations.groupBy(
        "pipeline_id", "pipeline_name", "update_id", "dataset_name"
    ).agg(
        # Timing: first START/RUNNING to last COMPLETED/FAILED timestamp
        F.min(
            F.when(F.col("status").isin("STARTING", "RUNNING", "COMPLETED"), F.col("timestamp"))
        ).alias("start_time"),
        F.max(
            F.when(F.col("status").isin("STARTING", "RUNNING", "COMPLETED"), F.col("timestamp"))
        ).alias("end_time"),
        
        # Final status (last non-null status among terminal states)
        F.max_by(
            F.col("status"),
            F.col("timestamp")
        ).alias("final_status"),
        
        # Row count totals
        F.sum(F.coalesce(F.col("num_output_rows"), F.lit(0))).alias("total_output_rows"),
        F.sum(F.coalesce(F.col("num_upserted_rows"), F.lit(0))).alias("total_upserted_rows"),
        F.sum(F.coalesce(F.col("num_deleted_rows"), F.lit(0))).alias("total_deleted_rows"),
        F.max(F.coalesce(F.col("num_dropped_records"), F.lit(0))).alias("total_dropped_rows"),
        
        # Data quality: collect all expectations
        F.max(F.col("expectations")).alias("expectations")
    )
    
    # Calculate duration in seconds
    result = aggregated.withColumn(
        "duration_seconds",
        F.when(
            (F.col("start_time").isNotNull()) & (F.col("end_time").isNotNull()),
            F.round((F.unix_timestamp("end_time") - F.unix_timestamp("start_time")))
        ).otherwise(F.lit(None))
    )
    
    # Add audit timestamp
    result = result.withColumn("audit_timestamp", F.current_timestamp())
    
    # Select final columns in logical order
    return result.select(
        "pipeline_id",
        "pipeline_name",
        "update_id",
        "dataset_name",
        "start_time",
        "end_time",
        "duration_seconds",
        "final_status",
        "total_output_rows",
        "total_upserted_rows",
        "total_deleted_rows",
        "total_dropped_rows",
        "expectations",
        "audit_timestamp"
    )


# ------------------------------
# Data Quality Summary
# ------------------------------
# Separate view for detailed data quality expectations analysis

@dp.materialized_view(
    comment="Data quality expectations summary from pipeline audit"
)
def pipeline_data_quality_summary():
    """
    Provides a detailed breakdown of data quality expectations per dataset.
    Explodes the expectations array to show individual constraint metrics.
    """
    
    # Read the audit metrics
    audit_df = spark.read.table("pipeline_audit_metrics")
    
    # Explode expectations array to get one row per expectation
    exploded = audit_df.select(
        "pipeline_id",
        "pipeline_name",
        "update_id",
        "dataset_name",
        "final_status",
        "audit_timestamp",
        F.explode(F.col("expectations")).alias("expectation")
    )
    
    # Extract expectation details
    result = exploded.select(
        "pipeline_id",
        "pipeline_name",
        "update_id",
        "dataset_name",
        F.col("expectation.name").alias("expectation_name"),
        F.col("expectation.dataset").alias("expectation_dataset"),
        F.col("expectation.passed_records").alias("passed_records"),
        F.col("expectation.failed_records").alias("failed_records"),
        "final_status",
        "audit_timestamp"
    ).filter(
        F.col("expectation").isNotNull()
    )
    
    return result
