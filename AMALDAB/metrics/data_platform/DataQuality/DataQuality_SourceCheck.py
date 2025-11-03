# Databricks notebook source
# MAGIC %md
# MAGIC * **Description:** To perform data quality checks on the tables to calcaulate the row count, null count, duplicate check for all the tables
# MAGIC * **Created Date:** 09/01/2025
# MAGIC * **Created By:** Deeraj
# MAGIC * **Modified Date:**
# MAGIC * **Modified By:** Deeraj
# MAGIC * **Changes Made:**

# COMMAND ----------

# MAGIC %md
# MAGIC ### Notebook Initialization

# COMMAND ----------

# MAGIC %run
# MAGIC ../General/NB_Configuration

# COMMAND ----------

catalog_name = dbutils.widgets.get("catalog_name")

# COMMAND ----------

spark.sql(f"use catalog {catalog_name}")

# COMMAND ----------

spark.sql(f"use schema {dqmetadata_schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Importing the required packages

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
from functools import *
import pytz
from datetime import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum
from datetime import datetime, timezone, timedelta
from functools import reduce
from pyspark.sql.functions import col, count, sum, lower, when, isnull, isnan


# COMMAND ----------

et_timezone = pytz.timezone('US/Eastern')
current_time = datetime.now(et_timezone)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Checking for Row Count, Null count, Duplicate Check

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType, DoubleType
from pyspark.sql import functions as F
from pyspark.sql.functions import col, lit, current_timestamp, when, concat, lower, isnull, isnan, count, sum
from datetime import datetime

def run_hourly_dq_check_v2(
    source_df,
    target_schema_name,
    target_table_name,
    source_system,
    merge_key,
    last_load_date,
    table_id,
    source_schema_name=None,
    source_table_name=None
):
    """
    Runs hourly DQ check using precomputed source DataFrame.
    Avoids recomputing source transformation.
    Compares against target table loaded in same hourly window.
    """
    try:
        # Get current run timestamp for logging
        current_run_ts = datetime.now()

        # === 1. Use PRECOMPUTED Source DataFrame (from your notebook) ===
        logger.info("Using precomputed source DataFrame for DQ...")
        DF_Source = source_df.dropDuplicates()

        # === 2. Build Target DataFrame (hourly window using dwh_modified_at) ===
        logger.info(f"Building target DataFrame from {target_schema_name}.{target_table_name}...")
        validation_date_row = spark.sql(f"""
            SELECT max(Validation_Date) as max_date 
            FROM metadata.dqcomparison 
            WHERE Target_Table = '{target_table_name}'
        """).collect()

        validation_date = validation_date_row[0]['max_date'] if validation_date_row else None

        # Step 2: Conditional select
        if validation_date is None:
            DF_Target = spark.sql(f"SELECT * FROM {target_schema_name}.{target_table_name}").dropDuplicates()
        else:
            DF_Target = spark.sql(f"""
                SELECT * FROM {target_schema_name}.{target_table_name}
                WHERE dwh_modified_at > '{validation_date}'
                AND dwh_modified_at < current_timestamp()
            """).dropDuplicates()

        # === 3. Compute DQ Metrics for SOURCE ===
        logger.info("Computing DQ metrics for SOURCE...")
        source_metrics = compute_dq_metrics(
            df=DF_Source,
            source_system=source_system,
            schema_name=source_schema_name or target_schema_name,  # fallback
            table_name=source_table_name or target_table_name,     # fallback
            pks=merge_key,
            validation_ts=current_run_ts,
            label="SOURCE"
        )

        # === 4. Compute DQ Metrics for TARGET ===
        logger.info("Computing DQ metrics for TARGET...")
        target_metrics = compute_dq_metrics(
            df=DF_Target,
            source_system=source_system,
            schema_name=target_schema_name,
            table_name=target_table_name,
            pks=merge_key,
            validation_ts=current_run_ts,
            label="TARGET"
        )

        # === 5. Log Metrics to DQlogs ===
        source_metrics.write.mode("append").insertInto("metadata.dqlogs")
        target_metrics.write.mode("append").insertInto("metadata.dqlogs")
        logger.info("Logged source and target metrics to DQlogs.")

        # === 6. Compare Source vs Target Metrics ===
        logger.info("Comparing source vs target metrics...")
        comparison_df = compare_dq_metrics(source_metrics, target_metrics, current_run_ts)

        # === 7. Log Comparison to dqcomparison ===
        comparison_df.write.mode("append").insertInto("metadata.dqcomparison")
        logger.info("Logged comparison results to dqcomparison.")

        print(f"DQ Check Completed for TableID: {table_id}")

    except Exception as e:
        logger.error(f"DQ Check Failed for TableID: {table_id} | Error: {str(e)}")
        raise e

# COMMAND ----------

def compute_dq_metrics(df, source_system, schema_name, table_name, pks, validation_ts, label=""):
    """
    Computes row count, null counts, duplicate counts.
    Returns DataFrame with schema matching DQlogs.
    """
    df = df.dropDuplicates()
    total_rows = df.count()

    # Determine columns to check for nulls (PKs or all)
    if pks and pks != "NA":
        key_columns = pks.split(",")
        columns_to_check = [col for col in df.dtypes if col[0] in key_columns]
    else:
        columns_to_check = df.dtypes

    results = []

    # --- Row Count ---
    results.append((
        source_system,
        schema_name,
        table_name,
        "All Columns",
        validation_ts,
        "Row Count",
        total_rows,
        "Passed",
        0,  # Value_Difference (not comparing to previous here)
        0.0,  # Threshold_Difference
        f"{label} Row Count"
    ))

    # --- Null Counts ---
    for col_name, col_type in columns_to_check:
        null_condition = (
            isnull(F.col(col_name)) |
            (F.lower(F.col(col_name).cast("string")) == "null") |
            (F.col(col_name).cast("string") == " ") |
            (F.col(col_name).cast("string") == "") |
            (F.col(col_name).cast("string").rlike(r"^\s*$"))
        )

        if "double" in col_type or "float" in col_type:
            null_condition = null_condition | isnan(F.col(col_name))

        null_count = df.filter(null_condition).count()

        results.append((
            source_system,
            schema_name,
            table_name,
            col_name,
            validation_ts,
            "Null Count",
            null_count,
            "Passed" if null_count == 0 else "Failed",
            0,
            0.0,
            f"{label} Null Count for {col_name}"
        ))

    # --- Duplicate Count ---
    if pks and pks != "NA":
        group_cols = pks.split(",")
        pks_str = ",".join(group_cols)
    else:
        group_cols = df.columns
        pks_str = "All Columns"

    dup_df = df.groupBy(group_cols).agg(count("*").alias("cnt")).filter(F.col("cnt") > 1)
    duplicate_count = dup_df.agg(sum("cnt")).collect()[0][0] if dup_df.count() > 0 else 0

    results.append((
        source_system,
        schema_name,
        table_name,
        pks_str,
        validation_ts,
        "Duplicate Count",
        duplicate_count,
        "Passed" if duplicate_count == 0 else "Failed",
        0,
        0.0,
        f"{label} Duplicate Count"
    ))

    # Define schema
    schema = StructType([
        StructField("Source_System", StringType(), True),
        StructField("Schema_Name", StringType(), True),
        StructField("Table_Name", StringType(), True),
        StructField("Column_Name", StringType(), True),
        StructField("Validation_Date", TimestampType(), True),
        StructField("Check_Name", StringType(), True),
        StructField("Count_Value", LongType(), True),
        StructField("Status", StringType(), True),
        StructField("Value_Difference", LongType(), True),
        StructField("Threshold_Difference", DoubleType(), True),
        StructField("Comments", StringType(), True),
    ])

    return spark.createDataFrame(results, schema=schema)

# COMMAND ----------

def compare_dq_metrics(source_df, target_df, validation_ts):
    """
    Compares source and target DQ metrics from the SAME validation run.
    Now joins on Validation_Date to avoid cross-run comparisons.
    RDD-FREE — compatible with Spark Connect.
    """
    # Join on Source_System, Check_Name, Column_Name, AND Validation_Date
    comparison_df = source_df.alias("src").join(
        target_df.alias("dest"),
        (F.col("src.Source_System") == F.col("dest.Source_System")) &
        (F.col("src.Check_Name") == F.col("dest.Check_Name")) &
        (F.trim(F.upper(F.col("src.Column_Name"))) == F.trim(F.upper(F.col("dest.Column_Name")))) &
        (F.col("src.Validation_Date") == F.col("dest.Validation_Date")), 
        "inner"
    )

    # Build result with required columns
    result_df = comparison_df.select(
        F.col("src.Validation_Date"),
        F.col("src.Source_System").alias("Source_System"),
        F.col("src.Check_Name").alias("Check_Name"),
        F.col("src.Schema_Name").alias("Source_Schema"),
        F.col("src.Table_Name").alias("Source_Table"),
        F.col("src.Column_Name").alias("Source_Column"),
        F.col("src.Count_Value").cast(StringType()).alias("Source_Count"),
        F.col("dest.Schema_Name").alias("Target_Schema"),
        F.col("dest.Table_Name").alias("Target_Table"),
        F.col("dest.Column_Name").alias("Target_Column"),
        F.col("dest.Count_Value").cast(StringType()).alias("Target_Count"),
        when(F.col("src.Count_Value") == F.col("dest.Count_Value"), "True").otherwise("False").alias("Is_Match")
    )

    # Add Comments column
    result_df = result_df.withColumn(
        "Comments",
        when(F.col("Is_Match") == "True", "Counts match")
        .otherwise(
            concat(
                lit("Counts differ - Source: "),
                F.col("Source_Count"),
                lit(", Target: "),
                F.col("Target_Count")
            )
        )
    )

    # Enforce final schema WITHOUT using .rdd
    columns_in_order = [
        "Validation_Date",
        "Source_System",
        "Check_Name",
        "Source_Schema",
        "Source_Table",
        "Source_Column",
        "Source_Count",
        "Target_Schema",
        "Target_Table",
        "Target_Column",
        "Target_Count",
        "Is_Match",
        "Comments"
    ]

    final_df = result_df.select([
        F.col("Validation_Date").cast(TimestampType()),
        F.col("Source_System").cast(StringType()),
        F.col("Check_Name").cast(StringType()),
        F.col("Source_Schema").cast(StringType()),
        F.col("Source_Table").cast(StringType()),
        F.col("Source_Column").cast(StringType()),
        F.col("Source_Count").cast(StringType()),
        F.col("Target_Schema").cast(StringType()),
        F.col("Target_Table").cast(StringType()),
        F.col("Target_Column").cast(StringType()),
        F.col("Target_Count").cast(StringType()),
        F.col("Is_Match").cast(StringType()),
        F.col("Comments").cast(StringType())
    ]).toDF(*columns_in_order)

    return final_df
