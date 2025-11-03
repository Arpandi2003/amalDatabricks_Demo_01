# Databricks notebook source
# MAGIC %md
# MAGIC ##Data Quality Source Check

# COMMAND ----------

# MAGIC %md
# MAGIC * **Description:** To perform data quality checks on the tables to calcaulate the row count, null count, duplicate check for all the tables
# MAGIC * **Created Date:** 21/01/2025
# MAGIC * **Created By:** Naveena
# MAGIC * **Modified Date:**
# MAGIC * **Modified By:** Naveena
# MAGIC * **Changes Made:**

# COMMAND ----------

# MAGIC %run "../General/NB_Configuration"

# COMMAND ----------

spark.sql(f"use catalog {catalog}")

# COMMAND ----------

# MAGIC %sql
# MAGIC Use schema config

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

def get_data_quality_metrics(
    df,
    source_system,
    schema_name,
    table_name,
    previous_metrics_df,
    pks,
    threshold_percentage=10,
):

    print("Getting Data Quality Metrics for: " + table_name)
    df = df.dropDuplicates()
    total_rows = df.count()
    print("here's the pks", pks)
    if pks is not None and pks != "NA":
        mergekey = pks.split(",")
        columns_with_types = [col for col in df.dtypes if col[0] in mergekey]
    else:
        columns_with_types = df.dtypes
    results = []

    def custom_abs(value):
        """Returns the absolute value of the given number."""
        #print("custom_abs")
        return value if value >= 0 else -value

    def calculate_threshold_difference(current_value, previous_value):
        print("calculate_threshold_difference")
        if previous_value is None:
            return (
                current_value,
                0,
                0.0,
            )  # Add a third value (zero) for the percentage difference.
        difference = custom_abs(current_value - previous_value)
        if previous_value == 0:  # Avoid division by zero.
            percentage_difference = float("inf")
        else:
            percentage_difference = (difference / float(previous_value)) * 100
            0.024

        return current_value, difference, percentage_difference

    def check_threshold_alert(percentage_difference, threshold_percentage):
        if percentage_difference > threshold_percentage:
            # send email function
            return "Alert: Threshold Exceeded"
        return "No Issues in Threshold"


    try:
        delcount = total_rows
        if "CurrentRecord" in df.columns:
            delcount = df.select("CurrentRecord").filter(col("CurrentRecord") == "Yes").count()

        
        (current_row_count,
            row_count_diff,
            row_count_diffpercentage,
        ) = calculate_threshold_difference(total_rows, delcount)
        row_count_alert = check_threshold_alert(
            row_count_diffpercentage, threshold_percentage
        )
        message = f"No Error | {row_count_alert}"

        results.append (
            (
                source_system,schema_name,table_name,"All Columns",current_time,"Row Count",delcount,"Passed",
                row_count_diff,row_count_diffpercentage,message,
            )
        )
        print(results)
        print("calculated row count")
    except Exception as e:
        # log the exception with the update statement for dict
        print(e)

    try:
        # Checking for the Null Counts
        for column_name, column_type in columns_with_types:
            null_condition = (
                isnull(col(column_name)) |
                (lower(col(column_name).cast("string")) == "null") |
                (col(column_name).cast("string") == " ") |
                (col(column_name).cast("string")=="") |
                (col(column_name).cast("string").rlike(r"^\s*$"))
            )

            if "double" in column_type or "float" in column_type:
                null_condition = null_condition | isnan(col(column_name))

            null_count = df.filter(null_condition).count()

            previous_null_count = (
                previous_metrics_df.filter(
                    (col("table_name") == table_name)
                    & (col("column_name") == column_name)
                    & (col("check_name") == "Null Count")
                )
                .select("count_value")
                .collect()
            )
            previous_null_count = (
                previous_null_count[0]["count_value"] if previous_null_count else None
            )

            (
                current_null_count,
                null_count_diff,
                null_count_diffpercentage,
            ) = calculate_threshold_difference(null_count, previous_null_count)
            null_count_alert = check_threshold_alert(
                null_count_diffpercentage, threshold_percentage
            )

            # log the exception  
            message = f"No Error | {null_count_alert}"
            results.append(
                (
                    source_system,
                    schema_name,
                    table_name,
                    column_name,
                    current_time,
                    "Null Count",
                    current_null_count,
                    "Passed",
                    null_count_diff,
                    null_count_diffpercentage,
                    message,
                )
            )
            print(f"calculated null count for {column_name}")
               
    except Exception as e:
        print(e)


    try:
        # Checking for the Duplicate Count
        if not pks or pks == "NA":
            pks = df.columns
            pks_str = "All Columns"
        else:
            pks = mergekey
            pks_str = pks

        duplicate_count_df = (
            df.groupBy(pks)
            .agg(count("*").alias("dupscount"))
            .filter(col("dupscount") > 1)
        )
        duplicate_count = (
            duplicate_count_df.agg(sum("dupscount")).collect()[0][0]
            if duplicate_count_df.count() > 0
            else 0
        )

        previous_duplicate_count = (
            previous_metrics_df.filter(
                (col("table_name") == table_name)
                & (col("check_name") == "Duplicate Count")
            )
            .select("count_value")
            .collect()
        )
        previous_duplicate_count = (
            previous_duplicate_count[0]["count_value"]
            if previous_duplicate_count
            else None
        )

        if duplicate_count > 0:
            message = f"Found {duplicate_count} duplicate records "
            status = "Failed"
        else:
            message = f"No duplicate records found "
            status = "Passed"
        (
            current_duplicate_count,
            duplicate_count_diff,
            duplicate_count_diffpercentage,
        ) = calculate_threshold_difference(duplicate_count, previous_duplicate_count)
        duplicate_count_alert = check_threshold_alert(
            duplicate_count_diffpercentage, threshold_percentage
        )

        results.append(
            (
                source_system,
                schema_name,
                table_name,
                pks_str,
                current_time,
                "Duplicate Count",
                duplicate_count,
                status,
                duplicate_count_diff,
                duplicate_count_diffpercentage,
                message,
            )
        )
        print("calculated duplicate count")
    except Exception as e:
        print(e)
        message = f"Error: {str(e)}"
        results.append
        (
            (
                source_system,
                schema_name,
                table_name,
                "All Columns",
                current_time,
                "Error",
                None,
                "Failed",
                None,
                None,
                message,
            )
        )
        raise Exception

    schema_fields = [
        StructField("Source_System", StringType(), True),
        StructField("Schema_Name", StringType(), True),
        StructField("Table_Name", StringType(), True),
        StructField("Column_Name", StringType(), True),
        StructField("Validation_Date", TimestampType(), True),
        StructField("Check_Name", StringType(), True),
        StructField("Count_Value", IntegerType(), True),
        StructField("Status", StringType(), True),
        StructField("Value_Difference", IntegerType(), True),
        StructField("Threshold_Difference", DoubleType(), True),
        StructField("Comments", StringType(), True),
    ]

    result_schema = StructType(schema_fields)
    result_df = spark.createDataFrame(results, schema=result_schema)

    return result_df

# COMMAND ----------


