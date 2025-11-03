# Databricks notebook source
# MAGIC %md
# MAGIC **Notebook Name:** NB_Referential_Integrity <br>
# MAGIC **Created By:** Deeraj Rajeev <br>
# MAGIC **Created Date:** 09/01/25<br>
# MAGIC **Modified By:** Deeraj Rajeev<br>
# MAGIC **Modified Date** 09/01/24<br>

# COMMAND ----------

# MAGIC %md
# MAGIC ### Importing necessary packages

# COMMAND ----------

from pyspark.sql import DataFrame
from pyspark.sql.functions import lit, current_timestamp
from typing import Dict, Tuple

# COMMAND ----------

# MAGIC %md
# MAGIC ### Notebook Initialization

# COMMAND ----------

# MAGIC %run
# MAGIC ../General/NB_Configuration

# COMMAND ----------

# MAGIC %run
# MAGIC ../General/NB_Logger

# COMMAND ----------

# MAGIC %md
# MAGIC ### Catalog Initialization

# COMMAND ----------

catalog_name = dbutils.widgets.get("catalog_name")

# COMMAND ----------

spark.sql(f"use catalog {catalog_name}")

# COMMAND ----------

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import lit, current_timestamp, col
from typing import Dict, Tuple

def run_referential_integrity_check_on_df(
    fact_df: DataFrame,
    fact_table_name: str,
    fk_pk_mapping: Dict[str, Tuple[str, str]]
) -> DataFrame:
    """
    Referential integrity check using a fact DataFrame (instead of table name).
    Dimension tables are still referenced by name (e.g., 'minion.gold_campaign').

    :param fact_df: The fact DataFrame to check (e.g., DF_Fact_SessionEvents_Load)
    :param fact_table_name: Logical name for audit/logging
    :param fk_pk_mapping: Dict of FK col -> (dim_table_name, pk_column)
    :return: DataFrame with violations: fact_table_name, column_name, value, message, load_time
    """
    spark = SparkSession.getActiveSession()
    if not spark:
        raise RuntimeError("No active Spark session found.")

    results = []

    for fk_col, (dim_table, pk_col) in fk_pk_mapping.items():
        # Skip if FK column doesn't exist in DataFrame
        if fk_col not in fact_df.columns:
            result_df = spark.createDataFrame(
                [(fact_table_name, fk_col, None, f"FK column {fk_col} not found in fact data")],
                schema="fact_table_name STRING, column_name STRING, value STRING, message STRING"
            )
            results.append(result_df)
            continue

        # Get distinct non-null FK values
        try:
            fk_distinct = fact_df.select(fk_col).distinct().na.drop(subset=[fk_col])
            if fk_distinct.count() == 0:
                continue
        except Exception as e:
            logger.warning(f"Failed to read FK {fk_col}: {e}")
            continue

        # Read dimension table by name
        try:
            dim_df = spark.table(dim_table)
            pk_distinct = dim_df.select(pk_col).distinct()
            pk_renamed = pk_distinct.withColumnRenamed(pk_col, fk_col)
        except Exception as e:
            result_df = fk_distinct.select(
                lit(fact_table_name).alias("fact_table_name"),
                lit(fk_col).alias("column_name"),
                col(fk_col).alias("value"),
                lit(f"Reference table {dim_table} inaccessible: {str(e)}").alias("message")
            )
            results.append(result_df)
            continue

        # Find FK values not in PK
        missing_in_dim = fk_distinct.join(pk_renamed, on=fk_col, how="left_anti")

        if missing_in_dim.count() > 0:
            result_df = missing_in_dim.select(
                lit(fact_table_name).alias("fact_table_name"),
                lit(fk_col).alias("column_name"),
                col(fk_col).alias("value"),
                lit(f"Missing in {dim_table} ({pk_col})").alias("message")
            )
            results.append(result_df)

    # Combine all results
    if results:
        final_df = results[0]
        for df in results[1:]:
            final_df = final_df.union(df)
    else:
        final_df = spark.createDataFrame(
            [],
            schema="fact_table_name STRING, column_name STRING, value STRING, message STRING"
        )

    return final_df.withColumn("load_time", current_timestamp())
