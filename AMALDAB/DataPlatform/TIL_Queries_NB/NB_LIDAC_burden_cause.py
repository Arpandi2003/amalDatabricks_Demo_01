# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC - **Description**: This notebook gathers 2010 zip census data to community data to asses causes of burdening.
# MAGIC - **Created Date**: 2025-09-19
# MAGIC - **Created By**:  Sara Iaccheo
# MAGIC - **Modified Date**: NA
# MAGIC - **Modified By**: NA
# MAGIC - **Changes Made**: NA

# COMMAND ----------

# DBTITLE 1,imports
from pyspark.sql.functions import lpad, col, cast, when, lit, split, lpad
from pyspark.sql.functions import sum as _sum
from pyspark.sql.functions import count as _count 
from pyspark.sql.functions import col, when, coalesce
from pyspark.sql.types import DoubleType, StringType, IntegerType, StructField, StructType, DateType, TimestampType, DecimalType, FloatType
from pyspark.sql.functions import *
from datetime import datetime
from delta.tables import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp
from pyspark.sql.types import *
from pyspark.sql.window import Window
import pytz
import pandas as pd
import os
import sys

# COMMAND ----------

# DBTITLE 1,logger
# MAGIC %run "../General/NB_AMAL_Logger"

# COMMAND ----------

# DBTITLE 1,utitilities
# MAGIC %run "../General/NB_AMAL_Utilities"

# COMMAND ----------

# DBTITLE 1,config
# MAGIC %run "../General/NB_Configuration"

# COMMAND ----------

# DBTITLE 1,catalog
spark.sql(f"use catalog {catalog}")

# COMMAND ----------

# This code initializes the error logger for the deposits pipeline.
# It then logs an informational message indicating the start of the pipeline for the given day.
ErrorLogger = ErrorLogs(f"burdened_stats_by_zip")
logger = ErrorLogger[0]
logger.info("Starting the pipeline for burdened_stats_by_zip")

# COMMAND ----------

DFMetadata = spark.read.table('config.metadata').filter(
    (col('TableID') == 1516
))
display(DFMetadata)

# COMMAND ----------

# Use metadata from config table
TableID = 1516
metadata = GetMetaDataDetails(TableID)
LoadType = metadata['LoadType']
LastLoadColumnName = metadata['LastLoadDateColumn']
DependencyTableIDs = metadata['DependencyTableIDs']
SourceDBName = metadata['SourceDBName']
LastLoadDate = metadata['LastLoadDateValue']
DWHSchemaName = metadata['DWHSchemaName']
DWHTableName = metadata['DWHTableName']
MergeKey = metadata['MergeKey']
MergeKeyColumn = metadata['MergeKeyColumn']
SourceSelectQuery = metadata['SourceSelectQuery']
SourcePath = metadata['SourcePath']
LoadedDependencies = True
SrcTableName = metadata['SourceTableName']
sourcesystem = metadata['SourceSystem']
schemanames = metadata['SourceSchema']

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reading in Data
# MAGIC Taking the communities dataset from the website and zips to census from HUD, making sure zips have leading zeros and that datatype are string (to keep leading zeros and do the join)

# COMMAND ----------

# DBTITLE 1,Communities and Scores
try:
    df_scores = spark.sql('''
    select 
      cast(`2010 FIPS Code` as string) as `Census tract 2010 ID`,
      `County Name`,
      `State/Territory`,
      `Water and Wastewater Disadvantaged`,
      `Workforce Development Disadvantaged`,
      `Climate Change Disadvantaged`,
      `Energy Disadvantaged`,
      `Transportation Disadvantaged`,
      `Housing Disadvantaged`,
      `Legacy Pollution Disadvantaged`,
      `Health Disadvantaged`,
      `Identified as disadvantaged`,
      `Total categories exceeded`, 
      `Total population`
    from default.disavd_scores_communities
    ''')
    df_scores.createOrReplaceTempView("vw_scores")
    df_scores.display()
except Exception as e:
    # Log errors as failed
    logger.error(f"Error getting communitiy statistical LIDAC data: {e}")
    UpdatePipelineStatusAndTime(TableID, "Failed")


# COMMAND ----------

# DBTITLE 1,ZIP Mapping
## push this to test 
try:
    df_zips = spark.table("default.zip_2010_census_tract") \
    .withColumn("Zip", lpad(col("Zip").cast("string"), 5, "0")) \
    .withColumn("TRACT", col("TRACT").cast("string")) \
    .select("Zip", "TRACT", "USPS_ZIP_PREF_CITY", "USPS_ZIP_PREF_STATE") \
    .dropDuplicates()
    df_zips.display()

except Exception as e:
    # Log errors as failed
    logger.error(f"Error getting 2010 census tracts: {e}")
    UpdatePipelineStatusAndTime(TableID, "Failed")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Joining on the 2010 census tract column

# COMMAND ----------

# DBTITLE 1,Join and Filter null Zips
try:
    df_scores = df_scores.withColumnRenamed("Identified as disadvantaged", "TF_Burdened")

    df_joined = df_zips.join(
        df_scores,
        df_zips["TRACT"] == df_scores["Census tract 2010 ID"],
        "full"
    ).filter(col("Zip").isNotNull())\
        .withColumn('population_water_wastewater_disadvantaged', when((col("TF_Burdened") == 1) & (col('Water and Wastewater Disadvantaged')==1), col('Total population')).otherwise(0))\
        .withColumn('population_workforce_development_disadvantaged', when((col("TF_Burdened") == 1) & (col('Workforce Development Disadvantaged')==1), col('Total population')).otherwise(0))\
        .withColumn('population_climate_change_disadvantaged', when((col("TF_Burdened") == 1) & (col('Climate Change Disadvantaged')==1), col('Total population')).otherwise(0))\
        .withColumn('population_energy_disadvantaged', when((col("TF_Burdened") == 1) & (col('Energy Disadvantaged')==1), col('Total population')).otherwise(0))\
        .withColumn('population_transportation_disadvantaged', when((col("TF_Burdened") == 1) & (col('Transportation Disadvantaged')==1), col('Total population')).otherwise(0))\
        .withColumn('population_housing_disadvantaged', when((col("TF_Burdened") == 1) & (col('Housing Disadvantaged')==1), col('Total population')).otherwise(0))\
        .withColumn('population_legacy_pollution_disadvantaged', when((col("TF_Burdened")==1) & (col('Legacy Pollution Disadvantaged')==1), col('Total population')).otherwise(0))\
        .withColumn('population_health_disadvantaged', when((col("TF_Burdened") == 1) & (col('Health Disadvantaged')==1), col('Total population')).otherwise(0))

    # Display the joined dataframe
    display(df_joined)
    df_joined.createOrReplaceTempView("vw_joined")
except Exception as e:
    # Log errors as failed
    logger.error(f"Error joining communitiy stats to census tracts: {e}")
    UpdatePipelineStatusAndTime(TableID, "Failed")

# COMMAND ----------

# DBTITLE 1,Disadvantage Aggregates


# include total number of census tracts per zip
# state, city, MSA
# metropolotian statistical area - HUD
# us census has county to MSA
# publish this to test
try:
    df_totals = df_joined.groupBy("Zip").agg(_count('*').alias("tract_count"),
        _sum("Water and Wastewater Disadvantaged").alias("total_water_wastewater_disadvantaged"),
        _sum("Workforce Development Disadvantaged").alias("total_workforce_development_disadvantaged"),
        _sum("Climate Change Disadvantaged").alias("total_climate_change_disadvantaged"),
        _sum("Energy Disadvantaged").alias("total_energy_disadvantaged"),
        _sum("Transportation Disadvantaged").alias("total_transportation_disadvantaged"),
        _sum("Housing Disadvantaged").alias("total_housing_disadvantaged"),
        _sum("Legacy Pollution Disadvantaged").alias("total_legacy_pollution_disadvantaged"),
        _sum("Health Disadvantaged").alias("total_health_disadvantaged"),
        _sum("Total categories exceeded").alias("total_total_categories_exceeded"),
        _sum("Total population").alias("total_population"),
        _sum("population_water_wastewater_disadvantaged").alias("total_population_water_wastewater_disadvantaged"),
        _sum("population_workforce_development_disadvantaged").alias("total_population_workforce_development_disadvantaged"),
        _sum("population_climate_change_disadvantaged").alias("total_population_climate_change_disadvantaged"),
        _sum("population_energy_disadvantaged").alias("total_population_energy_disadvantaged"),
        _sum("population_transportation_disadvantaged").alias("total_population_transportation_disadvantaged"),
        _sum("population_housing_disadvantaged").alias("total_population_housing_disadvantaged"),
        _sum("population_legacy_pollution_disadvantaged").alias("total_population_legacy_pollution_disadvantaged"),
        _sum("population_health_disadvantaged").alias("total_population_health_disadvantaged")
        )

    df_totals.createOrReplaceTempView("vw_totals")
    display(df_totals)
except Exception as e:
    # Log errors as failed
    logger.error(f"Error calculating aggregate communitiy stats to tract counts: {e}")
    UpdatePipelineStatusAndTime(TableID, "Failed")

# COMMAND ----------

# DBTITLE 1,get total counts and population
try:
    # Calculate the total count for each zip code
    total_count = df_joined.groupBy("zip").count().withColumnRenamed("count", "total_count")

    # Calculate the count of true and false values for TF_Burdened
    true_count = df_joined.filter(col("TF_Burdened") == 1).groupBy("zip").count().withColumnRenamed("count", "true_count")
    false_count = df_joined.filter(col("TF_Burdened") == 0).groupBy("zip").count().withColumnRenamed("count", "false_count")

    # calculate the total population for each zip code
    pop_true = df_joined.filter(col("TF_Burdened") == 1).groupBy("zip").agg(_sum("Total population").alias('population_true'))
    pop_false = df_joined.filter(col("TF_Burdened") == 0).groupBy("zip").agg(_sum("Total population").alias('population_false'))
except Exception as e:
    # Log errors as failed
    logger.error(f"Error total counts and population for each zip code: {e}")
    UpdatePipelineStatusAndTime(TableID, "Failed")

# COMMAND ----------

# DBTITLE 1,is tract burdened?
try:
    # Join the counts with the total count
    burdened_communities_final = total_count.join(true_count, "zip", "left").join(false_count, "zip", "left").join(df_totals, "zip", "left").join(pop_true, "zip", "left").join(pop_false, "zip", "left")

    # Calculate the percentage of true and false values
    burdened_communities_final = (
    burdened_communities_final
    .withColumn(
        "percent_true",
        expr("coalesce(try_divide(population_true, total_population), 0) * 100")
    )
    .withColumn(
        "percent_false",
        expr("coalesce(try_divide(population_false, total_population), 0) * 100")
    )
    .withColumn(
        "population_tract_avg",
        expr("coalesce(try_divide(total_population, total_count), 0)")
    )
    .na.fill(value=0, subset=['population_true', 'population_false']))



    # Add the final_isBurdened column
    burdened_communities_final = burdened_communities_final.withColumn(
        "final_isBurdened",
        when(col("population_true") >= col("population_false"), True)
        .otherwise(False) 
    )

    # Display the result
    burdened_communities_final.createOrReplaceTempView("vw_burdened_communities_by_zip")
    burdened_communities_final.display()

except Exception as e:
    # Log errors as failed
    logger.error(f"Error joining counts to decide whether tract is burdened: {e}")
    UpdatePipelineStatusAndTime(TableID, "Failed")

# COMMAND ----------

# DBTITLE 1,save to datalake
try:
    burdened_communities_final.write.mode("overwrite").option("mergeSchema", "true").saveAsTable("silver.burdened_stats_by_zip")
    UpdatePipelineStatusAndTime(TableID, "Succeeded")
except Exception as e:
        # Log errors as failed
    logger.error(f"Error cleaing up file: {e}")
    UpdatePipelineStatusAndTime(TableID, "Failed")
