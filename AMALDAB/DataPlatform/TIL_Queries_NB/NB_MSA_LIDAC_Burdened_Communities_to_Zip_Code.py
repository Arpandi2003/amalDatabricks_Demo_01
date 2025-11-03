# Databricks notebook source
# MAGIC %md
# MAGIC Importing necessary functions /packages 

# COMMAND ----------

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

# DBTITLE 1,utitlities
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
ErrorLogger = ErrorLogs(f"zip_county_msa")
logger = ErrorLogger[0]
logger.info("Starting the pipeline for zip_county_msa")

# COMMAND ----------

DFMetadata = spark.read.table('config.metadata').filter(
    (col('TableID') == 1517
))
display(DFMetadata)

# COMMAND ----------

# Use metadata from config table
TableID = 1517
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
# MAGIC
# MAGIC Taking the communities dataset from the website and zips to census from HUD, making sure zips have leading zeros and that datatype are string (to keep leading zeros and do the join)

# COMMAND ----------

try:
    msa_state = '''
    select zip.*,  co.*
    from default.zip_county_state zip left join default.county_msa co on zip.COUNTY = co.`County Code`
    '''
    df_msa_state = spark.sql(msa_state)
    df_msa_state.createOrReplaceTempView("vw_msa_state")
    display(df_msa_state)
except Exception as e:
    # Log errors as failed
    logger.error(f"Error joining zip,state level to county/msa level: {e}")
    UpdatePipelineStatusAndTime(TableID, "Failed")

# COMMAND ----------

# DBTITLE 1,cleaning up  fields
try:
    df_msa_state = df_msa_state.withColumnRenamed("MSA Code", "MSA_Code")\
    .withColumnRenamed("MSA Title", "MSA_Title")\
    .withColumnRenamed("CSA Code", "CSA_Code")\
    .withColumnRenamed("CSA Title", "CSA_Title")\
    .withColumnRenamed("County Code", "County_Code")\
    .withColumnRenamed("County Title", "County_Title")\
    .withColumn("Zip", lpad(col("Zip").cast("string"), 5, "0"))
    display(df_msa_state)
except Exception as e:
    # Log errors as failed
    logger.error(f"Error cleaning up fields: {e}")
    UpdatePipelineStatusAndTime(TableID, "Failed")

# COMMAND ----------

# MAGIC %md
# MAGIC # Write to Database

# COMMAND ----------

try:
    df_msa_state.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("silver.zip_county_msa")
    UpdatePipelineStatusAndTime(TableID, "Succeeded")
except Exception as e:
    # Log errors as failed
    logger.error(f"Error saving loanproceeds_by_zip_overtime_detail to schema: {e}")
    UpdatePipelineStatusAndTime(TableID, "Failed")
