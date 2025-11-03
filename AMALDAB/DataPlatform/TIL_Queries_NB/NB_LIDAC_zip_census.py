# Databricks notebook source
# MAGIC %md
# MAGIC - **Description**: This notebook gathers zip census data from an excel file.
# MAGIC - **Created Date**: 2025-09-19
# MAGIC - **Created By**:  Sara Iaccheo
# MAGIC - **Modified Date**: NA
# MAGIC - **Modified By**: NA
# MAGIC - **Changes Made**: NA

# COMMAND ----------

# MAGIC %md
# MAGIC Join communities and Cencus

# COMMAND ----------

# DBTITLE 1,pip install
try:
    %pip install openpyxl==3.0.10
    %restart_python
except Exception as e:
    print(f"An error occurred: {e}")

# COMMAND ----------

# DBTITLE 1,Imports
import pandas as pd
import openpyxl
from pyspark.sql.functions import count, sum, col, lit, split, lpad
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

# DBTITLE 1,utitl
# MAGIC %run "../General/NB_AMAL_Utilities"

# COMMAND ----------

# DBTITLE 1,config
# MAGIC %run "../General/NB_Configuration"

# COMMAND ----------

# DBTITLE 1,initialize catalog
spark.sql(f"use catalog {catalog}")

# COMMAND ----------

# This code initializes the error logger for the deposits pipeline.
# It then logs an informational message indicating the start of the pipeline for the given day.
ErrorLogger = ErrorLogs(f"zips_census")
logger = ErrorLogger[0]
logger.info("Starting the pipeline for zips_census")

# COMMAND ----------

DFMetadata = spark.read.table('config.metadata').filter(
    (col('TableID') == 1515
))
display(DFMetadata)

# COMMAND ----------

# Use metadata from config table
TableID = 1515
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

# DBTITLE 1,read excel
try:
    df = pd.read_excel("/Volumes/ab_dev_catalog/bronze/unstructured/Lidec/Zip_Census.xlsx", engine="openpyxl")
    df['zip5'] = df['zip'].astype(str).str.zfill(5)
except Exception as e:
    # Log errors as failed
    logger.error(f"Error reading file from volumes: {e}")
    UpdatePipelineStatusAndTime(TableID, "Failed")


# COMMAND ----------

# DBTITLE 1,clean up excel
try:
    df_clean = df.replace(-666666666, None)
    display(df_clean)
    spark.createDataFrame(df_clean).createOrReplaceTempView("vw_zip")
except Exception as e:
    # Log errors as failed
    logger.error(f"Error cleaing up file: {e}")
    UpdatePipelineStatusAndTime(TableID, "Failed")

# COMMAND ----------

# DBTITLE 1,Load Census exce to datalake


# import pandas as pd
# import openpyxl


try:
    spark.sql('''
    CREATE OR REPLACE TABLE silver.zips_census AS
    SELECT 
        zip5 AS ZIP,
        state,
        `DP02_0001E - Tot HHs` as Total_Households,
        `DP02_0002PE - Pct HHs Married-couple families` as Pct_Married_Couple,
        `DP02_0010PE - Pct HHs female householder, no spouse/partner present` as Pct_Single_Mother,
        `DP02_0093PE - PCT Foreign born` as Pct_Foreign_Born,
        `DP02_0105PE - PCT Foreign born - Europe` as Pct_Foreign_Born_Europe,
        `DP02_0106PE - PCT Foreign born - Asia` as Pct_Foreign_Born_Asia,
        `DP02_0107PE - PCT Foreign born - Africa` as Pct_Foreign_Born_Africa,
        `DP02_0108PE - PCT Foreign born - Oceania` as Pct_Foreign_Born_Oceania,
        `DP02_0109PE - PCT Foreign born - Latin American` as Pct_Foreign_Born_LatinAmerica,
        `DP02_0113PE - PCT Speak Language other than English` as Pct_Speak_Other_Language,
        `DP02_0114PE - PCT Who do not speak English "very well"` as Pct_Lack_English_Proficiency,
        `DP02_0152PE - PCT HHs with a computer` as Pct_Have_Computer,
        `DP02_0072PE - PCT of pop with a disability` as Pct_Disability,
        `DP02_0060PE - PCT Educational attainment of pop with <9th grade` as Pct_9th_Grade_or_less,
        `DP02_0061PE - PCT Educational attainment of pop with 9-12 no diploma` as Pct_HS_no_diploma,
        `DP02_0062PE - PCT Educational attainment of pop with <9th grade` as Pct_HS_grad,
        `DP02_0063PE - PCT Educational attainment of pop with 9-12 no diploma` as Pct_some_college,
        `DP02_0064PE - PCT Educational attainment of pop with assoc degree` as Pct_associate_degree,
        `DP02_0065PE - PCT Educational attainment of pop with bachelors degree` as Pct_bachelors_degree,
        `DP02_0066PE - PCT Educational attainment of pop with grad professional degree` as Pct_graduate_degree,
        `DP03_0004PE - PCT Employed` AS Pct_Employed,
        `DP03_0025E - PCT Mean travel time to work (minutes)` AS Commute_Time_to_work_mins,
        `DP03_0052PE - PCT HHs earning < than 10,000` AS Pct_HHs_income_under_10000,
        `DP03_0053PE - PCT HHs earning 10,000 to 14,999` AS Pct_HHs_income_10000_to_14999,
        `DP03_0054PE - PCT HHs earning 15,000 to 24,999` AS Pct_HHs_income_15000_to_24999,
        `DP03_0055PE - PCT HHs earning 25,000 to 34,999` AS Pct_HHs_income_25000_to_34999,
        `DP03_0056PE - PCT HHs earning 35,000 to 49,999` AS Pct_HHs_income_35000_to_49999,
        `DP03_0057PE - PCT HHs earning 50,000 to 74,999` AS Pct_HHs_income_50000_to_74999,
        `DP03_0058PE - PCT HHs earning 75,000 to 99,999` AS Pct_HHs_income_75000_to_99999,
        `DP03_0059PE - PCT HHs earning 100,000 to 149,999` AS Pct_HHs_income_100000_to_149999,
        `DP03_0060PE - PCT HHs earning 150,000 to 199,999` AS Pct_HHs_income_150000_to_199999,
        `DP03_0061PE - PCT HHs earning >= 200,000` AS Pct_HHs_income_200000_or_more,
        `DP03_0062E - Median household income (dollars)` AS Meadian_HH_Income,
        `DP03_0066PE - PCT HHs With Social Security earnings` AS Pct_HHs_Social_Security,
        `DP03_0072PE - PCT HHs With cash public assistance income` AS Pct_HHs_Cash_Public_Assistance,
        `DP03_0128PE - PCT all people below the poverty level` AS Pct_Poverty,
        `DP03_0074PE - PCT HHs With Food Stamp/SNAP benefits in the past 12 months` AS Pct_HHs_Food_Stamps,
        `DP03_0099PE - PCT people With No health insurance coverage` AS Pct_No_Health_Insurance,
        `DP04_0046PE - Homeownership rate` AS Pct_Homeownership,
        `DP04_0115PE - PCT HO Costs >=35.0 percent of income` AS Pct_HO_costs_35_percent_income,
        `DP04_0142PE - PCT Gross rent >=35.0 percent of income` AS Pct_rent_35_percent_income
    FROM vw_zip
    ''')
    UpdatePipelineStatusAndTime(TableID, "Succeeded")
except Exception as e:
        # Log errors as failed
    logger.error(f"Error cleaing up file: {e}")
    UpdatePipelineStatusAndTime(TableID, "Failed")
