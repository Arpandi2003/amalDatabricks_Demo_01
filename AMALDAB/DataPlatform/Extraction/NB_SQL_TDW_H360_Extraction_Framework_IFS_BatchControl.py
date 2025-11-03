# Databricks notebook source
# MAGIC %md
# MAGIC **Notebook Name:** NB_SourceToBronze <br>
# MAGIC **Created By:** Pandi Anbu <br>
# MAGIC **Created Date:** 12/20/24<br>
# MAGIC **Modified By:** Pandi Anbu<br>
# MAGIC **Modified Date** 01/23/24<br>
# MAGIC **Modification** : Updated framework to handle both H360 and TDW

# COMMAND ----------

# MAGIC %md
# MAGIC ## Import Package

# COMMAND ----------

# DBTITLE 1,Packages
#Importing the required packages
from pyspark.sql.functions import *
from datetime import datetime
from delta.tables import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp
from pyspark.sql.functions import current_date
from pyspark.sql.types import *
from pyspark.sql.window import Window
from pyspark.sql.functions import current_timestamp, from_utc_timestamp
import pytz
import pandas as pd
import os

# COMMAND ----------

# MAGIC %md
# MAGIC ##Initializing Notebooks

# COMMAND ----------

# MAGIC %run "../General/NB_AMAL_Logger"

# COMMAND ----------

# MAGIC %md ##Initializing Utilites

# COMMAND ----------

# MAGIC %run "../General/NB_AMAL_Utilities"

# COMMAND ----------

# MAGIC %run "../DataQuality/DataQuality_SourceCheck"

# COMMAND ----------

# MAGIC %md
# MAGIC #####Initializing Configutation Notebook

# COMMAND ----------

# MAGIC %run "../General/NB_Configuration"

# COMMAND ----------

# DBTITLE 1,List
# This code retrieves a widget value named "TableList" and splits it by commas.
# It then converts each value to an integer if it is a digit; otherwise, it raises a ValueError.
dbutils.widgets.text("TableList", "")
TableList = dbutils.widgets.get("TableList")
TableList = TableList.split(',')
TableList = [int(value) if value.isdigit() else _ for value in TableList]
# display(BatchValue)

# COMMAND ----------

# DBTITLE 1,Get Batch Details
# This code retrieves a widget value named "batch" and checks if it is a digit. 
# If valid, it converts the value to an integer; otherwise, it raises a ValueError.
#dbutils.widgets.text("batch", "")
BatchValue = dbutils.widgets.get("batch")
if BatchValue.isdigit():
    batch = int(BatchValue)
else:
    raise ValueError(f"Invalid integer value for batch: {BatchValue}")

# COMMAND ----------

# DBTITLE 1,Assign Catalog
spark.sql(f"""use catalog {catalog}""")

# COMMAND ----------

# MAGIC %sql
# MAGIC Use Schema bronze

# COMMAND ----------

# This code initializes an error logger specific to the current batch process.
# It then logs an informational message indicating the start of the pipeline for the given batch.

ErrorLogger = ErrorLogs(f"NB_SourceToRaw_{batch}")
logger = ErrorLogger[0]
logger.info("Starting the pipeline for batch : {}".format(batch))

# COMMAND ----------

from pyspark.sql.functions import col

# This code reads data from the 'config.metadata' table, filtering for the specified batch, 'SQL' sourcesystem, and 'Bronze' zone.
DFMetadata = spark.read.table('config.metadata').filter(
    (col('TableID').isin(TableList))
)

display(DFMetadata)

# COMMAND ----------

def process_table(metadata):
    TableID = metadata['TableID']
    LoadType = metadata['LoadType']
    LastLoadColumnName = metadata['LastLoadDateColumn']
    DependencyTableID = metadata['DependencyTableIDs']
    SourceDBName = metadata['SourceDBName']
    LastLoadDate = metadata['LastLoadDateValue']
    DWHSchemaName = metadata['DWHSchemaName']
    DWHTableName = metadata['DWHTableName']
    MergeKey = metadata['MergeKey']
    MergeKeyColumn = metadata['MergeKeyColumn']
    SelectQuery = metadata['SourceSelectQuery']
    SourcePath = metadata['SourcePath']
    SrcTableName = metadata['SourceTableName']
    sourcesystem = metadata['SourceSystem']
    schemanames = metadata['SourceSchema']

    # UpdatePipelineStartTime(TableID)

    AutoSkipperValue = 1

    if AutoSkipperValue == 1:
        try:
            LoadedDependencies = True
            ListDeptable = DependencyTableID.split(',')

            for Deptable in ListDeptable:
                if 'NA' in Deptable:
                    break
                else:
                    Depmetadata = GetMetaDataDetails(int(Deptable))
                    DepPipelineEndDate = Depmetadata['PipelineEndDate']

                    if DepPipelineEndDate is None or pd.to_datetime(DepPipelineEndDate).date() < pd.to_datetime('today').date():
                        LoadedDependencies = False
                        logging.info(f"Dependency Table {Deptable} is not loaded today. Skipping Table {TableID}.")
                        break

            if LoadedDependencies:
                if sourcesystem == 'H360' or sourcesystem == 'Horizon DB2':
                    jdbc_hostname = GetCredsKeyVault(scope, 'h360-hostname')
                    jdbc_port = GetCredsKeyVault(scope, 'h360-port')
                    jdbc_username = GetCredsKeyVault(scope, 'h360-username')
                    jdbc_password = GetCredsKeyVault(scope, 'h360-password')
                else:
                    logger.info("Login to Database Failed.")
                    return

                JdbcURL = f"jdbc:sqlserver://{jdbc_hostname}:{jdbc_port};databaseName={SourceDBName}"
                jdbc_driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
                ConnectionProperties = {
                    "user": jdbc_username,
                    "password": jdbc_password,
                    "driver": jdbc_driver,
                    "encrypt": "true",
                    "trustServerCertificate": "True"
                }

                
                df = spark.read.jdbc(url=JdbcURL, table=f"({SelectQuery}) as temp", properties=ConnectionProperties)
                # df.display()
                df = df.toDF(*[c.replace(' ', '_').replace(';', '_').replace('{', '_').replace('}', '_').replace('(', '_').replace(')', '_').replace('\n', '_').replace('\t', '_').replace('=', '_').replace('-', '_') for c in df.columns])
                df = df.dropDuplicates()
                print("source count", df.count())

                return df
                
            else:
                logger.info(f"Dependency Table {DependencyTableID} is not loaded today. Skipping Table {TableID}.")
        except Exception as e:
            logger.error(f"Error processing TableID {TableID}: {e}")
            # UpdatePipelineStatusAndTime(TableID, 'Failed')
            raise e
        print("Load is completed for table id " + str(TableID))
    else:
        logger.info(f"Skipping TableID {TableID} as AutoSkipper is set to 1")
    # return df


# COMMAND ----------

from pyspark.sql.functions import to_date, col, max as spark_max

while True:
    metadata_ifs_batch = GetMetaDataDetails('218')
    df_ifs_batch = process_table(metadata_ifs_batch)

    metadata_v_trend_summary = GetMetaDataDetails('203')
    df_v_trend_summary = process_table(metadata_v_trend_summary)
    
    max_datekey = df_v_trend_summary.select(spark_max("DateKey").alias("max_datekey")) \
        .select(to_date("max_datekey", "yyyyMMdd").alias("max_datekey_formatted")) \
        .collect()[0]['max_datekey_formatted']
    
    prev_process_date = df_ifs_batch.select(to_date(col('Previous_Process_Date')).alias('prev_process_date')).collect()[0]['prev_process_date']
    
    if prev_process_date == max_datekey:
        df_ifs_batch = df_ifs_batch.withColumn("Run_Date", current_timestamp()) \
                .withColumn("Load_Date_Delhi", from_utc_timestamp("Run_Date", 'Asia/Kolkata')) \
                .withColumn("Load_Date_NY", from_utc_timestamp("Run_Date", 'America/New_York')) 
        FullLoad(218,df_ifs_batch)
        break
    
    else:
        print("wait for 180 seconds")
        time.sleep(180)

# COMMAND ----------

# MAGIC %sql
# MAGIC Select * from bronze.ifs_batchcontrol
