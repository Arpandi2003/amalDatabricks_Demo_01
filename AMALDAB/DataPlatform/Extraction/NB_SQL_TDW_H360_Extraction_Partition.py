# Databricks notebook source
# MAGIC %md
# MAGIC **Notebook Name:** NB_SourceToBronze <br>
# MAGIC **Created By:** Pandi Anbu <br>
# MAGIC **Created Date:** 12/20/24<br>
# MAGIC **Modified By:** Pandi Anbu<br>
# MAGIC **Modified Date** 01/23/24 <br>
# MAGIC **Modification** : Updated framework to handle both H360 and TDW

# COMMAND ----------

# MAGIC %md
# MAGIC ## Import Package

# COMMAND ----------

# DBTITLE 1,Packages
from pyspark.sql.functions import col, lit, current_timestamp, year, month, max
from pyspark.sql.types import *
from pyspark.sql.window import Window
from delta.tables import *
from datetime import datetime
import logging
import time
import pandas as pd
import sys
import pytz
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

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

# MAGIC %run "../General/NB_Configuration"

# COMMAND ----------

from pyspark.sql import SparkSession

# Initialize Spark session with optimized configurations
spark = SparkSession.builder \
    .appName("OptimizedDataLoad") \
    .config("spark.executor.memory", "12g") \
    .config("spark.executor.cores", "4") \
    .config("spark.driver.memory", "40g") \
    .config("spark.dynamicAllocation.enabled", "true") \
    .config("spark.dynamicAllocation.minExecutors", "2") \
    .config("spark.dynamicAllocation.maxExecutors", "8") \
    .config("spark.sql.shuffle.partitions", "128") \
    .config("spark.executor.extraJavaOptions", "-XX:+UseG1GC") \
    .config("spark.sql.autoBroadcastJoinThreshold", "10485760") \
    .config("spark.memory.fraction", "0.8") \
    .config("spark.memory.storageFraction", "0.5") \
    .config("spark.default.parallelism", "64") \
    .config("spark.speculation", "true") \
    .config("spark.memory.offHeap.enabled", "true") \
    .config("spark.memory.offHeap.size", "2g") \
    .getOrCreate()

# COMMAND ----------

# DBTITLE 1,List
# This code retrieves a widget value named "TableList" and splits it by commas.
# It then converts each value to an integer if it is a digit; otherwise, it raises a ValueError.

TableList = dbutils.widgets.get("TableList")
TableList = TableList.split(',')
TableList = [int(value) if value.isdigit() else _ for value in TableList]
# display(BatchValue)

# COMMAND ----------

# DBTITLE 1,Get Batch Details
# This code retrieves a widget value named "batch" and checks if it is a digit. 
# If valid, it converts the value to an integer; otherwise, it raises a ValueError.

BatchValue = dbutils.widgets.get("batch")
if BatchValue.isdigit():
    batch = int(BatchValue)
else:
    raise ValueError(f"Invalid integer value for batch: {BatchValue}")

# COMMAND ----------

# DBTITLE 1,Assign Catalog
spark.sql(f"""use catalog {catalog}""")

# COMMAND ----------

# This code initializes an error logger specific to the current batch process.
# It then logs an informational message indicating the start of the pipeline for the given batch.

ErrorLogger = ErrorLogs(f"NB_SourceToRaw_{batch}")
logger = ErrorLogger[0]
logger.info("Starting the pipeline for batch : {}".format(batch))

# COMMAND ----------

# DBTITLE 0,Get MetaData Details
from pyspark.sql.functions import col

# This code reads data from the 'config.metadata' table, filtering for the specified batch, 'SQL' sourcesystem, and 'Bronze' zone.
DFMetadata = spark.read.table('config.metadata').filter(
    (col("Batch") == batch) & 
    (col('Zone') == 'Bronze') &
    (col('SourceSystem') != 'Flat_File')&
    (col('TableID').isin(TableList))
)

display(DFMetadata)

# COMMAND ----------

import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pyspark.sql.functions import lit
from pyspark.sql.types import DateType, TimestampType

# Create a lock for synchronization
writeLock = threading.Lock()

def auditLog(message):
    """Function to log audit messages with timestamps."""
    logger.info(f"{message} - {time.strftime('%Y-%m-%d %H:%M:%S')}")

def processPartition(tableId, partitionValue, jdbcURL, connectionProperties, selectQuery, type_define, dwhSchemaName, dwhTableName, loadType, firstRun, partitionColumn):
    """Process a single partition (year/month) for a table."""
    try:
        auditLog(f"Start processing partition {partitionValue} for TableID {tableId}")
       
        if type_define == "date":
            partitionQuery = f"{selectQuery} WHERE YEAR({partitionColumn})='{partitionValue}'"
        elif type_define == "character":
            partitionQuery = f"{selectQuery} WHERE {partitionColumn}='{partitionValue}'"
       
        df = spark.read.jdbc(url=jdbcURL, table=f"({partitionQuery}) as temp", properties=connectionProperties)
        df = df.toDF(*[c.replace(' ', '_').replace(';', '_').replace('{', '_').replace('}', '_').replace('(', '_').replace(')', '_').replace('\n', '_').replace('\t', '_').replace('=', '_') for c in df.columns])
       
        # Acquire the lock before writing to the table
        with writeLock:
            if loadType == 'Full Load':
                PartitionFullLoad(tableId, df)
            elif loadType == 'Truncate and Load':
                df = df.withColumn("CurrentRecord", lit('Yes'))
                df.createOrReplaceTempView("VW_existing_view")
                print("Performing Truncate and Load")
                SourceView = f"""
                    SELECT *,
                    'Databricks' AS DW_Created_By,                        
                    current_timestamp() AS DW_Created_Date,
                    'Databricks' AS DW_Modified_By,
                    current_timestamp() AS DW_Modified_Date
                    
                    FROM VW_existing_view
                """
                df = spark.sql(SourceView)

                df.write.mode('append').saveAsTable(f"{dwhSchemaName}.{dwhTableName}")
                # TruncateLoad(tableId, df)
        auditLog(f"End processing partition {partitionValue} for TableID {tableId}")
    except Exception as e:
        logger.error(f"Error processing partition {partitionValue} for TableID {tableId}: {e}")
        raise e

def processTable(table, firstRun):
    """Process a single table, including all its partitions."""
    tableId = table['TableID']
    try:
        auditLog(f"Start processing TableID {tableId}")
        UpdatePipelineStartTime(tableId)
        autoSkipperValue = AutoSkipper(tableId)

        print("here's the autoskipper value",autoSkipperValue)

        if autoSkipperValue == 1:
            metadata = GetMetaDataDetails(tableId)
            loadType = metadata['LoadType']
            dependencyTableId = metadata['DependencyTableIDs']
            sourceDBName = metadata['SourceDBName']
            dwhSchemaName = metadata['DWHSchemaName']
            dwhTableName = metadata['DWHTableName']
            selectQuery = metadata['SourceSelectQuery']
            sourceSystem = metadata['SourceSystem']
            partitionColumn = metadata['Verb']
            sourceTableName = metadata['SourceTableName']
            SrcTableName = metadata['SourceTableName']
            sourcesystem = metadata['SourceSystem']
            schemanames = metadata['SourceSchema']
            MergeKeyColumn = metadata['MergeKey']

            # Check dependencies
            loadedDependencies = True
            listDepTable = dependencyTableId.split(',')
            for depTable in listDepTable:
                if 'NA' not in depTable:
                    depMetadata = GetMetaDataDetails(int(depTable))
                    depPipelineEndDate = depMetadata['PipelineEndDate']
                    if depPipelineEndDate is None or pd.to_datetime(depPipelineEndDate).date() < pd.to_datetime('today').date():
                        loadedDependencies = False
                        logger.info(f"Dependency Table {depTable} is not loaded today. Skipping Table {tableId}.")
                        break

            if loadedDependencies:
                # Retrieve credentials
                start_time = time.time()
                if sourceSystem == 'H360' or sourceSystem == 'Horizon DB2':
                    jdbcHostname = GetCredsKeyVault(scope, 'h360-hostname')
                    jdbcPort = GetCredsKeyVault(scope, 'h360-port')
                    jdbcUsername = GetCredsKeyVault(scope, 'h360-username')
                    jdbcPassword = GetCredsKeyVault(scope, 'h360-password')
                elif sourceSystem == 'TDW':
                    jdbcHostname = GetCredsKeyVault(scope, 'sqlsrv-hostname')
                    jdbcPort = GetCredsKeyVault(scope, 'sqlsrv-port')
                    jdbcUsername = GetCredsKeyVault(scope, 'sqlsrv-username')
                    jdbcPassword = GetCredsKeyVault(scope, 'sqlsrv-password')
                elif sourceSystem == marketing_source_system:
                    jdbcHostname = GetCredsKeyVault(scope, 'dw-hostname')
                    jdbcPort = GetCredsKeyVault(scope, 'dw-port')
                    jdbcUsername = GetCredsKeyVault(scope, 'dw-username')
                    jdbcPassword = GetCredsKeyVault(scope, 'dw-password')
                else:
                    logger.info("Login to Database Failed.")
                    return

                jdbcURL = f"jdbc:sqlserver://{jdbcHostname}:{jdbcPort};databaseName={sourceDBName}"
                jdbcDriver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"

                connectionProperties = {
                    "user": jdbcUsername,
                    "password": jdbcPassword,
                    "driver": jdbcDriver,
                    "encrypt": "true",
                    "trustServerCertificate": "True"
                }

                auditLog(f"Start Full Load for TableID {tableId}")
                selectQuery_BackUp = selectQuery
                Backup = spark.read.jdbc(url=jdbcURL, table=f"({selectQuery}) as temp", properties=connectionProperties)
                partitionColumnType = Backup.schema[partitionColumn].dataType
                # try:
                #     dataframe = Backup
                #     table = SrcTableName
                #     previous_metrics_df = spark.sql(f"""select * from config.DQlogs where source_system = '{sourcesystem}' and schema_name = '{schemanames}' and table_name = '{table}' and Validation_Date = (select max(Validation_Date) from config.DQlogs where source_system = '{sourcesystem}' and schema_name = '{schemanames}' and table_name = '{dwhTableName}') """)
                #     targetdataframe = get_data_quality_metrics(dataframe, sourcesystem, schemanames, table, previous_metrics_df,MergeKeyColumn, threshold_percentage=10)    
                #     targetdataframe.createOrReplaceTempView("LogTable")
                #     spark.sql("INSERT INTO config.DQlogs select * from LogTable")
                #     print("Inserting into LogTable")
                # except Exception as e:
                #     logger.error(f"Unable to log the record count {e}")

                if partitionColumnType == DateType() or partitionColumnType == TimestampType():
                    selectQuery = f"select year({partitionColumn}) as partitioncol  from {schemanames}.{sourceTableName}"
                    type_define = "date"
                else:
                    selectQuery = f"select {partitionColumn} as partitioncol  from {schemanames}.{sourceTableName}"
                    type_define = "character"

                distinctPartitionDf = spark.read.jdbc(url=jdbcURL, table=f"({selectQuery}) as temp", properties=connectionProperties)
                distinctPartitionDf = distinctPartitionDf.select('partitioncol').distinct()
                distinctPartitions = [row['partitioncol'] for row in distinctPartitionDf.collect()]
                print(distinctPartitions)

                with ThreadPoolExecutor(max_workers=8) as executor:
                    futures = []
                    iteration = 0
                    for partitionValue in distinctPartitions:
                        if iteration == 0 and loadType == 'Truncate and Load':
                            spark.sql(f"TRUNCATE TABLE {dwhSchemaName}.{dwhTableName}")
                        futures.append(executor.submit(processPartition, tableId, partitionValue, jdbcURL, connectionProperties, selectQuery_BackUp, type_define, dwhSchemaName, dwhTableName, loadType, firstRun, partitionColumn))
                        iteration += 1
                    for future in as_completed(futures):
                        future.result()  # Wait for all partitions to complete

                UpdatePipelineStatusAndTime(tableId, 'Succeeded')
                auditLog(f"End Truncate and Load for TableID {tableId}")
            else:
                logger.info("Provide proper LoadType")
        auditLog(f"End processing TableID {tableId}")
    except Exception as e:
        logger.error(f"Error processing TableID {tableId}: {e}")
        UpdatePipelineStatusAndTime(tableId, 'Failed')
        auditLog(f"Failed processing TableID {tableId}")
        raise e 
    

def main():
    """Main function to process all tables."""
    try:
        auditLog("Start main processing")
        tables_list = DFMetadata.select(col('TableID')).collect()
        firstRun = True
        for table in tables_list:
            processTable(table, firstRun)
            firstRun = False  # Set firstRun to False after the first table is processed
        auditLog("End main processing")
    except Exception as e:
        logger.error(f"Critical failure in process: {e}")
        raise e

if __name__ == "__main__":
    main()
