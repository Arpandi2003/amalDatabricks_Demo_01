# Databricks notebook source
# MAGIC %md
# MAGIC **Notebook Name:** NB_SourceToBronze <br>
# MAGIC **Created By:** Pandi Anbu <br>
# MAGIC **Created Date:** 12/20/24<br>
# MAGIC **Modified By:** Pandi Anbu<br>
# MAGIC **Modified Date** 01/23/24 <br>
# MAGIC **Modification** : Updated framework to handle both H360 and TDW

# COMMAND ----------

# DBTITLE 1,Packages
#Importing the required packages
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

# COMMAND ----------

# DBTITLE 1,logger
# MAGIC %run "../General/NB_AMAL_Logger"

# COMMAND ----------

# DBTITLE 1,utilities
# MAGIC %run "../General/NB_AMAL_Logger"

# COMMAND ----------

# DBTITLE 1,DQ SourceCheck
# MAGIC %run "../General/NB_AMAL_Utilities"

# COMMAND ----------

# DBTITLE 1,configuration
# MAGIC %run "../General/NB_Configuration"

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
spark.sql(f"use catalog {catalog}")

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

# This code processes a list of table IDs retrieved from metadata, performs data loading operations, and updates the pipeline status based on the load type and execution results.

# Collecting table IDs from the metadata DataFrame
TablesList = DFMetadata.select(col('TableID')).collect()
# Looping through the table IDs
for TableID in TablesList:

    TableID = TableID['TableID']

    # Log the start time of the pipeline for the current TableID
    UpdatePipelineStartTime(TableID)

    # Check if AutoSkipper is enabled for the current TableID
    AutoSkipperValue = AutoSkipper(TableID)

    if AutoSkipperValue == 1:
        try:
            Flag=True
            # Retrieve metadata for the current TableID
            metadata = GetMetaDataDetails(TableID)
            LoadType = metadata['LoadType']
            LastLoadColumnName = metadata['LastLoadDateColumn']
            DependencyTableID = metadata['DependencyTableIDs']
            SourceDBName= metadata['SourceDBName']
            LastLoadDate = metadata['LastLoadDateValue']
            DWHSchemaName = metadata['DWHSchemaName']
            DWHTableName = metadata['DWHTableName']
            MergeKey = metadata['MergeKey']
            MergeKeyColumn = metadata['MergeKeyColumn']
            SelectQuery = metadata['SourceSelectQuery']
            SourcePath = metadata['SourcePath']
            LoadedDependencies = True
            ListDeptable = DependencyTableID.split(',')
            SrcTableName=metadata['SourceTableName']
            sourcesystem=metadata['SourceSystem']
            schemanames=metadata['SourceSchema']
            PipelineRunStatus = metadata['PipelineRunStatus']
            PipelineStartTime = metadata['PipelineStartTime']
            SubjectArea=metadata['SubjectArea']

            #validate the depencies are loaded successfully
            for Deptable in ListDeptable:
                if 'NA' in Deptable:
                    break
                else:
                    Depmetadata = GetMetaDataDetails(int(Deptable))
                    DepPipelineEndDate = Depmetadata['PipelineEndDate']

                # Check if all dependencies tables are loaded today
                    if DepPipelineEndDate is None or pd.to_datetime(DepPipelineEndDate).date() < pd.to_datetime('today').date():
                        LoadedDependencies = False
                        # Log information regarding dependencies not loaded
                        logging.info(f"Dependency Table {Deptable} is not loaded today. Skipping Table {TableID}.")
                        break

            if LoadedDependencies == True:

                if sourcesystem == 'H360' or sourcesystem == 'Horizon DB2':
                    # Get database credentials from Key Vault
                    jdbc_hostname = GetCredsKeyVault(scope,'h360-hostname')
                    jdbc_port = GetCredsKeyVault(scope,'h360-port')
                    jdbc_username = GetCredsKeyVault(scope,'h360-username')
                    jdbc_password = GetCredsKeyVault(scope,'h360-password')

                elif sourcesystem == 'TDW':

                    # Get database credentials from Key Vault
                    jdbc_hostname = GetCredsKeyVault(scope,'sqlsrv-hostname')
                    jdbc_port = GetCredsKeyVault(scope,'sqlsrv-port')
                    jdbc_username = GetCredsKeyVault(scope,'sqlsrv-username')
                    jdbc_password = GetCredsKeyVault(scope,'sqlsrv-password')
                
                elif sourcesystem == marketing_source_system:

                    # Get database credentials from Key Vault
                    jdbc_hostname = GetCredsKeyVault(scope,'dw-hostname')
                    jdbc_port = GetCredsKeyVault(scope,'dw-port')
                    jdbc_username = GetCredsKeyVault(scope,'dw-username')
                    jdbc_password = GetCredsKeyVault(scope,'dw-password')
                else:
                    logger.info("Login to Database Failed.")
                    continue

                # JDBC URL
                JdbcURL = f"jdbc:sqlserver://{jdbc_hostname}:{jdbc_port};databaseName={SourceDBName}"
                
                # JDBC driver class
                jdbc_driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
                
                # Set up the connection properties
                ConnectionProperties = {
                    "user": jdbc_username,
                    "password": jdbc_password,
                    "driver": jdbc_driver,
                    "encrypt": "true",  # Enable SSL encryption
                    "trustServerCertificate": "True"
                }
                # Process data based on the LoadType
                if LoadType == 'Incremental Load':

                    # Read data into a Spark DataFrame
                    LastLoadDateFormatted = pd.to_datetime(LastLoadDate).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3]
                    #print(LastLoadDate)
                    backup = spark.read.jdbc(url=JdbcURL, table=f"({SelectQuery}) as temp", properties=ConnectionProperties)
                    if 'where' in SelectQuery.lower():
                        SelectQuery = f"{SelectQuery} or {LastLoadColumnName} is null"
                    else:
                        SelectQuery = f"{SelectQuery} where {LastLoadColumnName} > '{LastLoadDateFormatted}' or {LastLoadColumnName} is null "
                    print(SelectQuery)
                                                           
                    df = spark.read.jdbc(url=JdbcURL, table=f"({SelectQuery}) as temp", properties=ConnectionProperties)
                    df=df.toDF(*[c.rstrip(' ') for c in df.columns])

                    df = df.toDF(*[c.replace(' ', '_').replace(';', '_').replace('{', '_').replace('}', '_').replace('(', '_').replace(')', '_').replace('=', '_').replace('_+_','_') for c in df.columns])
                    df=df.toDF(*[c.rstrip('_') for c in df.columns])
                    df=df.dropDuplicates()
                    print("source count",df.count())
                    
                    #Performing the incremental load 
                    IncrementalLoad(TableID, df)
                    # Update the last load date
                    try:
                        dataframe  = df
                        table = SrcTableName
                        previous_metrics_df = spark.sql(f"""select * from config.DQlogs where source_system = '{sourcesystem}' and schema_name = '{schemanames}' and table_name = '{table}' and Validation_Date = (select max(Validation_Date) from config.DQlogs where source_system = '{sourcesystem}' and schema_name = '{schemanames}' and table_name = '{DWHTableName}') """)
                        targetdataframe = get_data_quality_metrics(dataframe, sourcesystem, schemanames,table, previous_metrics_df,MergeKey)    
                        targetdataframe.createOrReplaceTempView("LogTable")
                        spark.sql("INSERT INTO config.DQlogs select * from LogTable")
                        print("Inserting into LogTable")
                    except Exception as e:
                        logger.error(f"Unable to log the record count {e}")
                    LastLoadDateSource = df.agg(max(LastLoadColumnName).alias('Max_Date')).collect()[0]['Max_Date']
                    # if LastLoadDateSource is None:
                    #     LastLoadDateSource = df.agg(max("dtRowCreated").alias('Max_Date')).collect()[0]['Max_Date']
                        # print(LastLoadDateSource)
                    print("last load value",LastLoadDateSource)
                    UpdateLastLoadDate(TableID, LastLoadDateSource)
                    UpdatePipelineStatusAndTime(TableID,'Succeeded')
                    ErrorMessage = "No error found"

                elif LoadType == 'Full Load':

                    # Read data into a Spark DataFrame
                    df = spark.read.jdbc(url=JdbcURL, table=f"({SelectQuery}) as temp", properties=ConnectionProperties)
                    #df.display()
                    df = df.toDF(*[c.replace(' ', '_').replace(';', '_').replace('{', '_').replace('}', '_').replace('(', '_').replace(')', '_').replace('\n', '_').replace('\t', '_').replace('=', '_') for c in df.columns])
                    df=df.dropDuplicates()
                    print("source count",df.count())
                    #Performing Full load
                    FullLoad(TableID,df)
                    try:
                        dataframe  = df
                        table = SrcTableName
                        previous_metrics_df = spark.sql(f"""select * from config.DQlogs where source_system = '{sourcesystem}' and schema_name = '{schemanames}' and table_name = '{table}' and Validation_Date = (select max(Validation_Date) from config.DQlogs where source_system = '{sourcesystem}' and schema_name = '{schemanames}' and table_name = '{DWHTableName}') """)
                        targetdataframe = get_data_quality_metrics(dataframe, sourcesystem, schemanames,table, previous_metrics_df,MergeKey)    
                        targetdataframe.createOrReplaceTempView("LogTable")
                        spark.sql("INSERT INTO config.DQlogs select * from LogTable")
                        print("Inserting into LogTable")
                    except Exception as e:
                        logger.error(f"Unable to log the record count {e}")
                    UpdatePipelineStatusAndTime(TableID,'Succeeded')
                    ErrorMessage = "No error found"
                
                elif LoadType == 'Truncate and Load':
                    # Read data into a Spark DataFrame
                    df = spark.read.jdbc(url=JdbcURL, table=f"({SelectQuery}) as temp", properties=ConnectionProperties)
                    #df.display()
                    df = df.toDF(*[c.replace(' ', '_').replace(';', '_').replace('{', '_').replace('}', '_').replace('(', '_').replace(')', '_').replace('\n', '_').replace('\t', '_').replace('=', '_') for c in df.columns])
                    df=df.dropDuplicates()
                    print("source count",df.count())
                    #Performing Truncate and Load   
                    TruncateLoad(TableID,df)
                    try:
                        dataframe  = df
                        table = SrcTableName
                        previous_metrics_df = spark.sql(f"""select * from config.DQlogs where source_system = '{sourcesystem}' and schema_name = '{schemanames}' and table_name = '{table}' and Validation_Date = (select max(Validation_Date) from config.DQlogs where source_system = '{sourcesystem}' and schema_name = '{schemanames}' and table_name = '{DWHTableName}') """)
                        targetdataframe = get_data_quality_metrics(dataframe, sourcesystem, schemanames,table, previous_metrics_df,MergeKey)    
                        targetdataframe.createOrReplaceTempView("LogTable")
                        spark.sql("INSERT INTO config.DQlogs select * from LogTable")
                        print("Inserting into LogTable")
                    except Exception as e:
                        logger.error(f"Unable to log the record count {e}")
                    UpdatePipelineStatusAndTime(TableID,'Succeeded')
                    ErrorMessage = "No error found"
                else:
                    logger.info("provide proper LoadType")
                    ErrorMessage = "Invalid LoadType"
                
            else:
                logger.info(f"Dependency Table {DependencyTableID} is not loaded today. Skipping Table {TableID}.")
                ErrorMessage = f"Dependency Table {DependencyTableID} is not loaded today."

        except Exception as e:
            # Log errors and update pipeline status as failed
            logger.error(f"Error processing TableID {TableID}: {e}")
            UpdatePipelineStatusAndTime(TableID,'Failed')
            ErrorMessage = str(e).split('java.lang.Exception: ')[0] if 'java.lang.Exception: ' in str(e) else str(e).split('at ')[0]
        finally:
            # Insert pipeline run status into the log table
            spark.sql(f"""
                INSERT INTO config.email_trigger (TableID, SourceSystem, DWHTableName, DWHSchemaName, PipelineRunstatus, ErrorMessage,Processed_Date)
                VALUES ('{TableID}', '{SubjectArea}', '{DWHTableName}', '{DWHSchemaName}', '{PipelineRunStatus}', '{ErrorMessage}','{PipelineStartTime}')
            """)
        print("Load is completed for table id "+str(TableID))
    else:
        logger.info(f"Skipping TableID {TableID} as AutoSkipper is set to 1")
