# Databricks notebook source
# MAGIC %md
# MAGIC **Notebook Name:** NB_SourceToRaw_FlatFile <br>
# MAGIC **Created By:** Pandi Anbu <br>
# MAGIC **Created Date:** 12/20/24<br>
# MAGIC **Modified By:** Pandi Anbu<br>
# MAGIC **Modified Date** 12/22/24

# COMMAND ----------

"""
Things to note:

1)If any new conditions needs to be added, include those metadata filters in the 11th Cell (Get MetaData Details)
2)Status of the tables can be seen in the Abank.Metadata tables.

"""

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
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DoubleType, StringType
from pyspark.sql.types import *
from pyspark.sql.window import Window
from datetime import datetime
import pytz
import os

# COMMAND ----------

# DBTITLE 1,Logger
# MAGIC %run "../General/NB_AMAL_Logger"

# COMMAND ----------

# DBTITLE 1,Utilities
# MAGIC %run "../General/NB_AMAL_Utilities"

# COMMAND ----------

# DBTITLE 1,Configuration
# MAGIC %run "../General/NB_Configuration"

# COMMAND ----------

# DBTITLE 1,DataQuality
# MAGIC %run "../DataQuality/DataQuality_SourceCheck"

# COMMAND ----------

# DBTITLE 1,Logger initilialization
NotebookName = f"NBSourceToRaw_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
# Initialize logger
logger, p_logfile, p_filename = ErrorLogs(NotebookName)
        
#Log the start of the pipeline
logger.info(f"Starting the pipeline for batch: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

# COMMAND ----------

# DBTITLE 1,initialize catalog
spark.sql(f"""use catalog {catalog}""")

# COMMAND ----------

# DBTITLE 1,Get MetaData Details
#Getting the required metadata details
DF_Metadata = spark.read.table('config.metadata').filter(
    (col('SourceSystem') == 'Flat_File') & (col('TableID') == 500)
)
DF_Metadata.display()

# COMMAND ----------

# Set the spark.sql.ansi.enabled configuration to false to bypass the timestamp parsing error
spark.conf.set("spark.sql.ansi.enabled", "false")

# COMMAND ----------

TableID=500
metadata = GetMetaDataDetails(TableID)
SourcePath = metadata["SourcePath"]
list_data = dbutils.fs.ls(SourcePath)

# COMMAND ----------

# DBTITLE 1,Dataload
from pyspark.sql import functions as F

# Collect table IDs from the metadata DataFrame
TablesList = DF_Metadata.select(F.col("TableID")).collect()
print(f"This is the list of tables: {TablesList}")

# Get the column names from the target table
columns_df = spark.sql("SHOW COLUMNS IN bronze.dmi_dcif")
column_names = [row.col_name for row in columns_df.collect() if row.col_name not in ["MergeHashKey", "MergeKey", "DW_Created_Date", "DW_Created_By", "DW_Modified_Date", "DW_Modified_By","CurrentRecord"]]

# Loop through the table IDs
for TableID in TablesList:
    TableID = TableID["TableID"]
    UpdatePipelineStartTime(TableID)

    auto_skipper_result = 1 #AutoSkipper(TableID)
    print(f"This is the auto skipper result: {auto_skipper_result}")

    if auto_skipper_result == 1:
        try:
            metadata = GetMetaDataDetails(TableID)
            LoadType = metadata["LoadType"]
            LastLoadColumnName = metadata["LastLoadDateColumn"]
            LastLoadDate = metadata["LastLoadDateValue"]
            DWHSchemaName = metadata["DWHSchemaName"]
            SourceTableName = metadata["SourceTableName"]
            DWHTableName = metadata["DWHTableName"]
            MergeKey = metadata["MergeKey"]
            MergeKeyColumn = metadata["MergeKeyColumn"]
            SourcePath = metadata["SourcePath"]
            SourceSelectQuery = metadata["SourceSelectQuery"]
            ColumnMapping = metadata["ColumnMapping"]
            sourcesystem=metadata['SourceSystem']
            schemanames=metadata['SourceSchema']
            PipelineRunStatus = metadata['PipelineRunStatus']
            PipelineStartTime = metadata['PipelineStartTime']
            SubjectArea=metadata['SubjectArea']
            
            list_data = dbutils.fs.ls(SourcePath)

            for data in list_data:

                # logger.info(f"processing file: {data.path}")

                if 'DCIF' in data.path:

                    logger.info(f"processing file: {data.path}")

                    # Read all files in the source directory in parallel
                    df = (
                        spark.read.format("csv")
                        .option("header", "false")
                        .option("delimiter", ",")
                        .option("quote", '"')
                        .option("escape", '"')
                        .option("skipRows", 23)
                        .load(f"{data.path}")
                    )  # Read all matching files

                    print(len(df.columns))

                    # Rename columns
                    df_renamed = df.toDF(*column_names)
                    # logger.info("completed the renaming")

                    # Type casting
                    df_typecasted = DataTypeChange(df_renamed, DWHSchemaName, DWHTableName)
                    logger.info("completed the df_typecasting")

                    #Performing data quality checks and updating the qa log table
                    try:
                        dataframe  = df_typecasted
                        table = SourceTableName
                        previous_metrics_df = spark.sql(f"""select * from config.DQlogs where source_system = '{sourcesystem}' and schema_name = '{schemanames}' and table_name = '{table}' and Validation_Date = (select max(Validation_Date) from config.DQlogs where source_system = '{sourcesystem}' and schema_name = '{schemanames}' and table_name = '{table}') """)
                        targetdataframe = get_data_quality_metrics(dataframe, sourcesystem, schemanames,table, previous_metrics_df,MergeKeyColumn,threshold_percentage=10)    
                        targetdataframe.createOrReplaceTempView("LogTable")
                        spark.sql("INSERT INTO config.DQlogs select * from LogTable")
                        print("Inserting into LogTable")
                    except Exception as e:
                        logger.error(f"Unable to log the record count {e}")

                    # Load data based on load type
                    if LoadType == "Full Load":
                        logger.info(
                            f"Full load to {DWHSchemaName}.{DWHTableName} started."
                        )
                        logger.info(f"Row Count in source: {df_typecasted.count()}")
                        try:
                            #df_typecasted.display()
                            FullLoad(TableID, df_typecasted)
                        except Exception as e:
                            logger.error(f"Error processing TableID {TableID}: {e}")
                            ErrorMessage = str(e).split('java.lang.Exception: ')[0] if 'java.lang.Exception: ' in str(e) else str(e).split('at ')[0]
                            raise e
                        logger.info(
                            f"Full load to {DWHSchemaName}.{DWHTableName} completed successfully."
                        )
                        # LastLoadDate_source = df_typecasted.agg(
                        #     F.max(LastLoadColumnName).alias("Max_Date")
                        # ).collect()[0]["Max_Date"]
                        # UpdateLastLoadDate(TableID, LastLoadDate_source)
                        ErrorMessage = "No error found"

                    elif LoadType == "Truncate and Load":
                        logger.info(
                            f"Truncate and load to {DWHSchemaName}.{DWHTableName} started."
                        )
                        logger.info(f"Row Count in source: {df_typecasted.count()}")
                        try:
                            TruncateLoad(TableID, df_typecasted)
                        except Exception as e:
                            logger.error(f"Error processing TableID {TableID}: {e}")
                            ErrorMessage = str(e).split('java.lang.Exception: ')[0] if 'java.lang.Exception: ' in str(e) else str(e).split('at ')[0]
                            raise e
                        logger.info(
                            f"Truncate and load to {DWHSchemaName}.{DWHTableName} completed successfully."
                        )
                        ErrorMessage = "No error found"

                    else:
                        logger.info("Provide proper LoadType")
                        UpdatePipelineStatusAndTime(TableID, "Succeeded")
                        ErrorMessage = "Invalid LoadType"

                    UpdatePipelineStatusAndTime(TableID, "Succeeded")

        except Exception as e:
            logger.error(f"Error processing TableID {TableID}: {e}")
            UpdatePipelineStatusAndTime(TableID, "Failed")
            ErrorMessage = str(e).split('java.lang.Exception: ')[0] if 'java.lang.Exception: ' in str(e) else str(e).split('at ')[0]
        finally:
            if 'ErrorMessage' not in locals():
                ErrorMessage = "No error found"
            # Insert pipeline run status into the log table
            spark.sql(f"""
                INSERT INTO config.email_trigger (TableID, SourceSystem, DWHTableName, DWHSchemaName, PipelineRunstatus, ErrorMessage,Processed_Date)
                VALUES ('{TableID}', '{SubjectArea}', '{DWHTableName}', '{DWHSchemaName}', '{PipelineRunStatus}', '{ErrorMessage}','{PipelineStartTime}')
            """)
    else:
        logger.info(f"Skipping TableID {TableID} as AutoSkipper is set to 1")
