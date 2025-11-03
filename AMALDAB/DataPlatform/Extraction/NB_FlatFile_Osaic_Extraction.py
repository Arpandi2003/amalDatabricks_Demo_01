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

# DBTITLE 1,utilities
# MAGIC %run "../General/NB_AMAL_Utilities"

# COMMAND ----------

# DBTITLE 1,DQ SourceCheck
# MAGIC %run "../DataQuality/DataQuality_SourceCheck"

# COMMAND ----------

# DBTITLE 1,configuration
# MAGIC %run "../General/NB_Configuration"

# COMMAND ----------

notebook_name = f"NB_SourceToRaw_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
ErrorLogger = ErrorLogs(f"{notebook_name}")
logger = ErrorLogger[0]
        # Log the start of the pipeline
logger.info(f"Starting the pipeline for batch: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

# COMMAND ----------

# DBTITLE 1,initialize catalog
spark.sql(f"""use catalog {catalog}""")

# COMMAND ----------

# DBTITLE 1,Get MetaData Details
#Getting the required metadata details
DF_Metadata = spark.read.table('config.metadata').filter(
(col('sourcesystem') == 'Flat_File') & 
(col('zone') == 'Bronze') & 
(col('SubjectArea') == 'Osaic')
)
display(DF_Metadata)

# COMMAND ----------

# DBTITLE 1,Dataload
# This code processes a list of table IDs retrieved from metadata, reads the corresponding data from source files,transforms it as needed, and loads it into the target system based on the specified load type, updating the pipeline status accordingly.

# Collect table IDs from the metadata DataFrame

TablesList = DF_Metadata.select(col("TableID")).collect()
print(f"This is the list of tables: {TablesList}")

# Looping through the table IDs

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
            SourcePath = metadata["SourcePath"]  # provide the folder path where the file is located
            SourceSelectQuery = metadata["SourceSelectQuery"]
            ColumnMapping = metadata["ColumnMapping"]
            sourcesystem = metadata["SourceSystem"]
            schemanames = metadata["SourceSchema"]
            PipelineRunStatus = metadata['PipelineRunStatus']
            PipelineStartTime = metadata['PipelineStartTime']
            SubjectArea=metadata['SubjectArea']

            # Read data into a Spark DataFrame
            ListFiles = [file.path for file in dbutils.fs.ls(f"{SourcePath}")]
            print(
                f"This is the output of ListFiles: {SourcePath}/{SourceTableName}.csv"
            )
            FilteredFiles = [
                file for file in ListFiles if f"{SourcePath}/{SourceTableName}_" in file
            ]

            # FilteredFiles = ListFiles
            print(
                f" This is the contents of the Filtered Files variable: {FilteredFiles}"
            )
            SourceDir = SourcePath.split("/")[-2]
            print(f"source dir: {SourceDir}")

            # Initialize df to None
            df = None
            for FileName in FilteredFiles:
                try:
                    # Read CSV file with specific options
                    try:
                        df_temp = (
                            spark.read.format("csv")
                            .option("header", "True")
                            .load(f"{FileName}", skipRows=1)
                        )

                        # Convert to Pandas DataFrame
                        pandas_df = df_temp.toPandas()
                        # pandas_df.display()
                        print("data read successfully")
                    except Exception as e:
                        logger.info(f"Error processing file {FileName}: {e}")
                        ErrorMessage = f"Error processing file {FileName}: {e}"
                        continue  # Skip the file and move to the next one

                    # Use the second row as header

                    # pandas_df.columns = pandas_df.iloc[1]

                    pandas_df.columns = [
                        col
                        for col in spark.table(
                            f"{DWHSchemaName}.{DWHTableName}"
                        ).columns
                        if col
                        not in [
                            "DW_Created_Date",
                            "DW_Created_By",
                            "DW_Modified_Date",
                            "DW_Modified_By",
                            "CurrentRecord",
                            "MergeKey",
                            "MergeHashKey"
                        ]
                    ]
                    # Drop the first two rows (index 0 and 1)
                    pandas_df = pandas_df.drop([0, 1]).reset_index(drop=True)

                    # Drop the last row

                    pandas_df = pandas_df[:-1].reset_index(drop=True)

                    # Convert back to Spark DataFrame with inferred schema

                    df = spark.createDataFrame(pandas_df)

                    # Proceed with column renaming and type casting
                    df_renamed = RenameColumns(df, ColumnMapping)
                    logger.info("Renaming Part is Completed")
                    df_typecasted = DataTypeChange(
                        df_renamed, DWHSchemaName, DWHTableName
                    )
                    df = df_typecasted
                    logger.info("Type casting Part is Completed")
                    print("transformations complete")
                except Exception as e:
                    logger.error(f"Error processing file {FileName}:{e}")
                    ErrorMessage = f"Error processing file {FileName}: {e}"
                    pass
                    continue  # Skip the file and move to the next one

                if df is not None:
                    if LoadType == "Incremental Load":
                        logger.info(
                            f"Incremental load to {DWHSchemaName}.{DWHTableName} started."
                        )
                        logger.info(f"Row Count in source: {df.count()}")
                        
                        try:
                            IncrementalLoad(TableID, df)
                            logger.info(
                                f"Incremental load to {DWHSchemaName}.{DWHTableName} completed successfully."
                            )
                            logger.info(
                                f"Row count in destination: {spark.read.table(f'{DWHSchemaName}.{DWHTableName}').count()}"
                            )
                            LastLoadDate_source = df.agg(
                                max(LastLoadColumnName).alias("Max_Date")
                            ).collect()[0]["Max_Date"]

                        except Exception as e:
                            logger.error(f"Error processing file {FileName}:{e}")
                            ErrorMessage = str(e).split('java.lang.Exception: ')[0] if 'java.lang.Exception: ' in str(e) else str(e).split('at ')[0]
                        try:
                            dataframe = df
                            table = SourceTableName
                            previous_metrics_df = spark.sql(
                                f"""select * from config.DQlogs where source_system = '{sourcesystem}' and schema_name = '{schemanames}' and table_name = '{DWHTableName}' and Validation_Date = (select max(Validation_Date) from config.DQlogs where source_system = '{sourcesystem}' and schema_name = '{schemanames}' and table_name = '{DWHTableName}') """
                            )
                            targetdataframe = get_data_quality_metrics(
                                dataframe,
                                sourcesystem,
                                schemanames,
                                table,
                                previous_metrics_df,
                                MergeKey,
                                threshold_percentage=10,
                            )
                            targetdataframe.createOrReplaceTempView("LogTable")
                            spark.sql(
                                "INSERT INTO config.DQlogs select * from LogTable"
                            )
                            print("Inserting into LogTable")
                        except Exception as e:
                            logger.error(f"Unable to log the record count {e}")
                        UpdateLastLoadDate(TableID, LastLoadDate_source)

                    elif LoadType == "Truncate and Load":
                        # Truncate and Load Type UpdatePipelineStatusAndTime(TableID, status='Failed')
                        print("performing truncate and load")
                        logger.info(
                            f"Truncate and load to {DWHSchemaName}.{DWHTableName} started."
                        )
                        logger.info(f"Row Count in source: {df.count()}")
                        
                        try:
                            TruncateLoad(TableID, df)
                            logger.info(
                                f"Truncate and load to {DWHSchemaName}.{DWHTableName} completed successfully."
                            )
                            logger.info(
                                f"Row count in destination: {spark.read.table(f'{DWHSchemaName}.{DWHTableName}').count()}"
                            )

                        except Exception as e:
                            logger.error(f"Error processing file {FileName}:{e}")
                            ErrorMessage = str(e).split('java.lang.Exception: ')[0] if 'java.lang.Exception: ' in str(e) else str(e).split('at ')[0]

                        try:
                            dataframe = df
                            table = SourceTableName
                            previous_metrics_df = spark.sql(
                                f"""select * from config.DQlogs where source_system = '{sourcesystem}' and schema_name = '{schemanames}' and table_name = '{DWHTableName}' and Validation_Date = (select max(Validation_Date) from config.DQlogs where source_system = '{sourcesystem}' and schema_name = '{schemanames}' and table_name = '{DWHTableName}') """
                            )
                            targetdataframe = get_data_quality_metrics(
                                dataframe,
                                sourcesystem,
                                schemanames,
                                table,
                                previous_metrics_df,
                                MergeKey,
                                threshold_percentage=10,
                            )
                            targetdataframe.createOrReplaceTempView("LogTable")
                            spark.sql(
                                "INSERT INTO config.DQlogs select * from LogTable"
                            )
                            print("Inserting into LogTable")
                        except Exception as e:
                            logger.error(f"Unable to log the record count {e}")

                    elif LoadType == "Full Load":
                        print("performing Full load")
                        logger.info(
                            f"Full Load to {DWHSchemaName}.{DWHTableName} started."
                        )
                        logger.info(f"Row Count in source: {df.count()}")
                        
                        try:
                            FullLoad(TableID, df)
                            #df.overwrite.mode("overWrite").option("overwriteSchema","true").saveAsTable(f"{DWHSchemaName}.{DWHTableName}")
                            logger.info(
                                f"Full load to {DWHSchemaName}.{DWHTableName} completed successfully."
                            )
                            logger.info(
                                f"Row count in destination: {spark.read.table(f'{DWHSchemaName}.{DWHTableName}').count()}"
                            )

                        except Exception as e:
                            logger.error(f"Error processing file {FileName}:{e}")
                            ErrorMessage = str(e).split('java.lang.Exception: ')[0] if 'java.lang.Exception: ' in str(e) else str(e).split('at ')[0]

                        try:
                            dataframe = df
                            table = SourceTableName
                            previous_metrics_df = spark.sql(
                                f"""select * from config.DQlogs where source_system = '{sourcesystem}' and schema_name = '{schemanames}' and table_name = '{DWHTableName}' and Validation_Date = (select max(Validation_Date) from config.DQlogs where source_system = '{sourcesystem}' and schema_name = '{schemanames}' and table_name = '{DWHTableName}') """
                            )
                            targetdataframe = get_data_quality_metrics(
                                dataframe,
                                sourcesystem,
                                schemanames,
                                table,
                                previous_metrics_df,
                                MergeKey,
                                threshold_percentage=10,
                            )
                            targetdataframe.createOrReplaceTempView("LogTable")
                            spark.sql(
                                "INSERT INTO config.DQlogs select * from LogTable"
                            )
                            print("Inserting into LogTable")
                        except Exception as e:
                            logger.error(f"Unable to log the record count {e}")

                    else:
                        logger.error(f"Invalid LoadType {LoadType} for TableID {TableID}")
                        ErrorMessage = "Invalid LoadType"
                        
                logger.info("provide proper LoadType")
                ErrorMessage = "Invalid LoadType"
                UpdatePipelineStatusAndTime(TableID, "Succeeded")

                # Update SQL statement with the formatted timestamp

            else:  # This else should be part of the try block
                logger.error(f"No valid files found for TableID {TableID}")
                ErrorMessage = f"No valid files found for TableID {TableID}"
        except Exception as e:
            logger.error(f"Error processing TableID {TableID}:{e}")
            UpdatePipelineStatusAndTime(TableID, "Failed")
            ErrorMessage = str(e).split('java.lang.Exception: ')[0] if 'java.lang.Exception: ' in str(e) else str(e).split('at ')[0]
        finally:
            # Insert pipeline run status into the log table
            spark.sql(f"""
                INSERT INTO config.email_trigger (TableID, SourceSystem, DWHTableName, DWHSchemaName, PipelineRunstatus, ErrorMessage,Processed_Date)
                VALUES ('{TableID}', '{SubjectArea}', '{DWHTableName}', '{DWHSchemaName}', '{PipelineRunStatus}', '{ErrorMessage}','{PipelineStartTime}')
            """)
    else:
        logger.info(f"Skipping TableID {TableID} as AutoSkipper is set to 1")
