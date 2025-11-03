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

# DBTITLE 1,DQ Source Check
# MAGIC %run "../DataQuality/DataQuality_SourceCheck"

# COMMAND ----------

# DBTITLE 1,Configuration
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

FilteredSubjectArea = dbutils.widgets.get("SubjectArea")

# COMMAND ----------

# DBTITLE 1,Get MetaData Details
#Getting the required metadata details
DF_Metadata = spark.read.table('config.metadata').filter(
(col('sourcesystem') == 'Flat_File') & 
(col('zone') == 'Bronze') & 
(col('SubjectArea').isin(FilteredSubjectArea)) 
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
            SourceTableName = metadata["SourceTableName"]
            SourcePath = metadata[
                "SourcePath"
            ]  # provide the folder path where the file is located
            SourceSelectQuery = metadata["SourceSelectQuery"]
            ColumnMapping = metadata["ColumnMapping"]
            sourcesystem = metadata["SourceSystem"]
            schemanames = metadata["SourceSchema"]

            # Read data into a Spark DataFrame
            ListFiles = [file.path for file in dbutils.fs.ls(f"{SourcePath}")]
            print(
                f"This is the output of ListFiles: {SourcePath}/{SourceTableName}.csv"
            )
            FilteredFiles = [
                file for file in ListFiles if f"{SourcePath}/{SourceTableName}" in file
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

                source_directory = FileName
                archive_directory = (
                    "/".join(source_directory.split("/")[:-3])
                    + "/archive/"
                    + "/".join(source_directory.split("/")[-2:])
                )
                display(f"Source Directory: {source_directory}")
                display(f"Archive Directory: {archive_directory}")

                # Construct the archive path
                archive_path = source_directory.replace(
                    source_directory, archive_directory
                )

                display(f"Archiving {source_directory} to {archive_directory}")

                dbutils.fs.mv(source_directory, archive_path)

                # pass

                logger.info("provide proper LoadType")
                UpdatePipelineStatusAndTime(TableID, "Succeeded")

            else:  # This else should be part of the try block
                logger.error(f"No valid files found for TableID {TableID}")
                exit(0)
        except Exception as e:
            logger.error(f"Error processing TableID {TableID}:{e}")
            UpdatePipelineStatusAndTime(TableID, "Failed")
            raise e
    else:
        logger.info(f"Skipping TableID {TableID} as AutoSkipper is set to 1")
