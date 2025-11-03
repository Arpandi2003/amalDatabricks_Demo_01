# Databricks notebook source
# MAGIC %md
# MAGIC **Notebook Name:** NB_FlatFile_Axiom_Extraction_Framework_History <br>
# MAGIC **Created By:** Pandi Anbu <br>
# MAGIC **Created Date:** 03/14/2025<br>
# MAGIC **Modified By:** Pandi Anbu<br>
# MAGIC **Modified Date** 03/14/2025

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

dbutils.widgets.text("DO_YOU_WANNA_TEST(YES/NO)", "YES")
testrun = dbutils.widgets.get("DO_YOU_WANNA_TEST(YES/NO)")

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

# DBTITLE 1,DataQuality
# MAGIC %run "../DataQuality/DataQuality_SourceCheck"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Calling Configuration Notebook

# COMMAND ----------

# MAGIC %run "../General/NB_Configuration"

# COMMAND ----------

NotebookName = f"NBSourceToRaw_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
# Initialize logger
logger, p_logfile, p_filename = ErrorLogs(NotebookName)
        
#Log the start of the pipeline
logger.info(f"Starting the pipeline for batch: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

# COMMAND ----------

# DBTITLE 1,initialize catalog
spark.sql(f"""use catalog {catalog}""")

# COMMAND ----------

Archive = axiom_archive
input = axiom_input
volume=f"/Volumes/{catalog}/config/axiom/"

# COMMAND ----------

# for row in dbutils.fs.ls(input):
#   dbutils.fs.mv(row.path,volume)

# COMMAND ----------

def movetoarchieve(path):
    if path:
        new_path = Archive + path.split('/')[-2]+'.zip'
        input_path = input + path.split('/')[-2]+'.zip'
        dbutils.fs.mv(input_path,new_path)
        logger.info("Moving Zip file to Archive")

# COMMAND ----------

def removefolders(path):
    if path:
        new_path = input + path.split('/')[-2] + '/'
        print("removed the file for ", new_path)
        logger.info("Moving Zip file to Archive")
        dbutils.fs.rm(new_path, recurse=True)
        movetoarchieve(path)
        logger.info("Zip File Has been moved for ", path)

# COMMAND ----------

from datetime import datetime

def generatedatefromInteger(LastLoadDate):
    if LastLoadDate is not None:
      if len(str(LastLoadDate))==6:
        LastLoadDate = str(LastLoadDate)+'01'
      time = datetime.strptime(LastLoadDate, '%Y%m%d')
      # LastLoadDate = datetime.strptime(LastLoadDate, '%Y-%m-%d ')
      return time
    else:
      return None
    

generatedatefromInteger('202401')

# COMMAND ----------

import time
def generateIntfromDate(LastLoadDate,duration):
    if LastLoadDate is not None:
        # LastLoadDate = datetime.strptime(LastLoadDate, "%Y-%m-%d %H:%M:%S")
        if duration == "Monthly":
            integer = int(LastLoadDate.strftime("%Y%m"))
        else:
            integer = int(LastLoadDate.strftime("%Y%m%d"))
        return integer
    else:
        return None


# print(generateIntfromDate("2024-01-01 00:00:00","Monthly"))

# COMMAND ----------

# DBTITLE 1,Get Volume Info
# Verify the path exists
try:
    data = dbutils.fs.ls(f"/Volumes/{catalog}/config/axiom/")
except Exception as e:
    print(f"Error: {e}")

# If the path exists, proceed with creating the DataFrame
if 'data' in locals():
    schema = ['path', 'name', 'size', 'modificationTime']
    df = spark.createDataFrame(data, schema)
    df.createOrReplaceTempView("vw_axiom")
    display(df)

# COMMAND ----------

# DBTITLE 1,unzip the data
import zipfile

# Ensure the paths are correct and the files are zip files
for row in df.collect():
    path = row['path']
    if path.endswith('.zip'):
        try:
            source_path = path.split(":")[1]
            foldername = path.split("/")[-1].split(".")[0]
            with zipfile.ZipFile(source_path, 'r') as zip_ref:
                dbutils.fs.mkdirs(volume + '/' + foldername)
                zip_ref.extractall(volume + '/' + foldername)
            print(f"Extraction completed for: {path}")
        except Exception as e:
            print(f"Error processing file {path}: {e}")
    else:
        print(f"Skipping non-zip file: {path}")

# COMMAND ----------

# DBTITLE 1,get the file list
data = dbutils.fs.ls(f"/Volumes/{catalog}/config/axiom")

schema = ['path','name','size','modificationTime']

df = spark.createDataFrame(data, schema) 

df.createOrReplaceTempView("vw_axiom")

# COMMAND ----------

# MAGIC %sql
# MAGIC Select * from vw_axiom

# COMMAND ----------

# DBTITLE 1,Axiom_cds -> YAMO
SourceTableList = sql(
    "select TableID,DWHSchemaName,DWHTableName,LoadType,MergeKey,SourcePath,LastLoadDateColumn,LastLoadDateValue from config.metadata where SubjectArea='Axiom'"
).collect()

data = []
for row in SourceTableList:
    SourcePath = row['SourcePath'].lower()
    SourcePathModified = SourcePath[:-1] + '_2'
    SourcePath = [
        row['path'] for row in spark.sql(
            f"select path from vw_axiom where (lower(path) like '%{SourcePath}%' or lower(path) like '%{SourcePathModified}%') and lower(path) not like '%.zip'"
        ).collect()
    ]
    TotalCount = spark.sql(f"select count(1) as count from {row['DWHSchemaName']}.{row['DWHTableName']}").collect()[0][0]
    # print(count)
    data.append([
        row['TableID'], row['DWHSchemaName'], row['DWHTableName'], row['LoadType'], row['MergeKey'], SourcePath,row['LastLoadDateColumn'],row['LastLoadDateValue'],TotalCount
    ])

schema = ['TableID', 'DWHSchemaName', 'DWHTableName', 'LoadType', 'MergeKey', 'SourcePath','LastLoadDateColumn','LastLoadDateValue',"RowCount"]
Final_df = spark.createDataFrame(data, schema=schema)
display(Final_df)

# COMMAND ----------

# Set the spark.sql.ansi.enabled configuration to false to bypass the timestamp parsing error
spark.conf.set("spark.sql.ansi.enabled", "false")

# COMMAND ----------

for info in Final_df.collect():
    DWHSchemaName = info["DWHSchemaName"]
    DWHTableName = info["DWHTableName"]
    SourcePath = info["SourcePath"]
    LoadType = info["LoadType"]
    TableID = info["TableID"]
    LastLoadColumn = info["LastLoadDateColumn"]
    LastLoadDate = info["LastLoadDateValue"]

    UpdatePipelineStartTime(TableID)
    auto_skipper = 1

    if auto_skipper == 1:
        for path_actual in SourcePath:
            path = path_actual.split(":")[1]
            try:
                for file in dbutils.fs.ls(path):
                    logger.info(f"Processing file: {path}")
                    df = spark.read.csv(
                        file.path.split(":")[1],
                        header=True,
                        inferSchema=True,
                        sep="|",
                    )

                    if df.count() == 0:
                        logger.info(f"Skipping file: {file.path} as it is empty")
                        removefolders(path_actual)
                        raise Exception("Empty file")
                    else:
                        if LoadType == "Truncate and Load":
                            logger.info(f"Truncate and Load in progress for {DWHTableName}")
                            try:
                                df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{DWHSchemaName}.{DWHTableName}")
                            except Exception as e:
                                logger.error(f"Error during TruncateLoad: {e}")
                            logger.info(f"Data loaded for {DWHTableName}")

                        elif LoadType == "Incremental Load":
                            try:
                                logger.info(f"Incremental load is in progress for {DWHTableName}")
                                df.createOrReplaceTempView("VW_Source")
                                
                                if LastLoadColumn == "YRMO":
                                    LastLoadDateFormatted = generateIntfromDate(LastLoadDate, "Monthly")
                                elif LastLoadColumn == "YRMODAY":
                                    LastLoadDateFormatted = generateIntfromDate(LastLoadDate, "Weekly")
                                else:
                                    logger.error("Please validate the lastloaddate column")

                                df_incremental = spark.sql(
                                    f"select * from VW_Source where {LastLoadColumn}>='{LastLoadDateFormatted}'"
                                )
                                
                                IncrementalLoad(TableID, df_incremental)
                                logger.info(f"Incremental load completed for {DWHTableName}")

                                maxdate = spark.sql(
                                    f"select max({LastLoadColumn}) as max_date from {DWHSchemaName}.{DWHTableName}"
                                ).collect()[0][0]
                                maxdate_formatted = generatedatefromInteger(str(maxdate))
                                logger.info(f"Max date formatted: {maxdate_formatted}")
                                UpdateLastLoadDate(TableID, maxdate_formatted)
                                logger.info(f"Data loaded for {DWHTableName}")
                            except Exception as e:
                                logger.error(f"Error during IncrementalLoad: {e}")
                                UpdatePipelineStatusAndTime(TableID, "Failed")
                    removefolders(path_actual)
            except Exception as e:
                print("Issue on the", path)
            UpdatePipelineStatusAndTime(TableID, "Succeeded")
    else:
        logger.error(f"Skipping file for {DWHSchemaName}.{DWHTableName}")
