# Databricks notebook source
import logging
import os
from datetime import datetime
import pytz
from pyspark.dbutils import DBUtils
from pyspark.sql import SparkSession
import io
from contextlib import redirect_stdout

# COMMAND ----------

catalog_name = dbutils.widgets.get('catalog_name')

# COMMAND ----------

spark.sql(f"use catalog {catalog_name}")

# COMMAND ----------

def ErrorLogger(NoteBookName):
    spark = SparkSession.builder.getOrCreate()
    dbutils = DBUtils(spark)
    # Setting the time for the log file name
    file_date = datetime.now(pytz.timezone('US/Central')).strftime('%Y%m%d')
    # Setting the path for the log file to be created with file name
    p_dir = f'/Volumes/{catalog_name}/metadata/audit/log/' # Adjust this path as needed
    p_filename = f"/{NoteBookName}_Extract_{file_date}.log"
    p_logfile = p_dir + file_date + p_filename
    print(f"Log file path: {p_logfile}")
   
    # Create a custom logging handler for Databricks Volumes
    class DatabricksVolumeHandler(logging.Handler):
        def __init__(self, log_file_path):
            super().__init__()
            self.log_file_path = log_file_path

        def emit(self, record):
            log_entry = self.format(record)
            
            # Read existing content
            try:
                existing_content = dbutils.fs.head(self.log_file_path)
            except Exception as e:
                # If file doesn't exist, set existing_content to an empty string
                existing_content = ""
            
            # Append new log entry
            updated_content = existing_content + log_entry + '\n'
            
            # Write updated content back to file
            try:
                with io.StringIO() as buf, redirect_stdout(buf):
                    dbutils.fs.put(self.log_file_path, updated_content, True)
            except Exception as e:
                print(f"Failed to write updated content: {e}")

    # Creating a logger and setting the level for the logger
    logger = logging.getLogger('Extract')
    logger.setLevel(logging.DEBUG)

    # Remove any existing handlers to avoid duplication
    logger.handlers = []

    # Create and add the custom handler
    vh = DatabricksVolumeHandler(p_logfile)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    vh.setFormatter(formatter)
    logger.addHandler(vh)

    return logger, p_logfile, p_filename
