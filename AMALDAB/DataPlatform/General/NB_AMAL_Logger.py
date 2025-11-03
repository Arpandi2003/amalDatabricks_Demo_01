# Databricks notebook source
# MAGIC %md
# MAGIC # NB_Logger
# MAGIC
# MAGIC ## Tracker Details
# MAGIC - Description: To get the logger  information and path for storing the log details
# MAGIC - Created Date: 12/20/2024
# MAGIC - Created By: User
# MAGIC - Modified Date: 12/30/2024
# MAGIC - Modified By: Brad Fiery
# MAGIC - Changes made:  Added path to logger Volume in Cell 4
# MAGIC

# COMMAND ----------

#Importing the required packages
import logging
from datetime import datetime
import pytz
from pyspark.dbutils import DBUtils
from pyspark.sql import SparkSession
import os
import io
from contextlib import redirect_stdout

# COMMAND ----------

# DBTITLE 1,configuration
# MAGIC %run "./NB_Configuration"

# COMMAND ----------

# DBTITLE 1,Description
"""
 This function, ErrorLogs, sets up a custom logging handler for Databricks notebooks.
 It primarily creates log files within specified directories on Databricks Volumes.

 The function takes two arguments:
   - NoteBookName (str): This is the name of the notebook to help identify the log file.
   - log_level (int, optional): The logging level, which defaults to logging.INFO.

 The function returns a tuple containing:
   - The configured logger object.
   - The log file path.
   - The log file name.

 Inside the function:
   - The current date is formatted and used to create unique log file names.
   - The path for storing the log file is set, ensuring that necessary directories exist.
   - A custom logging handler, DatabricksVolumeHandler, is defined which handles writing log entries to the specified file.
   - If writing to the file fails, logs are output to stderr (standard error output).
   - A logger is created and configured with the user-defined logging level and the custom handler.
   - An optional console handler is also added for immediate feedback on log entries.

 In case of any exceptions during setup, the function falls back to basic logging with the given log level.
"""

# COMMAND ----------

# DBTITLE 1,Logger Code
def ErrorLogs(NoteBookName, log_level=logging.INFO):
    try:
        # Setting the time for the log file name
        file_date = datetime.now(pytz.timezone('US/Central')).strftime('%Y%m%d')
        
        # Setting the path for the log file to be created with file name
        p_dir = logger_directory
          # Adjust this path as needed
        p_filename = f"{NoteBookName}_Extract_{file_date}.log"
        p_logfile = os.path.join(p_dir, file_date, p_filename)
        
        print(f"Log file path: {p_logfile}")
        
        # Create a custom logging handler for Databricks Volumes
        class DatabricksVolumeHandler(logging.Handler):
            def __init__(self, log_file_path):
                super().__init__()
                self.log_file_path = log_file_path
                
                # Ensure directory exists
                os.makedirs(os.path.dirname(log_file_path), exist_ok=True)

            def emit(self, record):
                try:
                    log_entry = self.format(record)
                    
                    # Append log entry to file
                    with open(self.log_file_path, 'a') as log_file:
                        log_file.write(log_entry + '\n')
                
                except Exception as e:
                    # print(f"Error writing to log file: {e}")
                    # Fallback to stderr if file writing fails
                    print(self.format(record), file=sys.stderr)

        # Creating a logger and setting the level
        logger = logging.getLogger(NoteBookName)
        logger.setLevel(log_level)

        # Remove any existing handlers to avoid duplication
        logger.handlers.clear()

        # Create and add the custom handler
        file_handler = DatabricksVolumeHandler(p_logfile)
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

        # Optional: Add console handler for immediate feedback
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

        return logger, p_logfile, p_filename

    except Exception as e:
        print(f"Error setting up logging: {e}")
        # Fallback to basic logging
        logging.basicConfig(level=log_level)
        logger = logging.getLogger(NoteBookName)
        return logger, None, None
