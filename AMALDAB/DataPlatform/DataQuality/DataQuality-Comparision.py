# Databricks notebook source
# MAGIC %md
# MAGIC ##Data Quality Comparison

# COMMAND ----------

# MAGIC %md
# MAGIC * **Description:** To perform a comparison check between the source and destination tables
# MAGIC * **Created Date:** 21/01/2025
# MAGIC * **Created By:** Naveena
# MAGIC * **Modified Date:**
# MAGIC * **Modified By:** Naveena
# MAGIC * **Changes Made:**

# COMMAND ----------

# MAGIC %md
# MAGIC #### Calling the Data Quality Source Count Check Notebook

# COMMAND ----------

# MAGIC %run "./DataQuality_SourceCheck"

# COMMAND ----------

# MAGIC %run "../General/NB_Configuration"

# COMMAND ----------

spark.sql(f"""use catalog {catalog}""")

# COMMAND ----------

# MAGIC %sql
# MAGIC Use schema config

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
from functools import *
import pytz
from datetime import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum
from datetime import datetime, timezone, timedelta
from pyspark.sql.functions import lit,when,ltrim,rtrim,upper,concat

# COMMAND ----------

et_timezone = pytz.timezone('US/Eastern')
current_time = datetime.now(et_timezone)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Updating the QA log table

# COMMAND ----------

try:
    DF_MasterMetadata = spark.sql("""
        SELECT * FROM config.dqmetadata 
        WHERE ISActive != 'false'
        ORDER BY TABLE_ID ASC
    """)
    TablesList = DF_MasterMetadata.select(col("TABLE_ID")).collect()

    for TableID in TablesList:
        try:
            TableID = TableID.TABLE_ID
            print(TableID)

            TableName = DF_MasterMetadata.select(col("Table_Name")).where(col("Table_ID") == TableID).collect()[0].Table_Name
            SchemaName = DF_MasterMetadata.select(col("Schema_Name")).where(col("Table_ID") == TableID).collect()[0].Schema_Name
            PK = DF_MasterMetadata.select(col("Primary_Key")).where(col("Table_ID") == TableID).collect()[0].Primary_Key
            source_system = DF_MasterMetadata.select(col("Source_System")).where(col("Table_ID") == TableID).collect()[0].Source_System
            LoadType = DF_MasterMetadata.select(col("Load_Type")).where(col("Table_ID") == TableID).collect()[0].Load_Type

            if SchemaName not in "source":
                if LoadType == "Incremental Load":
                    # lastloadcolumn = spark.sql(f"SELECT LastLoadDateColumn FROM config.metadata WHERE DWHTableName = '{TableName}'").collect()[0]['LastLoadDateColumn']
                    #dataframe = spark.sql(f"SELECT * FROM {SchemaName}.{TableName} WHERE DATE({lastloadcolumn}) = DATE(current_timestamp())")
                    dataframe = spark.sql(f"SELECT * FROM {SchemaName}.{TableName} WHERE DW_Modified_Date= (SELECT Max(DW_Modified_Date) FROM {SchemaName}.{TableName}) AND DATE(DW_Modified_Date) = DATE(current_timestamp())")
                else:
                    dataframe = spark.sql(f"SELECT * FROM {SchemaName}.{TableName}")

                previous_metrics_df = spark.sql(f"""
                    SELECT * FROM config.DQlogs 
                    WHERE Source_System = '{source_system}' 
                    AND Schema_Name = '{SchemaName}' 
                    AND Table_Name = '{TableName}' 
                    AND Validation_Date = (
                        SELECT MAX(Validation_Date) 
                        FROM config.DQlogs 
                        WHERE Source_System = '{source_system}' 
                        AND Schema_Name = '{SchemaName}' 
                        AND Table_Name = '{TableName}'
                    )
                """)

                targetdataframe = get_data_quality_metrics(
                    dataframe,
                    source_system,
                    SchemaName,
                    TableName,
                    previous_metrics_df,
                    PK
                )
                targetdataframe.createOrReplaceTempView("LogTable")
                spark.sql("INSERT INTO config.DQlogs SELECT * FROM LogTable")
                print("Inserted into DQlogs")
                # break
        except Exception as e:
            print(f"Error in TableID {TableID}: {e}")

except Exception as e:
    print(f"Error in MasterMetadata: {e}")

# COMMAND ----------

def comparing_metrics(source_df, dest_df):
    # Perform a join between source and bronze dataframes based on sourcesystem, tablename, and checkname

    comparison_df = source_df.alias('src').join(
        dest_df.alias('dest'),
        (col('src.Source_System') == col('dest.Source_System')) &
        (col('src.Check_Name') == col('dest.Check_Name')), 
        "inner"
    )


    # Select relevant columns and calculate if matched or not
    result_df = comparison_df.select(
        lit(current_timestamp()).alias("Validation_Date"),
        col("src.Source_System"),
        col("src.Check_Name"),
        col("src.Schema_Name").alias("Source_Schema_Name"),
        col("src.Table_Name").alias("Source_Table_Name"),
        col("src.Column_Name").alias("Source_Column_Name"),
        col("src.Count_Value").alias("Source_Count_Value"),
        col("dest.Schema_Name").alias("Dest_Schema_Name"),
        col("dest.Table_Name").alias("Dest_Table_Name"),
        col("dest.Column_Name").alias("Dest_Column_Name"),
        col("dest.Count_Value").alias("Dest_Count_Value"),
        when(col('src.Count_Value') == col('dest.Count_Value'), lit(True)).otherwise(lit(False)).alias("Is_match")
    ).where(ltrim(rtrim(upper(col('src.Column_Name')))) == ltrim(rtrim(upper(col('dest.Column_Name')))))


    # Add comments based on whether counts matched or not, capturing differences if any
    result_with_comments = result_df.withColumn(
        "Comments",
        when(col('Is_match'), lit("Counts match"))
        .otherwise(concat(lit("Counts differ - Source: "), 
                        col('Source_Count_Value').cast(StringType()), 
                        lit(", Bronze: "), 
                        col('Dest_Count_Value').cast(StringType())))
    )
    result_with_comments.createOrReplaceTempView("result_with_comments")
    spark.sql("Insert into config.dqcomparison select * from result_with_comments")
    print("Inserted into dqcomparison")

def get_table(source_system, SchemaName, TableName):
    schema_names = ','.join([f"'{schema}'" for schema in SchemaName])
    df = spark.sql(f"""
        SELECT * 
        FROM config.DQlogs 
        WHERE source_system = '{source_system}' 
          AND schema_name IN ({schema_names})
          AND table_name = '{TableName}' 
          AND Validation_Date = (
              SELECT MAX(Validation_Date) 
              FROM config.DQlogs 
              WHERE source_system = '{source_system}' 
                AND schema_name IN ({schema_names})
                AND table_name = '{TableName}'
          )
    """)
    return df

# COMMAND ----------

try:
  TablesList = DF_MasterMetadata.select(col('TABLE_ID')).collect()
  for TableID in TablesList:  
    TableID = TableID.TABLE_ID
    TableName = DF_MasterMetadata.filter(col('Table_ID') == TableID).select(col('Table_Name')).collect()[0].Table_Name
    SchemaName = DF_MasterMetadata.filter(col('Table_ID') == TableID).select(col('Schema_Name')).collect()[0].Schema_Name
    PK = DF_MasterMetadata.filter(col('Table_ID') == TableID).select(col('Primary_Key')).collect()[0].Primary_Key
    source_system = DF_MasterMetadata.filter(col('Table_ID') == TableID).select(col('Source_System')).collect()[0].Source_System
    RenamedTableName = DF_MasterMetadata.filter((col('Table_ID') == TableID)).select(col('Renamed_Table_Name')).collect()[0].Renamed_Table_Name
    print(TableID, TableName, SchemaName, PK, source_system, RenamedTableName)
    if RenamedTableName is None:
      RenamedTableName = TableName
    if SchemaName == 'Bronze':
      SourceSchemaName=['dbo','Flat_File']
      source_df = get_table(source_system,SourceSchemaName,RenamedTableName)
      dest_df = get_table(source_system,['Bronze'], TableName)
      comparing_metrics(source_df, dest_df)
    if SchemaName == 'Silver':
      source_df = get_table(source_system, 'bronze', RenamedTableName)
      dest_df = get_table(source_system, 'Silver', TableName)
      comparing_metrics(source_df, dest_df)
    if SchemaName == 'gold':
      source_df = get_table(source_system, 'silver', RenamedTableName)
      dest_df = get_table(source_system, 'gold', TableName)
      comparing_metrics(source_df, dest_df)

except Exception as e:
  print(e)

# COMMAND ----------

# MAGIC %sql
# MAGIC Select * from config.dqcomparison where Check_Name='Duplicate Count' and Date(Validation_Date)=Current_Date()
# MAGIC

# COMMAND ----------

def get_pipeline_summary(date):
    summary_query = f"""
    SELECT 
        t.TABLE_ID,
        t.TABLE_NAME,
        CASE 
            WHEN SUM(CASE WHEN q.STATUS = 'FAILED' THEN 1 ELSE 0 END) > 0 THEN 'FAILED'
            ELSE 'PASSED'
        END AS OVERALL_STATUS,
        COUNT(DISTINCT CASE WHEN q.STATUS = 'FAILED' THEN q.CHECK_NAME END) AS FAILED_CHECKS,
        COUNT(DISTINCT q.CHECK_NAME) AS TOTAL_CHECKS
    FROM 
        config.DQMetadata t
    LEFT JOIN 
        config.DQLOGS q ON t.Table_Name = q.Table_Name AND Cast(q.VALIDATION_DATE AS Date) =  Cast('{date}' AS Date)
    WHERE 
        t.ISACTIVE = TRUE
    GROUP BY 
        t.TABLE_ID, t.TABLE_NAME
    """
    
    return spark.sql(summary_query)

# COMMAND ----------

def get_comparision_summary(date):
    summary_query = f""" SELECT  * from config.DQComparison WHERE VALIDATION_DATE = '{date}' """
    return spark.sql(summary_query)

# COMMAND ----------

# MAGIC %sql
# MAGIC Select * from config.dqcomparison
