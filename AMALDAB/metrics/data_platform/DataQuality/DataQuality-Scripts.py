# Databricks notebook source
# MAGIC %md
# MAGIC ##Data Quality Scripts
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC * **Description:** To perform data quality checks on the tables
# MAGIC * **Created Date:** 09/03/2025
# MAGIC * **Created By:** Deeraj
# MAGIC * **Modified Date:**
# MAGIC * **Modified By - Changes Made:** Deeraj

# COMMAND ----------

catalog_name = dbutils.widgets.get("catalog_name")

# COMMAND ----------

spark.sql(f"use catalog {catalog_name}")

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace table metadata.DQMetadata
# MAGIC (
# MAGIC   Table_ID string,
# MAGIC   Source_System String,
# MAGIC   Schema_Name string,
# MAGIC   Table_Name string,
# MAGIC   Renamed_Table_Name string,
# MAGIC   Primary_Key string,
# MAGIC   Load_Type string,
# MAGIC   IsActive boolean
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace table metadata.DQlogs
# MAGIC (
# MAGIC     Source_System string,
# MAGIC     Schema_Name string,
# MAGIC     Table_Name string,
# MAGIC     Column_Name string,
# MAGIC     Validation_Date timestamp,
# MAGIC     Check_Name string,
# MAGIC     Count_Value bigint,
# MAGIC     `Status` string,
# MAGIC     Value_Difference bigint,
# MAGIC     Threshold_Difference double,
# MAGIC     Comments string
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace table metadata.DQComparison
# MAGIC (
# MAGIC   Validation_Date timestamp,
# MAGIC   Source_System string,
# MAGIC   Check_Name string,
# MAGIC   Source_Schema string,
# MAGIC   Source_Table string,
# MAGIC   Source_Column string,
# MAGIC   Source_Count string,
# MAGIC   Target_Schema string,
# MAGIC   Target_Table string,
# MAGIC   Target_Column string,
# MAGIC   Target_Count string,
# MAGIC   Is_Match string,
# MAGIC   Comments string
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO metadata.dqmetadata (Table_ID, Source_System, Schema_Name, Table_Name, Renamed_Table_Name, Primary_Key, Load_Type, IsActive) VALUES ('B1', 'databricks', 'metrics', 'dim_traffic_source', 'traffic_source_silver', 'traffic_source_id', 'Incremental load', 1);
# MAGIC INSERT INTO metadata.dqmetadata (Table_ID, Source_System, Schema_Name, Table_Name, Renamed_Table_Name, Primary_Key, Load_Type, IsActive) VALUES ('B4', 'databricks', 'metrics', 'fact_profile_request_match', 'bronze_request_logs', 'request_trace_id', 'Incremental load', 1);
# MAGIC INSERT INTO metadata.dqmetadata (Table_ID, Source_System, Schema_Name, Table_Name, Renamed_Table_Name, Primary_Key, Load_Type, IsActive) VALUES ('B7', 'databricks', 'metrics', 'fact_campaign_daily', 'offer_silver_clean1', 'dwh_campaign_id', 'Incremental load', 1);
# MAGIC INSERT INTO metadata.dqmetadata (Table_ID, Source_System, Schema_Name, Table_Name, Renamed_Table_Name, Primary_Key, Load_Type, IsActive) VALUES ('B8', 'databricks', 'metrics', 'fact_daily_metrics', 'offer_silver_clean', 'dwh_daily_id', 'Incremental load', 1);
