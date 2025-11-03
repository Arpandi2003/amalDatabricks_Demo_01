# Databricks notebook source
# MAGIC %run
# MAGIC ../General/NB_Configuration

# COMMAND ----------

catalog_name = dbutils.widgets.get('catalog_name')

# COMMAND ----------

spark.sql(f'use catalog {catalog_name}')

# COMMAND ----------

# DBTITLE 1,fact_session_events
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE metrics.fact_session_events (
# MAGIC     dwh_session_events_hash_id STRING,
# MAGIC     dwh_session_events_id STRING PRIMARY KEY,
# MAGIC     campaign_id STRING,
# MAGIC     session_id STRING,
# MAGIC     advertiser_id STRING,
# MAGIC     fluent_id STRING,
# MAGIC     partner_id STRING,
# MAGIC     traffic_source_id STRING,
# MAGIC     creative_id STRING,
# MAGIC     tune_offer_id STRING,
# MAGIC     transaction_id STRING,
# MAGIC     product_scope STRING,
# MAGIC     event_type STRING,
# MAGIC     conversion_goal_id STRING,
# MAGIC     action_timestamp TIMESTAMP,
# MAGIC     campaign_revenue DECIMAL(19,4),
# MAGIC     partner_revenue DECIMAL(19,4),
# MAGIC     position DECIMAL(19,4),
# MAGIC     email_coverage_flag BOOLEAN,
# MAGIC     emailmd5_coverage_flag BOOLEAN,
# MAGIC     emailsha256_coverage_flag BOOLEAN,
# MAGIC     telephone_coverage_flag BOOLEAN,
# MAGIC     phone_coverage_flag BOOLEAN,
# MAGIC     maid_coverage_flag BOOLEAN,
# MAGIC     first_name_coverage_flag BOOLEAN,
# MAGIC     order_id_coverage_flag BOOLEAN,
# MAGIC     transaction_value_coverage_flag BOOLEAN,
# MAGIC     ccBin_coverage_flag BOOLEAN,
# MAGIC     gender_coverage_flag BOOLEAN,
# MAGIC     zip_coverage_flag BOOLEAN,
# MAGIC     zippost_coverage_flag BOOLEAN,
# MAGIC     source_reference_id STRING,
# MAGIC     action_date_key INT,
# MAGIC     action_time_key INT,
# MAGIC     dwh_created_at TIMESTAMP,
# MAGIC     dwh_modified_at TIMESTAMP
# MAGIC )
# MAGIC
# MAGIC CLUSTER BY AUTO;

# COMMAND ----------

# DBTITLE 1,fact_session
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE metrics.fact_session (
# MAGIC     dwh_session_id STRING PRIMARY KEY,
# MAGIC     session_id STRING,
# MAGIC     campaign_id STRING,
# MAGIC     tune_offer_id STRING,
# MAGIC     fluent_id STRING,
# MAGIC     advertiser_id STRING,
# MAGIC     partner_id STRING,
# MAGIC     traffic_source_id STRING,
# MAGIC     creative_id STRING,
# MAGIC     product_scope STRING,
# MAGIC     total_actions INT,
# MAGIC     total_clicks INT,
# MAGIC     total_views INT,
# MAGIC     primary_conversions INT,
# MAGIC     secondary_conversions INT,
# MAGIC     first_conversion_window BIGINT,
# MAGIC     conversion_flag BOOLEAN,
# MAGIC     session_start_timestamp TIMESTAMP,
# MAGIC     session_start_date STRING,
# MAGIC     session_start_time STRING,
# MAGIC     session_end_timestamp TIMESTAMP,
# MAGIC     session_end_date STRING,
# MAGIC     session_end_time STRING,
# MAGIC     dwh_created_at TIMESTAMP,
# MAGIC     dwh_modified_at TIMESTAMP
# MAGIC )
# MAGIC
# MAGIC CLUSTER BY AUTO;
