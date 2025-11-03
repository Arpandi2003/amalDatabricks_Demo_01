# Databricks notebook source
# MAGIC %run
# MAGIC ../General/NB_Configuration

# COMMAND ----------

catalog_name = dbutils.widgets.get('catalog_name')

# COMMAND ----------

spark.sql(f'use catalog {catalog_name}')

# COMMAND ----------

spark.sql("CREATE SCHEMA IF NOT EXISTS metrics")

# COMMAND ----------

spark.sql("CREATE SCHEMA IF NOT EXISTS metadata")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE VOLUME IF NOT EXISTS metadata.audit;

# COMMAND ----------

# DBTITLE 1,dim_traffic_source
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE metrics.dim_traffic_source (
# MAGIC     dwh_traffic_source_id STRING,
# MAGIC     traffic_source_id STRING PRIMARY KEY,
# MAGIC     name STRING,
# MAGIC     partner_domain STRING,
# MAGIC     sub_vertical STRING,
# MAGIC     sub_vertical_id STRING,
# MAGIC     traffic_partner_name STRING,
# MAGIC     traffic_partner_type STRING,
# MAGIC     vertical STRING,
# MAGIC     vertical_id STRING,
# MAGIC     dwh_created_at TIMESTAMP,
# MAGIC     dwh_modified_at TIMESTAMP
# MAGIC );

# COMMAND ----------

# DBTITLE 1,fact_profile_request
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE metrics.fact_profile_request_match (
# MAGIC     dwh_profile_request_id STRING,
# MAGIC     request_trace_id STRING PRIMARY KEY,
# MAGIC     fluent_id STRING,
# MAGIC     match_type STRING,
# MAGIC     request_timestamp TIMESTAMP,
# MAGIC     response_timestamp TIMESTAMP,
# MAGIC     dwh_created_at TIMESTAMP,
# MAGIC     dwh_modified_at TIMESTAMP
# MAGIC )
# MAGIC CLUSTER BY AUTO;

# COMMAND ----------

# DBTITLE 1,dim_date
# MAGIC %sql
# MAGIC -- =============================================
# MAGIC -- Create dim_date table for Databricks (Delta)
# MAGIC -- Date Range: 2000-01-01 to 2050-01-01
# MAGIC -- =============================================
# MAGIC
# MAGIC CREATE OR REPLACE TABLE metrics.dim_date AS
# MAGIC
# MAGIC WITH date_series AS (
# MAGIC   SELECT explode(
# MAGIC     sequence(
# MAGIC       to_date('2000-01-01'),
# MAGIC       to_date('2050-01-01'),
# MAGIC       interval 1 day
# MAGIC     )
# MAGIC   ) AS full_date
# MAGIC ),
# MAGIC
# MAGIC base_fields AS (
# MAGIC   SELECT
# MAGIC     -- Surrogate Key
# MAGIC     row_number() OVER (ORDER BY full_date) AS dwh_dim_date_id,
# MAGIC
# MAGIC     -- Natural Key: YYYYMMDD
# MAGIC     cast(date_format(full_date, 'yyyyMMdd') as int) AS dwh_dim_date_key,
# MAGIC
# MAGIC     -- Core Date
# MAGIC     full_date,
# MAGIC     date_format(full_date, 'yyyy-MM-dd') AS full_date_text,
# MAGIC
# MAGIC     -- Day Suffix (1st, 2nd, etc.)
# MAGIC     CASE 
# MAGIC       WHEN day(full_date) IN (1, 21, 31) THEN 'st'
# MAGIC       WHEN day(full_date) IN (2, 22) THEN 'nd'
# MAGIC       WHEN day(full_date) IN (3, 23) THEN 'rd'
# MAGIC       ELSE 'th'
# MAGIC     END AS day_suffix,
# MAGIC
# MAGIC     -- Day of Month
# MAGIC     day(full_date) AS day_of_month,
# MAGIC
# MAGIC     -- Day Short Name (Mon, Tue)
# MAGIC     upper(substring(date_format(full_date, 'EEEE'), 1, 3)) AS day_short_name,
# MAGIC
# MAGIC     -- Day of Week (1=Sunday, 7=Saturday)
# MAGIC     (cast(datediff(full_date, '1900-01-07') as int) % 7 + 1) AS day_of_week,
# MAGIC
# MAGIC     -- Day Name (Monday, Tuesday...)
# MAGIC     date_format(full_date, 'EEEE') AS day_name,
# MAGIC
# MAGIC     -- Day of Week in Month (e.g., 1st Monday = 1)
# MAGIC     ceil(day(full_date) / 7.0) AS day_of_week_in_month,
# MAGIC
# MAGIC     -- Day of Week in Year (e.g., 1st Monday of year)
# MAGIC     row_number() OVER (
# MAGIC       PARTITION BY year(full_date), (cast(datediff(full_date, '1900-01-07') as int) % 7 + 1)
# MAGIC       ORDER BY full_date
# MAGIC     ) AS day_of_week_in_year,
# MAGIC
# MAGIC     -- Day of Quarter
# MAGIC     (dayofyear(full_date) - dayofyear(trunc(full_date, 'quarter')) + 1) AS day_of_quarter,
# MAGIC
# MAGIC     -- Day of Year
# MAGIC     dayofyear(full_date) AS day_of_year,
# MAGIC
# MAGIC     -- Week of Year Name (e.g., "Week 1 2024")
# MAGIC     concat('Week ', weekofyear(full_date), ' ', year(full_date)) AS week_of_year_name,
# MAGIC
# MAGIC     -- Week of Month
# MAGIC     (weekofyear(full_date) - weekofyear(trunc(full_date, 'month')) + 1) AS week_of_month,
# MAGIC
# MAGIC     -- Week of Quarter
# MAGIC     floor((datediff(full_date, trunc(full_date, 'quarter')) / 7)) + 1 AS week_of_quarter,
# MAGIC
# MAGIC     -- Week of Year
# MAGIC     weekofyear(full_date) AS week_of_year,
# MAGIC
# MAGIC     -- Month (1–12)
# MAGIC     month(full_date) AS month,
# MAGIC
# MAGIC     -- Month Short Name (Jan, Feb)
# MAGIC     upper(substring(date_format(full_date, 'MMMM'), 1, 3)) AS month_short_name,
# MAGIC
# MAGIC     -- Month Name (January, February)
# MAGIC     date_format(full_date, 'MMMM') AS month_name,
# MAGIC
# MAGIC     -- Month of Quarter (01, 02, 03)
# MAGIC     lpad(cast((month(full_date) - 1) % 3 + 1 as string), 2, '0') AS month_of_quarter,
# MAGIC
# MAGIC     -- Quarter (1–4)
# MAGIC     quarter(full_date) AS quarter,
# MAGIC
# MAGIC     -- Quarter Name (Q1, Q2...)
# MAGIC     concat('Q', quarter(full_date)) AS quarter_name,
# MAGIC
# MAGIC     -- Year
# MAGIC     year(full_date) AS year,
# MAGIC
# MAGIC     -- Year Name (FY 2024)
# MAGIC     concat('FY ', year(full_date)) AS year_name,
# MAGIC
# MAGIC     -- Month of Year Name (e.g., Jan 2024)
# MAGIC     date_format(full_date, 'MMM yyyy') AS month_of_year_name,
# MAGIC
# MAGIC     -- MMYYYY
# MAGIC     date_format(full_date, 'MMyyyy') AS mmyyyy,
# MAGIC
# MAGIC     -- First & Last Day of Week (Sunday = start)
# MAGIC     trunc(full_date, 'WEEK') AS first_day_of_week,
# MAGIC     date_add(trunc(full_date, 'WEEK'), 6) AS last_day_of_week,
# MAGIC
# MAGIC     -- First & Last Day of Month
# MAGIC     trunc(full_date, 'MONTH') AS first_day_of_month,
# MAGIC     last_day(full_date) AS last_day_of_month,
# MAGIC
# MAGIC     -- First & Last Day of Quarter
# MAGIC     trunc(full_date, 'QUARTER') AS first_day_of_quarter,
# MAGIC     date_add(add_months(trunc(full_date, 'QUARTER'), 3), -1) AS last_day_of_quarter,
# MAGIC
# MAGIC     -- First & Last Day of Year
# MAGIC     trunc(full_date, 'YEAR') AS first_day_of_year,
# MAGIC     date_add(add_months(trunc(full_date, 'YEAR'), 12), -1) AS last_day_of_year,
# MAGIC
# MAGIC     -- Flags
# MAGIC     0 AS is_holiday,  -- Placeholder
# MAGIC
# MAGIC     -- is_weekday: 1 if Mon-Fri (2–6), 0 if weekend
# MAGIC     CASE WHEN (cast(datediff(full_date, '1900-01-07') as int) % 7 + 1) IN (2,3,4,5,6) THEN 1 ELSE 0 END AS is_weekday,
# MAGIC
# MAGIC     cast(null as string) AS holiday_name,
# MAGIC
# MAGIC     -- is_working_day: same as is_weekday for now (no holidays)
# MAGIC     CASE WHEN (cast(datediff(full_date, '1900-01-07') as int) % 7 + 1) IN (2,3,4,5,6) THEN 1 ELSE 0 END AS is_working_day,
# MAGIC
# MAGIC     0 AS is_work_holiday,
# MAGIC     cast(null as string) AS work_holiday_name,
# MAGIC
# MAGIC     -- Business-specific fields (set to NULL)
# MAGIC     cast(null as date) AS first_day_of_commission_period,
# MAGIC     cast(null as date) AS last_day_of_commission_period,
# MAGIC     cast(null as date) AS adjusted_day_end_date,
# MAGIC
# MAGIC     current_timestamp() AS dss_update_time,
# MAGIC
# MAGIC     -- Temporary row number for business day logic
# MAGIC     row_number() OVER (PARTITION BY year(full_date), month(full_date) ORDER BY full_date) AS row_num_in_month,
# MAGIC
# MAGIC     -- Sort Keys
# MAGIC     (year(full_date) * 100 + month(full_date)) AS month_sort_yyyymm,
# MAGIC     (year(full_date) * 100 + weekofyear(full_date)) AS week_sort_yyyyww,
# MAGIC
# MAGIC     -- Metadata
# MAGIC     current_timestamp() AS dwh_created_at,
# MAGIC     current_timestamp() AS dwh_modified_at,
# MAGIC
# MAGIC     -- Helper: weekday flag for business day calc
# MAGIC     CASE WHEN (cast(datediff(full_date, '1900-01-07') as int) % 7 + 1) IN (2,3,4,5,6) THEN 1 ELSE 0 END AS is_weekday_calc
# MAGIC   FROM date_series
# MAGIC ),
# MAGIC
# MAGIC -- Final step: compute business_day_of_month only for weekdays
# MAGIC final_with_business_day AS (
# MAGIC   SELECT
# MAGIC     *,
# MAGIC     -- Assign business day number only to weekdays
# MAGIC     CASE 
# MAGIC       WHEN is_weekday_calc = 1 THEN
# MAGIC         row_number() OVER (
# MAGIC           PARTITION BY year(full_date), month(full_date), is_weekday_calc
# MAGIC           ORDER BY full_date
# MAGIC         )
# MAGIC       ELSE NULL
# MAGIC     END AS business_day_of_month
# MAGIC   FROM base_fields
# MAGIC )
# MAGIC
# MAGIC -- Final SELECT: exclude helper columns
# MAGIC SELECT
# MAGIC   dwh_dim_date_id,
# MAGIC   dwh_dim_date_key,
# MAGIC   full_date,
# MAGIC   full_date_text,
# MAGIC   day_suffix,
# MAGIC   day_of_month,
# MAGIC   day_short_name,
# MAGIC   day_of_week,
# MAGIC   day_name,
# MAGIC   day_of_week_in_month,
# MAGIC   day_of_week_in_year,
# MAGIC   day_of_quarter,
# MAGIC   day_of_year,
# MAGIC   week_of_year_name,
# MAGIC   week_of_month,
# MAGIC   week_of_quarter,
# MAGIC   week_of_year,
# MAGIC   month,
# MAGIC   month_short_name,
# MAGIC   month_name,
# MAGIC   month_of_quarter,
# MAGIC   quarter,
# MAGIC   quarter_name,
# MAGIC   year,
# MAGIC   year_name,
# MAGIC   month_of_year_name,
# MAGIC   mmyyyy,
# MAGIC   first_day_of_week,
# MAGIC   last_day_of_week,
# MAGIC   first_day_of_month,
# MAGIC   last_day_of_month,
# MAGIC   first_day_of_quarter,
# MAGIC   last_day_of_quarter,
# MAGIC   first_day_of_year,
# MAGIC   last_day_of_year,
# MAGIC   is_holiday,
# MAGIC   is_weekday,
# MAGIC   holiday_name,
# MAGIC   is_working_day,
# MAGIC   is_work_holiday,
# MAGIC   work_holiday_name,
# MAGIC   first_day_of_commission_period,
# MAGIC   last_day_of_commission_period,
# MAGIC   adjusted_day_end_date,
# MAGIC   dss_update_time,
# MAGIC   business_day_of_month,
# MAGIC   weekofyear(full_date) AS week_number,
# MAGIC   month_sort_yyyymm,
# MAGIC   week_sort_yyyyww,
# MAGIC   dwh_created_at,
# MAGIC   dwh_modified_at
# MAGIC FROM final_with_business_day
# MAGIC ORDER BY full_date;

# COMMAND ----------

# DBTITLE 1,dim_time
# MAGIC %sql
# MAGIC -- =============================================
# MAGIC -- Create dim_time table for Databricks (Delta)
# MAGIC -- Time Range: 00:00:00 to 23:59:59
# MAGIC -- =============================================
# MAGIC
# MAGIC CREATE OR REPLACE TABLE metrics.dim_time AS
# MAGIC
# MAGIC WITH time_series AS (
# MAGIC   SELECT explode(sequence(0, 86399)) AS sec  -- 0 to 86399 seconds in a day
# MAGIC ),
# MAGIC
# MAGIC time_base AS (
# MAGIC   SELECT
# MAGIC     to_timestamp(sec) AS ts  -- Convert seconds to timestamp
# MAGIC   FROM time_series
# MAGIC ),
# MAGIC
# MAGIC time_fields AS (
# MAGIC   SELECT
# MAGIC     row_number() OVER (ORDER BY ts) AS dwh_dim_time_id,
# MAGIC
# MAGIC     -- Time Key: HHMMSS as integer (e.g., 130545 for 13:05:45)
# MAGIC     (hour(ts) * 10000 + minute(ts) * 100 + second(ts)) AS dim_time_key,
# MAGIC
# MAGIC     -- Time as string (HH:mm:ss)
# MAGIC     date_format(ts, 'HH:mm:ss') AS time,
# MAGIC
# MAGIC     -- Hour (0–23)
# MAGIC     hour(ts) AS hour,
# MAGIC
# MAGIC     -- Hour Name (e.g., 1 AM, 12 PM)
# MAGIC     CASE
# MAGIC       WHEN hour(ts) = 0 THEN '12 AM'
# MAGIC       WHEN hour(ts) < 12 THEN concat(hour(ts), ' AM')
# MAGIC       WHEN hour(ts) = 12 THEN '12 PM'
# MAGIC       ELSE concat(hour(ts) - 12, ' PM')
# MAGIC     END AS hour_name,
# MAGIC
# MAGIC     -- Military Hour (same as hour)
# MAGIC     hour(ts) AS military_hour,
# MAGIC
# MAGIC     -- Military Hour Name (e.g., 00:00:00, 13:00:00)
# MAGIC     concat(lpad(hour(ts), 2, '0'), ':00:00') AS military_hour_name,
# MAGIC
# MAGIC     -- Minute (as string, 00–59)
# MAGIC     lpad(minute(ts), 2, '0') AS minute,
# MAGIC
# MAGIC     -- Second (as string, 00–59)
# MAGIC     lpad(second(ts), 2, '0') AS second,
# MAGIC
# MAGIC     -- AM/PM indicator
# MAGIC     CASE WHEN hour(ts) < 12 THEN 'AM' ELSE 'PM' END AS am_pm,
# MAGIC
# MAGIC     -- Standard Time (e.g., 1:05:30 PM)
# MAGIC     concat(
# MAGIC       CASE
# MAGIC         WHEN hour(ts) = 0 THEN '12'
# MAGIC         WHEN hour(ts) <= 12 THEN cast(hour(ts) AS string)
# MAGIC         ELSE cast(hour(ts) - 12 AS string)
# MAGIC       END,
# MAGIC       ':',
# MAGIC       lpad(minute(ts), 2, '0'),
# MAGIC       ':',
# MAGIC       lpad(second(ts), 2, '0'),
# MAGIC       ' ',
# MAGIC       CASE WHEN hour(ts) < 12 THEN 'AM' ELSE 'PM' END
# MAGIC     ) AS standard_time,
# MAGIC
# MAGIC     -- Metadata
# MAGIC     current_timestamp() AS dwh_modified_at,
# MAGIC     current_timestamp() AS dwh_created_at
# MAGIC
# MAGIC   FROM time_base
# MAGIC )
# MAGIC
# MAGIC -- Final SELECT
# MAGIC SELECT * FROM time_fields
# MAGIC ORDER BY dim_time_key;

# COMMAND ----------

# DBTITLE 1,fact_campaign_daily
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE metrics.fact_campaign_daily (
# MAGIC   dwh_campaign_id STRING PRIMARY KEY,
# MAGIC   event_date STRING,
# MAGIC   date_key INT,
# MAGIC   campaign_id STRING,
# MAGIC   partner_id STRING,
# MAGIC   advertiser_id STRING,
# MAGIC   current_ctr DECIMAL(19,4),
# MAGIC   ctr_seasonality DECIMAL(19,4),
# MAGIC   current_cvr DECIMAL(19,4),
# MAGIC   cvr_seasonality DECIMAL(19,4),
# MAGIC   dwh_created_at TIMESTAMP,
# MAGIC   dwh_modified_at TIMESTAMP
# MAGIC )
# MAGIC CLUSTER BY (event_date, campaign_id, partner_id, advertiser_id);
# MAGIC
# MAGIC

# COMMAND ----------

# DBTITLE 1,fact_daily_metrics
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE metrics.fact_daily_metrics (
# MAGIC     dwh_daily_id STRING,
# MAGIC     event_date DATE,
# MAGIC     campaign_id STRING,
# MAGIC     partner_id STRING,
# MAGIC     advertiser_id STRING,
# MAGIC     tune_offer_id STRING,
# MAGIC     source_id STRING,
# MAGIC     creative_id STRING,
# MAGIC     fluent_id STRING,
# MAGIC     product_scope STRING,
# MAGIC     position DECIMAL(19,4),
# MAGIC     clicks BIGINT,
# MAGIC     views BIGINT,
# MAGIC     sessions BIGINT,
# MAGIC     distinct_sessions BIGINT,
# MAGIC     possible_session BIGINT,
# MAGIC     suppressions BIGINT,
# MAGIC     partner_transactions BIGINT,
# MAGIC     primary_conversions BIGINT,
# MAGIC     funnel_conversions BIGINT,
# MAGIC     campaign_revenue DECIMAL(19,2),
# MAGIC     partner_revenue DECIMAL(19,2),
# MAGIC     email_coverage BIGINT,
# MAGIC     emailmd5_coverage BIGINT,
# MAGIC     emailsha256_coverage BIGINT,
# MAGIC     telephone_coverage BIGINT,
# MAGIC     phone_coverage BIGINT,
# MAGIC     maid_coverage BIGINT,
# MAGIC     first_name_coverage BIGINT,
# MAGIC     order_id_coverage BIGINT,
# MAGIC     transaction_value_coverage BIGINT,
# MAGIC     ccbin_coverage BIGINT,
# MAGIC     gender_coverage BIGINT,
# MAGIC     zip_coverage BIGINT,
# MAGIC     zippost_coverage BIGINT,
# MAGIC     first_conversion_window BIGINT,
# MAGIC     current_ctr DECIMAL(19, 2),
# MAGIC     ctr_seasonality DECIMAL(19, 2),
# MAGIC     current_cvr DECIMAL(19, 2),
# MAGIC     cvr_seasonality DECIMAL(19, 2),
# MAGIC     dwh_created_at TIMESTAMP,
# MAGIC     dwh_modified_at TIMESTAMP
# MAGIC )
# MAGIC CLUSTER BY (event_date, campaign_id, partner_id, advertiser_id);

# COMMAND ----------

# DBTITLE 1,referential_integrity_log
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE metrics.referential_integrity_log (
# MAGIC     fact_table_name STRING,
# MAGIC     column_name STRING,
# MAGIC     value STRING,
# MAGIC     message STRING,
# MAGIC     load_time TIMESTAMP
# MAGIC )

# COMMAND ----------

# DBTITLE 1,business_integrity
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE metrics.business_integrity_check (
# MAGIC     metric_name STRING,
# MAGIC     measure_value DOUBLE,
# MAGIC     message STRING,
# MAGIC     action_time TIMESTAMP
# MAGIC )
