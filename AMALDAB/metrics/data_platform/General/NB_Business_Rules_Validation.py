# Databricks notebook source
catalog_name = dbutils.widgets.get('catalog_name')

# COMMAND ----------

spark.sql(f"use catalog {catalog_name}")

# COMMAND ----------

# MAGIC %run
# MAGIC ../General/NB_Configuration

# COMMAND ----------

spark.sql(f"USE schema {schema_name}")

# COMMAND ----------

df = spark.sql('''
               -- Alert: Metrics below lower threshold
WITH derived_metrics AS (

  SELECT
    (count(case when fluent_id is not null then 1 end) * 100.0) / nullif(count(request_trace_id), 0) AS total_match_rate,
    (count(case when fluent_id is not null and lower(match_type) = 'email' then 1 end) * 100.0) / nullif(count(request_trace_id), 0) AS email_match_rate,
    (count(case when fluent_id is not null and lower(match_type) = 'phone' then 1 end) * 100.0) / nullif(count(request_trace_id), 0) AS phone_match_rate,
    (count(case when fluent_id is not null and lower(match_type) = 'maid' then 1 end) * 100.0) / nullif(count(request_trace_id), 0) AS maid_match_rate,
    (count(case when fluent_id is not null and lower(match_type) = 'ip' then 1 end) * 100.0) / nullif(count(request_trace_id), 0) AS ip_match_rate
  FROM fact_profile_request_match 

)

-- Final Alert Union

SELECT 
  'PossibleSession' AS metric_name,
  measure(possible_session) AS measure_value,
  CASE WHEN measure(possible_session) < 100 THEN 'Threshold deviated from lower bound value: < 100/day' END AS message,
  current_timestamp() AS action_time
FROM fact_daily_metric_view
where event_date = (select max(event_date) from fact_daily_metric_view)

UNION ALL

SELECT 
  'total_views' AS metric_name,
  measure(total_views) AS measure_value,
  CASE WHEN measure(total_views) < 100 THEN 'Threshold deviated from lower bound value: < 100/day' END AS message,
  current_timestamp() AS action_time
FROM fact_daily_metric_view
where event_date = (select max(event_date) from fact_daily_metric_view)

UNION ALL

SELECT 
  'TotalClicks' AS metric_name,
  measure(total_clicks) AS measure_value,
  CASE WHEN measure(total_clicks) < 100 THEN 'Threshold deviated from lower bound value: < 100/day' END AS message,
  current_timestamp() AS action_time
FROM fact_daily_metric_view
where event_date = (select max(event_date) from fact_daily_metric_view)

UNION ALL

SELECT 
  'PartnerTransaction' AS metric_name,
  measure(partner_transactions) AS measure_value,
  CASE WHEN measure(partner_transactions) = 0 THEN 'Threshold deviated: Partner Transaction = 0 for >1h on active partner' END AS message,
  current_timestamp() AS action_time
FROM fact_daily_metric_view
where event_date = (select max(event_date) from fact_daily_metric_view)
UNION ALL

SELECT 
  'PrimaryConversion' AS metric_name,
  measure(primary_conversions) AS measure_value,
  CASE WHEN measure(primary_conversions) = 0 THEN 'Threshold deviated: Primary Conversion = 0 over 24h with active traffic' END AS message,
  current_timestamp() AS action_time
FROM fact_daily_metric_view
where event_date = (select max(event_date) from fact_daily_metric_view)
UNION ALL

SELECT 
  'CampaignRevenue' AS metric_name,
  measure(campaign_revenue) AS measure_value,
  CASE WHEN measure(campaign_revenue) < 1 THEN 'Threshold deviated: Campaign Revenue < $1/day' END AS message,
  current_timestamp() AS action_time
FROM fact_daily_metric_view
where event_date = (select max(event_date) from fact_daily_metric_view)
UNION ALL

SELECT 
  'PartnerRevenue' AS metric_name,
  measure(partner_revenue) AS measure_value,
  CASE WHEN measure(partner_revenue) < 1 THEN 'Threshold deviated: Partner Revenue < $1/day' END AS message,
  current_timestamp() AS action_time
FROM fact_daily_metric_view
where event_date = (select max(event_date) from fact_daily_metric_view)
UNION ALL

SELECT 
  'CampaignRPS' AS metric_name,
  measure(campaign_rps) AS measure_value,
  CASE WHEN measure(campaign_rps) < 0.01 THEN 'Threshold deviated: Campaign RPS < $0.01' END AS message,
  current_timestamp() AS action_time
FROM fact_daily_metric_view
where event_date = (select max(event_date) from fact_daily_metric_view)
UNION ALL

SELECT 
  'PartnerRPS' AS metric_name,
  measure(partner_rps) AS measure_value,
  CASE WHEN measure(partner_rps) < 0.01 THEN 'Threshold deviated: Partner RPS < $0.01' END AS message,
  current_timestamp() AS action_time
FROM fact_daily_metric_view
where event_date = (select max(event_date) from fact_daily_metric_view)
UNION ALL

SELECT 
  'CampaignRPT' AS metric_name,
  measure(campaign_rpt) AS measure_value,
  CASE WHEN measure(campaign_rpt) < 0.10 THEN 'Threshold deviated: Campaign RPT < $0.10' END AS message,
  current_timestamp() AS action_time
FROM fact_daily_metric_view
where event_date = (select max(event_date) from fact_daily_metric_view)
UNION ALL

SELECT 
  'PartnerRPT' AS metric_name,
  measure(partner_rpt) AS measure_value,
  CASE WHEN measure(partner_rpt) < 0.10 THEN 'Threshold deviated: Partner RPT < $0.10' END AS message,
  current_timestamp() AS action_time
FROM fact_daily_metric_view
where event_date = (select max(event_date) from fact_daily_metric_view)
UNION ALL

SELECT 
  'CTR' AS metric_name,
  measure(ctr) AS measure_value,
  CASE WHEN measure(ctr) < 0.001 THEN 'Threshold deviated: CTR < 0.1%' END AS message,
  current_timestamp() AS action_time
FROM fact_daily_metric_view
where event_date = (select max(event_date) from fact_daily_metric_view)
UNION ALL

SELECT 
  'CR' AS metric_name,
  measure(cv) AS measure_value,
  CASE WHEN measure(cv) < 0.001 THEN 'Threshold deviated: Conversion Rate < 0.1%' END AS message,
  current_timestamp() AS action_time
FROM fact_daily_metric_view
where event_date = (select max(event_date) from fact_daily_metric_view)
UNION ALL

SELECT 
  'EmailCompleteness' AS metric_name,
  measure(email_completeness) AS measure_value,
  CASE WHEN measure(email_completeness) < 90 THEN 'Threshold deviated: Email Completeness < 90%' END AS message,
  current_timestamp() AS action_time
FROM fact_daily_metric_view
where event_date = (select max(event_date) from fact_daily_metric_view)
UNION ALL

SELECT 
  'TotalSuppressions' AS metric_name,
  measure(total_suppressions) AS measure_value,
  CASE WHEN measure(total_suppressions) > 5 THEN 'Threshold deviated: Total Suppressions > 5%' END AS message,
  current_timestamp() AS action_time
FROM fact_daily_metric_view
where event_date = (select max(event_date) from fact_daily_metric_view)

UNION ALL

SELECT 
  'TotalMatchRate' AS metric_name,
  total_match_rate AS measure_value,
  CASE WHEN total_match_rate < 40 THEN 'Threshold deviated: Total Match Rate < 40%' END AS message,
  current_timestamp() AS action_time
FROM derived_metrics
WHERE total_match_rate < 40

UNION ALL

SELECT 
  'EmailMatchRate' AS metric_name,
  email_match_rate AS measure_value,
  CASE WHEN email_match_rate < 30 THEN 'Threshold deviated: Email Match Rate < 30%' END AS message,
  current_timestamp() AS action_time
FROM derived_metrics
WHERE email_match_rate < 30

UNION ALL

SELECT 
  'PhoneMatchRate' AS metric_name,
  phone_match_rate AS measure_value,
  CASE WHEN phone_match_rate < 20 THEN 'Threshold deviated: Phone Match Rate < 20%' END AS message,
  current_timestamp() AS action_time
FROM derived_metrics
WHERE phone_match_rate < 20

UNION ALL

SELECT 
  'MAIDMatchRate' AS metric_name,
  maid_match_rate AS measure_value,
  CASE WHEN maid_match_rate < 20 THEN 'Threshold deviated: MAID Match Rate < 20%' END AS message,
  current_timestamp() AS action_time
FROM derived_metrics
WHERE maid_match_rate < 20

UNION ALL

SELECT 
  'IPMatchRate' AS metric_name,
  ip_match_rate AS measure_value,
  CASE WHEN ip_match_rate < 20 THEN 'Threshold deviated: IP Match Rate < 20%' END AS message,
  current_timestamp() AS action_time
FROM derived_metrics
WHERE ip_match_rate < 20;''')

# COMMAND ----------

df.write.mode("append").saveAsTable("business_integrity_check")
