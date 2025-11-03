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

# DBTITLE 1,fact_daily_metric_view
# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW fact_daily_metric_view
# MAGIC WITH METRICS
# MAGIC LANGUAGE YAML
# MAGIC AS $$
# MAGIC
# MAGIC version: 0.1
# MAGIC
# MAGIC source: fact_daily_metrics
# MAGIC
# MAGIC joins:
# MAGIC   - name: campaign
# MAGIC     source: minion.gold_campaign
# MAGIC     on: source.campaign_id = campaign.id
# MAGIC
# MAGIC   - name: partner
# MAGIC     source: minion.gold_partner
# MAGIC     on: source.partner_id = partner.partner_id
# MAGIC
# MAGIC   - name: advertiser
# MAGIC     source: minion.gold_advertiser
# MAGIC     on: source.advertiser_id = advertiser.advertiser_id
# MAGIC
# MAGIC   - name: creative
# MAGIC     source: minion.gold_creative
# MAGIC     on: source.campaign_id = creative.campaignid
# MAGIC
# MAGIC   - name: fluent
# MAGIC     source: customer.gold_customer_v1
# MAGIC     on: source.fluent_id = fluent.fluent_id
# MAGIC
# MAGIC   - name: previous_year
# MAGIC     source: fact_daily_metrics
# MAGIC     on:  source.event_date = add_months(previous_year.event_date,-12) 
# MAGIC         and source.campaign_id = previous_year.campaign_id
# MAGIC         and source.advertiser_id = previous_year.advertiser_id
# MAGIC         and source.partner_id = previous_year.partner_id
# MAGIC         and source.fluent_id = previous_year.fluent_id
# MAGIC         and source.source_id = previous_year.source_id
# MAGIC         and source.creative_id = previous_year.creative_id
# MAGIC
# MAGIC dimensions:
# MAGIC
# MAGIC   - name: vertical
# MAGIC     expr: campaign.vertical
# MAGIC
# MAGIC   - name: subvertical
# MAGIC     expr: campaign.subvertical
# MAGIC
# MAGIC   - name: campaign_name
# MAGIC     expr: campaign.name
# MAGIC
# MAGIC   - name: partner_name
# MAGIC     expr: partner.name
# MAGIC
# MAGIC   - name: advertiser_name
# MAGIC     expr: advertiser.name
# MAGIC
# MAGIC   - name: event_date
# MAGIC     expr: event_date
# MAGIC
# MAGIC   - name: year
# MAGIC     expr: year(event_date)
# MAGIC
# MAGIC   - name: month
# MAGIC     expr: month(event_date)
# MAGIC
# MAGIC   - name: date
# MAGIC     expr: day(event_date)
# MAGIC
# MAGIC   - name: creative_name
# MAGIC     expr: creative.name
# MAGIC
# MAGIC   - name: tune_offer_id
# MAGIC     expr: tune_offer_id
# MAGIC
# MAGIC   - name: source_id
# MAGIC     expr: source_id
# MAGIC
# MAGIC   - name: gender
# MAGIC     expr: fluent.gender
# MAGIC
# MAGIC   - name: age_range
# MAGIC     expr: fluent.age_range
# MAGIC
# MAGIC   - name: position
# MAGIC     expr: position
# MAGIC
# MAGIC   - name: product_scope
# MAGIC     expr: product_scope
# MAGIC
# MAGIC measures:
# MAGIC   - name: total_clicks
# MAGIC     expr: SUM(clicks)
# MAGIC
# MAGIC   - name: total_views
# MAGIC     expr: SUM(views)
# MAGIC
# MAGIC   - name: sessions
# MAGIC     expr: SUM(sessions)
# MAGIC
# MAGIC   - name: distinct_sessions
# MAGIC     expr: SUM(distinct_sessions)
# MAGIC
# MAGIC   - name: possible_session
# MAGIC     expr: SUM(possible_session)
# MAGIC
# MAGIC   - name: partner_transactions
# MAGIC     expr: SUM(partner_transactions)
# MAGIC
# MAGIC   - name: primary_conversions
# MAGIC     expr: SUM(primary_conversions)
# MAGIC
# MAGIC   - name: funnel_conversions
# MAGIC     expr: SUM(funnel_conversions)
# MAGIC
# MAGIC   - name: campaign_revenue
# MAGIC     expr: sum(campaign_revenue)
# MAGIC
# MAGIC   - name: partner_revenue
# MAGIC     expr: sum(partner_revenue)
# MAGIC
# MAGIC   - name: total_suppressions
# MAGIC     expr: (sum(source.possible_session)-sum(source.views) )/ NULLIF(sum(source.possible_session),0)
# MAGIC
# MAGIC   - name: previous_total_clicks
# MAGIC     expr: sum(previous_year.clicks)
# MAGIC
# MAGIC   - name: previous_total_views
# MAGIC     expr: sum(previous_year.views)
# MAGIC
# MAGIC
# MAGIC   - name: previous_sessions
# MAGIC     expr: sum(previous_year.sessions)
# MAGIC
# MAGIC   - name: previous_primary_conversions
# MAGIC     expr: sum(previous_year.primary_conversions)
# MAGIC
# MAGIC
# MAGIC # Completeness (%)
# MAGIC
# MAGIC   - name: email_completeness
# MAGIC     expr: CAST(SUM(source.email_coverage) * 100.0 / NULLIF(COUNT(DISTINCT source.dwh_daily_id), 0) AS DECIMAL(19,2))
# MAGIC   - name: emailmd5_completeness
# MAGIC     expr: CAST(SUM(source.emailmd5_coverage) * 100.0 / NULLIF(COUNT(DISTINCT source.dwh_daily_id), 0) AS DECIMAL(19,2))
# MAGIC   - name: emailsha256_completeness
# MAGIC     expr: CAST(SUM(source.emailsha256_coverage) * 100.0 / NULLIF(COUNT(DISTINCT source.dwh_daily_id), 0) AS DECIMAL(19,2))
# MAGIC   - name: telephone_completeness
# MAGIC     expr: CAST(SUM(source.telephone_coverage) * 100.0 / NULLIF(COUNT(DISTINCT source.dwh_daily_id), 0) AS DECIMAL(19,2))
# MAGIC   - name: phone_completeness
# MAGIC     expr: CAST(SUM(source.phone_coverage) * 100.0 / NULLIF(COUNT(DISTINCT source.dwh_daily_id), 0) AS DECIMAL(19,2))
# MAGIC   - name: maid_completeness
# MAGIC     expr: CAST(SUM(source.maid_coverage) * 100.0 / NULLIF(COUNT(DISTINCT source.dwh_daily_id), 0) AS DECIMAL(19,2))
# MAGIC   - name: first_name_completeness
# MAGIC     expr: CAST(SUM(source.first_name_coverage) * 100.0 / NULLIF(COUNT(DISTINCT source.dwh_daily_id), 0) AS DECIMAL(19,2))
# MAGIC   - name: order_id_completeness
# MAGIC     expr: CAST(SUM(source.order_id_coverage) * 100.0 / NULLIF(COUNT(DISTINCT source.dwh_daily_id), 0) AS DECIMAL(19,2))
# MAGIC   - name: transaction_value_completeness
# MAGIC     expr: CAST(SUM(source.transaction_value_coverage) * 100.0 / NULLIF(COUNT(DISTINCT source.dwh_daily_id), 0) AS DECIMAL(19,2))
# MAGIC   - name: cc_bin_completeness
# MAGIC     expr: CAST(SUM(source.ccBin_coverage) * 100.0 / NULLIF(COUNT(DISTINCT source.dwh_daily_id), 0) AS DECIMAL(19,2))
# MAGIC   - name: gender_completeness
# MAGIC     expr: CAST(SUM(source.gender_coverage) * 100.0 / NULLIF(COUNT(DISTINCT source.dwh_daily_id), 0) AS DECIMAL(19,2))
# MAGIC   - name: zip_completeness
# MAGIC     expr: CAST(SUM(source.zip_coverage) * 100.0 / NULLIF(COUNT(DISTINCT source.dwh_daily_id), 0) AS DECIMAL(19,2))
# MAGIC   - name: zip_code_completeness
# MAGIC     expr: CAST(SUM(source.zippost_coverage) * 100.0 / NULLIF(COUNT(DISTINCT source.dwh_daily_id), 0) AS DECIMAL(19,2))
# MAGIC
# MAGIC
# MAGIC # Sparsity (%)
# MAGIC
# MAGIC   - name: email_sparsity
# MAGIC     expr: 100.0 - CAST(SUM(source.email_coverage) * 100.0 / NULLIF(COUNT(DISTINCT source.dwh_daily_id), 0) AS DECIMAL(19,2))
# MAGIC   - name: emailmd5_sparsity
# MAGIC     expr: 100.0 - CAST(SUM(source.emailmd5_coverage) * 100.0 / NULLIF(COUNT(DISTINCT source.dwh_daily_id), 0) AS DECIMAL(19,2))
# MAGIC   - name: emailsha256_sparsity
# MAGIC     expr: 100.0 - CAST(SUM(source.emailsha256_coverage) * 100.0 / NULLIF(COUNT(DISTINCT source.dwh_daily_id), 0) AS DECIMAL(19,2))
# MAGIC   - name: telephone_sparsity
# MAGIC     expr: 100.0 - CAST(SUM(source.telephone_coverage) * 100.0 / NULLIF(COUNT(DISTINCT source.dwh_daily_id), 0) AS DECIMAL(19,2))
# MAGIC   - name: phone_sparsity
# MAGIC     expr: 100.0 - CAST(SUM(source.phone_coverage) * 100.0 / NULLIF(COUNT(DISTINCT source.dwh_daily_id), 0) AS DECIMAL(19,2))
# MAGIC   - name: maid_sparsity
# MAGIC     expr: 100.0 - CAST(SUM(source.maid_coverage) * 100.0 / NULLIF(COUNT(DISTINCT source.dwh_daily_id), 0) AS DECIMAL(19,2))
# MAGIC   - name: first_name_sparsity
# MAGIC     expr: 100.0 - CAST(SUM(source.first_name_coverage) * 100.0 / NULLIF(COUNT(DISTINCT source.dwh_daily_id), 0) AS DECIMAL(19,2))
# MAGIC   - name: order_id_sparsity
# MAGIC     expr: 100.0 - CAST(SUM(source.order_id_coverage) * 100.0 / NULLIF(COUNT(DISTINCT source.dwh_daily_id), 0) AS DECIMAL(19,2))
# MAGIC   - name: transaction_value_sparsity
# MAGIC     expr: 100.0 - CAST(SUM(source.transaction_value_coverage) * 100.0 / NULLIF(COUNT(DISTINCT source.dwh_daily_id), 0) AS DECIMAL(19,2))
# MAGIC   - name: cc_bin_sparsity
# MAGIC     expr: 100.0 - CAST(SUM(source.ccBin_coverage) * 100.0 / NULLIF(COUNT(DISTINCT source.dwh_daily_id), 0) AS DECIMAL(19,2))
# MAGIC   - name: gender_sparsity
# MAGIC     expr: 100.0 - CAST(SUM(source.gender_coverage) * 100.0 / NULLIF(COUNT(DISTINCT source.dwh_daily_id), 0) AS DECIMAL(19,2))
# MAGIC   - name: zip_sparsity
# MAGIC     expr: 100.0 - CAST(SUM(source.zip_coverage) * 100.0 / NULLIF(COUNT(DISTINCT source.dwh_daily_id), 0) AS DECIMAL(19,2))
# MAGIC   - name: zip_code_sparsity
# MAGIC     expr: 100.0 - CAST(SUM(source.zippost_coverage) * 100.0 / NULLIF(COUNT(DISTINCT source.dwh_daily_id), 0) AS DECIMAL(19,2))
# MAGIC
# MAGIC   # Derived ratio metrics
# MAGIC
# MAGIC   - name: campaign_rps
# MAGIC     expr: MEASURE(campaign_revenue) / NULLIF(MEASURE(possible_session), 0)
# MAGIC   - name: campaign_rpt
# MAGIC     expr: MEASURE(campaign_revenue) / NULLIF(MEASURE(partner_transactions), 0)
# MAGIC   - name: partner_rps
# MAGIC     expr: MEASURE(partner_revenue) / NULLIF(MEASURE(possible_session), 0)
# MAGIC   - name: partner_rpt
# MAGIC     expr: MEASURE(partner_revenue) / NULLIF(MEASURE(partner_transactions), 0)
# MAGIC   - name: ctr
# MAGIC     expr: ROUND((MEASURE(total_clicks) * 100.0) / NULLIF(MEASURE(total_views), 0), 2)
# MAGIC
# MAGIC   - name: previous_sessions_ctr
# MAGIC     expr: MEASURE(previous_total_clicks) /  NULLIF(MEASURE(previous_total_views),0)
# MAGIC
# MAGIC   - name: cv
# MAGIC     expr: ROUND((MEASURE(primary_conversions) * 100.0) / NULLIF(MEASURE(total_clicks), 0), 2)
# MAGIC
# MAGIC   - name: previous_session_cv
# MAGIC     expr: MEASURE(previous_primary_conversions) / NULLIF(MEASURE(previous_total_clicks), 0)
# MAGIC
# MAGIC   # seasonality
# MAGIC
# MAGIC   - name: ctr_seasonality
# MAGIC     expr: ((MEASURE(ctr) - MEASURE(previous_sessions_ctr)) / NULLIF(MEASURE(previous_sessions_ctr), 0)) * 100.0
# MAGIC
# MAGIC   - name: cv_seasonality
# MAGIC     expr: ((MEASURE(cv) - MEASURE(previous_session_cv)) / NULLIF(MEASURE(previous_session_cv), 0)) * 100.0
# MAGIC
# MAGIC $$;

# COMMAND ----------

# DBTITLE 1,fact_profile_request_match_view
# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW fact_profile_request_match_view
# MAGIC WITH METRICS
# MAGIC LANGUAGE YAML
# MAGIC AS $$
# MAGIC version: 1.0
# MAGIC
# MAGIC source: fact_profile_request_match
# MAGIC
# MAGIC measures:
# MAGIC   - name: total_match_rate
# MAGIC     expr: (count(case when fluent_id is not null then 1 end) * 100.0) / count(request_trace_id)
# MAGIC
# MAGIC   - name: email_match_rate
# MAGIC     expr: CAST((COUNT(CASE WHEN lower(match_type) like '%email%' THEN 1 END) * 100.0 / NULLIF(COUNT(request_trace_id), 0)) AS DECIMAL(19,2))
# MAGIC
# MAGIC   - name: phone_match_rate
# MAGIC     expr: CAST((COUNT(CASE WHEN lower(match_type) = 'phone' THEN 1 END) * 100.0 / NULLIF(COUNT(request_trace_id), 0)) AS DECIMAL(19,2))
# MAGIC
# MAGIC   - name: maid_match_rate
# MAGIC     expr: CAST((COUNT(CASE WHEN lower(match_type) = 'maid' THEN 1 END) * 100.0 / NULLIF(COUNT(request_trace_id), 0)) AS DECIMAL(19,2))
# MAGIC
# MAGIC   - name: ip_match_rate
# MAGIC     expr: CAST((COUNT(CASE WHEN lower(match_type) = 'ip' THEN 1 END) * 100.0 / NULLIF(COUNT(request_trace_id), 0)) AS DECIMAL(19,2))
# MAGIC
# MAGIC dimensions:
# MAGIC   - name: match_type
# MAGIC     expr: match_type
# MAGIC $$

# COMMAND ----------

# DBTITLE 1,fact_campaign_view
# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW fact_campaign_view
# MAGIC WITH METRICS
# MAGIC LANGUAGE YAML
# MAGIC AS $$
# MAGIC version: 0.1
# MAGIC
# MAGIC source: minion.gold_adgroup_creative
# MAGIC
# MAGIC dimensions:
# MAGIC   - name: adgroup
# MAGIC     expr: adgroup_id  
# MAGIC
# MAGIC measures:
# MAGIC   - name: creative_enabled
# MAGIC     expr: count(distinct case when active = true then 1 end)
# MAGIC
# MAGIC $$;

# COMMAND ----------

# DBTITLE 1,repeat_users_view
# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW repeat_users_view
# MAGIC WITH METRICS
# MAGIC LANGUAGE YAML
# MAGIC AS $$
# MAGIC version: 0.1
# MAGIC
# MAGIC source: customer.gold_customer_repeat_history
# MAGIC
# MAGIC joins:
# MAGIC   - name: campaign
# MAGIC     source: minion.gold_campaign
# MAGIC     on: source.campaign_id = campaign.id
# MAGIC
# MAGIC dimensions:
# MAGIC   - name: vertical
# MAGIC     expr: campaign.vertical
# MAGIC   - name: subvertical
# MAGIC     expr: campaign.subvertical
# MAGIC   - name: campaign
# MAGIC     expr: campaign.name
# MAGIC
# MAGIC measures:
# MAGIC   - name: repeat_users
# MAGIC     expr: count(fluent_id)/count(distinct fluent_id)
# MAGIC $$
