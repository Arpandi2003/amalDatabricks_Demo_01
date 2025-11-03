# Databricks notebook source
# MAGIC %md
# MAGIC # 
# MAGIC
# MAGIC ## Tracker Details
# MAGIC - Description: NB_LandingKPIS
# MAGIC - Created Date: 08/12/2025
# MAGIC - Created By: Pandi Anbu
# MAGIC - Modified Date: 08/12/2025
# MAGIC - Modified By: Pandi Anbu
# MAGIC - Changes made:  
# MAGIC

# COMMAND ----------

# MAGIC %run "../General/NB_Configuration"

# COMMAND ----------

spark.sql(f"use catalog {catalog}")

# COMMAND ----------

# MAGIC %sql
# MAGIC /* Requires yearly maintenance and updates; budget tables must be flagged as Budget_Flag = TRUE, otherwise FALSE */
# MAGIC CREATE OR REPLACE VIEW finance.tab_landing_KPIs AS
# MAGIC WITH yearly_ratio_import as (
# MAGIC   select
# MAGIC     *,
# MAGIC     FALSE AS Budget_Flag
# MAGIC   from
# MAGIC     bronze.axiom_ratio2025
# MAGIC ),
# MAGIC /* For future years or to bring in 2024 data for comparison, create import CTEs for those tables and add a union.
# MAGIC Unless there is a year column in the ratios tables, we will need to somehow grab year values from table names
# MAGIC and add those values to month_pivot.
# MAGIC Do this before the "joined" CTE*/
# MAGIC yearly_ratio_budget_import as (
# MAGIC   select
# MAGIC     *,
# MAGIC     TRUE AS Budget_Flag
# MAGIC   from
# MAGIC     bronze.axiom_ratiobud2025
# MAGIC ),
# MAGIC /* Future budget years should have own import CTEs and be unioned in a separate step unless a wildcard union is implemented*/
# MAGIC ratio_lookup_import as (
# MAGIC   select
# MAGIC     *
# MAGIC   from
# MAGIC     bronze.axiom_ratio
# MAGIC ),
# MAGIC /* Adds Description from ratio lookup table. Add additional context columns here if needed*/
# MAGIC ratios_joined AS (
# MAGIC   SELECT
# MAGIC     m.RatioID,
# MAGIC     m.Budget_Flag,
# MAGIC     m.M1,
# MAGIC     m.M2,
# MAGIC     m.M3,
# MAGIC     m.M4,
# MAGIC     m.M5,
# MAGIC     m.M6,
# MAGIC     m.M7,
# MAGIC     m.M8,
# MAGIC     m.M9,
# MAGIC     m.M10,
# MAGIC     m.M11,
# MAGIC     m.M12,
# MAGIC     l.Description
# MAGIC   FROM
# MAGIC     yearly_ratio_import as m
# MAGIC       LEFT JOIN ratio_lookup_import as l
# MAGIC         ON m.RatioID = l.RatioID
# MAGIC ),
# MAGIC joined_target AS (
# MAGIC   SELECT
# MAGIC     r.RatioID,
# MAGIC     r.Budget_Flag,
# MAGIC     r.M1,
# MAGIC     r.M2,
# MAGIC     r.M3,
# MAGIC     r.M4,
# MAGIC     r.M5,
# MAGIC     r.M6,
# MAGIC     r.M7,
# MAGIC     r.M8,
# MAGIC     r.M9,
# MAGIC     r.M10,
# MAGIC     r.M11,
# MAGIC     r.M12,
# MAGIC     l.Description
# MAGIC   FROM
# MAGIC     yearly_ratio_budget_import as r
# MAGIC       LEFT JOIN ratio_lookup_import as l
# MAGIC         ON r.RatioID = l.RatioID
# MAGIC ),
# MAGIC /* Currently hardcodes 2025 for DATE construction; we need a year field created in underlying tables or parsed from table names*/
# MAGIC month_pivot AS (
# MAGIC   SELECT
# MAGIC     RatioID,
# MAGIC     Description,
# MAGIC     Budget_Flag,
# MAGIC     DATE(CONCAT('2025-', LPAD(SUBSTRING(Month, 2), 2, '0'), '-01')) AS Date,
# MAGIC     Value
# MAGIC   FROM
# MAGIC     (
# MAGIC       SELECT
# MAGIC         RatioID,
# MAGIC         Description,
# MAGIC         Budget_Flag,
# MAGIC         STACK(
# MAGIC           12,
# MAGIC           'M1',
# MAGIC           M1,
# MAGIC           'M2',
# MAGIC           M2,
# MAGIC           'M3',
# MAGIC           M3,
# MAGIC           'M4',
# MAGIC           M4,
# MAGIC           'M5',
# MAGIC           M5,
# MAGIC           'M6',
# MAGIC           M6,
# MAGIC           'M7',
# MAGIC           M7,
# MAGIC           'M8',
# MAGIC           M8,
# MAGIC           'M9',
# MAGIC           M9,
# MAGIC           'M10',
# MAGIC           M10,
# MAGIC           'M11',
# MAGIC           M11,
# MAGIC           'M12',
# MAGIC           M12
# MAGIC         ) AS (Month, Value)
# MAGIC       FROM
# MAGIC         (
# MAGIC           SELECT
# MAGIC             *
# MAGIC           FROM
# MAGIC             ratios_joined
# MAGIC         ) t
# MAGIC     )
# MAGIC ),
# MAGIC /* Currently hardcodes 2025 for DATE construction; we need a year field created in underlying tables or parsed from table names*/
# MAGIC month_pivot_target AS (
# MAGIC   SELECT
# MAGIC     RatioID,
# MAGIC     Description || ' target' AS Description,
# MAGIC     Budget_Flag,
# MAGIC     DATE(CONCAT('2025-', LPAD(SUBSTRING(Month, 2), 2, '0'), '-01')) AS Date,
# MAGIC     Value
# MAGIC   FROM
# MAGIC     (
# MAGIC       SELECT
# MAGIC         RatioID,
# MAGIC         Description,
# MAGIC         Budget_Flag,
# MAGIC         STACK(
# MAGIC           12,
# MAGIC           'M1',
# MAGIC           M1,
# MAGIC           'M2',
# MAGIC           M2,
# MAGIC           'M3',
# MAGIC           M3,
# MAGIC           'M4',
# MAGIC           M4,
# MAGIC           'M5',
# MAGIC           M5,
# MAGIC           'M6',
# MAGIC           M6,
# MAGIC           'M7',
# MAGIC           M7,
# MAGIC           'M8',
# MAGIC           M8,
# MAGIC           'M9',
# MAGIC           M9,
# MAGIC           'M10',
# MAGIC           M10,
# MAGIC           'M11',
# MAGIC           M11,
# MAGIC           'M12',
# MAGIC           M12
# MAGIC         ) AS (Month, Value)
# MAGIC       FROM
# MAGIC         (
# MAGIC           SELECT
# MAGIC             *
# MAGIC           FROM
# MAGIC             joined_target
# MAGIC         ) t
# MAGIC     )
# MAGIC ),
# MAGIC combined AS (
# MAGIC   SELECT
# MAGIC     *
# MAGIC   FROM
# MAGIC     month_pivot
# MAGIC   UNION ALL
# MAGIC   SELECT
# MAGIC     *
# MAGIC   FROM
# MAGIC     month_pivot_target
# MAGIC ),
# MAGIC /* Do Prior Month calculation in Databricks instead of Tableau; we also find the latest date with a value for each ratio, partitioned by budget status */
# MAGIC month_with_prior AS (
# MAGIC   SELECT
# MAGIC     RatioID,
# MAGIC     Description,
# MAGIC     Budget_Flag,
# MAGIC     Date,
# MAGIC     Value,
# MAGIC     ADD_MONTHS(Date, -1) AS Prior_Month,
# MAGIC     MAX(
# MAGIC       CASE
# MAGIC         WHEN Value > .00001 THEN Date
# MAGIC         ELSE NULL
# MAGIC       END
# MAGIC     ) OVER (PARTITION BY RatioID, Budget_Flag ORDER BY Date) AS Max_Date_With_Value
# MAGIC   FROM
# MAGIC     combined
# MAGIC )
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   month_with_prior
# MAGIC ORDER BY
# MAGIC   Date ASC;
