# Databricks notebook source
# MAGIC %md
# MAGIC # 
# MAGIC ## Tracker Details
# MAGIC - Description: NB_CombinedDepositsGenerator
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
# MAGIC /* This brings in more values than currently used in order to permit the creation of a trial balance dashboard */
# MAGIC /* Schedule after Pivoted GL Daily to allow up to date loan reporting*/
# MAGIC
# MAGIC CREATE OR REPLACE VIEW finance.tab_processed_loans
# MAGIC AS
# MAGIC with yrmodayimport AS (
# MAGIC     select 
# MAGIC         YRMODAY AS lookup_YRMODAY, /* YRMODAY and YRMO are valuable for high-efficiency */
# MAGIC         Description, 
# MAGIC         DayValue, 
# MAGIC         MonthValue, 
# MAGIC         QuarterValue, 
# MAGIC         YearValue, 
# MAGIC         YRMO AS lookup_YRMO, 
# MAGIC         to_date(DateValue, 'M/d/yyyy h:mm:ss a') as EntryDate, 
# MAGIC         to_date(PriorDataDate, 'M/d/yyyy h:mm:ss a') as PriorDate,
# MAGIC         IsBD
# MAGIC
# MAGIC     from `bronze`.`yrmoday`
# MAGIC ),
# MAGIC
# MAGIC
# MAGIC /* We need the last business day of each month in order to:
# MAGIC 1) Push last reporting date of each month, as this is what we report in the dashboard
# MAGIC 2) Identify second to last reporting date of each month later in the query based on that last reporting date
# MAGIC 3) Have values to push forward the purchased loans portfolio to */
# MAGIC max_entry AS (
# MAGIC   SELECT lookup_YRMO as max_lookup_YRMO, MAX(EntryDate) AS max_EntryDate, MAX(lookup_YRMODAY) as max_YRMODAY
# MAGIC   FROM yrmodayimport
# MAGIC   WHERE IsBD = 1
# MAGIC   GROUP BY max_lookup_YRMO 
# MAGIC ),
# MAGIC
# MAGIC daily_loans_import AS (
# MAGIC   SELECT *
# MAGIC   FROM bronze.axiom_daily_loans
# MAGIC ),
# MAGIC
# MAGIC monthly_loans_import AS (
# MAGIC   SELECT *
# MAGIC   FROM bronze.axiom_loans
# MAGIC ),
# MAGIC
# MAGIC residential_loans_import AS (
# MAGIC   SELECT * FROM bronze.axiom_pm_consolidated
# MAGIC ),
# MAGIC /* We need a specific value from the pivoted GL data to account for charged-off loans, especially in the delinquent loans balances*/
# MAGIC gl_override_import AS (SELECT *, CAST(DATE_FORMAT(`Date`, 'yyyyMMdd') AS INT) AS YRMODAY
# MAGIC FROM finance.tab_gl_data_pivoted
# MAGIC WHERE ACCT = 126312 AND CAST(DATE_FORMAT(`Date`, 'yyyyMMdd') AS INT) <= (SELECT MAX(YRMODAY) FROM daily_loans_import)
# MAGIC ORDER BY DATE),
# MAGIC
# MAGIC /* Nonaccrual flag set to 1 for inclusion in Delinquent Loans analysis, with DaysPastDue over 90 for more complex analysis edge cases*/
# MAGIC gl_override_raw AS (
# MAGIC   SELECT
# MAGIC     YRMODAY,
# MAGIC     YRQTR AS YRMO,
# MAGIC     NULL AS InstrumentID,
# MAGIC     NULL AS ITYPE,
# MAGIC     ACCT,
# MAGIC     DEPT,
# MAGIC     NULL AS CustomerName,
# MAGIC     NULL AS CommitmentDescription,
# MAGIC     NULL AS LastReviewDate,
# MAGIC     NULL AS NextReviewDate,
# MAGIC     NULL AS MasterTrancheFlag,
# MAGIC     NULL AS CurrentOutstanding,
# MAGIC     Value AS CurrentBalance,
# MAGIC     NULL AS Exposure,
# MAGIC     NULL as MaturityDate,
# MAGIC     '>=90' AS DaysUntilMaturity,
# MAGIC     NULL AS MaturityTerm,
# MAGIC     NULL AS OriginalLoanAmount,
# MAGIC     NULL AS CurrentNoteDate,
# MAGIC     NULL AS ScheduledPayment,
# MAGIC     NULL AS AvailableBalance,
# MAGIC     NULL AS HoldAmount,
# MAGIC     NULL AS InterestIndex,
# MAGIC     NULL AS AX_IntRate,
# MAGIC     NULL AS InterestSpread,
# MAGIC     'Override' as Status,
# MAGIC     NULL as Address1,
# MAGIC     NULL as Address2,
# MAGIC     NULL as Address3,
# MAGIC     NULL as City,
# MAGIC     NULL as State,
# MAGIC     NULL as  ZipCode,
# MAGIC     NULL as Branch,
# MAGIC     NULL AS BranchID,
# MAGIC     NULL as Officer1Name,
# MAGIC     NULL as Officer2Name,
# MAGIC     NULL as CollateralType,
# MAGIC     NULL as CollateralTypeDesc,
# MAGIC     NULL AS PaymentType,
# MAGIC     NULL as RiskRating,
# MAGIC     1 AS NonAccrual,
# MAGIC     NULL as NonPerformingLoan,
# MAGIC     91 AS DaysPastDue,
# MAGIC     NULL as System_Type,
# MAGIC     'Daily' AS TableSource
# MAGIC     FROM gl_override_import
# MAGIC ),
# MAGIC /* Daily Loans and Monthly Loans table exclude accidental 0 account errors. Daily Loans only pulls from current month.
# MAGIC MaturityDate based information for trial balance view
# MAGIC ZipCode must be cast as string because some tables store it as string, others as int 
# MAGIC We exclude account 125210 as it is a misclassified HTM security */
# MAGIC daily_loans_raw as (
# MAGIC   SELECT
# MAGIC     YRMODAY,
# MAGIC     YRMO,
# MAGIC     InstrumentID,
# MAGIC     ITYPE,
# MAGIC     ACCT,
# MAGIC     DEPT,
# MAGIC     CustName AS CustomerName,
# MAGIC     Commitment_Description AS CommitmentDescription,
# MAGIC     Commitment_Last_Review_Date AS LastReviewDate,
# MAGIC     Commitment_Next_Review_Date AS NextReviewDate,
# MAGIC     MasterTrancheFlag,
# MAGIC     Book_Balance AS CurrentOutstanding,
# MAGIC     AX_CurBal AS CurrentBalance,
# MAGIC     Exposure,
# MAGIC     to_date(AX_MatDate, 'M/d/yyyy hh:mm:ss a') as MaturityDate,
# MAGIC     (CASE 
# MAGIC       WHEN DATEDIFF(MaturityDate, CURRENT_DATE) < 30 THEN '<30'
# MAGIC       WHEN DATEDIFF(MaturityDate, CURRENT_DATE) < 90 THEN '<90'
# MAGIC       ELSE '>=90'
# MAGIC     END) AS DaysUntilMaturity,
# MAGIC     AX_MatTerm AS MaturityTerm,
# MAGIC     AX_OrigBal AS OriginalLoanAmount,
# MAGIC     AX_OrigDate AS CurrentNoteDate,
# MAGIC     AX_Payment AS ScheduledPayment,
# MAGIC     Available_Balance AS AvailableBalance,
# MAGIC     Hold_Amount AS HoldAmount,
# MAGIC     AX_IntIndex AS InterestIndex,
# MAGIC     AX_IntRate,
# MAGIC     AX_IntSpread AS InterestSpread,
# MAGIC     LNSTATUS as Status,
# MAGIC     Address1,
# MAGIC     Address2,
# MAGIC     Address3,
# MAGIC     City,
# MAGIC     State,
# MAGIC     CAST(ZipCode AS String) AS ZipCode,
# MAGIC     Branch,
# MAGIC     BranchNumber AS BranchID,
# MAGIC     Officer1Name,
# MAGIC     Officer2Name,
# MAGIC     CollateralType,
# MAGIC     CollateralTypeDesc,
# MAGIC     AX_PmtType AS PaymentType,
# MAGIC     RiskRating,
# MAGIC     AX_NonAccrual AS NonAccrual,
# MAGIC     NonPerformingLoan,
# MAGIC     Days_Past_Due AS DaysPastDue,
# MAGIC     System_Type,
# MAGIC     'Daily' AS TableSource
# MAGIC   FROM
# MAGIC     daily_loans_import
# MAGIC   WHERE YRMO = (SELECT MAX(YRMO) FROM daily_loans_import) AND ACCT NOT IN(125210, 0)
# MAGIC ),
# MAGIC
# MAGIC daily_loans_max AS (
# MAGIC   SELECT MAX(YRMO) AS max_daily_month
# MAGIC   FROM daily_loans_raw
# MAGIC ),
# MAGIC /* Only reports from this table as of end of prior month*/
# MAGIC loans_raw as (
# MAGIC   SELECT
# MAGIC     me.max_YRMODAY AS YRMODAY, /* Moves all monthly loan values to maximum valid reporting date for each month, taken from YRMODAY calendar*/
# MAGIC     ml.YRMO,
# MAGIC     ml.InstrumentID,
# MAGIC     ml.ITYPE,
# MAGIC     ml.ACCT,
# MAGIC     ml.DEPT,
# MAGIC     ml.CustName AS CustomerName,
# MAGIC     ml.Commitment_Description AS CommitmentDescription,
# MAGIC     ml.Commitment_Last_Review_Date AS LastReviewDate,
# MAGIC     ml.Commitment_Next_Review_Date AS NextReviewDate,
# MAGIC     ml.MasterTrancheFlag,
# MAGIC     ml.Book_Balance AS CurrentOutstanding,
# MAGIC     ml.AX_CurBal AS CurrentBalance,
# MAGIC     ml.Exposure,
# MAGIC     to_date(ml.AX_MatDate, 'M/d/yyyy hh:mm:ss a') as MaturityDate,
# MAGIC     (CASE 
# MAGIC       WHEN DATEDIFF(MaturityDate, CURRENT_DATE) < 30 THEN '<30'
# MAGIC       WHEN DATEDIFF(MaturityDate, CURRENT_DATE) < 90 THEN '<90'
# MAGIC       ELSE '>=90'
# MAGIC     END) AS DaysUntilMaturity,
# MAGIC     ml.AX_MatTerm As MaturityTerm,
# MAGIC     ml.AX_OrigBal AS OriginalLoanAmount,
# MAGIC     ml.AX_OrigDate AS CurrentNoteDate,
# MAGIC     ml.AX_Payment as ScheduledPayment,
# MAGIC     ml.Available_Balance AS AvailableBalance,
# MAGIC     ml.Hold_Amount AS HoldAmount,
# MAGIC     ml.AX_IntIndex AS InterestIndex,
# MAGIC     ml.AX_IntRate,
# MAGIC     ml.AX_IntSpread AS InterestSpread,
# MAGIC     ml.LNSTATUS as Status,
# MAGIC     ml.Address1,
# MAGIC     ml.Address2,
# MAGIC     ml.Address3,
# MAGIC     ml.City,
# MAGIC     ml.State,
# MAGIC     ml.ZipCode,
# MAGIC     ml.Branch,
# MAGIC     ml.BranchNumber AS BranchID,
# MAGIC     ml.Officer1Name,
# MAGIC     ml.Officer2Name,
# MAGIC     ml.CollateralType,
# MAGIC     ml.CollateralTypeDesc,
# MAGIC     ml.AX_PmtType AS PaymentType,
# MAGIC     ml.RiskRating,
# MAGIC     AX_NonAccrual AS NonAccrual,
# MAGIC     ml.NonPerformingLoan,
# MAGIC     ml.Days_Past_Due AS DaysPastDue,
# MAGIC     ml.System_Type,
# MAGIC     'monthly' AS TableSource
# MAGIC   FROM
# MAGIC     monthly_loans_import ml
# MAGIC   JOIN max_entry me ON ml.YRMO = me.max_lookup_YRMO
# MAGIC   WHERE ml.YRMO >= 202412 AND ml.YRMO < (SELECT max_daily_month FROM daily_loans_max) AND ACCT NOT IN(125210, 0)
# MAGIC ),
# MAGIC
# MAGIC /* Dept is provisionally cleared but assignment not fully validated
# MAGIC This table combines information from multiple providers, which report different levels of detail
# MAGIC 'Anonymized Purchase' for loan providers that choose not to report CustomerName and to ensure entries are still reported in Instruments view
# MAGIC Finance policy is to flag purchased loans late by 90 days as Nonaccrual with Risk Rating 9*/
# MAGIC
# MAGIC residential_raw as (
# MAGIC   SELECT
# MAGIC     me.max_YRMODAY AS YRMODAY,
# MAGIC     rl.YRMO,
# MAGIC     rl.InstrumentID,
# MAGIC     NULL AS ITYPE,
# MAGIC     rl.ACCT AS ACCT,
# MAGIC     '1001321005' AS DEPT,
# MAGIC     IFNULL(rl.CustomerName, 'Anonymized Purchase') AS CustomerName,
# MAGIC     'Purchased' AS CommitmentDescription,
# MAGIC     NULL AS LastReviewDate,
# MAGIC     NULL AS NextReviewDate,
# MAGIC     NULL AS MasterTrancheFlag,
# MAGIC     rl.Book_Balance AS CurrentOutstanding,
# MAGIC     rl.EndBookBal AS CurrentBalance,
# MAGIC     0 AS Exposure,
# MAGIC     to_date(rl.Maturity_Date, 'M/d/yyyy hh:mm:ss a') AS MaturityDate,
# MAGIC     (CASE 
# MAGIC       WHEN DATEDIFF(to_date(rl.Maturity_Date, 'M/d/yyyy hh:mm:ss a'), CURRENT_DATE) < 30 THEN '<30'
# MAGIC       WHEN DATEDIFF(to_date(rl.Maturity_Date, 'M/d/yyyy hh:mm:ss a'), CURRENT_DATE) < 90 THEN '<90'
# MAGIC       ELSE '>=90'
# MAGIC     END) AS DaysUntilMaturity,
# MAGIC     rl.OriginalTerm As MaturityTerm,
# MAGIC     rl.original_loan_amount as OriginalLoanAmount,
# MAGIC     rl.Origination_Date AS CurrentNoteDate,
# MAGIC     rl.ScheduledPayment,
# MAGIC     0 AS AvailableBalance,
# MAGIC     0 AS HoldAmount,
# MAGIC     'Purchased' AS InterestIndex,
# MAGIC     rl.CouponRate AS AX_IntRate,
# MAGIC     0 AS InterestSpread,
# MAGIC     'Residential' as Status, /* Not a formal assignment in Axiom*/
# MAGIC     rl.Address AS Address1,
# MAGIC     NULL AS Address2,
# MAGIC     NULL AS Address3,
# MAGIC     rl.City,
# MAGIC     rl.State,
# MAGIC     rl.ZipCode,
# MAGIC     'Purchased Residential' AS Branch, /* For filtering only*/
# MAGIC     NULL AS BranchID,
# MAGIC     rl.PORTFOLIO AS Officer1Name,
# MAGIC     rl.Portfolio AS Officer2Name,
# MAGIC     NULL AS CollateralType,
# MAGIC     'Residential Home Value' AS CollateralTypeDesc,
# MAGIC     rl.PaymentType,
# MAGIC     (CASE WHEN rl.Days_Late >= 90 THEN 9 ELSE NULL END) AS RiskRating,
# MAGIC     (CASE WHEN rl.Days_Late >= 90 THEN 1 ELSE 0 END) AS NonAccrual,
# MAGIC     NULL AS NonPerformingLoan,
# MAGIC     rl.Days_Late AS DaysPastDue,
# MAGIC     'Mortgage' AS System_Type,
# MAGIC     'Purchased' AS TableSource
# MAGIC   FROM
# MAGIC     residential_loans_import rl
# MAGIC   JOIN max_entry me ON rl.YRMO = me.max_lookup_YRMO
# MAGIC   WHERE rl.YRMO >= 202412 AND rl.YRMO <= (SELECT max_daily_month-1 FROM daily_loans_max) AND ACCT NOT IN(125210, 0)
# MAGIC ),
# MAGIC
# MAGIC combined_loans_without_imputed_values AS (
# MAGIC   SELECT *
# MAGIC   FROM daily_loans_raw
# MAGIC   UNION ALL
# MAGIC   SELECT * from loans_raw
# MAGIC   UNION ALL
# MAGIC   SELECT * from residential_raw
# MAGIC   UNION ALL
# MAGIC   SELECT * FROM gl_override_raw
# MAGIC ),
# MAGIC
# MAGIC max_residential AS(
# MAGIC   SELECT *
# MAGIC   FROM residential_raw r
# MAGIC   WHERE r.YRMO = (
# MAGIC   SELECT MAX(YRMO) AS MaxResidentialData
# MAGIC   FROM residential_raw)
# MAGIC ),
# MAGIC /* Could technically be combined with daily_loans_max CTE*/
# MAGIC max_date AS (
# MAGIC   SELECT MAX(YRMO) as latest_LoanMonth, MAX(YRMODAY) as latest_LoanEntry
# MAGIC   FROM daily_loans_import
# MAGIC ),
# MAGIC
# MAGIC max_prior AS (
# MAGIC   SELECT lookup_YRMO as max_lookup_YRMO2, MAX(PriorDate) as max_PriorDate, TRY_CAST(DATE_FORMAT(max_PriorDate, 'yyyyMMdd') AS INT) AS prior_YRMODAY
# MAGIC   FROM yrmodayimport y
# MAGIC   INNER JOIN max_date md ON y.lookup_YRMODAY = md.latest_LoanEntry
# MAGIC   GROUP BY max_lookup_YRMO2  
# MAGIC ),
# MAGIC /* Expands calendar with calculated values from max_EntryDate, max_prior, and max_date */
# MAGIC filtered_yrmoimport AS (
# MAGIC   SELECT *, DATE_TRUNC('month', EntryDate) AS EntryMonth
# MAGIC   FROM yrmodayimport y
# MAGIC   LEFT JOIN max_date md ON y.lookup_YRMODAY = md.latest_LoanEntry
# MAGIC   LEFT JOIN max_entry my ON y.lookup_YRMO = my.max_lookup_YRMO
# MAGIC   LEFT JOIN max_prior mp ON y.lookup_YRMO = mp.max_lookup_YRMO2
# MAGIC   WHERE y.EntryDate = my.max_EntryDate
# MAGIC   OR y.EntryDate = mp.max_PriorDate
# MAGIC   OR y.lookup_YRMODAY = md.latest_LoanEntry
# MAGIC ),
# MAGIC
# MAGIC /* We need to impute values from the purchased residential portfolio, but *only* if the prior date isn't already accounted for as of the end of the last mont - we don't want to duplicate our data 
# MAGIC Example case: If we are running this on July 2nd, capturing July 1st data, we already have June 30th (if a business day) from the pm_consolidated table, so this will skip and the next CTE will run*/
# MAGIC duplicate_residential_cross_prior AS (
# MAGIC   SELECT (SELECT prior_YRMODAY FROM max_prior) AS YRMODAY,
# MAGIC   (SELECT max_lookup_YRMO2 FROM max_prior) AS YRMO,
# MAGIC   r.InstrumentID,
# MAGIC   r.ITYPE,
# MAGIC   r.ACCT,
# MAGIC   r.DEPT,
# MAGIC   r.CustomerName,
# MAGIC   r.CommitmentDescription,
# MAGIC   r.LastReviewDate,
# MAGIC   r.NextReviewDate,
# MAGIC   r.MasterTrancheFlag,
# MAGIC   r.CurrentOutstanding,
# MAGIC   r.CurrentBalance,
# MAGIC   r.Exposure,
# MAGIC   r.MaturityDate,
# MAGIC   r.DaysUntilMaturity,
# MAGIC   r.MaturityTerm,
# MAGIC   r.OriginalLoanAmount,
# MAGIC   r.CurrentNoteDate,
# MAGIC   r.ScheduledPayment,
# MAGIC   r.AvailableBalance,
# MAGIC   r.HoldAmount,
# MAGIC   r.InterestIndex,
# MAGIC   r.AX_IntRate,
# MAGIC   r.InterestSpread,
# MAGIC   r.Status,
# MAGIC   r.Address1,
# MAGIC   r.Address2,
# MAGIC   r.Address3,
# MAGIC   r.City,
# MAGIC   r.State,
# MAGIC   r.ZipCode,
# MAGIC   r.Branch,
# MAGIC   r.BranchID,
# MAGIC   r.Officer1Name,
# MAGIC   r.Officer2Name,
# MAGIC   r.CollateralType,
# MAGIC   r.CollateralTypeDesc,
# MAGIC   r.PaymentType,
# MAGIC   r.RiskRating,
# MAGIC   r.NonAccrual,
# MAGIC   r.NonPerformingLoan,
# MAGIC   r.DaysPastDue,
# MAGIC   r.System_Type,
# MAGIC   r.TableSource
# MAGIC   FROM max_residential r
# MAGIC   WHERE r.YRMODAY <> (SELECT TRY_CAST(DATE_FORMAT(max_PriorDate, 'yyyyMMdd') AS INT) FROM max_prior)
# MAGIC ),
# MAGIC
# MAGIC /* If we do not have data from the current month, we need to impute last month's data to today's date. Example case: It is July 2nd, and we have data from pm_consolidated for June 30th but not July 1st.
# MAGIC pm_consolidated June 30th data is imputed to July 1st; previous CTE does not run but this one does.
# MAGIC If it is July 1st and we are reporting data as of June 30th, previous CTE will run but this one will not
# MAGIC All other times this CTE runs*/
# MAGIC duplicate_residential_cross_current AS (
# MAGIC   SELECT (SELECT latest_LoanEntry from max_date) AS YRMODAY,
# MAGIC   (SELECT latest_LoanMonth FROM max_date) AS YRMO,
# MAGIC   r.InstrumentID,
# MAGIC   r.ITYPE,
# MAGIC   r.ACCT,
# MAGIC   r.DEPT,
# MAGIC   r.CustomerName,
# MAGIC   r.CommitmentDescription,
# MAGIC   r.LastReviewDate,
# MAGIC   r.NextReviewDate,
# MAGIC   r.MasterTrancheFlag,
# MAGIC   r.CurrentOutstanding,
# MAGIC   r.CurrentBalance,
# MAGIC   r.Exposure,
# MAGIC   r.MaturityDate,
# MAGIC   r.DaysUntilMaturity,
# MAGIC   r.MaturityTerm,
# MAGIC   r.OriginalLoanAmount,
# MAGIC   r.CurrentNoteDate,
# MAGIC   r.ScheduledPayment,
# MAGIC   r.AvailableBalance,
# MAGIC   r.HoldAmount,
# MAGIC   r.InterestIndex,
# MAGIC   r.AX_IntRate,
# MAGIC   r.InterestSpread,
# MAGIC   r.Status,
# MAGIC   r.Address1,
# MAGIC   r.Address2,
# MAGIC   r.Address3,
# MAGIC   r.City,
# MAGIC   r.State,
# MAGIC   r.ZipCode,
# MAGIC   r.Branch,
# MAGIC   r.BranchID,
# MAGIC   r.Officer1Name,
# MAGIC   r.Officer2Name,
# MAGIC   r.CollateralType,
# MAGIC   r.CollateralTypeDesc,
# MAGIC   r.PaymentType,
# MAGIC   r.RiskRating,
# MAGIC   r.NonAccrual,
# MAGIC   r.NonPerformingLoan,
# MAGIC   r.DaysPastDue,
# MAGIC   r.System_Type,
# MAGIC   r.TableSource
# MAGIC   FROM max_residential r
# MAGIC   WHERE r.YRMODAY <> (SELECT latest_LoanEntry from max_date)
# MAGIC ),
# MAGIC
# MAGIC combine_imputed_and_combined_loans AS (
# MAGIC   select *
# MAGIC   FROM combined_loans_without_imputed_values
# MAGIC   UNION ALL 
# MAGIC   SELECT *
# MAGIC   FROM duplicate_residential_cross_prior
# MAGIC   UNION ALL
# MAGIC   SELECT *
# MAGIC   FROM duplicate_residential_cross_current
# MAGIC )
# MAGIC
# MAGIC select * 
# MAGIC   from filtered_yrmoimport y
# MAGIC   INNER JOIN combine_imputed_and_combined_loans d ON y.lookup_YRMODAY = d.YRMODAY
# MAGIC   WHERE y.lookup_YRMO >= 202412
# MAGIC   AND d.InstrumentID IN (
# MAGIC     SELECT InstrumentID
# MAGIC     FROM combine_imputed_and_combined_loans
# MAGIC     WHERE Status <> 'InActive'
# MAGIC   )
