# Databricks notebook source
# MAGIC %md 
# MAGIC
# MAGIC #Create views for each Column
# MAGIC
# MAGIC

# COMMAND ----------

#Importing the required packages

from pyspark.sql.functions import *
from datetime import datetime
from delta.tables import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp
from pyspark.sql.types import *
from pyspark.sql.window import Window
import pytz
import pandas as pd
import os

# COMMAND ----------

# MAGIC %run "../General/NB_AMAL_Logger"

# COMMAND ----------

# MAGIC %run "../General/NB_AMAL_Utilities"

# COMMAND ----------

# MAGIC %run "../General/NB_Configuration"

# COMMAND ----------

spark.sql(f"""use catalog {catalog}""")

# COMMAND ----------

# DBTITLE 1,County_Tax_Amount_Monthly_Escrow_Amount view

df_CountyTaxAmountMonthlyEscrowAmount = spark.sql(''' 
    SELECT
    case when Investor_ID = '40H' and Category_Code = '002' then '13-ML-' || lpad(cast(Loan_Number as string),20,'0')
    when Investor_ID = '40H' and Category_Code = '003' then '13-LN-' || lpad(cast(Loan_Number as string),20,'0')
        end AS FinancialAccountNumber
    ,County_Tax_Amount_Monthly_Escrow_Amount
    FROM bronze.dmi_dcif dcifML
    WHERE dcifML.As_of_Date = (SELECT max(As_of_Date) FROM bronze.dmi_dcif)
        and CurrentRecord = 'Yes'
        and Category_Code in ('002', '003')
        and Investor_ID != '40G'
    ''').dropDuplicates(['FinancialAccountNumber'])
df_CountyTaxAmountMonthlyEscrowAmount.createOrReplaceTempView("vw_CountyTaxAmountMonthlyEscrowAmount")

# COMMAND ----------

# MAGIC %sql
# MAGIC Select FinancialAccountNumber, count(*) from vw_CountyTaxAmountMonthlyEscrowAmount
# MAGIC group by 1
# MAGIC having count(*) > 1

# COMMAND ----------

# DBTITLE 1,vw_CurrApprAmt
#per note to use Current Appraisal Amount instead of Date
# CPCAAT from CTPROP 
df_CurrApprAmt = spark.sql('''
                           select crel.acct_skey as FinancialAccountNumber,
                           prop.CPCAAT as CurrentAppraisalAmount
                           from bronze.ods_ctprop prop
                            left join bronze.ods_ctactrel crel
                            on crel.CO_SKEY = prop.CO_SKEY
                            where prop.CurrentRecord = "Yes" and crel.CRPRIM = "Y" and crel.CurrentRecord = "Yes"
                            -- possible solution to dupe
                            and crel.CRRLDT = '0001-01-01'
                            and crel.acct_skey is not null
                            
                        union all
                                                        
                        SELECT
                          case when Investor_ID = '40H' and Category_Code = '002' then '13-ML-' || lpad(cast(Loan_Number as string),20,'0')
                          when Investor_ID = '40H' and Category_Code = '003' then '13-LN-' || lpad(cast(Loan_Number as string),20,'0')
                               end AS FinancialAccountNumber
                        ,Property_Value_Amount AS  `CurrentAppraisalAmount`
                        FROM bronze.dmi_dcif dcifML
                        WHERE dcifML.As_of_Date = (SELECT max(As_of_Date) FROM bronze.dmi_dcif)
                            and CurrentRecord = 'Yes'
                            and Category_Code in ('002', '003')
                            and Investor_ID != '40G'
                        ''').dropDuplicates(['FinancialAccountNumber'])
df_CurrApprAmt.createOrReplaceTempView("vw_CurrApprAmt")

# COMMAND ----------

# MAGIC %sql
# MAGIC Select FinancialAccountNumber, count(*) from vw_CurrApprAmt
# MAGIC group by 1
# MAGIC having count(*) > 1

# COMMAND ----------

# DBTITLE 1,vw_CurrLTV
df_CurrLTV = spark.sql('''
                        select 
                        ml.Acct_skey AS FinancialAccountNumber
                        ,ml.MSCLTV as  CurrentLTV
                        --  ,'ml' as source
                        from bronze.ods_ml_master ml
                        left join vw_CurrApprAmt appr on appr.FinancialAccountNumber = ml.Acct_skey 
                        where ml.CurrentRecord = 'Yes' -- ML

                        UNION ALL

                        select 
                        ln.Acct_skey AS FinancialAccountNumber
                        ,Case
                          when ln.LMCBAL = 0 then null
                          else (try_divide(ln.LMCBAL, appr.CurrentAppraisalAmount)) end as CurrentLTV
                        --  ,'loan' as source
                        from bronze.ods_ln_master ln
                        left join vw_CurrApprAmt appr on appr.FinancialAccountNumber = ln.Acct_skey 
                        where ln.CurrentRecord = 'Yes' and ln.LMPART = 0  -- LN

                        UNION ALL

                        SELECT
                          case when Investor_ID = '40H' and Category_Code = '002' then '13-ML-' || lpad(cast(Loan_Number as string),20,'0')
                          when Investor_ID = '40H' and Category_Code = '003' then '13-LN-' || lpad(cast(Loan_Number as string),20,'0')
                               end AS FinancialAccountNumber
                            ,Case
                          when First_Principal_Balance = 0 then null
                          else try_divide(First_Principal_Balance, Property_Value_Amount) end as CurrentLTV
                        --  ,'resi' as source
                        FROM bronze.dmi_dcif dmi
                        WHERE dmi.As_of_Date = (SELECT max(As_of_Date) FROM bronze.dmi_dcif)
                              and CurrentRecord = 'Yes'
                              and Category_Code in ('002', '003')
                              and Investor_ID != '40G'

''').dropDuplicates(['FinancialAccountNumber'])

df_CurrLTV.filter("CurrentLTV is not null").createOrReplaceTempView("vw_CurrLTV")

# COMMAND ----------

# MAGIC %sql
# MAGIC Select FinancialAccountNumber, count(*) from vw_CurrLTV
# MAGIC group by 1
# MAGIC having count(*) > 1

# COMMAND ----------

# DBTITLE 1,vw_Flood
df_flood = spark.sql(''' 
    SELECT
        case when Investor_ID = '40H' and Category_Code = '002' then '13-ML-' || lpad(cast(Loan_Number as string),20,'0')
        when Investor_ID = '40H' and Category_Code = '003' then '13-LN-' || lpad(cast(Loan_Number as string),20,'0')
            end AS FinancialAccountNumber
        , dmi.Flood_Certification_Number as FloodCertificationNumber
        , CASE
            when Flood_Coverage_Amounts is null then null 
            else (Flood_Coverage_Amounts)
            end as Flood_Coverage_Amounts
        , to_timestamp(Flood_Insurance_expiration_date, 'yyyy-MM-dd HH:mm:ss') as FloodInsuranceExpirationDate
        , Flood_Zone
        FROM bronze.dmi_dcif dmi
        WHERE dmi.As_of_Date = (SELECT max(As_of_Date) FROM bronze.dmi_dcif)
                and CurrentRecord = 'Yes'
                and Category_Code in ('002', '003')
                and Investor_ID != '40G'
        ''').dropDuplicates(['FinancialAccountNumber'])
df_flood.createOrReplaceTempView("vw_Flood")

# COMMAND ----------

# MAGIC %sql
# MAGIC Select FinancialAccountNumber, count(*) from vw_Flood
# MAGIC group by 1
# MAGIC having count(*) > 1

# COMMAND ----------

# MAGIC %md
# MAGIC **** This needs to be sourced from axiom daily loans,axiom daily CDs, axiom daily deposits 

# COMMAND ----------

# DBTITLE 1,vw_ImpactSector
df_impactsector = spark.sql('''
    select tab.ACCT_SKEY as FinancialAccountNumber
    ,udf.Field_Value_Description as ImpactSector
    from
    (
      select "LN" Appl
      ,ln.ACCT_SKEY
      from bronze.ods_ln_master ln
      where ln.LMPART = 0
      AND currentrecord = 'Yes'
    )tab
    inner join
    (
    select udf.Application
    ,udf.Account_Key
    ,udf.Field_Value_Description
    from bronze.ods_SIUDF udf
    where udf.Application = "LN"
    and udf.Field_Name = "UF0038"
    AND currentrecord = 'Yes'
    ) udf on tab.Appl = udf.Application and tab.ACCT_SKEY = udf.Account_Key
    union all
    select tab.ACCT_SKEY
    ,udf.Field_Value_Description
    from
    (
      select "ML" Appl
      ,ml.ACCT_SKEY
      from bronze.ods_ml_master ml
      WHERE currentrecord = 'Yes'
    )tab
    inner join
    (
    select udf.Application
    ,udf.Account_Key
    ,udf.Field_Value_Description
    from bronze.ods_SIUDF udf
    where udf.Application = "ML"
    and udf.Field_Name = "UF0035"
    AND currentrecord = 'Yes'
    ) udf on tab.Appl = udf.Application and tab.ACCT_SKEY = udf.Account_Key
    ''').dropDuplicates(['FinancialAccountNumber'])
df_impactsector.createOrReplaceTempView("vw_impactsector")



# COMMAND ----------

# MAGIC %sql
# MAGIC Select FinancialAccountNumber, count(*) from vw_impactsector
# MAGIC group by 1
# MAGIC having count(*) > 1

# COMMAND ----------

# DBTITLE 0,vw_Segment
df_segment = spark.sql('''
    select tab.ACCT_SKEY as FinancialAccountNumber
    ,udf.Field_Value_Description as Segment
    from
    (
      select "LN" Appl
      ,ln.ACCT_SKEY
      from bronze.ods_ln_master ln
      where ln.LMPART = 0
      AND ln.currentrecord = "Yes"
    )tab
    inner join
    (
    select udf.Application
    ,udf.Account_Key
    ,udf.Field_Value_Description
    from bronze.ods_SIUDF udf
    where udf.Application = "LN"
    and udf.Field_Name = "UF0035"
    AND currentrecord = "Yes"
    ) udf on tab.Appl = udf.Application and tab.ACCT_SKEY = udf.Account_Key
    union all
    select tab.ACCT_SKEY
    ,udf.Field_Value_Description
    from
    (
      select "ML" Appl
      ,ml.ACCT_SKEY
      from bronze.ods_ml_master ml
      WHERE currentrecord = "Yes"
    )tab
    inner join
    (
    select udf.Application
    ,udf.Account_Key
    ,udf.Field_Value_Description
    from bronze.ods_SIUDF udf
    where udf.Application = "ML"
    and udf.Field_Name = "UF0031"
    AND currentrecord = "Yes"
    ) udf on tab.Appl = udf.Application and tab.ACCT_SKEY = udf.Account_Key
    ''').dropDuplicates(['FinancialAccountNumber'])
df_segment.createOrReplaceTempView("vw_segment")

# COMMAND ----------

# MAGIC %sql
# MAGIC Select count(1), FinancialAccountNumber from vw_segment Group by all having count(1)>1

# COMMAND ----------

# DBTITLE 1,vw_Interest_Index
df_IntIndex = spark.sql(''' 
    SELECT 
        case when Investor_ID = '40H' and Category_Code = '002' then '13-ML-' || lpad(cast(Loan_Number as string),20,'0')
        when Investor_ID = '40H' and Category_Code = '003' then '13-LN-' || lpad(cast(Loan_Number as string),20,'0')
            end AS FinancialAccountNumber
        ,dmi.Rate_Index as IntIndex
        -- , 'resi' as source
        FROM bronze.dmi_dcif dmi
        WHERE dmi.As_of_Date = (SELECT max(As_of_Date) FROM bronze.dmi_dcif)
                and CurrentRecord = 'Yes'
                and Category_Code in ('002', '003')
                and Investor_ID != '40G'

    UNION ALL

    --  ##############   Horizon  ###############

    Select
        ACCT_SKEY as FANumber
        ,LIPPTR as IntIndex 
        --  , 'LN' as source
        FROM bronze.ods_lnprim
        where CurrentRecord = 'Yes'
        and LIPART = 0
        and LIEXPD = 0 -- LN

      UNION ALL

      Select
        ACCT_SKEY as FANumber
        ,MUVRTB as IntIndex 
        --  , 'ML' as source
        FROM bronze.ods_dmmlnevl
        where CurrentRecord = 'Yes' -- ML
    ''').dropDuplicates(['FinancialAccountNumber'])
df_IntIndex.filter("IntIndex IS NOT NULL").createOrReplaceTempView("vw_IntIndex")


# COMMAND ----------

# MAGIC %sql
# MAGIC Select count(1), FinancialAccountNumber from vw_IntIndex Group by all having count(1)>1

# COMMAND ----------

# DBTITLE 1,df_FICO_PMI

df_FICO_PMI = spark.sql('''
    SELECT
        case when Investor_ID = '40H' and Category_Code = '002' then '13-ML-' || lpad(cast(Loan_Number as string),20,'0')
        when Investor_ID = '40H' and Category_Code = '003' then '13-LN-' || lpad(cast(Loan_Number as string),20,'0')
            end AS FinancialAccountNumber
        ,FICO
        ,PMI_Coverage_Amounts
        ,PMI_Rate
        ,Pool_PMI_Payee_Code
        ,Pool_PMI_Policy_Number
        -- 'resi' as source
        FROM bronze.dmi_dcif dmi
        WHERE dmi.As_of_Date = (SELECT max(As_of_Date) FROM bronze.dmi_dcif)
                and CurrentRecord = 'Yes'
                and Category_Code in ('002', '003')
                and Investor_ID != '40G'
''').dropDuplicates(['FinancialAccountNumber'])
df_FICO_PMI.createOrReplaceTempView("vw_FICO_PMI_DMI")

# COMMAND ----------

# MAGIC %sql
# MAGIC Select count(1), FinancialAccountNumber from vw_FICO_PMI_DMI Group by all having count(1)>1

# COMMAND ----------

# DBTITLE 1,vw_OrigLTV
df_OrigLTV = spark.sql('''
                        select 
                        ml.Acct_skey AS FinancialAccountNumber
                        ,ml.MSOLTV as OriginalLTV
                        --  ,'ml' as source
                        from bronze.ods_ml_master ml
                        where ml.CurrentRecord = 'Yes' --ML

                        UNION ALL 

                        select 
                        ln.Acct_skey AS FinancialAccountNumber
                        ,Case
                          when ln.LMTTLO = 0 then null
                          else (try_divide(ln.LMTTLO, OrigAppr.OriginalAppraisalAmount))
                        end as OriginalLTV
                        --  ,'loan' as source
                        from bronze.ods_ln_master ln
                        left join 
                            (select crel.acct_skey as FinancialAccountNumber, 
                            (prop.CPAPAT) as OriginalAppraisalAmount
                            from bronze.ods_ctprop prop
                            left join bronze.ods_ctactrel crel 
                            on crel.CO_SKEY = prop.CO_SKEY 
                            where prop.CurrentRecord = "Yes" and crel.CRPRIM = "Y" and crel.CurrentRecord = "Yes"
                                                -- possible solution to dupe 
                            and crel.CRRLDT = '0001-01-01'
                            and crel.acct_skey is not null ) OrigAppr  on OrigAppr.FinancialAccountNumber = ln.Acct_skey 
                        where ln.CurrentRecord = 'Yes' and ln.LMPART = 0-- LN 

                        UNION ALL

                      SELECT
                          case when Investor_ID = '40H' and Category_Code = '002' then '13-ML-' || lpad(cast(Loan_Number as string),20,'0')
                          when Investor_ID = '40H' and Category_Code = '003' then '13-LN-' || lpad(cast(Loan_Number as string),20,'0')
                              end AS FinancialAccountNumber
                            ,Case
                          when Original_Mortgage_Amount = 0 then null
                          else (try_divide(Original_Mortgage_Amount, Property_Value_Amount))
                        end as OriginalLTV
                        --  ,'resi' as source
                      FROM bronze.dmi_dcif dmi
                      WHERE dmi.As_of_Date = (SELECT max(As_of_Date) FROM bronze.dmi_dcif)
                              and CurrentRecord = 'Yes'
                              and Category_Code in ('002', '003')
                              and Investor_ID != '40G'

''').dropDuplicates(['FinancialAccountNumber'])

df_OrigLTV.createOrReplaceTempView("vw_OrigLTV")



# COMMAND ----------

# MAGIC %sql
# MAGIC Select count(1), FinancialAccountNumber from vw_OrigLTV Group by all having count(1)>1

# COMMAND ----------

# DBTITLE 1,vw_LastPaymementDate
df_lpmtdt = spark.sql('''
                        select
                        ml.Acct_skey AS FinancialAccountNumber,
                        CASE
                          when cast(ml.MBLPDT as string) = '99991231' or cast(ml.MBLPDT as string)=0 then null 
                          else try_to_timestamp(cast(ml.MBLPDT as string), 'yyyyMMdd') 
                        end AS lastPaymentDate
                        -- 'ml' as source
                        from bronze.ods_ml_master ml
                        where ml.CurrentRecord = 'Yes' --ML

                        UNION ALL

                        select
                        ln.Acct_skey AS FinancialAccountNumber,
                        CASE
                          when cast(ln.LMLSPD as string) = '99991231' or cast(ln.LMLSPD as string)=0 then null 
                          else to_timestamp(cast(ln.LMLSPD as string), 'yyyyMMdd') 
                        end AS lastPaymentDate
                        -- 'loan' as source
                        from bronze.ods_ln_master ln
                        where ln.CurrentRecord = 'Yes' and ln.LMPART = 0 -- LN   

                        UNION ALL

                      SELECT
                          case when Investor_ID = '40H' and Category_Code = '002' then '13-ML-' || lpad(cast(Loan_Number as string),20,'0')
                          when Investor_ID = '40H' and Category_Code = '003' then '13-LN-' || lpad(cast(Loan_Number as string),20,'0')
                              end AS FinancialAccountNumber
                          ,to_timestamp(cast(dmi.Last_Transaction_Date as string), 'yyyy-MM-dd') AS lastPaymentDate
                            -- 'resi' as source
                      FROM bronze.dmi_dcif dmi
                      WHERE dmi.As_of_Date = (SELECT max(As_of_Date) FROM bronze.dmi_dcif)
                              and CurrentRecord = 'Yes'
                              and Category_Code in ('002', '003')
                              and Investor_ID != '40G'
                      ''').dropDuplicates(['FinancialAccountNumber'])

df_lpmtdt.createOrReplaceTempView("vw_lpmtdt")

# COMMAND ----------

# MAGIC %sql
# MAGIC Select count(1), FinancialAccountNumber from vw_lpmtdt Group by all having count(1)>1

# COMMAND ----------

# DBTITLE 1,vw_occu_code
df_occu_code = spark.sql('''
                        

                      SELECT
                          case when Investor_ID = '40H' and Category_Code = '002' then '13-ML-' || lpad(cast(Loan_Number as string),20,'0')
                          when Investor_ID = '40H' and Category_Code = '003' then '13-LN-' || lpad(cast(Loan_Number as string),20,'0')
                              end AS FinancialAccountNumber
                            ,Occupancy_Code
                      FROM bronze.dmi_dcif dmi
                      WHERE dmi.As_of_Date = (SELECT max(As_of_Date) FROM bronze.dmi_dcif)
                              and CurrentRecord = 'Yes'
                              and Category_Code in ('002', '003')
                              and Investor_ID != '40G'
                      ''').dropDuplicates(['FinancialAccountNumber'])

df_occu_code.createOrReplaceTempView("vw_occu_code")

# COMMAND ----------

# MAGIC %sql
# MAGIC Select count(1), FinancialAccountNumber from vw_occu_code Group by all having count(1)>1

# COMMAND ----------

# DBTITLE 1,vw_InterestPaidTo
df_intpaidto = spark.sql('''
                        select 
                        ml.Acct_skey AS FinancialAccountNumber,
                        CASE
                            when cast(ml.MBIPDT as string) = '99991231' then null
                            else to_timestamp(cast(ml.MBIPDT as string), 'yyyyMMdd') 
                        end AS InterestPaidTo
                        -- 'ml' as source
                        from bronze.ods_ml_master ml
                        where ml.CurrentRecord = 'Yes' --ML

                        UNION ALL 

                        select 
                        ln.Acct_skey AS FinancialAccountNumber,
                        CASE
                            when cast(ln.LMIPDT as string) = '99991231' then null
                            else to_timestamp(cast(ln.LMIPDT as string), 'yyyyMMdd') 
                        end AS InterestPaidTo
                        -- 'loan' as source
                        from bronze.ods_ln_master ln
                        where ln.CurrentRecord = 'Yes' and ln.LMPART = 0 -- LN 

''').dropDuplicates(['FinancialAccountNumber'])

df_intpaidto.createOrReplaceTempView("vw_intpaidto")

# COMMAND ----------

# MAGIC %sql
# MAGIC Select count(1), FinancialAccountNumber from vw_intpaidto Group by all having count(1)>1

# COMMAND ----------

# DBTITLE 1,vw_last review date
df_lreviewdt = spark.sql('''
                        select
                        ml.Acct_Skey as FinancialAccountNumber,
                        mcmast.LastReviewDate,
                        Case
                            when cast(ml.MSNANL as string) = '99991231' or cast(ml.MSNANL as string) = '0'  then null
                            else to_TIMESTAMP(cast(ml.MSNANL as string), 'yyyyMMdd')
                        end as NextReviewDate
                        from bronze.ods_ml_master ml
                        left join 
                            (select ACCT_SKEY, MC_SKEY 
                            from bronze.ods_mcxref
                            where CurrentRecord = 'Yes') xref on xref.Acct_Skey = ml.Acct_Skey
                        left join 
                            (select 
                            Case 
                                when cast(MCLSRV as string) = '99991231' or cast(MCLSRV as string) = 0  then null 
                                else to_TIMESTAMP(cast(MCLSRV as string), 'yyyyMMdd') 
                            end as LastReviewDate, 
                            ACCT_SKEY as Acct_SKey_MC 
                            from bronze.ods_mc_master 
                            where CurrentRecord = 'Yes' ) mcmast on mcmast.Acct_SKey_MC = xref.MC_SKEY
                        Where ml.CurrentRecord = 'Yes'

                        UNION ALL 

                        select
                        ln.Acct_Skey as FinancialAccountNumber,
                        mcmast.LastReviewDate,
                        Case
                            when cast(ln.LMDTNAN as string) = '99991231' or cast(ln.LMDTNAN as string) = '0' then null
                            else to_TIMESTAMP(cast(ln.LMDTNAN as string), 'yyyyMMdd')
                        end as NextReviewDate
                        from bronze.ods_LN_Master ln
                        left join 
                            (select ACCT_SKEY, MC_SKEY 
                            from bronze.ods_mcxref
                            where CurrentRecord = 'Yes') xref on xref.Acct_Skey = ln.Acct_Skey
                        left join 
                            (select Case 
                                when cast(MCLSRV as string) = '99991231' or cast(MCLSRV as string) = 0 then null 
                                else to_TIMESTAMP(cast(MCLSRV as string), 'yyyyMMdd') 
                            end as LastReviewDate, 
                            ACCT_SKEY as Acct_SKey_MC 
                            from bronze.ods_mc_master 
                            where CurrentRecord = 'Yes' ) mcmast on mcmast.Acct_SKey_MC = xref.MC_SKEY
                        Where ln.CurrentRecord = 'Yes' and ln.LMPART = 0

                        UNION ALL
                        SELECT
                        case when Investor_ID = '40H' and Category_Code = '002' then '13-ML-' || lpad(cast(Loan_Number as string),20,'0')
                          when Investor_ID = '40H' and Category_Code = '003' then '13-LN-' || lpad(cast(Loan_Number as string),20,'0')
                              end AS FinancialAccountNumber,
                        Last_Analysis_Date as LastReviewDate,
                        null as NextReviewDate
                        FROM bronze.dmi_dcif dmi
                        WHERE dmi.As_of_Date = (SELECT max(As_of_Date) FROM bronze.dmi_dcif)
                              and CurrentRecord = 'Yes'
                              and Category_Code in ('002', '003')
                              and Investor_ID != '40G'
                      ''').dropDuplicates(['FinancialAccountNumber'])

df_lreviewdt.filter("LastReviewDate IS NOT NULL").createOrReplaceTempView("vw_lreviewdt")

# COMMAND ----------

# MAGIC %sql
# MAGIC Select count(1), FinancialAccountNumber from vw_lreviewdt Group by all having count(1)>1

# COMMAND ----------

# DBTITLE 1,RepaymentType
df_repaytyp = spark.sql('''
                        select
                        ml.Acct_Skey as FinancialAccountNumber,
                        case 
                            when ml.MBICMT = 0 then 'P&I in Arrears'
                            when ml.MBICMT = 1 then 'P&I in Advance'
                            when ml.MBICMT = 2 then 'Int Only in Arrears'    
                            when ml.MBICMT = 3 then 'Int Only in Advance'
                            when ml.MBICMT = 4 then 'Interest 1st in Arrears'
                            when ml.MBICMT = 5 then 'Int Only in Advance'
                            when ml.MBICMT = 6 then 'Fixed Prin Pmt + Int-Arrears'
                            when ml.MBICMT = 7 then 'Fixed Prin Pmt + Int-Advance'
                        end as RepaymentType
                        -- 'ml' as source
                        from bronze.ods_ML_Master ml
                        Where ml.CUrrentRecord = 'Yes' -- ml

                        UNION ALL 

                        Select 
                        tab.FinancialAccountNumber
                        ,tab.RepaymentType
                        -- ,'ln' as source
                        From 
                            (select 
                            ln.ACCT_SKEY as FinancialAccountNumber, 
                            CASE
                              when ln.LPTYPE = 1 then 'Principal Only'
                              when ln.LPTYPE = 2 then 'Interest and Insurance Only'
                              when ln.LPTYPE = 3 then 'Principal, Interest and Insurance'
                              when ln.LPTYPE = 4 then 'Fixed Principal, Interest and Insurance'
                              when ln.LPTYPE = 5 then 'Percent of Current Loan Amt with Prin + Int + Ins'
                              when ln.LPTYPE = 6 then 'Percent of Balance applied to Principal'
                              when ln.LPTYPE = 8 then 'Percent of Balance applied to Principal'
                              when ln.LPTYPE = 9 then 'Percent of Balance plus Interest and Insurance'
                            End as RepaymentType,
                            row_number() over (partition by ln.ACCT_SKEY , ln.LPPART order by ln.LPNXDT desc) as rownum
                            from bronze.ods_lnpsch ln
                            where ln.CurrentRecord = 'Yes') tab
                        where tab.rownum = 1 --LN
''').dropDuplicates(['FinancialAccountNumber'])

df_repaytyp.createOrReplaceTempView("vw_repaytyp")

# COMMAND ----------

# MAGIC %sql
# MAGIC Select count(1), FinancialAccountNumber from vw_repaytyp Group by all having count(1)>1

# COMMAND ----------

# DBTITLE 1,AnnualPIAmount
# DOES NOT INCLUDE PURCHASED LOANS

df = spark.sql('''
select 
  "13-ML-" || lpad(cast(MBACCT as string), 20, "0") AS FinancialAccountNumber 
  ,((MBMPIP * 12)) as AnnualPIAmount
  ,((MBMPIP )) as MonthlyPIAmount
, 'Horizon' as source
  from bronze.DMMLNBAL where currentrecord = 'Yes'

UNION ALL 

select
tab.FinancialAccountNumber
,((tab.MonthlyPIAmount * 12)) as AnnualPIAmount
,((tab.MonthlyPIAmount )) as MonthlyPIAmount
, 'Horizon' as source
from
  (select 
    ln.ACCT_SKEY as FinancialAccountNumber, 
    ln.LPAMT as MonthlyPIAmount,
    row_number() over (partition by ln.ACCT_SKEY order by ln.LPNXDU desc) as rownum
  from bronze.ods_lnpsch ln
  where ln.CurrentRecord = 'Yes'
    and ln.LPTYPE in (3,4)
    and ln.LPFREQ = 'M') tab
where tab.rownum = 1 --LN

Union all

SELECT CASE WHEN Investor_ID = '40H' AND Category_Code = '002' THEN "13-ML-" || lpad(cast(Loan_Number as string), 20, "0")
            WHEN Investor_ID = '40H' AND Category_Code = '003' THEN "13-LN-" || lpad(cast(Loan_Number as string), 20, "0")
            ELSE 'NA'
            END AS FinancialAccountNumber
        ,First_P_I_Amount * 12 as AnnualPIAmount
        ,First_P_I_Amount  as MonthlyPIAmount
        , 'DMI' as source
    FROM bronze.dmi_dcif
    WHERE As_of_Date = (SELECT max(As_of_Date) FROM bronze.dmi_dcif)
        AND CurrentRecord = 'Yes'
        AND dmi_dcif.Category_Code in ('002','003')
        AND Investor_ID = '40H'
''').dropDuplicates(['FinancialAccountNumber'])
df.createOrReplaceTempView("vw_AnnualPIAmt")
# df.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC Select count(1), FinancialAccountNumber from vw_AnnualPIAmt Group by all having count(1)>1

# COMMAND ----------

# DBTITLE 1,HazardPremiumMonthlyEscrow
df = spark.sql('''
select case when Investor_ID = '40H' AND Category_Code = '003' then "13-LN-" || lpad(cast(Loan_Number as string), 20, "0")
      when Investor_ID = '40H' AND Category_Code = '002' then "13-ML-" || lpad(cast(Loan_Number as string), 20, "0") END AS FinancialAccountNumber
      , Hazard_Premium_Monthly_Escrow_Amount as HazardPremiumMonthlyEscrowAmount

from bronze.dmi_dcif where CurrentRecord='Yes'
and As_of_Date in (SELECT MAX(as_of_date) from bronze.dmi_dcif)
and Category_Code in ('002','003') and Investor_ID = '40H'
and Hazard_Premium_Monthly_Escrow_Amount != 0

''').dropDuplicates(['FinancialAccountNumber'])
df.createOrReplaceTempView("vw_HazardPremiumMonthlyEscrowAmount")

# COMMAND ----------

# MAGIC %sql
# MAGIC Select count(1), FinancialAccountNumber from vw_HazardPremiumMonthlyEscrowAmount Group by all having count(1)>1

# COMMAND ----------

# DBTITLE 1,PaymentDueDate
# LN, ML, ICS/CDAR, HELOC/RESI
#  
df = spark.sql('''
    select ACCT_SKEY AS FinancialAccountNumber,
    case
    when LMDUED=0 then null 
    else to_timestamp(cast(LMDUED as varchar(10)),"yyyyMMdd")
    end PaymentDueDate
    ,'HORIZON' as Source
    from bronze.ods_ln_master
    where CurrentRecord='Yes'
    AND LMDUED !=0 AND LMPART = 0
    
    union all
    select ACCT_SKEY AS FinancialAccountNumber,
    case
    when MBDDAT=0 then null
    else to_timestamp(cast(MBDDAT as varchar(10)),"yyyyMMdd")
    end paymentduedate
    ,'HORIZON' as Source
    from bronze.ods_ml_master
    where CurrentRecord='Yes'
    AND MBDDAT!=0

    union all

    select CASE WHEN Investor_ID = '40H' AND Category_Code = '002' THEN "13-ML-" || lpad(cast(Loan_Number as string), 20, "0")
            WHEN Investor_ID = '40H' AND Category_Code = '003' THEN "13-LN-" || lpad(cast(Loan_Number as string), 20, "0")
            ELSE 'NA'
            END AS FinancialAccountNumber
    , to_timestamp(Next_Payment_Due_Date) paymentduedate
    ,'DMI' as Source
    from bronze.dmi_dcif 
    where CurrentRecord='Yes' AND Investor_ID = '40H' AND Category_Code IN('002', '003')
    and As_of_Date in (select max(As_of_Date) from bronze.dmi_dcif)
''').dropDuplicates(['FinancialAccountNumber'])
df.createOrReplaceTempView("vw_PaymentDueDate")
# df.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC Select count(1), FinancialAccountNumber from vw_PaymentDueDate Group by all having count(1)>1

# COMMAND ----------

# DBTITLE 1,BorrowerFSDate
# LN/ML
#  
df = spark.sql('''
select ACCT_SKEY as FinancialAccountNumber
, to_timestamp(cast(SIBFSD as varchar(10)),"yyyyMMdd") as `BorrowerFSDate` 
from bronze.ods_sifina 
where CurrentRecord='Yes'
AND SIBFSD is not null and SIBFSD != 0
''').dropDuplicates(['FinancialAccountNumber'])
df.createOrReplaceTempView("BorrowerFSDate")

# COMMAND ----------

# MAGIC %sql
# MAGIC Select count(1), FinancialAccountNumber from BorrowerFSDate Group by all having count(1)>1

# COMMAND ----------

# DBTITLE 1,CountyName
# LN/ML
# 
df = spark.sql('''
select b.ACCT_SKEY as FinancialAccountNumber, a.CPCNTY as `CountyName`
from bronze.ods_ctprop a
left join bronze.ods_ctactrel b
  on a.CO_SKEY = b.CO_SKEY and b.CRPRIM='Y'
where a.CurrentRecord='Yes' and b.CurrentRecord='Yes'
and CPCNTY is not null and CPCNTY != '' and CPCNTY != ' '
and b.CRID is not null and b.CRID != '' and b.CRID != ' '
''').dropDuplicates(['FinancialAccountNumber'])
df.createOrReplaceTempView("vw_CountyName")

# COMMAND ----------

# MAGIC %sql
# MAGIC Select count(1), FinancialAccountNumber from vw_CountyName Group by all having count(1)>1

# COMMAND ----------

# DBTITLE 1,Debttoncome
# LN/ML
#  
df = spark.sql('''
select case when Investor_ID = '40H' AND Category_Code = '003' then "13-LN-" || lpad(cast(Loan_Number as string), 20, "0")
      when Investor_ID = '40H' AND Category_Code = '002' then "13-ML-" || lpad(cast(Loan_Number as string), 20, "0") END AS FinancialAccountNumber
      ,Debt_to_Income_ratio as `DebttoIncomeRatios`

from bronze.dmi_dcif where CurrentRecord='Yes'
and As_of_Date in (SELECT MAX(as_of_date) from bronze.dmi_dcif)
and Debt_to_Income_ratio is not null 
and Category_Code in ('002','003') and Investor_ID = '40H'
''').dropDuplicates(['FinancialAccountNumber'])
df.createOrReplaceTempView("vw_DebttoIncomeRatios")
# df.display()


# COMMAND ----------

# MAGIC %sql
# MAGIC Select count(1), FinancialAccountNumber from vw_DebttoIncomeRatios Group by all having count(1)>1

# COMMAND ----------

# DBTITLE 1,InterestEarned
df = spark.sql('''
select acct_skey as FinancialAccountNumber, DBYDIP as `InterestEarned` from bronze.ods_dd_master where CurrentRecord='Yes' and DBYDIP > 0
union all
select acct_skey as FinancialAccountNumber, TMIPYD as `InterestEarned` from bronze.ods_td_master where CurrentRecord='Yes' and TMIPYD > 0
''').dropDuplicates(['FinancialAccountNumber'])
df.createOrReplaceTempView("vw_InterestEarned")

# COMMAND ----------

# MAGIC %sql
# MAGIC Select count(1), FinancialAccountNumber from vw_InterestEarned Group by all having count(1)>1

# COMMAND ----------

# DBTITLE 1,Margin
# LN/ML
#
df = spark.sql('''
select ACCT_SKEY as FinancialAccountNumber, round(cast(lifact as decimal(38, 20)), 6) as `Margin` from bronze.ods_lnprim where CurrentRecord='Yes'
union all
select ACCT_SKEY as FinancialAccountNumber, round(cast(MUMARG as decimal(38, 20)), 6) as `Margin` from bronze.ods_DMMLNEVL where CurrentRecord='Yes'
''').dropDuplicates(['FinancialAccountNumber'])
df.createOrReplaceTempView("vw_Margin")
# display(df)

# COMMAND ----------

# MAGIC %sql
# MAGIC Select count(1), FinancialAccountNumber from vw_Margin Group by all having count(1)>1

# COMMAND ----------

# DBTITLE 1,MortgageInsurancePremiumMonthlyEscrowAmount
# LN/ML
#  
df = spark.sql('''Select case when Investor_ID = '40H' AND Category_Code = '003' then '13-LN-' || lpad(cast(Loan_Number as string), 20, '0')
      when Investor_ID = '40H' AND Category_Code = '002' then '13-ML-' || lpad(cast(Loan_Number as string), 20, '0') END AS FinancialAccountNumber
      ,Mortgage_Insurance_Premium_Monthly_Escrow_Amount
from bronze.dmi_dcif 
where CurrentRecord='Yes'
        and As_of_Date in (SELECT MAX(as_of_date) from bronze.dmi_dcif)
        and Category_Code in ('002','003') and Investor_ID = '40H'
        and Mortgage_Insurance_Premium_Monthly_Escrow_Amount > 0
''').dropDuplicates(['FinancialAccountNumber'])
df.createOrReplaceTempView("vw_MortgageInsurancePremiumMonthlyEscrowAmount")
# display(df)

# COMMAND ----------

# MAGIC %sql
# MAGIC Select count(1), FinancialAccountNumber from vw_MortgageInsurancePremiumMonthlyEscrowAmount Group by all having count(1)>1

# COMMAND ----------

# DBTITLE 1,OriginalAppraisalAmount
# LN/ML
#  
#  edit added by sara on 5/22 in WHERE dcifML.As_of_Date = (SELECT max(As_of_Date) FROM bronze.dmi_dcif)
df = spark.sql('''
select b.ACCT_SKEY as FinancialAccountNumber, CPAPAT as OriginalAppraisalAmount
from bronze.ods_CTPROP
left join bronze.ods_ctactrel b
  on ods_CTPROP.CO_SKEY = b.CO_SKEY and b.CRPRIM='Y'
where ods_CTPROP.CurrentRecord='Yes' and b.CurrentRecord='Yes'
union all
select case when Investor_ID = '40H' AND Category_Code = '003' then '13-LN-' || lpad(cast(Loan_Number as string), 20, '0')
      when Investor_ID = '40H' AND Category_Code = '002' then '13-ML-' || lpad(cast(Loan_Number as string), 20, '0') END AS FinanicalAccountNumber
      , Property_Value_Amount as OriginalAppraisalAmount
from bronze.dmi_dcif
where As_of_Date in (SELECT MAX(as_of_date) from bronze.dmi_dcif)
      and CurrentRecord = 'Yes'
      and Category_Code in ('002', '003')
      and Investor_ID != '40G'
''').dropDuplicates(['FinancialAccountNumber'])
df.createOrReplaceTempView("vw_OriginalAppraisalAmount")
# df.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC Select count(1), FinancialAccountNumber from vw_OriginalAppraisalAmount Group by all having count(1)>1

# COMMAND ----------

# DBTITLE 1,PropertyType
df= spark.sql('''
--     --W-000897 Missing Property Type for CRE Loans
--     select b.ACCT_SKEY as FinancialAccountNumber, a.CPPTYP as PropertyType
--     from bronze.ods_CTPROP a
--     left join bronze.ods_ctactrel b
--     on a.CO_SKEY = b.CO_SKEY and b.CRPRIM='Y'
--     where a.CurrentRecord='Yes' and b.CurrentRecord='Yes'

--     union all   
          
    select case when Investor_ID = '40H' and Category_Code = '002' then '13-ML-' || lpad(cast(Loan_Number as string),20,'0')
         when Investor_ID = '40H' and Category_Code = '003' then '13-LN-' || lpad(cast(Loan_Number as string),20,'0')
         else Loan_Number end as FinancialAccountNumber
         ,dmi_dcif.Property_Type_FNMA_Code as PropertyType

    from bronze.dmi_dcif
    where Property_Type_FNMA_Code is not null
    and dmi_dcif.As_of_Date = (SELECT max(As_of_Date) FROM bronze.dmi_dcif)
      and CurrentRecord = 'Yes'
      and Category_Code in ('002', '003')
      and Investor_ID != '40G';
''').dropDuplicates(['FinancialAccountNumber'])
df.createOrReplaceTempView("vw_PropertyType")
# df.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC Select count(1), FinancialAccountNumber from vw_PropertyType Group by all having count(1)>1

# COMMAND ----------

# DBTITLE 1,TypeOfParticipation
# LN/ML
#  
# edit made by sara on 5/22, added LMPART = 0 to where clause

# W-000899 Adding Code and Description for Type of Participation
df = spark.sql('''
select ln.ACCT_SKEY as FinancialAccountNumber, concat_ws("-",ln.lmus7,cod1.C1DESC) as TypeofParticipation
from bronze.ods_ln_master ln
left join
bronze.ods_sicod1 cod1  on cod1.c1appl = 'LN' and ln.lmus7 = cod1.C1USER and cod1.C1TYPE ='US7'
where ln.CurrentRecord='Yes' and cod1.CurrentRecord ='Yes'
and LMPART = 0
and LMUS7 != ''
and LMUS7 != ' '
''').dropDuplicates(['FinancialAccountNumber'])
df.createOrReplaceTempView("vw_TypeofParticipation")
# display(df)

# COMMAND ----------

# MAGIC %sql
# MAGIC Select count(1), FinancialAccountNumber from vw_TypeofParticipation Group by all having count(1)>1

# COMMAND ----------

# DBTITLE 1,AgentBank
# LN/ML
#
# edit made by sara on 5/22, added LMPART = 0 to where clause

# W-000899 Adding Code and Description for AgentBank
df = spark.sql('''
select ACCT_SKEY as FinancialAccountNumber,concat_ws("-",ln.lmus8,cod1.C1DESC) as AgentBank 
from bronze.ods_ln_master ln
left join
bronze.ods_sicod1 cod1  on cod1.c1appl = 'LN' and ln.lmus8 = cod1.C1USER and cod1.C1TYPE ='US8'
where ln.CurrentRecord='Yes' and cod1.CurrentRecord ='Yes' and  LMPART = 0
and LMUS8 != ''

and LMUS8 != ' '
''').dropDuplicates(['FinancialAccountNumber'])
df.createOrReplaceTempView("vw_AgentBank")
# df.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC Select count(1), FinancialAccountNumber from vw_AgentBank Group by all having count(1)>1

# COMMAND ----------

# DBTITLE 1,BorrowingBaseLoan
sql_query = '''select ACCT_skey as FinancialAccountNumber, SIBOBC as BorrowingBaseLoan from bronze.ods_sifina where CurrentRecord = 'Yes' and SIBOBC is not null and SIBOBC != '' and SIBOBC != ' '
'''
df = spark.sql(sql_query)
df.createOrReplaceTempView("vw_BorrowingBaseLoan")

# COMMAND ----------

# MAGIC %sql
# MAGIC Select FinancialAccountNumber, count(1) from vw_BorrowingBaseLoan
# MAGIC Group by FinancialAccountNumber having count(1)>1

# COMMAND ----------

# DBTITLE 1,BorrowingBaseDate
base_query = '''select ACCT_skey as FinancialAccountNumber, to_date(SIBOBD,'yyyyMMdd') as BorrowingBaseDate from bronze.ods_sifina where CurrentRecord = 'Yes' and SIBOBD is not null and SIBOBD != 0
'''
df = spark.sql(base_query)
df.createOrReplaceTempView("vw_BorrowingBaseDate")
# df.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC Select count(1), FinancialAccountNumber from vw_BorrowingBaseDate Group by all having count(1)>1

# COMMAND ----------

# DBTITLE 1,Interest

#AccruedInterest - confirmed monthly accrued not annual

df = spark.sql(''' 
    select 
    ACCT_SKEY as FinancialAccountNumber
    ,LMACRC as AccruedInterest
    from bronze.ods_ln_master
    where CurrentRecord = 'Yes'
    and LMPART = 0

    union all 

    Select
    "13-ML-" || lpad(cast(MBACCT as string), 20, "0") AS FinancialAccountNumber 
    ,MBINAC as AccruedInterest
    from bronze.DMMLNBAL
    where CurrentRecord = 'Yes'
                                ''').dropDuplicates(['FinancialAccountNumber'])
df.createOrReplaceTempView("vw_AccruedInterest")

# COMMAND ----------

# MAGIC %sql
# MAGIC Select count(1), FinancialAccountNumber from vw_AccruedInterest Group by all having count(1)>1

# COMMAND ----------

# DBTITLE 1,CIProductCode
df_CIProductCode = spark.sql(''' 
    select 
    ACCT_SKEY as FinancialAccountNumber
    ,SI10C3 as CIProductCode
    from bronze.ods_sifina 
    where CurrentRecord = 'Yes'
                                ''').dropDuplicates(['FinancialAccountNumber'])
df_CIProductCode.createOrReplaceTempView("vw_CIProductCode")
#df_CIProductCode.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC Select count(1), FinancialAccountNumber from vw_CIProductCode Group by all having count(1)>1

# COMMAND ----------

# DBTITLE 1,Listrate
df_ListRate = spark.sql(''' 
select 
dd.ACCT_SKEY as financialaccountnumber
,dd.GlobalIndexKey
,indx.RTRATE as ListRate
from bronze.ods_dd_master dd
left join 
      (select 
      tb.GlobalIndexKey
      ,tb.rtrate
      ,tb.RTAPPL
      ,tb.RTEFDT
      from (Select 
      GlobalIndexKey 
      ,RTRATE
      ,RTAPPL
      ,RTEFDT
      ,CurrentRecord
      ,row_number() over(partition by GlobalIndexKey order by RTEFDT desc) as rn
      from bronze.ods_sirtindx where CurrentRecord = 'Yes') tb
      where tb.rn = 1) indx
on dd.GlobalIndexKey = indx.GlobalIndexKey
where dd.CurrentRecord = 'Yes' 
''').dropDuplicates(['FinancialAccountNumber'])
df_ListRate.createOrReplaceTempView("vw_ListRate")
# df_ListRate.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC Select count(1), FinancialAccountNumber from vw_ListRate Group by all having count(1)>1

# COMMAND ----------

# DBTITLE 1,CI

df_CityTaxAmtMonthlyEscAmt = spark.sql(''' 
    SELECT 
        CASE
            when dcif.Category_Code = '002' AND dcif.Investor_ID = '40H' then "13-ML-" || lpad(cast(dcif.Loan_Number as string), 20, "0") 
            when dcif.Category_Code = '003' AND dcif.Investor_ID = '40H' then "13-LN-" || lpad(cast(dcif.Loan_Number as string), 20, "0")
        End AS FinancialAccountNumber, 
        dcif.City_Tax_Amount_Monthly_Escrow_Amount as CityTaxAmountMonthlyEscrowAmount
    FROM bronze.dmi_dcif dcif
    WHERE dcif.As_of_Date = (SELECT max(As_of_Date) FROM bronze.dmi_dcif)
        AND dcif.CurrentRecord = 'Yes'
        AND dcif.Investor_ID = '40H'
        AND dcif.Category_Code in ('002', '003')
                                ''').dropDuplicates(['FinancialAccountNumber'])
df_CityTaxAmtMonthlyEscAmt.createOrReplaceTempView("vw_CityTaxAmtMonthlyEscAmt")
#df_CityTaxAmtMonthlyEscAmt.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC Select count(1), FinancialAccountNumber from vw_CityTaxAmtMonthlyEscAmt Group by all having count(1)>1

# COMMAND ----------

# DBTITLE 1,GuarantorFSDate
df = spark.sql('''
                        select 
                        ACCT_SKEY as FinancialAccountNumber
                        ,to_timestamp(cast(SIGFSD as string), 'yyyyMMdd') as GuarantorFSDate
                        from bronze.ods_sifina 
                        where CurrentRecord = 'Yes'
''').dropDuplicates(['FinancialAccountNumber'])

df.createOrReplaceTempView("vw_GuarantorFSDate")

# COMMAND ----------

# MAGIC %sql
# MAGIC Select count(1), FinancialAccountNumber from vw_GuarantorFSDate Group by all having count(1)>1

# COMMAND ----------

# DBTITLE 1,CurrentRiskRating
df_riskanalysis = spark.sql('''
WITH InstrumentRiskMovements AS (
    SELECT
        dl.InstrumentID,
        dl.RiskRating,
        to_timestamp(CAST(dl.yrmoday AS STRING), 'yyyyMMdd') AS date,
        LAG(dl.RiskRating, 1)
          OVER (PARTITION BY dl.InstrumentID 
                ORDER BY to_timestamp(CAST(dl.yrmoday AS STRING), 'yyyyMMdd')) AS PreviousRiskRating
    FROM bronze.axiom_daily_loans dl
    WHERE EXTRACT(YEAR FROM to_timestamp(CAST(dl.yrmoday AS STRING), 'yyyyMMdd')) = EXTRACT(YEAR FROM current_date())
),
RiskFlags AS (
    SELECT
        irm.InstrumentID,
        EXTRACT(MONTH FROM irm.date) AS Month,
        irm.RiskRating,
        irm.PreviousRiskRating,
        irm.RiskRating - irm.PreviousRiskRating AS RiskJump,
        CASE WHEN COALESCE(irm.RiskRating - irm.PreviousRiskRating, 0) >= 2
                AND irm.RiskRating >= 7
            THEN TRUE
            ELSE FALSE
        END AS RiskJumpFlag
    FROM InstrumentRiskMovements irm
    WHERE irm.PreviousRiskRating IS NOT NULL
)
SELECT
    al.InstrumentID_Key as FinancialAccountNumber,
    MAX(CASE WHEN rf.Month = 1 THEN CASE WHEN rf.RiskJumpFlag THEN 1 ELSE 0 END ELSE 0 END) AS January,
    MAX(CASE WHEN rf.Month = 2 THEN CASE WHEN rf.RiskJumpFlag THEN 1 ELSE 0 END ELSE 0 END) AS February,
    MAX(CASE WHEN rf.Month = 3 THEN CASE WHEN rf.RiskJumpFlag THEN 1 ELSE 0 END ELSE 0 END) AS March,
    MAX(CASE WHEN rf.Month = 4 THEN CASE WHEN rf.RiskJumpFlag THEN 1 ELSE 0 END ELSE 0 END) AS April,
    MAX(CASE WHEN rf.Month = 5 THEN CASE WHEN rf.RiskJumpFlag THEN 1 ELSE 0 END ELSE 0 END) AS May,
    MAX(CASE WHEN rf.Month = 6 THEN CASE WHEN rf.RiskJumpFlag THEN 1 ELSE 0 END ELSE 0 END) AS June,
    MAX(CASE WHEN rf.Month = 7 THEN CASE WHEN rf.RiskJumpFlag THEN 1 ELSE 0 END ELSE 0 END) AS July,
    MAX(CASE WHEN rf.Month = 8 THEN CASE WHEN rf.RiskJumpFlag THEN 1 ELSE 0 END ELSE 0 END) AS August,
    MAX(CASE WHEN rf.Month = 9 THEN CASE WHEN rf.RiskJumpFlag THEN 1 ELSE 0 END ELSE 0 END) AS September,
    MAX(CASE WHEN rf.Month = 10 THEN CASE WHEN rf.RiskJumpFlag THEN 1 ELSE 0 END ELSE 0 END) AS October,
    MAX(CASE WHEN rf.Month = 11 THEN CASE WHEN rf.RiskJumpFlag THEN 1 ELSE 0 END ELSE 0 END) AS November,
    MAX(CASE WHEN rf.Month = 12 THEN CASE WHEN rf.RiskJumpFlag THEN 1 ELSE 0 END ELSE 0 END) AS December,
    CASE WHEN (
        MAX(CASE WHEN rf.Month = 1 THEN CASE WHEN rf.RiskJumpFlag THEN 1 ELSE 0 END ELSE 0 END) +
        MAX(CASE WHEN rf.Month = 2 THEN CASE WHEN rf.RiskJumpFlag THEN 1 ELSE 0 END ELSE 0 END) +
        MAX(CASE WHEN rf.Month = 3 THEN CASE WHEN rf.RiskJumpFlag THEN 1 ELSE 0 END ELSE 0 END) +
        MAX(CASE WHEN rf.Month = 4 THEN CASE WHEN rf.RiskJumpFlag THEN 1 ELSE 0 END ELSE 0 END) +
        MAX(CASE WHEN rf.Month = 5 THEN CASE WHEN rf.RiskJumpFlag THEN 1 ELSE 0 END ELSE 0 END) +
        MAX(CASE WHEN rf.Month = 6 THEN CASE WHEN rf.RiskJumpFlag THEN 1 ELSE 0 END ELSE 0 END) +
        MAX(CASE WHEN rf.Month = 7 THEN CASE WHEN rf.RiskJumpFlag THEN 1 ELSE 0 END ELSE 0 END) +
        MAX(CASE WHEN rf.Month = 8 THEN CASE WHEN rf.RiskJumpFlag THEN 1 ELSE 0 END ELSE 0 END) +
        MAX(CASE WHEN rf.Month = 9 THEN CASE WHEN rf.RiskJumpFlag THEN 1 ELSE 0 END ELSE 0 END) +
        MAX(CASE WHEN rf.Month = 10 THEN CASE WHEN rf.RiskJumpFlag THEN 1 ELSE 0 END ELSE 0 END) +
        MAX(CASE WHEN rf.Month = 11 THEN CASE WHEN rf.RiskJumpFlag THEN 1 ELSE 0 END ELSE 0 END) +
        MAX(CASE WHEN rf.Month = 12 THEN CASE WHEN rf.RiskJumpFlag THEN 1 ELSE 0 END ELSE 0 END)
    ) > 0 THEN 'TRUE' ELSE 'FALSE' END AS CurrentYearRiskRatingTripped
FROM RiskFlags rf
LEFT JOIN (
    SELECT InstrumentID, InstrumentID_Key, YRMO
    FROM bronze.axiom_loans 
    WHERE YRMO = (SELECT MAX(YRMO) FROM bronze.axiom_loans)
) al ON rf.InstrumentID = al.InstrumentID
GROUP BY al.InstrumentID_Key
ORDER BY al.InstrumentID_Key
''').dropDuplicates(['FinancialAccountNumber'])

# Create or replace the view named "vw_yearly_flags_pivoted"
df_riskanalysis.createOrReplaceTempView("vw_CurrentYearRiskRatingTripped")



# COMMAND ----------

# MAGIC %sql
# MAGIC Select count(1), FinancialAccountNumber from vw_CurrentYearRiskRatingTripped Group by all having count(1)>1

# COMMAND ----------

# DBTITLE 1,vw_axiomFANumber
df = spark.sql ('''
                select instrumentID, 
case when instrumentID_key is not null then instrumentID_key 
 when AppCode = "ML" then '13-ML-' || lpad(cast(InstNum as string),20,'0') 
 when AppCode in ('CC' , 'CI', 'CL', 'IL')  then '13-LN-' || lpad(cast(InstNum as string),20,'0')
 else 'error'
 end as FinancialAccountNumber
from bronze.axiom_daily_loans 
group by 1, 2''')
df.createOrReplaceTempView('vw_axiomFinancialAccountNumber')

# COMMAND ----------

# MAGIC %sql
# MAGIC Select FinancialAccountNumber, count(1) from vw_axiomFinancialAccountNumber group by all having count(1)>1

# COMMAND ----------

# DBTITLE 1,MultifamilyPropertyRentStabilized
df_MF = spark.sql ('''
select reg.rs as MultifamilyPropertyRentStabilized, 
reg.instrumentID, 
vw.FinancialaccountNumber 
from vw_axiomFinancialAccountNumber vw 
inner join bronze.axiom_daily_loans reg on reg.InstrumentId = vw.instrumentID where reg.yrmoday = (select max(yrmoday) from bronze.axiom_daily_loans where reg.RS is not null )
''' )
df_MF_final = df_MF.createOrReplaceTempView('vw_MFRentStabilized')

# COMMAND ----------

# MAGIC %sql
# MAGIC Select count(1), FinancialAccountNumber from vw_MFRentStabilized Group by all having count(1)>1

# COMMAND ----------

# MAGIC %md
# MAGIC #Checks
# MAGIC

# COMMAND ----------

# DBTITLE 1,OccupancyCodeCheck
# MAGIC %sql
# MAGIC select * from silver.financial_account where OccupancyCode is not null limit 3
# MAGIC --  limit 2

# COMMAND ----------

# DBTITLE 1,Appraisal Amount Check
# %sql 
# select *
# from vw_CurrApprAmt
# -- group by 1
# -- having count(*) > 1

# COMMAND ----------

# DBTITLE 1,CountyEscrow Check
# %sql
# select  * from vw_CountyTaxAmountMonthlyEscrowAmount 

# COMMAND ----------

# DBTITLE 1,Repayment Type Check
# %sql
# SELECT * FROM vw_repaytyp

# COMMAND ----------

# DBTITLE 1,CurrLTV_Check
# %sql
# select FinancialAccountNumbet, count(*) 
# from vw_CurrLTV
# group by 1
# having count(*) > 1


# COMMAND ----------

# DBTITLE 1,vw_CurrApprAmt - Check
# %sql 
# select * 
# from vw_CurrApprAmt
# -- group by 1
# -- having count(*) > 1

# COMMAND ----------

# DBTITLE 1,Flood Check
# %sql select * from vw_flood where financialaccountnumber = '13-ML-00000000001508182159'

# COMMAND ----------

# DBTITLE 1,Flood Dup Check
# %sql 
# select FinancialAccountNumber, count(*) from vw_Flood
# group by 1
# having count(*) > 1

# COMMAND ----------

# DBTITLE 1,Impact Sector Check
# %sql 
# select FinancialAccountNumber, count(*) from vw_impactsector
# group by 1
# having count(*) > 1

# COMMAND ----------

# DBTITLE 1,impact sector - dup check
# %sql
# select FinancialAccountNumber, count(*) 
# from vw_testing_impact
# group by 1
# having count(*) > 1

# COMMAND ----------

# DBTITLE 1,Interest_Index Check
# %sql
# select * from vw_IntIndex

# COMMAND ----------

# DBTITLE 1,IntIndex - Dup Check
# %sql 

# select FinancialAccountNumber, count(*)
# from vw_IntIndex
# group by 1 
# having count(*) > 1

# COMMAND ----------

# DBTITLE 1,vw_OrigLTV - Check
# MAGIC %sql
# MAGIC select FinancialAccountNumber, count(*) from vw_OrigLTV
# MAGIC group by 1
# MAGIC having count(*) > 1

# COMMAND ----------

# DBTITLE 1,vw_FICO_PMI_DMI - Checl
# %sql SELECT * FROM vw_FICO_PMI_DMI

# COMMAND ----------

# DBTITLE 1,vw_FICO_PMI_DMI - checking dupes
# %sql
# select FinancialAccountNumber, count(*) from vw_FICO_PMI_DMI
# group by 1
# having count(*) > 1

# COMMAND ----------

# DBTITLE 1,lpmtdt dupe check
# MAGIC %sql
# MAGIC select FinancialAccountNumber, count(*) from vw_lpmtdt
# MAGIC group by 1
# MAGIC having count(*) > 1

# COMMAND ----------

# DBTITLE 1,vw_intpaidto dup check
# MAGIC %sql
# MAGIC select FinancialAccountNumber, count(*)
# MAGIC from vw_intpaidto
# MAGIC group by 1 
# MAGIC having count(*) > 1

# COMMAND ----------

# DBTITLE 1,vw_occu_code dupe check
# MAGIC %sql
# MAGIC select FinancialAccountNumber, count(*) from vw_occu_code
# MAGIC group by 1
# MAGIC having count(*) > 1

# COMMAND ----------

# DBTITLE 1,vw_intpaidto - Check
# %sql
# SELECT * FROM vw_intpaidto

# COMMAND ----------

# DBTITLE 1,vw_lpmtdt - Check
# MAGIC %sql
# MAGIC select * from vw_lpmtdt

# COMMAND ----------

# DBTITLE 1,vw_OrigLTV - Check
# %sql
# SELECT * FROM vw_OrigLTV

# COMMAND ----------

# DBTITLE 1,vw_occu_code - Check
# MAGIC %sql
# MAGIC SELECT * FROM vw_occu_code

# COMMAND ----------

# DBTITLE 1,repayment type dupe check
# MAGIC %sql 
# MAGIC select FinancialAccountNumber, count(*)
# MAGIC from vw_repaytyp
# MAGIC group by 1
# MAGIC having count(*) > 1
# MAGIC     
# MAGIC

# COMMAND ----------

# DBTITLE 1,vw_lreviewdt - Check
# %sql
# select * FROM vw_lreviewdt

# COMMAND ----------

# DBTITLE 1,vw_lreviewdt dupe Check
# %sql 
# select FinancialAccountNumber, count(*)
# from vw_lreviewdt
# group by 1
# having count(*) > 1

# COMMAND ----------

# MAGIC %md
# MAGIC #Add the columns in CRM

# COMMAND ----------

from pyspark.sql.functions import lower

# COMMAND ----------

df_base = spark.sql('''
    SELECT FinancialAccountNumber,*, ROW_NUMBER() OVER (PARTITION BY FinancialAccountNumber ORDER BY FinancialAccountNumber) as rownum
    FROM default.financial_account_holder
    WHERE CurrentRecord = 'Yes'
    --AND FinancialAccountNumber in (SELECT DISTINCT Financial_AccountNumber_skey FROM silver.financial_account_party WHERE CurrentRecord = 'Yes')
''').dropDuplicates(['FinancialAccountNumber'])
df_base = df_base.filter("rownum = 1").drop("rownum")
df_base.createOrReplaceTempView("vw_base")
# df_base.display()
df_base.count()

# COMMAND ----------

# MAGIC %sql
# MAGIC Select FinancialAccountNumber, count(1) from vw_base group by all having count(1)>1

# COMMAND ----------

# MAGIC %md
# MAGIC #### Large Left Join

# COMMAND ----------

df_addon_FA = spark.sql(f"""select
    base.financialaccountnumber
  ,tax.County_Tax_Amount_Monthly_Escrow_Amount as CountyTaxAmountMonthlyEscrowAmount,
  ApprAmt.CurrentAppraisalAmount as CurrentAppraisalAmount,
  LTV.CurrentLTV  as CurrentLTV,
  flood.floodCertificationNumber as FloodCertificationNumber,
  flood.Flood_Coverage_Amounts as FloodCoverageAmount,
  flood.FloodInsuranceExpirationDate as FloodInsuranceExpirationDate,
  flood.flood_zone as FloodZone,
  impact.ImpactSector as ImpactSector,
  IntIndex.IntIndex as InterestIndex,
  PI.MonthlyPIAmount as MonthlyPIAmount,
  PI.AnnualPIAmount as AnnualPIAmount,
  pmi.FICO FICO,
  pmi.PMI_Coverage_Amounts as PMICoverageAmounts,
  pmi.PMI_Rate as PMIRate,
  pmi.Pool_PMI_Payee_Code as PoolPMIPayeeCode,
  pmi.Pool_PMI_Policy_Number as PoolPMIPolicyNumber,
  originalLTV.OriginalLTV as  OriginalLTV,
  lpd.lastPaymentDate as LastPaymentDate,
  pmtdue.PaymentDueDate as PaymentDueDate,
  occu.Occupancy_Code as OccupancyCode ,
  intpadito.InterestPaidTo as InterestPaidTo  ,
  vw_lreviewdt.NextReviewDate as NextReviewDate,
  vw_lreviewdt.LastReviewDate as LastReviewDate,
  vw_repaytyp.RepaymentType as RepaymentType,
  bfs.BorrowerFSDate as BorrowerFSDate,
  ct.CountyName as CountyName,
  dti.DebttoIncomeRatios as DebttoIncomeRatio,
  ie.InterestEarned as InterestEarned,
  margin.margin as MarginSpread,
  escr.HazardPremiumMonthlyEscrowAmount as HazardPremiumMonthlyEscrowAmount,
  escr2.Mortgage_Insurance_Premium_Monthly_Escrow_Amount as MortgageInsurancePremiumMonthlyEscrowAmount,
  oa.OriginalAppraisalAmount as OriginalAppraisalAmount,
  pty.PropertyType as PropertyType, 
  tp.TypeofParticipation as TypeofParticipation,
  ab.AgentBank as AgentBank,
  seg.Segment as Segment,
  baseln.BorrowingBaseLoan as BorrowingBaseLoan,
  basdt.BorrowingBaseDate as BorrowingBaseDate,
  accint.AccruedInterest as AccruedInterest,
  cip.CIProductCode as CIProductCode,
  ltr.GlobalIndexKey as GlobalIndexKey,
  ltr.ListRate as ListRate,
  ctam.CityTaxAmountMonthlyEscrowAmount as CityTaxAmountMonthlyEscrowAmount,
  gfsdate.GuarantorFSDate,
  trip.CurrentYearRiskRatingTripped,
  mf.MultifamilyPropertyRentStabilized


from
  vw_base as base
    left join vw_CountyTaxAmountMonthlyEscrowAmount as tax on base.FinancialAccountNumber = tax.FinancialAccountNumber
    left join vw_CurrApprAmt as ApprAmt on base.FinancialAccountNumber = ApprAmt.FinancialAccountNumber
    left join vw_CurrLTV as LTV on base.FinancialAccountNumber = LTV.FinancialAccountNumber
    left join vw_Flood as flood on base.FinancialAccountNumber = flood.FinancialAccountNumber
    left join vw_impactsector as impact on base.FinancialAccountNumber = impact.FinancialAccountNumber
    left join vw_IntIndex as IntIndex on base.FinancialAccountNumber = IntIndex.FinancialAccountNumber
    left join vw_AnnualPIAmt as PI on base.FinancialAccountNumber = PI.FinancialAccountNumber
    left join vw_FICO_PMI_DMI as pmi ON base.FinancialAccountNumber = pmi.FinancialAccountNumber
    left join vw_OrigLTV as originalLTV on base.FinancialAccountNumber = originalLTV.FinancialAccountNumber
    left join vw_lpmtdt as lpd on base.FinancialAccountNumber = lpd.FinancialAccountNumber
    left join vw_occu_code as occu on base.FinancialAccountNumber = occu.FinancialAccountNumber
    left join vw_intpaidto as intpadito on base.FinancialAccountNumber = intpadito.FinancialAccountNumber
    left join vw_lreviewdt as vw_lreviewdt on base.FinancialAccountNumber = vw_lreviewdt.FinancialAccountNumber
    left join vw_repaytyp as vw_repaytyp on base.FinancialAccountNumber = vw_repaytyp.FinancialAccountNumber
    LEFT JOIN vw_AnnualPIAmt piamt ON base.financialaccountnumber = piamt.FinancialAccountNumber
    LEFT JOIN vw_PaymentDueDate pmtdue on base.financialaccountnumber = pmtdue.FinancialAccountNumber
    LEFT JOIN BorrowerFSDate bfs on base.financialaccountnumber = bfs.financialaccountnumber
    LEFT JOIN vw_CountyName ct on base.financialaccountnumber = ct.financialaccountnumber
    LEFT JOIN vw_DebttoIncomeRatios dti on dti.financialaccountnumber = base.financialaccountnumber
    LEFT JOIN vw_HazardPremiumMonthlyEscrowAmount escr on base.financialaccountnumber = escr.financialaccountnumber
    LEFT JOIN vw_InterestEarned ie on base.financialaccountnumber = ie.financialaccountnumber
    LEFT JOIN vw_Margin margin on base.financialaccountnumber = margin.financialaccountnumber
    LEFT JOIN vw_MortgageInsurancePremiumMonthlyEscrowAmount escr2 on base.financialaccountnumber = escr2.financialaccountnumber
    LEFT JOIN vw_OriginalAppraisalAmount oa on oa.financialaccountnumber = base.financialaccountnumber
    LEFT JOIN vw_PropertyType pty on pty.financialaccountnumber = base.financialaccountnumber
    LEFT JOIN vw_TypeofParticipation tp on tp.financialaccountnumber = base.financialaccountnumber
    LEFT JOIN vw_AgentBank ab on ab.financialaccountnumber = base.financialaccountnumber
    LEFT JOIN vw_segment seg on seg.financialaccountnumber = base.financialaccountnumber
    LEFT JOIN vw_BorrowingBaseLoan baseln on baseln.FinancialAccountNumber = base.FinancialAccountNumber
    LEFT JOIN vw_BorrowingBaseDate basdt on basdt.FinancialAccountNumber =base.FinancialAccountNumber
    LEFT JOIN vw_AccruedInterest accint on accint.financialaccountnumber = base.financialaccountnumber
    LEFT JOIN vw_CIProductCode cip on cip.financialaccountnumber = base.financialaccountnumber
    LEFT JOIN vw_ListRate ltr on ltr.financialaccountnumber = base.financialaccountnumber
    LEFT JOIN vw_CityTaxAmtMonthlyEscAmt ctam on ctam.financialaccountnumber = base.financialaccountnumber
    LEFT JOIN vw_GuarantorFSDate gfsdate on gfsdate.financialaccountnumber = base.financialaccountnumber
    LEFT JOIN vw_CurrentYearRiskRatingTripped trip on trip.FinancialAccountNumber = base.FinancialAccountNumber
    left join vw_MFRentStabilized mf on mf.Financialaccountnumber = base.financialaccountnumber
        
""")
df_addon_FA.createOrReplaceTempView("vw_addon_FA")
df_addon_FA.count()

# COMMAND ----------

# DBTITLE 1,vw_dup_check
# MAGIC %sql
# MAGIC select FinancialAccountNumber ,count(1) from vw_addon_FA
# MAGIC group by FinancialAccountNumber
# MAGIC having count(1) > 1

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dynamic Merge

# COMMAND ----------

DestinationSchema = dbutils.widgets.get('DestinationSchema')
DestinationTable = dbutils.widgets.get('DestinationTable')
AddOnType = dbutils.widgets.get("AddOnType")
print(DestinationSchema, DestinationTable, AddOnType)

# COMMAND ----------

# DBTITLE 1,Extract and Filter the required columns from base table
base_column = spark.read.table(f"{DestinationSchema}.{DestinationTable}").columns #get all the base columns
set_addon=df_addon_FA.columns #get only the addon columns
get_pk=spark.sql(f"""select * from config.metadata where lower(DWHTableName)='financial_account' and lower(DWHSchemaName)='silver' """).collect()[0]['MergeKey']
set_addon.remove(get_pk.lower()) #remove pk from the addon
excluded_columns = ['Start_Date', 'End_Date', 'DW_Created_By', 'DW_Created_Date', 'DW_Modified_By', 'DW_Modified_Date','MergeHashKey','CurrentRecord'] + set_addon
filtered_basetable_columns = [col for col in base_column if col.lower() not in [ex_col.lower() for ex_col in excluded_columns]]

# COMMAND ----------

#get required columns from base table
df_base_required = spark.sql(f"select {','.join(filtered_basetable_columns)} from {DestinationSchema}.{DestinationTable} where currentrecord='Yes' ")
df_base_required.createOrReplaceTempView("vw_base")  #use this as a base table
if AddOnType.strip() == 'AddOn':

  if df_base_required.count() > 0:
      join_conditions = " and ".join([f"vw_base.{col.strip()} = vw_addon_FA.{col.strip()}" for col in get_pk.split(',')])

      df_final_base_with_addon = spark.sql(
          f"""
          select
              vw_base.*,
              {','.join([f'vw_addon_FA.{col} as {col}' for col in set_addon])}
          from 
              vw_base 
          left join 
              vw_addon_FA 
          on 
              {join_conditions}
      """)
      df_final_base_with_addon.createOrReplaceTempView("vw_final_base_with_addon")
      print("got the base for addon columns")
      df_final_base_with_addon.count()
  else:
    df_addon_FA.createOrReplaceTempView("vw_final_base_with_addon")
    count = df_addon_FA.count()
    display(count)
else:
    df_addon_FA.createOrReplaceTempView("vw_final_base_with_addon")
    count = df_addon_FA.count()
    display(count)
 

# COMMAND ----------

# Generate the concatenated string
base_without_pk=filtered_basetable_columns.copy()
base_without_pk.remove(get_pk)
Mergehashkey_columns = list(set(set_addon + filtered_basetable_columns))
concatenated_columns = ','.join(Mergehashkey_columns)

# COMMAND ----------

# Use the concatenated string in the SQL query
query = f"""
select
 *,
 MD5(
    CONCAT_WS(',', {concatenated_columns})
  ) AS MergeHashKey
  from
  vw_final_base_with_addon
"""
df_source = spark.sql(query)
set_addon.append('MergeHashKey')
set_addon=set(set_addon)
df_source.createOrReplaceTempView("vw_source")

# COMMAND ----------

target_count = spark.sql(f"SELECT COUNT(*) FROM {DestinationSchema}.{DestinationTable}").collect()[0][0]

select_columns = []
for col in filtered_basetable_columns:
    if col in get_pk.split(','):
        if target_count > 0:
            select_columns.append(f"CASE WHEN {' AND '.join([f'target.{pk} IS NULL' for pk in get_pk.split(',')])} THEN source.{col} ELSE target.{col} END AS {col}")
        else:
            select_columns.append(f"source.{col}")
    else:
        select_columns.append(f"target.{col}")

query = f"""
select 
  {','.join(select_columns)},
  {','.join([f'source.{col}' for col in set_addon])},
  current_user() as DW_Created_By,
  current_timestamp() as DW_Created_Date,
  current_user() as DW_Modified_By,
  current_timestamp() as DW_Modified_Date,
  current_timestamp() as Start_Date,
  NULL as End_Date,
  'Yes' as CurrentRecord,
  CASE 
    WHEN { ' AND '.join([f'target.{col} IS NULL' for col in get_pk.split(',')]) } THEN 'Insert'
    WHEN { ' AND '.join([f'target.{col} = source.{col}' for col in get_pk.split(',')]) } AND source.MergeHashKey != target.MergeHashKey THEN 'Update'
    ELSE 'No Changes' 
  END As Action_Code  
from (select {','.join(set_addon)}, {','.join([f'{col}' for col in get_pk.split(',')])} from vw_source group by all) as source
left join (select * from {DestinationSchema}.{DestinationTable} where currentrecord='Yes' ) as target
on { ' AND '.join([f'target.{col} = source.{col}' for col in get_pk.split(',')]) }
"""

df_source = spark.sql(query)
df_source = df_source.dropDuplicates()
df_source.createOrReplaceTempView("vw_silver")
final_col = df_source.columns
final_col.remove('Action_Code')

# COMMAND ----------

# MAGIC %sql
# MAGIC Select FinancialAccountNumber, count(1) from default.financial_account_holder
# MAGIC Where CurrentRecord='Yes'
# MAGIC group by all having count(1)>1

# COMMAND ----------

spark.sql(f"""
    INSERT INTO {DestinationSchema}.{DestinationTable}({','.join(final_col)})
    SELECT {','.join(final_col)} FROM vw_silver WHERE Action_Code IN ('Insert', 'Update')
""")

spark.sql(f"""
    MERGE INTO {DestinationSchema}.{DestinationTable} AS Target
    USING (SELECT {','.join(final_col)} FROM VW_silver WHERE Action_Code='Update') AS Source
    ON { ' AND '.join([f'Target.{col} = Source.{col}' for col in get_pk.split(',')])} AND Target.MergeHashKey != Source.MergeHashKey and target.currentrecord = 'Yes'
    WHEN MATCHED THEN UPDATE SET
    Target.End_Date = CURRENT_TIMESTAMP(),
    Target.DW_Modified_Date = Source.DW_Modified_Date,
    Target.DW_Modified_By = Source.DW_Modified_By,
    Target.CurrentRecord = 'No'
""")

# COMMAND ----------

df = spark.sql(f"select * from {DestinationSchema}.{DestinationTable} where End_Date is null")

df.createOrReplaceTempView("target_view")

DFSourceNull = spark.sql(f"""
                SELECT t.*,
                    CASE WHEN s.{get_pk.split(',')[0]} IS NULL THEN 'No' ELSE 'Yes' END AS CurrentRecordTmp
                FROM target_view t
                FULL JOIN VW_silver s
                ON { ' AND '.join([f's.{col} = t.{col}' for col in get_pk.split(',')]) }
            """)
# Filter out the 'DeleteFlag' rows for next steps
DFSourceNull.createOrReplaceTempView("SourcetoInsertUpdate")

# Merge operation
MergeQuery = f"""
        MERGE INTO {DestinationSchema}.{DestinationTable} AS target
        USING SourcetoInsertUpdate AS source
        ON { ' AND '.join([f'target.{col} = source.{col}' for col in get_pk.split(',')]) }
        AND source.CurrentRecordTmp = 'No'
        WHEN MATCHED THEN
            UPDATE SET target.CurrentRecord = 'Deleted', target.end_date=current_timestamp(), target.DW_modified_Date=current_timestamp(),target.DW_Modified_By='Databricks'
    """

spark.sql(MergeQuery)

# COMMAND ----------

# MAGIC %sql
# MAGIC Select * from default.financial_account_holder where FinancialAccountNumber='13-CD-00000000005001040784'

# COMMAND ----------

# DBTITLE 1,Dupes Check
# MAGIC %sql
# MAGIC Select FinancialAccountNumber, Count(1) from default.financial_account_holder
# MAGIC Where CurrentRecord='Yes'
# MAGIC Group by all having count(1)>1
