# Databricks notebook source
# MAGIC %md
# MAGIC ####Calling Configuration Notebook

# COMMAND ----------

# MAGIC %run "../General/NB_Configuration"

# COMMAND ----------

spark.sql(f"""use catalog {catalog}""")

# COMMAND ----------

current_time = spark.sql("SELECT current_timestamp()").collect()[0][0]
 
batch_ts = current_time.strftime('%Y-%m-%d %H:%M:%S')
batch_ts

# COMMAND ----------

# MAGIC %run "../General/NB_AMAL_Utilities"

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Global Plus Data Source

# COMMAND ----------

vw_gp = spark.sql(f'''
SELECT '' AS GID,
       regexp_replace(Taxid, '-', '') AS OID,
       'EIN' AS TID,
       'C4DB61A7-8DEA-4481-8B27-D0C00286E5C7' AS SID,
       '{batch_ts}' AS Timestamp
FROM bronze.account a
WHERE TAXID IS NOT NULL
''')

vw_gp.createOrReplaceTempView("vw_gp")
vw_gp.count()

# COMMAND ----------

# MAGIC %md
# MAGIC OSAIC Data Source 
# MAGIC

# COMMAND ----------

vw_osaic = spark.sql(
f'''select
    '' GID,
    SSN_Tax_ID OID,
    Case when upper(a.IRS_Code) = 'T' then 'EIN'
         when upper(a.IRS_Code) = 'S' then 'SSN'
    end as TID,
    '5442CD29-1B99-4274-AFE3-76778E67BC89' SID,
    '{batch_ts}' AS Timestamp
from bronze.account_osaic a
where SSN_Tax_ID != ' ' ''')

vw_osaic.createOrReplaceTempView("vw_osaic")
vw_osaic.count()

# COMMAND ----------

# MAGIC %md
# MAGIC DMI DCIF File load

# COMMAND ----------

vw_dcif = spark.sql(f'''
select
    '' GID,
    Mortgagor_SSN OID,
    'SSN' TID,
    '46E060DF-8768-43D7-9A9B-A6BE107E0516' SID,
    '{batch_ts}' AS Timestamp
    -- *
    -- Mortgagor_Name_Formatted_for_CBR_Reporting, 
    --     split(Mortgagor_Name_Formatted_for_CBR_Reporting, ' ')[0] as Last_Name,
    --     split(Mortgagor_Name_Formatted_for_CBR_Reporting, ' ')[1] as First_Name,
    --     split(Mortgagor_Name_Formatted_for_CBR_Reporting, ' ')[2] as Middle_Name
from bronze.dmi_dcif
where mortgagor_ssn != ' '
UNION ALL
select
    '' GID,
    Co_Mortgagor_SSN OID,
    'SSN' TID,
    '46E060DF-8768-43D7-9A9B-A6BE107E0516' SID,
    '{batch_ts}' AS Timestamp
    -- *
    -- Co_Mortgagor_Name_Formatted_for_CBR_Reporting,
    -- split(Co_Mortgagor_Name_Formatted_for_CBR_Reporting, ' ')[0] as Co_Mortgagor_Last_Name,
    -- split(Co_Mortgagor_Name_Formatted_for_CBR_Reporting, ' ')[1] as Co_Mortgagor_First_Name,
    -- split(Co_Mortgagor_Name_Formatted_for_CBR_Reporting, ' ')[2] as Co_Mortgagor_Middle_Name
from bronze.dmi_dcif
where Co_Mortgagor_SSN != ' '
''')

vw_dcif.createOrReplaceTempView("vw_dcif")
vw_dcif.count()


# COMMAND ----------

# MAGIC %sql
# MAGIC select 
# MAGIC oid,tid,sid,count(1) 
# MAGIC -- * 
# MAGIC from vw_dcif
# MAGIC  group by oid,tid,sid having count(1)>1

# COMMAND ----------

# MAGIC %md
# MAGIC Horizon Data Source 

# COMMAND ----------

vw_horizon = spark.sql(f'''
    SELECT
        '' AS GID,
        CASE
            WHEN rmmast.RMTIN = '' THEN upper(rmmast.RMFRST) || upper(rmmast.RMMIDL) || upper(rmmast.RMLAST)
            ELSE mast.SSN
        END AS OID,
        CASE
            WHEN rmmast.RMTINT = 'E' THEN 'EIN'
            WHEN rmmast.RMTINT = 'S' THEN 'SSN'
            WHEN rmmast.RMTINT = ' ' THEN 'Name'
            WHEN rmmast.RMTINT = ''  THEN 'Name'
            WHEN rmmast.RMTINT = 'O' THEN 'OTHER'
            ELSE 'SSN'
        END AS TID,
        'F51392CF-6C1A-4DDA-B3E2-23BFB581649D' SID,
        '{batch_ts}' AS Timestamp
    FROM bronze.RMMAST_ssn mast
    LEFT JOIN silver.ods_RMMAST rmmast
    ON rmmast.CUST_SKEY = mast.skey
''')
vw_horizon.createOrReplaceTempView("vw_horizon")
vw_horizon.count()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from vw_horizon
# MAGIC where OID = '634201220'

# COMMAND ----------

# MAGIC %sql
# MAGIC select oid,tid,sid,count(1) from vw_horizon group by oid,tid,sid having count(1)>1

# COMMAND ----------

vw_combined = spark.sql('''
                       SELECT * from vw_gp
                       UNION ALL
                       SELECT * from vw_dcif
                       UNION ALL
                       SELECT * from vw_osaic
                       UNION ALL
                       SELECT * from vw_horizon
                       ''')
vw_combined.createOrReplaceTempView("vw_combined")
vw_combined.count()

# COMMAND ----------

# MAGIC %sql
# MAGIC select oid,tid,sid,count(1) from vw_combined group by oid,tid,sid having count(1)>1

# COMMAND ----------

# MAGIC %md
# MAGIC Combined Customer Key Data Source - Horizon, DMI, Osaic

# COMMAND ----------

# SQL to drop duplicates from the combined view
vw_ranked = spark.sql('''SELECT GID, OID, TID, SID, Timestamp FROM (
    SELECT *, row_number() over (partition by OID, TID, SID order by Timestamp) as rnk
    FROM vw_combined
    WHERE OID <> ' '
    AND OID <> '000000000'
    AND OID <> ''
    AND OID <> '555555555'
    AND OID <> '999999999'
    AND OID <> '123456789'
) A
WHERE A.rnk = 1''')
vw_ranked.createOrReplaceTempView("vw_ranked")
vw_ranked.count()

# COMMAND ----------

# MAGIC %md
# MAGIC encryp conversion

# COMMAND ----------

# MAGIC %md
# MAGIC Check for any unknown SID/TID

# COMMAND ----------

distinct_unknown_sids = spark.sql('''SELECT SID 
    FROM vw_ranked
    WHERE NOT EXISTS (SELECT 1 FROM bronze.systems WHERE vw_ranked.SID = bronze.systems.SID)''')

unknown_sids = ', '.join(row.SID for row in distinct_unknown_sids.collect())
if unknown_sids != '':
    raise Exception(f"Unknown SIDs ({unknown_sids}) in the dataset. Please check the data and fix the issue.")
display(unknown_sids)

# COMMAND ----------

distinct_unknown_tids = spark.sql('''SELECT TID 
    FROM vw_ranked 
    WHERE NOT EXISTS (SELECT 1 FROM bronze.id_types WHERE vw_ranked.TID = bronze.id_types.TID)''')

unknown_tids = ', '.join(row.TID for row in distinct_unknown_tids.collect())
if unknown_tids != '':
    raise Exception(f"Unknown TIDs ({unknown_tids}) in the dataset. Please check the data and fix the issue.")
display(unknown_tids)

# COMMAND ----------

# MAGIC %md
# MAGIC - Records with no existing GID for ID/TID combination. need to generate UID.
# MAGIC - Take distinct to populate one guid per OID/TID/SID combination

# COMMAND ----------

# Create a data frame with records without a match between ww_ranked and customer_idmap
vw_nogid = spark.sql('''SELECT *
FROM vw_ranked 
WHERE NOT EXISTS (SELECT 1 FROM silver.customer_idmap
    WHERE vw_ranked.TID = customer_idmap.TID
    AND vw_ranked.SID = customer_idmap.SID
    AND vw_ranked.OID = customer_idmap.OID
    )''')
vw_nogid.createOrReplaceTempView("vw_nogid")
vw_nogid.count()

# COMMAND ----------

dedup_oid_tid = spark.sql(''' select oid from vw_nogid group by 1''')
dedup_oid_tid.createOrReplaceTempView("dedup_oid_tid")
dedup_oid_tid.count()

# COMMAND ----------

# MAGIC %md
# MAGIC create GID for each OID/TID combination  Problem 1 
# MAGIC This was changed to create a unique GID for each OID only

# COMMAND ----------

vw_gid_oid_tid = spark.sql('''Select 
                           UPPER(uuid()) as GID, OID 
                           FROM dedup_oid_tid group by 1, 2 ''')
vw_gid_oid_tid.createOrReplaceTempView("vw_gid_oid_tid")
vw_gid_oid_tid.count()

# COMMAND ----------

# Do not include Name TID for now
vw_records_no_gid_assigned_gid = spark.sql('''
SELECT b.GID, b.OID, a.TID, a.SID, Timestamp
FROM vw_nogid a 
LEFT JOIN vw_gid_oid_tid b
on a.OID = b.OID
WHERE a.TID <> 'Name'
''')
vw_records_no_gid_assigned_gid.createOrReplaceTempView("vw_records_no_gid_assigned_gid")
vw_records_no_gid_assigned_gid.count()

# COMMAND ----------

# MAGIC %md
# MAGIC -Insert Records with no GID into the customer_idmap

# COMMAND ----------

# MAGIC %md 
# MAGIC - When a new record is ingested for potential assignment of a new gid - Check using the OID, TID to determine whether the GID exists for this combination before assigning a new GID. And be able to detect that than existing GID is now appearing in a new SID. 
# MAGIC
# MAGIC

# COMMAND ----------

# DBTITLE 1,only for not matched
spark.sql(
    f""" MERGE INTO silver.customer_idmap AS TARGET
USING (
    select
    coalesce(b.gid,a.gid) as gid,
    a.oid,
    a.tid,
    a.sid,
    a.TIMESTAMP 
from vw_records_no_gid_assigned_gid a
left join (select gid,oid from silver.customer_idmap group by 1,2) b
on a.OID = b.OID
-- and a.TID = b.TID
) AS SOURCE

ON TARGET.oid = SOURCE.oid AND TARGET.tid = SOURCE.tid AND TARGET.sid = SOURCE.sid
WHEN NOT MATCHED THEN INSERT (gid, oid, tid, sid,dt_lastupdate,CurrentRecord)
VALUES (SOURCE.gid, SOURCE.oid, SOURCE.tid, SOURCE.sid,SOURCE.TIMESTAMP,'Yes')
"""
)

# COMMAND ----------

# MAGIC %sql
# MAGIC select OID,TID,SID,count(1) from silver.customer_idmap group by OID,TID,SID having count(1)>1

# COMMAND ----------


