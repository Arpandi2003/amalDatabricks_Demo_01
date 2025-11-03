# Databricks notebook source
# MAGIC %md
# MAGIC **Notebook Name:** NB_Silver_CRM_Landing_Missing_Info_V1 <br>
# MAGIC **Created By:** Pandi Anbu <br>
# MAGIC **Created Date:** 08/26/25<br>
# MAGIC **Modified By:** Pandi Anbu<br>
# MAGIC **Modified Date** 08/26/25<br>
# MAGIC **Modification** : Updated framework to handle both H360 and TDW

# COMMAND ----------

# MAGIC %md
# MAGIC ### Import Package 

# COMMAND ----------

# DBTITLE 1,Import Package
from pyspark.sql.functions import lit, current_timestamp

# COMMAND ----------

# DBTITLE 1,Configuration
# MAGIC %run "../General/NB_Configuration"

# COMMAND ----------

# DBTITLE 1,declare catalog
spark.sql(f"use catalog {catalog}")

# COMMAND ----------

# DBTITLE 1,Create a static Timestamp
current_time = spark.sql("SELECT current_timestamp()").collect()[0][0]
modified_timestamp = current_time.strftime('%Y-%m-%d %H:%M:%S')
modified_timestamp

# COMMAND ----------

# DBTITLE 1,Get the List of Tables and it's constrains
# Mapping dictionary format:
# Silver Table : (landing_table, Silver_Merge_Key (list), landing_merge_keys (list), extra_condition, source_query, Mergekey/select query for FinancialAccount, Cooreponding Silver TableName)

table_merge_mapping = {
    "crm.Financial_account": (
        "landing.Financialaccount",
        ["Financialaccountnumber"], 
        ["Financial_Account_Key__c"],
        "a.status != 'PURGED' ",
        None,
        None,
        None
    ),
    "crm.financial_account_party": (
        "landing.Financialaccountparty",
        ["FAAcct_NumberSF_Custkey_Key","RoleID"],
        ["Financial_Account_Party_Key__c","Role"], 
        None,
        None,
        ['Financial_AccountNumber_skey'],
        None
    ),
    "crm.account": (
        "landing.account",
        ["SF_Cust_Key"],
        ["Customer_Key__c"],
        None,
        None,
        [f'''select distinct fap.SF_CustKey_fkey as SF_CustKey_fkey
            from silver.financial_account fa
            INNER JOIN silver.financial_account_party fap
            ON fa.financialaccountnumber = fap.Financial_AccountNumber_skey 
            AND UPPER(fap.CurrentRecord) = 'YES'
            WHERE UPPER(fa.CurrentRecord) = 'YES' 
            AND fa.status != 'PURGED' 
            AND fa.dw_modified_date != date('{modified_timestamp}')
            group by fap.SF_CustKey_fkey ''','SF_CustKey_fkey'],
        'Customer_Master'
    ),
    "crm.financial_account_balance": (
        "landing.financialaccountbalance",
        ["uid","type"],
        ["Financial_Account_Balance_Key__c","Type"],
        None,
        None,
        ['FinancialAccountId'],
        None
    ),
    "crm.financial_account_fee": (
        "landing.financialaccountfee",
        ["UID"],
        ["Financial_Account_Fee_Key__c"],
        None,
        None,
        ['FinancialAccountNumber'],
        None
    ),
    "crm.service_xref": (
        "landing.service__c",
        ["ServiceID"],
        ["Service_Id__c"],
        None,
        None,
        None,
        None
    ),
    "crm.financial_account_service": (
        "landing.financial_account_service__c",
        ["FinancialAccountServiceKey"],
        ["Financial_Account_Service_Key__c"],
        None,
        None,
        ['FinancialAccount'],
        None
    ),
    "crm.branch": (
        "landing.branchunit",
        ["Branch_Code"],
        ["Branch_Id__c"],
        None,
        None,
        None,
        None
    ),
    "crm.product": (
        "landing.product2",
        ["ProductCode"],
        ["ProductCode"],
        None,
        None,
        None,
        None
    ),
    # Example of table using custom source query instead of crm table directly
    "crm.financial_account_address": (
        "landing.financialaccountaddress",
        ["FinancialAccountNumber","Addressline"],
        ["Financial_Account_Key__c","Street"],
        None,
        """
        SELECT fa.Financial_Account_Key__c,faa.street
        FROM landing.financialaccountaddress faa
        INNER JOIN landing.financialaccount fa
        ON faa.FinancialAccountId = fa.Id
        """,
        ['FinancialAccountNumber'],
        None
    )
}

# COMMAND ----------

# DBTITLE 1,Missing Data Validation and updation
def get_required_columns(df, exclude_cols, updated_table, silver_table):
    # Get columns from updated_table
    updated_table_cols = [col.lower() for col in spark.table(updated_table).columns]
    
    # Get columns from silver_table
    silver_table_cols = [col.lower() for col in spark.table(silver_table).columns]
    
    # Convert exclude columns to lowercase set for comparison
    exclude_set = {col.lower() for col in exclude_cols}
    
    # Find common columns between updated_table and silver_table
    common_cols = set(updated_table_cols).intersection(silver_table_cols)
    
    # Return columns from updated_table that are not in the exclude set and are common
    return [col for col in updated_table_cols if col not in exclude_set and col in common_cols]

def perform_merge_for_table(
    silver_table: str,
    landing_table: str,
    silver_keys: list,
    landing_keys: list,
    modified_timestamp: str,
    user_name: str = "Inserting Duplicated Record for Salesforce",
    extra_condition: str = None,
    source_query: str = None,
    fa_key: list = None,
    RenameTable: str = None,
):
    # Step 2: Construct join conditions for the query
    join_conditions = " AND ".join(
        [f"a.{silver_key} = b.{landing_key}" for silver_key, landing_key in zip(silver_keys, landing_keys)]
    )
    # Step 3: Define base condition for filtering records
    base_condition = (
        f"a.currentrecord = 'Yes' AND b.{landing_keys[0]} IS NULL "
        f"AND date(dw_modified_date) != date('{modified_timestamp}')"
    )
    # Step 4: Combine base condition with any extra condition -- having this so that we can add few adon condition on top of the source(CRM) data like getting only the status = 'closed' and closeddate > '2023-01-01'

    full_condition = f"({base_condition}) AND ({extra_condition})" if extra_condition else base_condition

    # Step 5: Determine source of data for the query
    if source_query:
        print(f"Using custom source query for {silver_table}")
        source_df = spark.sql(source_query)
        source_df.createOrReplaceTempView("vw_source")
        query = f"""
            SELECT a.*
            FROM {silver_table} a
            LEFT JOIN vw_source b
            ON {join_conditions}
            WHERE {full_condition}
        """
    else:
        print(f"Using silver table {silver_table} directly")
        query = f"""
            SELECT a.*
            FROM {silver_table} a
            LEFT JOIN {landing_table} b
            ON {join_conditions}
            WHERE {full_condition}
        """

    # Step 6: Execute query to get missing records
    print(f"Query to get missing records:\n{query}")
    df_to_update = spark.sql(query)
    df_to_update.createOrReplaceTempView("vw_to_update")
    print(f"Before comparison {silver_table}: {df_to_update.count()}")
    df_to_update.display()

    # Step 7: Additional filtering based on fa_key if provided
    if fa_key is not None:
        if 'select' in fa_key[0]:
            print(f"""
                      select a.* from vw_to_update a left join 
                      (
                          {fa_key[0]}
                      ) as b on b.{fa_key[1]}=a.sf_cust_key 
                      where b.{fa_key[1]} is not null  
                       """)
            df_to_update = spark.sql(f"""
                      select a.* from vw_to_update a left join 
                      (
                          {fa_key[0]}
                      ) as b on b.{fa_key[1]}=a.sf_cust_key 
                      where b.{fa_key[1]} is not null  
                       """)
        else:
            print(f"""
                SELECT a.* 
                FROM vw_to_update a 
                LEFT JOIN CRM.Financial_account b 
                ON a.{fa_key[0]} = b.FinancialAccountNumber 
                WHERE b.FinancialAccountNumber IS NOT NULL 
                AND b.currentrecord='Yes' 
                AND a.currentrecord='Yes' 
                AND status !='PURGED'  
                AND date(a.dw_modified_date) != date('{modified_timestamp}')""")
            
            df_to_update = spark.sql(f"""
                SELECT a.* 
                FROM vw_to_update a 
                LEFT JOIN CRM.Financial_account b 
                ON a.{fa_key[0]} = b.FinancialAccountNumber 
                WHERE b.FinancialAccountNumber IS NOT NULL 
                AND b.currentrecord='Yes' 
                AND a.currentrecord='Yes' 
                AND status !='PURGED'  
                AND date(a.dw_modified_date) != date('{modified_timestamp}')
            """)
        df_to_update.display()
        df_to_update.createOrReplaceTempView("vw_to_update")
        print(f"Records after comparison with Financial_account: {df_to_update.count()}")

    # Step 8: Count missing records
    missing_count = df_to_update.count()
    print(f"Missing records in {silver_table}: {missing_count}")

    # Step 9: Determine columns to exclude from the update
    exclude_columns = [
        'start_date', 'end_date', 'dw_modified_date', 'dw_modified_by',
        'dw_created_date', 'dw_created_by', 'currentrecord'
    ]
    updated_table = silver_table.replace("crm","silver")
    if RenameTable is not None:
        updated_table = f"silver.{RenameTable}"
    required_columns = get_required_columns(df_to_update, exclude_columns,updated_table, silver_table)
    cols_select = ", ".join(required_columns)

    # Step 10: Perform merge update on the silver table
    merge_update_sql = f"""
        MERGE INTO {updated_table} AS t
        USING vw_to_update AS s
        ON {" AND ".join([f"t.{silver_key} = s.{silver_key}" for silver_key in silver_keys])} 
        AND t.currentrecord = 'Yes'
        WHEN MATCHED THEN UPDATE SET
            t.dw_modified_by = 'Archiving Record to Create Duplicate',
            t.end_date = '{modified_timestamp}',
            t.currentrecord = 'No'
    """
    spark.sql(merge_update_sql)
    updated_count = spark.sql(f"SELECT COUNT(*) FROM {updated_table} WHERE end_date = '{modified_timestamp}'").collect()[0][0]
    print(f"Updated records in {updated_table}: {updated_count}")

    # Step 11: Prepare data for insertion
    df_insert_source = spark.sql(f"""
        SELECT {cols_select}
        FROM {updated_table}
        WHERE end_date = '{modified_timestamp}'
    """)
    df_insert = (
        df_insert_source
        .withColumn("dw_created_date", current_timestamp())
        .withColumn("dw_created_by", lit(user_name))
        .withColumn("dw_modified_date", current_timestamp())
        .withColumn("dw_modified_by", lit(user_name))
        .withColumn("start_date", current_timestamp())
        .withColumn("currentrecord", lit("Yes"))
        .withColumn("end_date", lit(None).cast("timestamp"))
    )
    df_insert.createOrReplaceTempView("vw_to_insert")

    # Step 12: Perform merge insert on the silver table
    merge_insert_sql = f"""
        MERGE INTO {updated_table} AS t
        USING vw_to_insert AS s
        ON {" AND ".join([f"t.{silver_key} = s.{silver_key}" for silver_key in silver_keys])} 
        AND t.currentrecord = 'Yes'
        WHEN NOT MATCHED THEN INSERT *
    """
    spark.sql(merge_insert_sql)
    inserted_count = df_insert.count()
    print(f"Inserted records in {silver_table}: {inserted_count}")

def run_framework_for_all_tables(mapping, modified_timestamp, user_name="Inserting Duplicated Record for Salesforce"):
    # Iterate over each table in the mapping and perform merge
    for silver_table, values in mapping.items():
        landing_table, silver_keys, landing_keys, extra_condition, source_query, fa_key, RenameTable = values
        print(f"Running merge for silver table {silver_table} against landing table {landing_table}")
        perform_merge_for_table(
            silver_table=silver_table,
            landing_table=landing_table,
            silver_keys=silver_keys,
            landing_keys=landing_keys,
            modified_timestamp=modified_timestamp,
            user_name=user_name,
            extra_condition=extra_condition,
            source_query=source_query,
            fa_key=fa_key,
            RenameTable=RenameTable,
        )

run_framework_for_all_tables(table_merge_mapping, modified_timestamp)
