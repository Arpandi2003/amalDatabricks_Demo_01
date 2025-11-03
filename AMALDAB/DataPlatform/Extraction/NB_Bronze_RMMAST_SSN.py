# Databricks notebook source
# MAGIC %md 
# MAGIC
# MAGIC # The Following Cell Pulls the unencrupted ssn/ein from Bank 13 Horizon 

# COMMAND ----------

# MAGIC %run "../General/NB_AMAL_Utilities"

# COMMAND ----------

# MAGIC %run "../General/NB_AMAL_Logger"

# COMMAND ----------

# MAGIC %run "../General/NB_Configuration"

# COMMAND ----------

spark.sql(f"use catalog {catalog}")

# COMMAND ----------

jdbc_hostname = GetCredsKeyVault(
    scope, "h360-hostname"
)
jdbc_port = GetCredsKeyVault(scope, "h360-port")
jdbc_username = GetCredsKeyVault(
    scope, "h360-username"
)
jdbc_password = GetCredsKeyVault(
    scope, "h360-password"
)

jdbcDatabase = "Bank13"
jdbcUrl = f"jdbc:sqlserver://{jdbc_hostname}:{jdbc_port};database={jdbcDatabase}"

connectionProperties = {
    "user": jdbc_username,
    "password": jdbc_password,
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
    "trustServerCertificate": "true",
}

query = """SELECT skey,
    Token1 AS EncryptedSSN
	,CASE WHEN "Token1" IS NULL THEN ''
		WHEN CHARINDEX("rl", '''CAMID(":Report Administrators")'',''CAMID(":Authors")'',''CAMID(":AB Cognos Roles:AB MIS Authors")'',''CAMID(":Secure_PCI_")'',''CAMID(":Metrics Authors")'',''CAMID(":360 BI PCI Secure")'',''CAMID(":360 BI SSN Secure")'',''CAMID(":360 BI UID Secure")'',''CAMID(":Analytics Explorers")''',
		0) > 0 THEN ISNULL(CAST(DecryptByKeyAutoCert(cert_id("crt"), NULL, "EDValues1") AS VARCHAR(20)), '')
		ELSE "nlr"
	END AS SSN
	FROM dbo.ods_SINPIDTA_UII_XREF
	CROSS JOIN dbo.lkup_UII_RoleSSN 
	WHERE TableName = 'RMMAST' 
	AND DataType = "dttp" """

df = spark.read.format("jdbc").options(
    url=jdbcUrl,
    dbtable=f"({query}) AS tmp",
    **connectionProperties
).option("allowLeadingWildcard", "true").load()

df.createOrReplaceTempView("ssn")

SSN_VW = f"""
            CREATE OR REPLACE TEMP VIEW SSN_VW AS
            SELECT *,                         
            Current_user AS DW_Created_By,                        
            current_timestamp() AS DW_Created_Date,
            Current_user AS DW_Modified_By,
            current_timestamp() AS DW_Modified_Date
            FROM ssn
        """
spark.sql(SSN_VW)

# COMMAND ----------

# MAGIC %sql 
# MAGIC
# MAGIC create or replace table Bronze.RmMast_SSN as(
# MAGIC select * from ssn_vw) 
