# Databricks notebook source
# MAGIC %md
# MAGIC **Notebook Name:** Utilities <br>
# MAGIC **Created By:** Pandi Anbu <br>
# MAGIC **Created Date:** 12/20/24<br>
# MAGIC **Modified By:** Pandi Anbu<br>
# MAGIC **Modified Date** 12/20/24

# COMMAND ----------

# DBTITLE 1,Import Lib
from datetime import datetime, timedelta
import pytz
from pyspark.sql.functions import col,lit
from pyspark.sql import functions as F
from pyspark.sql import DataFrame

from pyspark.sql.functions import col, lit, current_timestamp
from pyspark.sql.types import IntegerType, DoubleType, StringType
import builtins

# COMMAND ----------

# DBTITLE 1,Configuration'
# MAGIC %run "./NB_Configuration"

# COMMAND ----------

# DBTITLE 1,Logger
# MAGIC %run "./NB_AMAL_Logger"

# COMMAND ----------

# DBTITLE 1,Description
"""
1. GetMetaDataDetails(TableID): Fetches metadata details from the abank.metadata table for a given TableID and returns them as a dictionary.
   
2. AutoSkipper(TableID): Determines whether to auto-skip a pipeline run based on the pipeline's last run status and activity.
   
3. UpdatePipelineStartTime(tableId: int): Updates the pipelineStartTime in the abank.metadata table for a specific TableID.
   
4. UpdatePipelineStatusAndTime(TableId: int, Status: str): Updates the PipelineEndTime and PipelineRunStatus in the abank.metadata table for a specific TableID.
   
5. UpdateLastLoadDate(TableId: int, lastLoadDate: str): Updates the LastLoadDateValue in the abank.metadata table for a specific TableID.
   
6. TruncateLoad(TableId: int, df): Overwrites the data in the destination table specified by the schema and table name extracted from metadata.
   
7. IncrementalLoad(TableId: int, df): Performs incremental loading of data to the target destination using the merge key and date columns.
   
8. SourceDeletion(TableId: int, SourceDF: DataFrame, DWHSchemaName: str, DWHTableName: str): Handles the addition of new rows and soft deletion of rows that no longer exist in the source data.

CurrentRecord=1 then the data has been deleted in the source.
   
9. DataTypeChange(SourceDF, DWHSchemaName, DWHTableName): Ensures that the source DataFrame columns' data types match the destination table's schema.
   
10. RenameColumns(df, ColumnMapping): Renames columns of the DataFrame according to the provided column mapping if not marked as 'NA'. 

11. RenameColumns(df, ColumnMapping): Renames columns of a DataFrame according to a provided column mapping if the mapping is not 'NA'.
   
12. GetTodaysDate(): Returns today's date formatted as 'YYYYMMDD'.

13. GetCredsKeyVault(scope, key): Retrieves a secret from Azure Key Vault using Databricks utilities with the provided scope and key.
"""

# COMMAND ----------

# DBTITLE 1,Declaring Variables
Metadataschema = "config"
Metadatatable = "metadata"

# COMMAND ----------

# DBTITLE 1,Assign Catalog
spark.sql(f"""use catalog {catalog}""")

# COMMAND ----------

# DBTITLE 1,GetMetadata
##defining user define function GetMetaDateDetails to get the information from the metadata by passing the tableID

def GetMetaDataDetails(TableID):
    ##Assign variable MetaQuery store the select Query to extract the information from the metadata table.   
    MetaQuery= f'''Select SourceSystem
                ,SourceSecretName
                ,TableID
                ,SubjectArea
                ,SourceDBName
                ,SourceSchema
                ,SourceTableName
                ,LoadType
                ,IsActive
                ,Frequency
                ,BronzePath
                ,SilverPath
                ,GoldPath
                ,DWHSchemaName
                ,DWHTableName
                ,ErrorLogPath
                ,LastLoadDateColumn
                ,MergeKey
                ,DependencyTableIDs
                ,LastLoadDateValue
                ,PipelineEndTime
                ,PipelineStartTime
                ,PipelineRunStatus
                ,Zone
                ,Endpoint
                ,Verb
                ,Structure
                ,SourceSelectQuery
                ,ColumnMapping
                ,Batch
                ,SourcePath
                ,MergeKeyColumn
                 From {Metadataschema}.{Metadatatable}
                Where TableID = {TableID}
                '''
	
    ##store the data into the dataframe Result using spark function
    Results= spark.sql(MetaQuery)
    ##Get the required value from to metadata to the specific variable which is store in the Result dataframe by using collect funtion.
    SourceSystem= Results.collect()[0][0]
    SourceSecretName=Results.collect()[0][1]
    TableID=Results.collect()[0][2]
    SubjectArea=Results.collect()[0][3]
    SourceDBName=Results.collect()[0][4]
    SourceSchema=Results.collect()[0][5]
    SourceTableName=Results.collect()[0][6]
    LoadType=Results.collect()[0][7]
    IsActive=Results.collect()[0][8]
    Frequency=Results.collect()[0][9]
    BronzePath=Results.collect()[0][10]
    SilverPath=Results.collect()[0][11]
    GoldPath=Results.collect()[0][12]
    DWHSchemaName=Results.collect()[0][13]
    DWHTableName=Results.collect()[0][14]
    ErrorLogPath=Results.collect()[0][15]
    LastLoadDateColumn=Results.collect()[0][16]
    MergeKey=Results.collect()[0][17]
    DependencyTableIDs=Results.collect()[0][18]
    LastLoadDateValue=Results.collect()[0][19]
    PipelineEndTime=Results.collect()[0][20]
    PipelineStartTime=Results.collect()[0][21]
    PipelineRunStatus=Results.collect()[0][22]
    Zone=Results.collect()[0][23]
    Endpoint=Results.collect()[0][24]
    Verb=Results.collect()[0][25]
    Structure=Results.collect()[0][26]
    SourceSelectQuery=Results.collect()[0][27]
    ColumnMapping = Results.collect()[0][28]
    Batch=Results.collect()[0][29]
    SourcePath = Results.collect()[0][30]
    MergeKeyColumn=Results.collect()[0][31]
    MetadataDictionary = {}
    ## This loop is dynamically populating a dictionary called MetadataDictionary with key-value pairs and evaluvate the values.
    for variable in ["SourceSystem","SourceSecretName","TableID","SubjectArea","SourceDBName","SourceSchema","SourceTableName","LoadType","IsActive","Frequency","BronzePath","SilverPath","GoldPath","DWHSchemaName","DWHTableName","ErrorLogPath","LastLoadDateColumn","MergeKey","DependencyTableIDs","LastLoadDateValue","PipelineEndTime","PipelineStartTime","PipelineRunStatus","Zone","Endpoint","Verb","Structure","SourceSelectQuery",
                     'ColumnMapping','Batch','SourcePath',"MergeKeyColumn"]:
        MetadataDictionary[variable] = eval(variable)
    return MetadataDictionary

# COMMAND ----------

# DBTITLE 1,AutoSkipper
def AutoSkipper(TableID):
    #initialize catalog
    # catalog = get_creds_keyvault('databricks-amal','secret-catalogname')

    #spark.sql(f"use catalog {catalog}")

    #query to get the pipelineendtime,pipelinerunstatus and Isactive values against the table ID
    AutoSkipperQuery=f"Select cast(PipelineEndTime as varchar(100)) as PipelineEndTime,pipelineRunStatus,IsActive,Zone from {Metadataschema}.{Metadatatable} where TableID = {TableID}"
    AutoSkipper= spark.sql(AutoSkipperQuery)
      
    #Collecting the values and storing in respective variables
    PipelineEndTime=AutoSkipper.collect()[0][0]
    PipelineRunStatus=AutoSkipper.collect()[0][1] ### Succeeded
    IsActive=AutoSkipper.collect()[0][2] ### 1
    loadZone=AutoSkipper.collect()[0][3] ##bronze

    #Time to string conversions
    Currentdate= datetime.now(pytz.timezone('America/New_York'))
    PreviousDate = Currentdate - timedelta(days=1) 
    PipelineEndTime=str(PipelineEndTime)[0:10]
    PreviousDate=str(PreviousDate)[0:10]
    
    #Comparison and Autoskipper value
    if ( (PipelineEndTime == PreviousDate) or (PipelineEndTime < PreviousDate))  and (PipelineRunStatus=='Succeeded') and (IsActive==1):
        AutoSkipper=1
    else:
        AutoSkipper=0
    
    return AutoSkipper

# COMMAND ----------

# DBTITLE 1,UpdatePipelineStartTime
def UpdatePipelineStartTime(tableId: int) -> None:
    from datetime import datetime
    import pytz
    try:
        query = f"""
            UPDATE {Metadataschema}.{Metadatatable} SET pipelineStartTime ='{datetime.now()}'
            WHERE tableid = {tableId}
        """
        success = False
        for i in range(10):
            try:
                spark.sql(query)
                success = True
                break
            except:
                pass
        return success
    except Exception as e:
        print(e)

# COMMAND ----------

# DBTITLE 1,UpdatePipelineStatusAndTime
def UpdatePipelineStatusAndTime(TableId: int, Status: str) -> None:
    current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    query = f"""
        UPDATE {Metadataschema}.{Metadatatable}
            SET PipelineEndTime = '{current_time}', PipelineRunStatus = '{Status}'
            WHERE TableID = {TableId} 
    """
    success = False
    for i in range(10):
        try:
            spark.sql(query)
            success = True
            break
        except Exception as e:
            print(f"Error: {e}")
            raise e
            # pass
        
    return success

# COMMAND ----------

# DBTITLE 1,UpdateLastLoadDate
def UpdateLastLoadDate(TableId: int, lastLoadDate: str):
    query = f"""
        UPDATE {Metadataschema}.{Metadatatable} SET LastLoadDateValue = '{lastLoadDate}' WHERE TableID = {TableId}
    """	    
    success = False
    for i in range(10):
        try:
            spark.sql(query)
            success = True
            break
        except:
            pass
        
    return success

# COMMAND ----------

Current_user = spark.sql("SELECT Current_user").collect()[0][0]
#Current_user

# COMMAND ----------

# DBTITLE 1,truncate and load
def TruncateLoad(TableId: int, df):
    # Fetch metadata for the specified TableId
    dfSelect = spark.sql(f"""
        SELECT TableId, DWHSchemaName, DWHTableName 
        FROM {Metadataschema}.{Metadatatable} 
        WHERE TableId = {TableId}
    """).collect()
    
    # Check if any records were returned
    if not dfSelect:
        print(f"No records found for TableId: {TableId}")
        return
    
    # Extract schema and table names
    schemaname = dfSelect[0]['DWHSchemaName']
    tablename = dfSelect[0]['DWHTableName']
    
    # Load the existing table into a DataFrame

    if spark.catalog.tableExists(f"{schemaname}.{tablename}"):
            
        existing_df = spark.table(f"{schemaname}.{tablename}")
        
        # Check for the existence of the "DeleteFlag" column
        spark.sql(f"TRUNCATE TABLE {schemaname}.{tablename}")
        df=df.withColumn("CurrentRecord",lit("Yes"))
        df.createOrReplaceTempView("VW_existing_view")
        print("Performing Truncate and Load")
        SourceView = f"""
            SELECT *,
            Current_user AS DW_Created_By,                        
            current_timestamp() AS DW_Created_Date,
            Current_user AS DW_Modified_By,
            current_timestamp() AS DW_Modified_Date
            
            FROM VW_existing_view
        """
        df_updated = spark.sql(SourceView)

        df_updated=DataTypeChange(df_updated,schemaname,tablename)

        try:
            df_updated.write.mode("overwrite").saveAsTable(f"{schemaname}.{tablename}")
        except Exception as e:
            print(f"Error: {e}")
            raise e 
    else:
        # Drop the existing table if "DeleteFlag" does not exist
        spark.sql(f"DROP TABLE IF EXISTS {schemaname}.{tablename}")

        df = df.withColumn("CurrentRecord",lit("Yes"))
        df.createOrReplaceTempView("VW_existing_view")
        SourceView = f"""
            SELECT *,                         
            Current_user AS DW_Created_By,                        
            current_timestamp() AS DW_Created_Date,
            Current_user AS DW_Modified_By,
            current_timestamp() AS DW_Modified_Date
            FROM VW_existing_view
        """
        df_updated = spark.sql(SourceView)

        # df_updated=DataTypeChange(df_updated,schemaname,tablename)
 
        # Write the updated DataFrame back to the table
        df_updated.write.mode("overwrite").saveAsTable(f"{schemaname}.{tablename}")

# COMMAND ----------

# DBTITLE 1,Full Load
def FullLoad(TableId: int, df):

    if df is not None:

        try:
            # Fetch table metadata based on tableid
            res = spark.sql(f"""
                SELECT LoadType, LastLoadDateColumn, LastLoadDateValue, DWHSchemaName, DWHTableName, MergeKey,SourceSelectQuery
                FROM {Metadataschema}.{Metadatatable}
                WHERE TableID = {TableId}
            """)

            # Collect the results from the query
            metadata = res.collect()[0]
            LastLoadColumnName = metadata['LastLoadDateColumn']
            LastLoadDate = metadata['LastLoadDateValue']
            DWHSchemaName = metadata['DWHSchemaName']
            DWHTableName = metadata['DWHTableName']
            MergeKey = metadata['MergeKey']
            ListKey = MergeKey.split(',')
            SelectQuery = metadata['SourceSelectQuery']
            logger.info(f"Performing Full Load for tableid: {TableId} and schema: {DWHSchemaName}.{DWHTableName}.")

            SourceDeletion(TableId, df, DWHSchemaName, DWHTableName)


        except Exception as e:
            # Log error
            # logger.error(f"Error in incremental load for Table ID {TableId}: {str(e)}")
            raise e
    else:
        print(f"No data found for Table ID {TableId}")

# COMMAND ----------

# DBTITLE 1,IncrementalLoad
# def IncrementalLoad(TableId: int, df: DataFrame, schema: str):
def IncrementalLoad(TableId: int, df):

    if df is not None:

        try:
            # Fetch table metadata based on tableid
            res = spark.sql(f"""
                SELECT LoadType, LastLoadDateColumn, LastLoadDateValue, DWHSchemaName, DWHTableName, MergeKey,SourceSelectQuery
                FROM {Metadataschema}.{Metadatatable}
                WHERE TableID = {TableId}
            """)

            # Collect the results from the query
            metadata = res.collect()[0]
            LastLoadColumnName = metadata['LastLoadDateColumn']
            LastLoadDate = metadata['LastLoadDateValue']
            DWHSchemaName = metadata['DWHSchemaName']
            DWHTableName = metadata['DWHTableName']
            MergeKey = metadata['MergeKey']
            ListKey = MergeKey.split(',')
            SelectQuery = metadata['SourceSelectQuery']

            logger.info(f"Performing Incremental load for Table ID: {TableId}")

            # Create initial temp view
            df.createOrReplaceTempView('SourcetoBronze')
            
            # Update temp view
            df.createOrReplaceTempView('SourcetoBronzeSelect')

            column_list = df.columns

            #print("column list")

            Hashkeycolumns = ','.join([f"`{col}`" if any(c in col for c in [' ', '-', '.', '/','#','$',':']) else col for col in column_list if col not in ListKey])

            #print("hashccolumn")

            # Creating MergeHashKey and Audit Columns
            Source = f"""
                SELECT *,
                MD5(CONCAT_WS(',', {','.join(Hashkeycolumns.split(','))})) AS MergeHashKey, 
                MD5(CONCAT_WS(',', {','.join(MergeKey.split(','))})) AS MergeKey,                            
                current_timestamp() AS DW_Created_Date,
                Current_user AS DW_Created_By,
                current_timestamp() AS DW_Modified_Date,
                Current_user AS DW_Modified_By
                FROM SourcetoBronzeSelect
            """
            DfMerge = spark.sql(Source)
            #print("dfmerge")

            # Update temp view with hash key
            DfMerge.createOrReplaceTempView("SourcetoBronzeFinal")

            # Merge Query
            MergeQuery = f"""
                MERGE INTO {DWHSchemaName}.{DWHTableName} AS target
                USING SourcetoBronzeFinal AS source
                ON target.MergeKey = source.MergeKey
                WHEN MATCHED AND target.MergeHashKey != source.MergeHashKey THEN
                    UPDATE SET *
                WHEN NOT MATCHED THEN
                    INSERT *
            """

            # Execute merge
            spark.sql(MergeQuery)

            # Log success
            logger.info(f"Incremental load completed successfully for Table ID: {TableId}")

        except Exception as e:
            # Log error
            # logger.error(f"Error in incremental load for Table ID {TableId}: {str(e)}")
            raise e
    else:
        logger.info(f"No data found for Table ID {TableId}")


# COMMAND ----------

# DBTITLE 1,Source Deleletion
from pyspark.sql import DataFrame
import pyspark.sql.functions as F

def SourceDeletion(TableId: int, df: DataFrame, DWHSchemaName: str, DWHTableName: str):

    if df is not None:

        try:
            # Fetch table metadata based on tableid
            res = spark.sql(f"""
                SELECT LoadType, LastLoadDateColumn, LastLoadDateValue, DWHSchemaName, DWHTableName, MergeKey, SourceSelectQuery
                FROM {Metadataschema}.{Metadatatable}
                WHERE TableID = {TableId}
            """)

            # Collect the results from the query
            metadata = res.collect()[0]
            LastLoadColumnName = metadata['LastLoadDateColumn']
            LastLoadDate = metadata['LastLoadDateValue']
            DWHSchemaName = metadata['DWHSchemaName']
            DWHTableName = metadata['DWHTableName']
            MergeKey = metadata['MergeKey']
            ListKey = MergeKey.split(',')
            SelectQuery = metadata['SourceSelectQuery']

            # Create initial temp view
            df.createOrReplaceTempView('SourcetoBronzeSelect')

            column_list = df.columns

            Hashkeycolumns = ','.join([f"`{col}`" if any(c in col for c in [' ', '-', '.', '/','#','$',':']) else col for col in column_list if col not in ListKey])

            # Creating MergeHashKey and Audit Columns
            Source = f"""
                SELECT *,
                MD5(CONCAT_WS(',', {','.join(Hashkeycolumns.split(','))})) AS MergeHashKey, 
                MD5(CONCAT_WS(',', {','.join(MergeKey.split(','))})) AS MergeKey,                            
                current_timestamp() AS DW_Created_Date,
                Current_user AS DW_Created_By,
                current_timestamp() AS DW_Modified_Date,
                Current_user AS DW_Modified_By
                FROM SourcetoBronzeSelect
            """
            DfMerge = spark.sql(Source)
            # Fetching target data
            TargetDF = spark.sql(f"""SELECT *
                FROM {DWHSchemaName}.{DWHTableName}
            """)

            df = DfMerge.withColumn('CurrentRecord', F.lit("Yes"))
            #display(df)
            df.createOrReplaceTempView("source_view")
            # Add MergeKey to TargetDF
            TargetDF.createOrReplaceTempView("target_view")
    
            # Merge operation
            MergeQueryInit = f"""
                    MERGE INTO {DWHSchemaName}.{DWHTableName} AS target
                    USING source_view AS source
                    ON target.MergeKey = source.MergeKey
                    WHEN MATCHED AND target.MergeHashKey != source.MergeHashKey THEN
                    UPDATE SET *
                    WHEN NOT MATCHED THEN
                    INSERT *
                """

            spark.sql(MergeQueryInit)

            # Deletes Capture
            # Identify rows in the source that aren't in the target (union logic)
            DFSourceNull = spark.sql(f"""
                SELECT s.*, 
                    CASE WHEN s.MergeKey IS NULL THEN 'No' ELSE 'Yes' END AS T_CurrentRecord, t.MergeKey as TMergeKey
                FROM target_view t
                FULL JOIN source_view s
                ON { ' AND '.join([f's.{col} = t.{col}' for col in ListKey]) }
            """)
            # Filter out the 'DeleteFlag' rows for next steps
            DFSourceNull.createOrReplaceTempView("SourcetoInsertUpdate")

            # Merge operation
            MergeQuery = f"""
                MERGE INTO {DWHSchemaName}.{DWHTableName} AS target
                USING SourcetoInsertUpdate AS source
                ON target.MergeKey = source.TMergeKey 
                WHEN MATCHED THEN
                    UPDATE SET target.CurrentRecord = CASE 
                        WHEN source.T_CurrentRecord = 'Yes' THEN 'Yes'
                        ELSE 'Deleted'
                    END
            """
            spark.sql(MergeQuery)

        except Exception as e:
            logger.error(f"Error in SourceDeletion: {str(e)}")

# COMMAND ----------

# DBTITLE 1,Partition-FullLoad
def PartitionFullLoad(TableId: int, df):

    if df is not None:

        try:
            # Fetch table metadata based on tableid
            res = spark.sql(f"""
                SELECT LoadType, LastLoadDateColumn, LastLoadDateValue, DWHSchemaName, DWHTableName, MergeKey,SourceSelectQuery
                FROM {Metadataschema}.{Metadatatable}
                WHERE TableID = {TableId}
            """)

            # Collect the results from the query
            metadata = res.collect()[0]
            LastLoadColumnName = metadata['LastLoadDateColumn']
            LastLoadDate = metadata['LastLoadDateValue']
            DWHSchemaName = metadata['DWHSchemaName']
            DWHTableName = metadata['DWHTableName']
            MergeKey = metadata['MergeKey']
            ListKey = MergeKey.split(',')
            SelectQuery = metadata['SourceSelectQuery']
            logger.info(f"Performing Full Load for tableid: {TableId} and schema: {DWHSchemaName}.{DWHTableName}.")

            # Create initial temp view
            df.createOrReplaceTempView('SourcetoBronzeSelect')

            column_list = df.columns

            Hashkeycolumns = ','.join([f"`{col}`" if any(c in col for c in [' ', '-', '.', '/','#','$',':']) else col for col in column_list if col not in ListKey])

            # Creating MergeHashKey and Audit Columns
            Source = f"""
                SELECT *,
                MD5(CONCAT_WS(',', {','.join(Hashkeycolumns.split(','))})) AS MergeHashKey, 
                MD5(CONCAT_WS(',', {','.join(MergeKey.split(','))})) AS MergeKey,                            
                current_timestamp() AS DW_Created_Date,
                Current_user AS DW_Created_By,
                current_timestamp() AS DW_Modified_Date,
                Current_user AS DW_Modified_By
                FROM SourcetoBronzeSelect
            """
            DfMerge = spark.sql(Source)
            # Fetching target data
            TargetDF = spark.sql(f"""SELECT *
                FROM {DWHSchemaName}.{DWHTableName}
            """)

            DfMerge = DfMerge.withColumn('CurrentRecord', F.lit("Yes"))
            #display(df)
            DfMerge.createOrReplaceTempView("source_view")
            # Add MergeKey to TargetDF
            TargetDF.createOrReplaceTempView("target_view")
    
            # Merge operation
            MergeQueryInit = f"""
                    MERGE INTO {DWHSchemaName}.{DWHTableName} AS target
                    USING source_view AS source
                    ON target.MergeKey = source.MergeKey
                    WHEN MATCHED AND target.MergeHashKey != source.MergeHashKey THEN
                    UPDATE SET *
                    WHEN NOT MATCHED THEN
                    INSERT *
                """

            spark.sql(MergeQueryInit)

            # SourceDeletion(TableId, df, DWHSchemaName, DWHTableName)

        except Exception as e:
            # Log error
            # logger.error(f"Error in incremental load for Table ID {TableId}: {str(e)}")
            raise e
    else:
        print(f"No data found for Table ID {TableId}")

# COMMAND ----------

# DBTITLE 1,Partition-Source Deletion
from pyspark.sql import DataFrame
import pyspark.sql.functions as F

def PartitionSourceDeletion(TableId: int, df: DataFrame, DWHSchemaName: str, DWHTableName: str):

    if df is not None:

        try:
            # Fetch table metadata based on tableid
            res = spark.sql(f"""
                SELECT LoadType, LastLoadDateColumn, LastLoadDateValue, DWHSchemaName, DWHTableName, MergeKey, SourceSelectQuery
                FROM {Metadataschema}.{Metadatatable}
                WHERE TableID = {TableId}
            """)

            # Collect the results from the query
            metadata = res.collect()[0]
            LastLoadColumnName = metadata['LastLoadDateColumn']
            LastLoadDate = metadata['LastLoadDateValue']
            DWHSchemaName = metadata['DWHSchemaName']
            DWHTableName = metadata['DWHTableName']
            MergeKey = metadata['MergeKey']
            ListKey = MergeKey.split(',')
            SelectQuery = metadata['SourceSelectQuery']

            # Create initial temp view
            df.createOrReplaceTempView('SourcetoBronzeSelect')

            column_list = df.columns

            Hashkeycolumns = ','.join([f"`{col}`" if any(c in col for c in [' ', '-', '.', '/','#','$',':']) else col for col in column_list if col not in ListKey])

            # Creating MergeHashKey and Audit Columns
            Source = f"""
                SELECT *,
                MD5(CONCAT_WS(',', {','.join(Hashkeycolumns.split(','))})) AS MergeHashKey, 
                MD5(CONCAT_WS(',', {','.join(MergeKey.split(','))})) AS MergeKey,                            
                current_timestamp() AS DW_Created_Date,
                Current_user AS DW_Created_By,
                current_timestamp() AS DW_Modified_Date,
                Current_user AS DW_Modified_By
                FROM SourcetoBronzeSelect
            """
            DfMerge = spark.sql(Source)
            # Fetching target data
            TargetDF = spark.sql(f"""SELECT *
                FROM {DWHSchemaName}.{DWHTableName}
            """)

            df = DfMerge.withColumn('CurrentRecord', F.lit("Yes"))
            #display(df)
            df.createOrReplaceTempView("source_view")
            # Add MergeKey to TargetDF
            TargetDF.createOrReplaceTempView("target_view")
    

            # Deletes Capture
            # Identify rows in the source that aren't in the target (union logic)
            DFSourceNull = spark.sql(f"""
                SELECT s.*, 
                    CASE WHEN s.MergeKey IS NULL THEN 'No' ELSE 'Yes' END AS T_CurrentRecord, t.MergeKey as TMergeKey
                FROM target_view t
                FULL JOIN source_view s
                ON { ' AND '.join([f's.{col} = t.{col}' for col in ListKey]) }
            """)
            # Filter out the 'DeleteFlag' rows for next steps
            DFSourceNull.createOrReplaceTempView("SourcetoInsertUpdate")

            # Merge operation
            MergeQuery = f"""
                    MERGE INTO {DWHSchemaName}.{DWHTableName} AS target
                    USING SourcetoInsertUpdate AS source
                    ON target.MergeKey = source.TMergeKey AND source.T_CurrentRecord = 'Yes'
                    WHEN MATCHED THEN
                        UPDATE SET CurrentRecord = 'Deleted'
                """
            spark.sql(MergeQuery)

        except Exception as e:
            logger.error(f"Error in SourceDeletion: {str(e)}")

# COMMAND ----------

# DBTITLE 1,DataTypeChange
from pyspark.sql.types import DateType, TimestampType, BooleanType, LongType, FloatType

def DataTypeChange(SourceDF, DWHSchemaName, DWHTableName):
    # Get destination table schema
    DestinationSchema = spark.table(f"{DWHSchemaName}.{DWHTableName}")
    
    # Create a mapping of destination column types
    DestinationColumnTypes = {field.name: field.dataType for field in DestinationSchema.schema.fields}
    
    # Iterate and convert source DataFrame columns
    for column, DestType in DestinationColumnTypes.items():
        if column in SourceDF.columns:
            # Advanced type conversion with error handling
            if isinstance(DestType, IntegerType):
                SourceDF = SourceDF.withColumn(column, 
                    F.when(F.col(column).rlike('^[0-9]+$'), 
                           F.col(column).cast(IntegerType()))
                    .otherwise(None)
                )
            
            elif isinstance(DestType, DoubleType):
                SourceDF = SourceDF.withColumn(column, 
                    F.when(F.col(column).rlike('^[0-9.]+$'), 
                           F.col(column).cast(DoubleType()))
                    .otherwise(None)
                )
            
            elif isinstance(DestType, StringType):
                SourceDF = SourceDF.withColumn(column, 
                    F.trim(F.col(column)).cast(StringType())
                )
            
            elif isinstance(DestType, DateType):
                SourceDF = SourceDF.withColumn(column, 
                    F.to_date(F.col(column), 'MM/dd/yyyy')
                )
            
            elif isinstance(DestType, TimestampType):
                SourceDF = SourceDF.withColumn(column, 
                    F.to_timestamp(F.col(column), 'MM/dd/yyyy HH:mm:ss')
                )
            
            elif isinstance(DestType, BooleanType):
                SourceDF = SourceDF.withColumn(column, 
                    F.when(F.col(column).rlike('^(true|false)$'), 
                           F.col(column).cast(BooleanType()))
                    .otherwise(None)
                )
            
            elif isinstance(DestType, LongType):
                SourceDF = SourceDF.withColumn(column, 
                    F.when(F.col(column).rlike('^[0-9]+$'), 
                           F.col(column).cast(LongType()))
                    .otherwise(None)
                )
            
            elif isinstance(DestType, FloatType):
                SourceDF = SourceDF.withColumn(column, 
                    F.when(F.col(column).rlike('^[0-9.]+$'), 
                           F.col(column).cast(FloatType()))
                    .otherwise(None)
                )
            
            else:
                # Default casting for other types
                SourceDF = SourceDF.withColumn(column, 
                    F.col(column).cast(DestType)
                )
    
    return SourceDF

# COMMAND ----------

# DBTITLE 1,RenameColumns
def RenameColumns(df,ColumnMapping):
    if 'NA' not in ColumnMapping:
        for actual, rename in ColumnMapping.items():
            df = df.withColumnRenamed(actual, rename)
    return df

# COMMAND ----------

# DBTITLE 1,GetTodaysDate

def GetTodaysDate():
    today = datetime.now().strftime('%Y%m%d')
    return today

# COMMAND ----------

# DBTITLE 1,GetCredsKeyVault
def GetCredsKeyVault(scope, key):
    creds = dbutils.secrets.get(scope=scope, key=key)
    return creds
