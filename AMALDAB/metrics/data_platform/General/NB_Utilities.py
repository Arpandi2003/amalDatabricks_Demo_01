# Databricks notebook source
##Importing Packages
##Importing Packages
from pyspark.sql.functions import *
import datetime
from datetime import timedelta
from delta.tables import *
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import pytz
from datetime import datetime
from pyspark.sql.functions import udf
from pyspark.sql.types import *

# COMMAND ----------

catalog_name = dbutils.widgets.get('catalog_name')

# COMMAND ----------

spark.sql(f"""use catalog {catalog_name}""")

# COMMAND ----------

##defining user define function GetMetaDateDetails to get the information from the metadata by passing the tableID
def GetMetaDataDetails(TableID):
    ##Assign variable MetaQuery store the select Query to extract the information from the metadata table.   
    MetaQuery="Select SourceTableName,SourceDBName,SourceSchema, StagePath, RawPath, CuratedPath, DWHSchemaName, DWHTableName, ErrorLogPath, SourceSecretName,LastLoaddateColumn,LastLoadDateValue,MergeKey from Metadata.MasterMetadata where TableID='"+TableID+"'"
    ##store the data into the dataframe Result using spark function
    Results= spark.sql(MetaQuery)
    ##Get the required value from to metadata to the specific variable which is store in the Result dataframe by using collect funtion.
    SrcTableName= Results.collect()[0][0]
    SrcDataBaseName=Results.collect()[0][1]
    SrcSchemaName=Results.collect()[0][2]
    StagePath=Results.collect()[0][3]
    RawPath=Results.collect()[0][4]
    CuratedPath=Results.collect()[0][5]
    DSDWHSchemaName=Results.collect()[0][6]
    DSDWHTableName=Results.collect()[0][7]
    ErrorLogPath=Results.collect()[0][8]
    Secret=Results.collect()[0][9]
    LastLoadDateColumn=Results.collect()[0][10]
    LastLoadDateValue=Results.collect()[0][11]
    MergeKey=Results.collect()[0][12]
    MetadataDictionary = {}
    ## This loop is dynamically populating a dictionary called MetadataDictionary with key-value pairs and evaluvate the values.
    for variable in ["SrcTableName", "SrcDataBaseName", "SrcSchemaName", "StagePath", "RawPath", "CuratedPath","DSDWHSchemaName","DSDWHTableName","ErrorLogPath","Secret","LastLoadDateColumn","LastLoadDateValue","MergeKey"]:
        MetadataDictionary[variable] = eval(variable)
    return MetadataDictionary

# COMMAND ----------

def AutoSkipper(TableID,Zone):
    
    #Importing packages
    from datetime import datetime
    
    #query to get the pipelineendtime,pipelinerunstatus and Isactive values against the table ID
    AutoSkipperQuery="Select cast(PipelineEndTime as varchar(100)) as PipelineEndTime,pipelineRunStatus,IsActive,Zone from Metadata.MasterMetadata where TableID =" + "'" + TableID + "'"
    AutoSkipper= spark.sql(AutoSkipperQuery)
      
    #Collecting the values and storing in respective variables
    PipelineEndTime=AutoSkipper.collect()[0][0]
    PipelineRunStatus=AutoSkipper.collect()[0][1]
    IsActive=AutoSkipper.collect()[0][2]
    loadZone=AutoSkipper.collect()[0][3]

    #Time to string conversions
    Currentdate= datetime.now(pytz.timezone('US/Central')) 
    interval = timedelta(days=0, hours=0, minutes=30)
    PastDateTime = Currentdate - interval
    PastDateTime = str(PastDateTime)[0:23]
    PipelineEndTime=str(PipelineEndTime)
    # [0:10]
    Currentdate=str(Currentdate)[0:23]
    
    #Comparison and Autoskipper value
    if (PipelineEndTime>=Currentdate) and (PipelineEndTime<=PastDateTime) and (PipelineRunStatus=='Succeeded') and (IsActive==1) and (loadZone==Zone):
        AutoSkipper=1
    else:
        AutoSkipper=0
    
    return AutoSkipper
 

# COMMAND ----------

##Define a User-define function to update the lastloaddate column in the Metadata table for the tableID.
def UpdateLastLoadDate (TableID,DF_MaxDateValue):
    TableID = TableID
    ##Declare variable UpdateQuery,Store the update query in the variable.
    UpdateQuery = "Update Metadata.MasterMetadata set LastLoadDateValue = '{0}' where TableID = '{1}' "
    ##Declare LastLoadDate variable and get the maxDate from the data frame DF_MaxDateValue and store in it.
    LastLoadDate = DF_MaxDateValue.select(col('Max_Date')).collect()[0][0]
    ##Replaces the placeholders in the UpdateQuery string with the values of LastLoadDate and TableID.
    UpdateQuery = UpdateQuery.format(LastLoadDate,TableID)
    ##Using if loop to avoid the concurrency issue.
    if LastLoadDate != None:
        for i in range(1,10):
            try:
                spark.sql(UpdateQuery)
            except Exception as e:
                continue 
            break

# COMMAND ----------

##Define user-define function to update the pipelineEndTime and PipelineRunStatus for the TableID in the Metadata table.
def UpdatePipelineStartTime (TableID,Zone):
    TableID = TableID
    ##Declare variable "UpdateQuery" to store the update query in the variable.
    UpdateQuery = ''' Update Metadata.MasterMetadata set PipelineStartTime = current_timestamp() where TableID = '{0}' '''
    ##Replaces the placeholders in the UpdateQuery string with the values of Zone and TableID.
    UpdateQuery = UpdateQuery.format(TableID)
    
    #Adding for each to avoid concurrency
    for i in range(1,10):
        try:
            spark.sql(UpdateQuery)
        except Exception as e:
            continue 
        break

# COMMAND ----------

##Define user-define function to update the pipelineEndTime and PipelineRunStatus for the TableID in the Metadata table.
def UpdatePipelineStatusAndTime (TableID,Zone):
    TableID = TableID
    ##Declare variable "UpdateQuery" to store the update query in the variable.
    UpdateQuery = ''' Update Metadata.MasterMetadata set PipelineEndTime = current_timestamp(),PipelineRunStatus = 'Succeeded',Zone='{0}'  where TableID = '{1}' '''
    ##Replaces the placeholders in the UpdateQuery string with the values of Zone and TableID.
    UpdateQuery = UpdateQuery.format(Zone,TableID)
    
    #Adding for each to avoid concurrency
    for i in range(1,10):
        try:
            spark.sql(UpdateQuery)
        except Exception as e:
            continue 
        break

# COMMAND ----------

##Define a user-define function to update the failed status in the pipelineRunstatus column for the tableID in the Metadata table.
def UpdateFailedStatus(TableID,Zone):
    TableID = TableID
    ##Declare variable "UpdateQuery" to store the update query in the variable.
    UpdateQuery = '''Update Metadata.MasterMetadata set PipelineEndTime = current_timestamp() ,PipelineRunStatus = 'Failed',Zone='{0}'  where TableID = '{1}' '''
    ##Replaces the placeholders in the UpdateQuery string with the values of Zone and TableID.
    UpdateQuery = UpdateQuery.format(Zone,TableID)
    
    #Adding for each to avoid concurrency
    for i in range(1,10):
        try:
            spark.sql(UpdateQuery)
        except Exception as e:
            continue 
        break

# COMMAND ----------

##Define a user-define function that filters data in a data frame using the "LastLoadDateColumn" and "LastLoadDateValue" columns
def FilterDataframe(Df,TableID):
    TableID = TableID
    ##Declare variable "Query" to store the select query in the variable.
    Query = "Select LastLoadDateColumn, LastLoadDateValue from Metadata.MasterMetadata where TableID = '{0}'"
    ##Replaces the placeholders in the Query variable with the values of TableID.
    Query = Query.format(TableID)
    ##Store the data into a data frame DFMaxDate using spark.sql function.
    DFMaxDate = spark.sql(Query)
    ##Get the maxdate column from the Data frame using collect.
    MaxDateColumn = DFMaxDate.collect()[0][0]
    ##Get the maxdate value from the Data frame using collect.
    MaxDate = DFMaxDate.collect()[0][1]
    ##Format the value as string.
    MaxDate = str(MaxDate)
    ##Create view "Filterdata" for the data frame Df.
    Df.createOrReplaceTempView('Filterdata')
    ##Declare the variable "SelectQuery" store the select query in the variable
    SelectQuery = "Select * from Filterdata where "+MaxDateColumn+" >= '"+MaxDate+"'"
    ##Store the data into a data frame filtered_Df using spark.sql function and return the dataframe.
    Filtered_Df = spark.sql(SelectQuery)
    return Filtered_Df

# COMMAND ----------

##Define a user-define function to check the tables in the list are execute successfully or not.
def DependencyChecker(TablesList):
    TablesList = TablesList
    ##Using for loop to iterate the tableID's in the tablelist.
    for TableID in TablesList:
        ##Store the select query to extract the PipelineEndTime,PipelineRunStatus, from the metadata table using filter condtion to filter the data based on the tableID.
        AutoSkipperQuery="Select cast(PipelineEndTime as varchar(100)) as PipelineEndTime,pipelineRunStatus,IsActive,Zone from Metadata.MasterMetadata where TableID =" + "'" + TableID + "'"
        AutoSkipper= spark.sql(AutoSkipperQuery)

        #Collecting the values and storing in respective variables
        PipelineEndTime=AutoSkipper.collect()[0][0]
        PipelineRunStatus=AutoSkipper.collect()[0][1]
        IsActive=AutoSkipper.collect()[0][2]

        #Time to string conversions
        Currentdate= datetime.now(pytz.timezone('US/Central')).strftime('%Y-%m-%d')
        PipelineEndTime=str(PipelineEndTime)[0:10]
        Currentdate=str(Currentdate)

        #Comparison and Autoskipper value
        if (PipelineEndTime==Currentdate) and (PipelineRunStatus=='Succeeded') and (IsActive==1):
            AutoSkipper =1
        else:
            AutoSkipper =0
            break

    return AutoSkipper

# COMMAND ----------

def UpdateLogStatus(Job_ID,Table_ID,Notebook_ID,Table_Name,Zone,Run_Status,Error_Statement,Error_Type,Is_Failed,Instance):
    Insert_Query = f"insert into metadata.Error_Log_Table (Job_ID,Table_ID,Notebook_ID,Table_Name, Zone, Run_Status,Error_Statement,Error_Type,Is_Failed, Instance, Created_At) Values('{Job_ID}','{Table_ID}','{Notebook_ID}','{Table_Name}','{Zone}','{Run_Status}','{Error_Statement}','{Error_Type}','{Is_Failed}','{Instance}',current_timestamp())"

    for i in range(1,10):
        try:
            spark.sql(Insert_Query)
        except Exception as e:
            continue 
        break
    return Insert_Query
