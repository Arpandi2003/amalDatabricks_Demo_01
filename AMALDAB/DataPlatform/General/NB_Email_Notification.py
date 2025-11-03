# Databricks notebook source
# MAGIC %pip install openpyxl
# MAGIC dbutils.library.restartPython

# COMMAND ----------

# MAGIC %pip install certifi
# MAGIC dbutils.library.restartPython

# COMMAND ----------

# MAGIC %run "./NB_Configuration"

# COMMAND ----------

import requests
import json
from pyspark.sql.functions import col
import certifi
import base64

# COMMAND ----------

spark.sql(f"use catalog {catalog}")

# COMMAND ----------

CLIENT_ID = dbutils.secrets.get(scope, key='email-client-id')
TENANT_ID = dbutils.secrets.get(scope, key='email-tenant-id')
CLIENT_SECRET = dbutils.secrets.get(scope, key='email-client-secret')
user_email = 'PandiAnbu@AmalgamatedBank.com'

# COMMAND ----------

send_mail_url = f'https://graph.microsoft.com/v1.0/users/{user_email}/sendMail'
send_mail_url

# COMMAND ----------

FROM_EMAIL = "PandiAnbu@AmalgamatedBank.com"

# Hardcoded email parameters
TO_EMAIL = "naveenaperiyasamy@amalgamatedbank.com"  # Single recipient
CC_EMAILS = ["PandiAnbu@AmalgamatedBank.com","DavidFiery@AmalgamatedBank.com","saraiaccheo@amalgamatedbank.com",'dsamonte@amalgamatedbank.com',"karanvijayakumar@amalgamatedbank.com"]  # List of CC emails
EMAIL_SUBJECT = "Daily Table Status Report"

# COMMAND ----------

# Get access token for Microsoft Graph API
def get_access_token():
    url = f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/v2.0/token"
    headers = {
        "Content-Type": "application/x-www-form-urlencoded"
    }
    body = {
        "client_id": CLIENT_ID,
        "scope": "https://graph.microsoft.com/.default",
        "client_secret": CLIENT_SECRET,
        "grant_type": "client_credentials"
    }
   
    response = requests.post(url, headers=headers, data=body)
    response.raise_for_status()
    return response.json()["access_token"]

# COMMAND ----------

# Convert Spark DataFrame to HTML table
def df_to_html(df):
    # Convert Spark DataFrame to Pandas for easier HTML conversion
    pdf = df.toPandas()
    return pdf.to_html(index=False, border=1, justify="left")

# COMMAND ----------

def send_email_with_table(df, verify):
    access_token = get_access_token()
   
    if df.count() == 0:
        html_body = """
        <html>
        <head>
        <style>
            p {{
                font-size: 16px;
                font-family: Arial, sans-serif;
            }}
            h2 {{
                color: #4CAF50;
                font-family: Arial, sans-serif;
            }}
        </style>
        </head>
        <body>
        <p>Hey Team,<br><br> Great day to start! There are no table failures in the datawarehouse today.<br> I Have attached the metadata details for reference.</p>
        <p>Best regards,<br>Team zeb</p>
        </body>
        </html>
        """
        df_succeeded = spark.sql(
            """
            select * from config.metadata where lower(pipelinerunstatus) = 'succeeded' and lower(DWHSchemaName)='bronze'
            """
        )  
        pandas_df = df_succeeded.toPandas()

        df_regression_succeeded = spark.sql(
            """
                select * from config.regression_testresults where current_processed_date in (select max(current_processed_date) from config.regression_testresults)
            """
        )  
        pandas_regression_df = df_regression_succeeded.toPandas()

        # Function to create Excel file from Pandas DataFrame
        def create_excel_from_dataframe(df_succeeded, file_name):
            df_succeeded.to_excel(file_name, index=False)

        # Create Excel file
        create_excel_from_dataframe(pandas_df, 'MetadataInfo.xlsx')
        create_excel_from_dataframe(pandas_regression_df, 'RegressionTestResults.xlsx')

        print("Excel file created successfully.")

    else:
        html_body = f"""
        <html>
        <head>
        <style>
            table {{
                border-collapse: collapse;
                width: 100%;
                font-family: Arial, sans-serif;
            }}
            th, td {{
                border: 1px solid #ddd;
                padding: 8px;
                text-align: left;
            }}
            th {{
                background-color: #FF0000;
                color: white;
            }}
            tr:nth-child(even) {{
                background-color: #f9f9f9;
            }}
            h2 {{
                color: #FF0000;
                font-family: Arial, sans-serif;
            }}
            p {{
                font-size: 16px;
                font-family: Arial, sans-serif;
            }}
        </style>
        </head>
        <body>
        <h2>Daily Table Status Report</h2>
        <p>Please find below the current status of Failed tables:</p>
        </body>
        </html>"""

        html_segments = []  # List to hold HTML segments for each source system

        source_systems = ['TDW', 'Osaic', 'DMI', 'Axiom','Marketing','H360BI','H360Bi','Horizon DB2','H360']
       
        for source in source_systems:
            if df.filter(df.SourceSystem == source).count() > 0:
                sql_df = spark.sql(
                    f"""
                    select * from config.Email_Trigger where lower(pipelinerunstatus) = 'failed' and sourcesystem='{source}' and date(processed_Date)=current_date()
                    """
                )
                html_segment = f"""
                <html>
                <head>
                <body>
                <h3>{source}</h3>
                {df_to_html(sql_df)}      
                </body>
                </html>
                """
                html_segments.append(html_segment)

        html_closure = f"""
        </body>
        </html>
        <p>Best regards,<br>Team zeb</p>
        </body>
        </html>
        """

    if df.count() == 0:
        email_payload = {
            "message": {
                "subject": EMAIL_SUBJECT,
                "body": {
                    "contentType": "HTML",
                    "content": html_body
                },
                "toRecipients": [{"emailAddress": {"address": TO_EMAIL}}],
                "ccRecipients": [{"emailAddress": {"address": email}} for email in CC_EMAILS],
                "from": {
                    "emailAddress": {
                        "address": FROM_EMAIL
                    }
                }
            },
            "saveToSentItems": "true"
        }
    else:
        email_payload = {
            "message": {
                "subject": EMAIL_SUBJECT,
                "body": {
                    "contentType": "HTML",
                    "content": html_body + ''.join(html_segments) + html_closure
                },
                "toRecipients": [{"emailAddress": {"address": TO_EMAIL}}],
                "ccRecipients": [{"emailAddress": {"address": email}} for email in CC_EMAILS],
                "from": {
                    "emailAddress": {
                        "address": FROM_EMAIL
                    }
                }
            },
            "saveToSentItems": "true"
        }

    with open('MetadataInfo.xlsx', 'rb') as file:
        attachment_content = file.read()
    with open('RegressionTestResults.xlsx', 'rb') as file:
        regression_attachment_content = file.read()
   
    email_payload["message"]["attachments"] = [
        {
            "@odata.type": "#microsoft.graph.fileAttachment",
            "name": "MetadataInfo.xlsx",
            "contentBytes": base64.b64encode(attachment_content).decode('utf-8')
        },
        {
            "@odata.type": "#microsoft.graph.fileAttachment",
            "name": "RegressionTestResults.xlsx",
            "contentBytes": base64.b64encode(regression_attachment_content).decode('utf-8')
        }
    ]
    
   
    url = f"https://graph.microsoft.com/v1.0/users/{FROM_EMAIL}/sendMail"
    headers = {
        "Authorization": "Bearer " + access_token,
        "Content-Type": "application/json"
    }
   
    response = requests.post(url, headers=headers, json=email_payload, verify=verify)
    response.raise_for_status()
    return response.status_code


# COMMAND ----------

def send_email_trigger(Message=None,df=None, verify=None):
    access_token = get_access_token()
   
    if df is not None or Message is not None:
        html_body = f"""
        <html>
        <head>
        <style>
            p {{
                font-size: 16px;
                font-family: Arial, sans-serif;
            }}
            h2 {{
                color: #4CAF50;
                font-family: Arial, sans-serif;
            }}
        </style>
        </head>
        <body>
        <p>Hey Team,<br><br>{Message}</p>
        <p>Best regards,<br>Team zeb</p>
        </body>
        </html>
        """
        # df_succeeded = spark.sql(
        #     """
        #     select * from config.metadata where lower(pipelinerunstatus) = 'succeeded' and lower(DWHSchemaName)='bronze'
        #     """
        # )  
        # pandas_df = df_succeeded.toPandas()

        

    if df is not None or Message is not None:
        email_payload = {
            "message": {
                "subject": EMAIL_SUBJECT,
                "body": {
                    "contentType": "HTML",
                    "content": html_body
                },
                "toRecipients": [{"emailAddress": {"address": TO_EMAIL}}],
                "ccRecipients": [{"emailAddress": {"address": email}} for email in CC_EMAILS],
                "from": {
                    "emailAddress": {
                        "address": FROM_EMAIL
                    }
                }
            },
            "saveToSentItems": "true"
        }
    
   
    url = f"https://graph.microsoft.com/v1.0/users/{FROM_EMAIL}/sendMail"
    headers = {
        "Authorization": "Bearer " + access_token,
        "Content-Type": "application/json"
    }
   
    response = requests.post(url, headers=headers, json=email_payload, verify=verify)
    response.raise_for_status()
    return response.status_code


# COMMAND ----------

from pyspark.sql import Row

# Main execution
if __name__ == "__main__":
    # Example DataFrame with table names and statuses
    # Replace this with your actual DataFrame loading logic
    df = spark.sql(
        """
        select * from config.Email_Trigger where lower(pipelinerunstatus) = 'failed' and date(processed_Date)=current_date()
    """
    )

    try:
        status = send_email_with_table(df, verify=False)
        print(f"Email sent successfully with status code: {status}")
    except Exception as e:
        print(f"Failed to send email: {str(e)}")
        raise e
