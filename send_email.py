#In this code, I want to compare the records from my production database (DB) with those in my data warehouse (BQ) 
#and send the missing data to the appropriate stakeholders.
#Additionally, I want to perform actions on different tables concurrently.

import os
import requests
import json
import certifi
from datetime import datetime, tzinfo, timezone,timedelta,date
import threading
import mysql.connector
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
import smtplib
from email import encoders
import ssl
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication


print("Started")

def send_notification(text):
    webhook_url = '[insert slack hook here]'
    slack_data = {'text': text}

    response = requests.post(
        webhook_url, data=json.dumps(slack_data),
        headers={'Content-Type': 'application/json'})

def send_email_with_attachment(to_emails, subject, body, file_path):
    smtp_server = 'smtp.gmail.com'  # Replace with your SMTP server
    smtp_port = 465  # Replace with your SMTP server port e.g 589
    sender_email = 'your email address'  # Replace with your email
    sender_password = 'your password'  # Replace with your email password or create an app specific password

    # Create a secure SSL context
    context = ssl.create_default_context()

    # Create the email message
    msg = MIMEMultipart()
    msg['From'] = sender_email
    msg['To'] = ', '.join(to_emails)
    msg['Subject'] = subject

    # Attach the body text
    msg.attach(MIMEText(body, 'plain'))

    # Attach the file
    with open(file_path, 'rb') as attachment:
        part = MIMEBase('application', 'octet-stream')
        part.set_payload(attachment.read())
        encoders.encode_base64(part)
        part.add_header('Content-Disposition', f'attachment; filename={os.path.basename(file_path)}')
        msg.attach(part)

    try:
        # Send the email
        with smtplib.SMTP_SSL(smtp_server, smtp_port, context=context) as server:
            server.login(sender_email, sender_password)
            server.sendmail(sender_email, to_emails, msg.as_string())
        print("Email sent successfully.")
    except Exception as e:
        print(f"Error: {e}")

     # Delete the file after sending the email
    os.remove(file_path)
    print(f"File {file_path} deleted successfully.")

dataset_id = 'dataset'  
project_id = 'project_name'

current_datetime = datetime.now()
start_of_today = datetime(current_datetime.year, current_datetime.month, current_datetime.day)
start_of_yesterday = start_of_today - timedelta(days=1) # you can set the date to 2, 3 or more days interval depending on your requirements.
start_date = start_of_yesterday
end_date = start_of_today

print("start date:", start_date) 
print("end date:", end_date) 

path = os.getcwd()
os.chdir(path)
os.system('cd {}'.format(path))
os.system('pwd')

credentials = service_account.Credentials.from_service_account_file(
   '[link/to/json/file]') 
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '[link/to/json/file]'
print("Credentials Loaded")

client = bigquery.Client(project=project_id)
print("Client connected")


def getAWSData(host, user, password, db, query, table_name):
    mydb = mysql.connector.connect(
        host=host,
        user=user,
        password=password,
        database=db

    )
    df = pd.read_sql(query, mydb)
    print(f'Successfully fetched data from {table_name}')
    return df

def actionFunc(client, table_name):
    mysql_query = f"SELECT distinct id FROM {table_name} WHERE created_at BETWEEN '{start_date}' AND '{end_date}'"
    print(mysql_query)

    mysql_ids = getAWSData(
       # replace with your connection string
        mysql_query, 
        table_name
    )

    mysql_ids = mysql_ids["id"].tolist()
    print(f"Mysql data fetched from {table_name}. Lenght:",len(mysql_ids))

# Get bigquery ids
    bq_ids = pd.read_gbq(f"""select distinct id from {dataset_id}.{table_name} where created_at between '{start_date}' and '{end_date}' """, project_id=project_id)
    bq_ids = bq_ids["id"].tolist()
    print(f"Bigquery data fetched from {table_name}. Lenght:",len(bq_ids))

# Search for missing ids
    missing_ids = [mysql_id for mysql_id in mysql_ids if mysql_id not in bq_ids]
    print(f"Missing data fetched from {table_name}. Length:", len(missing_ids))

    if len(missing_ids) > 0:
        missing_df[table_name] = pd.DataFrame(missing_ids, columns=["id"])
        print(f"Missing data from {table_name}: {len(missing_ids)} records found")
        send_notification(f"Successfully saved missing records from {table_name} :{len(missing_ids)} records found between '{start_date}' and '{end_date}' ✅ at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    else:
        print(f"no missing records found for {table_name} between '{start_date}' and '{end_date}' ")
        # send_notification(f"no missing records from {table_name} between '{start_date}' and '{end_date}' ✅ at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")


table_names = {  
    'table1',
    'table2',
    'table3',
    'table4',
    'table5',
    "table6",
    'table7' #replace with your table names and add as many tables as you want.

}
# dictionary to store missing records
missing_df = {}

 # Create a list of threads to perform actions on each table concurrently
threads = [
        threading.Thread(
            target=actionFunc,
            args=(client, table_name)
        ) 
        for table_name in table_names
    ]

    # Start all threads
for thread in threads:
    thread.start()

    # Wait for all threads to complete
for thread in threads:
    thread.join()

# Save all missing records to one Excel file
if missing_df:
    with pd.ExcelWriter('missing_data.xlsx') as writer:
        for table_name, df in missing_df.items():
            sheet_name = f'{table_name} between {start_date} and {end_date}'
            sheet_name = sheet_name.replace(':', '-').replace('/', '-').replace('\\', '-').replace('?', '').replace('*', '').replace('[', '').replace(']', '') #replace special characters.
            df.to_excel(writer, sheet_name=sheet_name, index=False)
    print("All missing records saved to 'missing_data.xlsx'")

    # Send the email with the attached Excel file
    to_emails = ['receiver_email1','receiver_email2','receiver_email3']  # Replace with your recipient email addresses
    subject = 'Missing Data'
    body = 'Please find attached the missing data for the following tables.\n\nRegards'
    file_path = 'missing_data.xlsx'
    send_email_with_attachment(to_emails, subject, body, file_path)

print('process ended')
print("LAST RUN AT: {}".format(datetime.now()))