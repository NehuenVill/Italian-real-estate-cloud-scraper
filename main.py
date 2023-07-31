import json
import google.auth
from googleapiclient.discovery import build
from google.oauth2 import service_account
from google.cloud import bigquery
from os import environ
import smtplib
import ssl
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import requests


environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'C:/Users/nehue/Documents/programas_de_python/Upwork_tasks/italian_real_estate_cloud_scraper/credentials.json'

def upload_to_gs(input:dict):

    sheet_id = None

    with open('keys_pass.json', 'r') as json_file:

        sheet_id = json.load(json_file)['sheet_id']

    # Replace 'YOUR_TELEGRAM_API_TOKEN' with the actual token of your Telegram bot
    telegram_api_token = keys['tel_api']

    credentials = service_account.Credentials.from_service_account_file(
        'C:/Users/nehue/Documents/programas_de_python/Upwork_tasks/italian_real_estate_cloud_scraper/credentials.json')
        
    service = build('sheets', 'v4', credentials=credentials)

    body = {
        'values':[
            ['Hola', 'todo'],
            ['Hola', 'todo'],
            ['Hola', 'todo', 'arhjehr']
            ]
    }

    result = service.spreadsheets().values().append(spreadsheetId=sheet_id,range = 'A1:D5000000',
                 body=body, valueInputOption='USER_ENTERED').execute()


def insert_to_bq(input:dict):

    client = bigquery.Client()

    query = """
        CREATE TABLE Trieste_properties.Persons (
        PersonID int
        );
    """
    query_job = client.query(query)  # Make an API request.

    print("The query data:")
    for row in query_job:
        # Row values can be accessed by field name or index.
        print("name={}, count={}".format(row[0], row["total_people"]))

def alert_gmail(alert:str, alert_subject:str):

    keys = {}

    with open('keys_pass.json', 'r') as json_file:

        keys = json.load(json_file)

    # Replace 'YOUR_TELEGRAM_API_TOKEN' with the actual token of your Telegram bot
    mail = keys['gmail']
    passw = keys['app_pass']

    smtp_server = "smtp.gmail.com"
    port = 587  # For starttls

    # Create a secure SSL context
    context = ssl.create_default_context()

    # Create a multipart message
    msg = MIMEMultipart()
    msg["From"] = 'test.recs386@gmail.com'
    msg["To"] = 'test.recs386@gmail.com'
    msg["Subject"] = alert_subject

    # Attach the body of the email
    msg.attach(MIMEText(alert, "plain"))

    try:
        # Establish a connection to the SMTP server
        server = smtplib.SMTP(smtp_server, port)
        server.starttls(context=context)

        # Log in to the SMTP server with your Gmail account
        server.login(mail, passw)

        # Send the email
        server.sendmail('test.recs386@gmail.com', 'test.recs386@gmail.com', msg.as_string())

        print("Email sent successfully!")
    except Exception as e:
        print("Error: Unable to send the email.")
        print(e)
    finally:
        # Close the connection to the SMTP server
        server.quit()

def alert_tel(alert):

    keys = {}

    with open('keys_pass.json', 'r') as json_file:

        keys = json.load(json_file)

    # Replace 'YOUR_TELEGRAM_API_TOKEN' with the actual token of your Telegram bot
    telegram_api_token = keys['tel_api']
    
    # Replace 'YOUR_TELEGRAM_USER_ID' with your actual Telegram user ID
    telegram_user_id = '6072056422'

    url = f'https://api.telegram.org/bot{telegram_api_token}/sendMessage'
    data = {
        'chat_id': telegram_user_id,
        'text': alert,
    }
    response = requests.post(url, data=data)
    return response.json()


if __name__ == '__main__':

    pass

