import json
from tabnanny import check
from googleapiclient.discovery import build
from google.oauth2 import service_account
from google.cloud import bigquery
from os import environ
import smtplib
import ssl
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import requests
from datetime import datetime


environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'C:/Users/nehue/Documents/programas_de_python/Upwork_tasks/italian_real_estate_cloud_scraper/credentials.json'

def get_values_for_comparing_already_existing_props(table_name:str):

    client = bigquery.Client()

    query = f"""
        SELECT (Titolo, Area, Prezzo, Locali, MQ, Bagni, Piano, Dettagli) FROM Trieste_properties.{table_name} WHERE Date_delisted = NULL;
    """

    query_job = client.query(query)

    return query_job

def get_all_url_values(table_name:str, from_website:str):
    
    client = bigquery.Client()

    query = f"""
        SELECT (Url{from_website}) FROM Trieste_properties.{table_name} WHERE Date_delisted = NULL;
    """

    query_job = client.query(query)

    return query_job

def get_delisted_properties(db_values_url, scraped_urls:list, from_web_site:str):

    db_urls = []

    for row in db_values_url:

        url = list(dict(row)['f0_'].values())[0]

        db_urls.append(url)


    delisted_properties = list(set(db_urls) - set(scraped_urls))

    new_properties = list(set(scraped_urls) - set(db_urls))

    return delisted_properties

def update_delisted_properties(table_name:str, from_website:str, url:str):

    client = bigquery.Client()

    query = f"""UPDATE Trieste_properties.{table_name}
        SET Delisted_date = TIMESTAMP({datetime.now().strftime('%Y-%m-%d %H:%M:%S')})
        WHERE Url_{from_website} = {url};
        """

    query_job = client.query(query)

    print(f"Property: {url} has been updated to delisted.")

def is_on_db_from_same_site(db_values_url, url:str, from_web_site:str):

    is_already = False

    for row in db_values_url:

        link = list(dict(row)['f0_'].values())[0]

        if url == link:

            is_already = True

            break

        else:

            continue

    return is_already

def is_on_db_from_another_site(db_values,title:str, area:str, price:int, rooms:int, details:str, mq:int, bathrooms:int, floors:int) -> bool:

    is_already = False

      # Make an API request.

    for row in db_values:

        values = list(dict(row)['f0_'].values())

        if title in values[0] and details in values[7] and values[2]*0.9<price<values[2]*1.1 and values[4]*0.9<mq<values[4]*1.1 \
            and bathrooms == values[5] and floors == values[6] and rooms == values[3] and area in values[1]:

            is_already = True
        
        elif title in values[0] and details in values[7] and values[2]*0.75<price<values[2]*1.25 and values[4]*0.75<mq<values[4]*1.25

            is_already = True

    return is_already

def insert_to_gs(input:dict):

    headers = ['Date_GMT', 'Titolo', 'Area', 'Link', 'Prezzo', 'Locali', 'MQ', 'Bagni','Piano', 'Descrizione', 'Dettagli', 'Agenzia']

    sheet_id = None

    with open('keys_pass.json', 'r') as json_file:

        sheet_id = json.load(json_file)['sheet_id']

    credentials = service_account.Credentials.from_service_account_file(
        'C:/Users/nehue/Documents/programas_de_python/Upwork_tasks/italian_real_estate_cloud_scraper/credentials.json')
        
    service = build('sheets', 'v4', credentials=credentials)

    body = {
        'values':[
            [input[head] for head in headers]
        ]
    }

    result = service.spreadsheets().values().append(spreadsheetId=sheet_id,range = 'A1:D5000000',
                 body=body, valueInputOption='USER_ENTERED').execute()

def get_lat_and_long():

    lat = None

    lon = None

    return [lat,lon]

def insert_to_bq(input:dict, table_name:str, from_website:str):

    client = bigquery.Client()

    if is_on_db_from_same_site(get_all_url_values(), input['Link']):

        return "The property already exists in the database and was scraped from the same website again."

    if is_on_db_from_another_site(get_values_for_comparing_already_existing_props(), 
    input['Titolo'], input['Area'], input['Prezzo'], input['Locali'], input['Dettagli'], input['MQ'], input['Bagni'], input['Piano']):

        print("The property already exists in the database but from another website")

        # Function to scrape the Lat and long from ide or casa.

        lat_long = get_lat_and_long()

        query = f"UPDATE Trieste_properties.{table_name} SET Latitude = {lat_long[0]}, Longitude = {lat_long[1]}, Url_{} = {}"



    query = f"""
        INSERT INTO Trieste_properties.{table_name} VALUES
    """

    query_job = client.query(query)  # Make an API request.

    print("The query data:")
    for row in query_job:
        # Row values can be accessed by field name or index.
        print("name={}, count={}".format(row[0], row["total_people"]))

def alert(price:int, url:str) -> bool:

    sheet_id = None

    with open('keys_pass.json', 'r') as json_file:

        sheet_id = json.load(json_file)['sheet_id']

    credentials = service_account.Credentials.from_service_account_file(
        'C:/Users/nehue/Documents/programas_de_python/Upwork_tasks/italian_real_estate_cloud_scraper/credentials.json')
        
    service = build('sheets', 'v4', credentials=credentials)

    result = service.spreadsheets().values().get(spreadsheetId=sheet_id,range = 'Alerts!A1:B5000000').execute()

    price_alert = result['values'][0][1]

    if price < int(price_alert):

        alert_gmail(f'The property from this url: {url} is {price}',f'Property price lower than €{price_alert}')
        alert_tel(f'The property from this url: {url} is €{price}')

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

    print('Telegram message sent successfully')

    return response.json()


if __name__ == '__main__':

    is_on_db_from_another_site("n","n",1532,1,"n",1,1.5,1.5)


