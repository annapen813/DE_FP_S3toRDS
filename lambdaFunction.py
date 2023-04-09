import requests
import json
import logging
import psycopg2
from psycopg2 import sql
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
import dataConfig as config


def lambda_handler(event, context):
    # TODO implement
    if chectAPIStatus('http://api.open-notify.org/iss-now.json'):
        return {
            'statusCode': 200,
            'body': json.dumps('Success')
        }
    else:
        return {
            'statusCode': 200,
            'body': json.dumps('Something went wrong. Please try again.')
        }

def chectAPIStatus(url):
    apiStatus = False
    try:
        headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36'}
        response = requests.get(url, headers=headers, stream=True)   
        if response and response.status_code == 200:
            json_data = response.json() 
            openNotifyData(json_data)
            apiStatus = True
        else:
             postMessageToSlackCommunity("Hello! API server is down. Response is null.")
             apiStatus = True
    except Exception as ex:
        logging.exception(str(ex))
        apiStatus = False

    return apiStatus

def openNotifyData(json_data):
    try:
        if json_data is not None:
            timeStamp = json_data['timestamp']
            messageStatus = json_data['message']
            latitude = json_data['iss_position']['latitude']
            longitude = json_data['iss_position']['longitude']

            dataInsertion(timeStamp, messageStatus, latitude, longitude)
    except Exception as ex:
        logging.exception(str(ex))    

def getConnection():
    conn = psycopg2.connect("host='guviprojects.c3rptuhsrmxf.eu-west-1.rds.amazonaws.com' port=5432 user='postgres' password='1.Password'")
    cur = conn.cursor()
    conn.autocommit = True #!

    #Creating DB for the first time
    chkDb = "SELECT EXISTS (SELECT * FROM pg_catalog.pg_database WHERE lower(datname) = lower('guvi_api_monitoring'))" 
    cur.execute(chkDb)
    dbIsExist = cur.fetchone()[0]
    conn.commit()
    if dbIsExist == False:
        dbname = sql.Identifier(f'guvi_api_monitoring')
        create_cmd = sql.SQL('CREATE DATABASE {}').format(dbname)
        grant_cmd = sql.SQL('GRANT ALL PRIVILEGES ON DATABASE {} TO postgres').format(dbname)       
        cur.execute(create_cmd)
        cur.execute(grant_cmd)
        conn = psycopg2.connect("host='guviprojects.c3rptuhsrmxf.eu-west-1.rds.amazonaws.com' port=5432 user='postgres' password='1.Password' dbname=guvi_api_monitoring")
    else:
        conn = psycopg2.connect("host='guviprojects.c3rptuhsrmxf.eu-west-1.rds.amazonaws.com' port=5432 user='postgres' password='1.Password' dbname=guvi_api_monitoring")

    cur = conn.cursor()
    conn.autocommit = True #!
    cur.execute("select * from information_schema.tables where table_name=%s", ('apiresult',))
    if bool(cur.rowcount) == False:
        cur.execute('CREATE TABLE "apiresult" (UnixTime varchar, MessageStatus varchar, Latitude varchar, Longitude varchar)') 
    
    return conn


def dataInsertion(timeStamp, messageStatus, latitude, longitude):
    conn = getConnection()
    postgres_insert_query = """ INSERT INTO apiresult (UnixTime, MessageStatus, Latitude, Longitude) VALUES (%s,%s,%s,%s)"""
    record_to_insert = (str(timeStamp), messageStatus, latitude, longitude)
    
    cursor = conn.cursor()
    try:
        cursor.execute(postgres_insert_query, record_to_insert)
        conn.commit()
        postMessageToSlackCommunity("API data inserted successfully!")
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        cursor.close()
        return 1
    finally:
        if conn:
            cursor.close()
            conn.close()

def postMessageToSlackCommunity(message):
    # ID of the channel you want to send the message to
    channel_id = "C05201GMXFW"
    try:
        client = WebClient(config.slack_oauth_token) #token=os.environ.get("SLACK_BOT_TOKEN"))
        # Call the conversations.list method using the WebClient
        result = client.chat_postMessage(
            channel=channel_id,
            text= message
            # You could also use a blocks[] array to send richer content
        )
    except SlackApiError as e:
        logging.error(f"Error posting message: {e}")


def checkDataCount():
    conn = psycopg2.connect("host='guviprojects.c3rptuhsrmxf.eu-west-1.rds.amazonaws.com' port=5432 user='postgres' password='1.Password' dbname=guvi_api_monitoring")
    cur = conn.cursor()
    cur.execute("select * from apiresult")
    print(cur.rowcount)


#chectAPIStatus('http://api.open-notify.org/iss-now.json')
#postMessageToSlackCommunity()
#checkDataCount()