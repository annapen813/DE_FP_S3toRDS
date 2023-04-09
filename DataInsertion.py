import pandas as pd
import pymysql
import dataConfig as config
import DataExtraction as dataExtract

#SQL
from sqlalchemy import create_engine

db = pymysql.connect(host=config.host, user = config.user, password=config.password, port=config.port)
# you have cursor instance here
cursor = db.cursor()
cursor.execute("select version()")

#Creating DB for the first time
sql = "SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = 'sec_edgar'" 

dbIsExist = cursor.execute(sql)
if(dbIsExist == 0):
    sql = "create database sec_edgar end"
    cursor.execute(sql)
    cursor.connection.commit()

db_data = 'mysql+mysqldb://' + config.user + ':' + config.password + '@' + config.host + ':3306/' + config.dbname + '?charset=utf8mb4'

engine = create_engine(db_data)

df_CompanyFactsData = dataExtract.getCompanyFacts()
# Execute the to_sql for writting DF into SQL
df_CompanyFactsData.to_sql('Company_Facts_Data', engine, if_exists='append', index=False)  