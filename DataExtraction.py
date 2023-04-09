import dataConfig as config
import logging
import boto3, botocore
from botocore.exceptions import ClientError
import json
import pandas as pd
import os, pickle

s3_client = boto3.client(service_name='s3', region_name=config.region_name,
                         aws_access_key_id=config.aws_access_key_id,
                         aws_secret_access_key=config.aws_secret_access_key)


def getCompanyFacts():
    columns = {'CIK': [], 'EntityName': [], 'FactName': [], 'ProductLabel': [],
               'ProductDescription': [], 'StartDate': [], 'EndDate': [], 'EndValue': [],  
               'AccountNumber': [], 'FinancialYear': [], 'FinancialQuarter': [], 'FormID': [],  
               'FiledDate': [], 'FrameID': []}
    #pickle_in = open("dict.pickle","rb")
    lstCompanyFacts = s3_client.list_objects(Bucket=config.companyFactS3)['Contents']

    for element in lstCompanyFacts:
        content_object = s3_client.get_object(Bucket=config.companyFactS3, Key=element['Key'])
        file_content = content_object['Body'].read().decode('utf-8')
        json_content = json.loads(file_content)

        #Table Data Extraction
        try:
            CIK = str(json_content['cik'])
            if json_content['entityName'] != "":
                EntityName = json_content['entityName']
                if len(json_content['facts']) > 0:
                    for fact in json_content['facts']:
                        FactName = fact
                        if len(json_content['facts'][fact]) > 0:
                            for product in json_content['facts'][fact]:
                                if len(json_content['facts'][fact][product]) > 0:
                                    if json_content['facts'][fact][product]['label'] is None:
                                        ProductLabel = ''.join(' ' + char if char.isupper() else char.strip() for char in product).strip()
                                    else:
                                        ProductLabel = json_content['facts'][fact][product]['label']
                                    ProductDescription = json_content['facts'][fact][product]['description']
                                    if len(json_content['facts'][fact][product]['units']) > 0:
                                        for unit in json_content['facts'][fact][product]['units']:
                                            if len(json_content['facts'][fact][product]['units'][unit]) > 0:
                                                for value in json_content['facts'][fact][product]['units'][unit]:
                                                    if "start" in value:
                                                        StartDate = value['start']
                                                    else:
                                                        StartDate = ""
                                                        
                                                    EndDate = value['end']
                                                    EndValue = value['val']
                                                    AccountNumber = value["accn"]
                                                    FinancialYear = value["fy"] 
                                                    if value["fp"] == 'FY' :
                                                        FinancialQuarter = 'Q4'
                                                    else:
                                                        FinancialQuarter = value["fp"] 
                                                        
                                                    FormID = value["form"] 
                                                    FiledDate = value["filed"]
                                                    if "frame" in value:
                                                        FrameID = value["frame"]
                                                    else:
                                                        FrameID = ""

                                                    columns['CIK'].append(CIK)
                                                    columns['EntityName'].append(EntityName)
                                                    columns['FactName'].append(FactName)
                                                    columns['ProductLabel'].append(ProductLabel)
                                                    columns['ProductDescription'].append(ProductDescription)
                                                    columns['StartDate'].append(StartDate)
                                                    columns['EndDate'].append(EndDate)
                                                    columns['EndValue'].append(EndValue)
                                                    columns['AccountNumber'].append(AccountNumber)
                                                    columns['FinancialYear'].append(FinancialYear)
                                                    columns['FinancialQuarter'].append(FinancialQuarter)
                                                    columns['FormID'].append(FormID)
                                                    columns['FiledDate'].append(FiledDate)
                                                    columns['FrameID'].append(FrameID)
        except Exception as ex:
            logging.exception(str(ex))

    CompanyFact_Data = pd.DataFrame(columns)
    return CompanyFact_Data



CompanyFact_Data = getCompanyFacts()

