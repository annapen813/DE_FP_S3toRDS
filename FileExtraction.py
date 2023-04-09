import requests, zipfile, io
import dataConfig as config
import logging
import boto3, botocore
from botocore.exceptions import ClientError
import os
import pickle


s3_client = boto3.client(service_name='s3', region_name=config.region_name,
                         aws_access_key_id=config.aws_access_key_id,
                         aws_secret_access_key=config.aws_secret_access_key)

def download_file(url, path_to_save):
    headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36',
                'Upgrade-Insecure-Requests':'1',
                'Accept-Encoding': 'gzip, deflate, br',
                'Accept-Language': 'en-US,en;q=0.9',
                'Connection': 'keep-alive',
                'Host': 'www.sec.gov',
                'Referer':'https://www.sec.gov/edgar/sec-api-documentation',
                'Sec-Fetch-Dest': 'empty',
                'Sec-Fetch-Mode': 'cors',
                'Sec-Fetch-Site': 'same-origin'
                }
    r = requests.get(url, headers=headers, stream=True)            
    z = zipfile.ZipFile(io.BytesIO(r.content))
    z.extractall(path_to_save)
    lstFileNames = z.namelist()

    return lstFileNames



def upload_file(file_name, file_path, bucket, object_name=None):
    """Upload a file to an S3 bucket

    :param file_name: File to upload
    :param bucket: Bucket to upload to
    :param object_name: S3 object name. If not specified then file_name is used
    :return: True if file was uploaded, else False
    """
    
    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = os.path.basename(file_name)

    # Upload the file
    file_name = file_path + file_name
    
    try:
        response = s3_client.upload_file(file_name, bucket, object_name)
    except ClientError as e:
        error_code = int(e.response['Error']['Code'])
        if error_code == 403:
            print("Private Bucket. Forbidden Access!")
            return True
        elif error_code == 404:
            print("Bucket Does Not Exist!")
            create_bucket(config.companyFactS3)
            return False
    return True

def create_bucket(bucket_name, region=None):
    """Create an S3 bucket in a specified region

    If a region is not specified, the bucket is created in the S3 default
    region (us-east-1).

    :param bucket_name: Bucket to create
    :param region: String region to create bucket in, e.g., 'us-west-2'
    :return: True if bucket created, else False
    """
    # Create bucket
    try:
        if region is None:
            s3_client.create_bucket(Bucket=bucket_name)
        else:
            location = {'LocationConstraint': config.region_name}
            s3_client.create_bucket(Bucket=bucket_name,
                                    CreateBucketConfiguration=location)
    except ClientError as e:
        logging.error(e)
        return False
    return True


def chk_bucket(bucket_name):
    try:
        s3_client.head_bucket(Bucket=bucket_name)
        print("Bucket Exists!")
        return True
    except botocore.exceptions.ClientError as e:
        # If a client error is thrown, then check that it was a 404 error.
        # If it was a 404 error, then the bucket does not exist.
        error_code = int(e.response['Error']['Code'])
        if error_code == 403:
            print("Private Bucket. Forbidden Access!")
            return True
        elif error_code == 404:
            print("Bucket Does Not Exist!")
            return False
    except AttributeError as ex:
        return False



lstCompanyFacts = download_file(config.downloadCompanyFactsUrl, config.companyFactsSavePath)
#pickle_out = open("dict.pickle","wb")
#pickle.dump(lstCompanyFacts, pickle_out)
#pickle_out.close()


if chk_bucket(config.companyFactS3) == False:
  create_bucket(config.companyFactS3, config.region_name)

for element in lstCompanyFacts:
    print(element)
    upload_file(element, config.companyFactsSavePath, config.companyFactS3)
    
