Python-Project-DataExtraction-DataTransformation-DataStoring-APIStatusCheck-LambdaFunction

Steps To Follow
Run the required dependencies given in the requirementALL.txt

1. DataExtraction-DataTransformation-DataStoring
   In this project, from the FileExtraction.py, I have downloaded the list of "Company Facts" json files from the https://www.sec.gov/edgar/sec-api-documentation website.
   The downloaded files are then pushed to the AWS S3 bucket.
   From the DataExtraction.py, I have read the files from S3 bucket and convert the .json files into a structured Data.
   From the DataInsertion.py, I have connected my MYSQL AWS RDS Database and storing it for future retrival and report generation.

2. API StatusCheck Lambda Function
   In this project, from the lambdaFunction.py, I have called the http://api.open-notify.org/iss-now.json, which will give you the ISS current location coordinates. 
   Based on the API response, I am storing the data in the PostgreSQL Database table. If I am getting invalid response, then I am sending the alert message to my Slack channel.
   I have created a Slack account and corresponding channel to post this message, Slack account allows me to authentication using the OAuth token.
   Also, based on the requirement I am triggering this API for every 15 seconds from the CloudWatch Event. Since by default, minimum tiggering rate in AWS lambda is 1 minute. 
   So to acheive this, I have created a Step Function and an Iterator as a wrapper between the Cloudwatch and the APICheckLambda.

   All the related parameters are in the dataConfig.py file.





