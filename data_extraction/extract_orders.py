import json
import boto3
import configparser
import os


# Get the directory of the current script
script_dir = os.path.dirname(os.path.abspath(__file__))
root_dir = os.path.normpath(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))


# Define the relative path to your data directory
relative_path = "Data_Raw"
# Combine the script directory and relative path to get the full path
base_path = os.path.join(root_dir, relative_path)

local_file_name = os.path.join(base_path, "Order.json")

# Reading the local file:
with open(local_file_name, "r",encoding='utf-8') as file:
    reader = json.load(file)
    data = [row for row in reader]

print(f'number of rows in the file: {len(data)} \nnumber of columns in the file: {len(data[0])}')


orders_file_name = "orders_extract.csv"


#reading the pipeline config
parser = configparser.ConfigParser()
conf_path = os.path.join(script_dir, "pipeline.conf")
parser.read(conf_path) 



## Uploading the Local file to S3:
# Load the botot3 credentials:
access_key = parser.get("aws_boto_credentials","access_key")
secret_key = parser.get("aws_boto_credentials","secret_key")
bucket_name = parser.get("aws_boto_credentials","bucket_name_bronze")

s3 = boto3.client('s3',
                  aws_access_key_id = access_key,
                  aws_secret_access_key = secret_key)

s3_file = orders_file_name

# Uploading the file to S3
s3.upload_file(local_file_name,bucket_name,s3_file)