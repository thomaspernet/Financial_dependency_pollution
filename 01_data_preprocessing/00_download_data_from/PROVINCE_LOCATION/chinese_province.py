import pandas as pd
from awsPy.aws_s3 import service_s3
from awsPy.aws_glue import service_glue
from awsPy.aws_authorization import aws_connector
from GoogleDrivePy.google_drive import connect_drive
from GoogleDrivePy.google_authorization import authorization_service
from pathlib import Path
import os
import json
from tqdm import tqdm
# Download stata file
path = os.getcwd()
parent_path = str(Path(path).parent.parent.parent)
name_credential = 'financial_dep_SO2_accessKeys.csv'
region = 'eu-west-3'
bucket = 'datalake-datascience'
path_cred = "{0}/creds/{1}".format(parent_path, name_credential)


con = aws_connector.aws_instantiate(credential=path_cred,
                                    region=region)
client = con.client_boto()
s3 = service_s3.connect_S3(client=client,
                           bucket=bucket, verbose=True)

# Create schema
# Load schema from
# https://docs.google.com/spreadsheets/d/1gfdmBKzZ1h93atSMFcj_6YgLxC7xX62BCxOngJwf7qE
project = 'valid-pagoda-132423'
auth = authorization_service.get_authorization(
    path_credential_gcp="{}/creds/service.json".format(parent_path),
    path_credential_drive="{}/creds".format(parent_path),
    verbose=False
)

gd_auth = auth.authorization_drive()
drive = connect_drive.drive_operations(gd_auth)

spreadsheet_id = drive.find_file_id('provinces_location', to_print=False)
var = (
    drive.upload_data_from_spreadsheet(
        sheetID=spreadsheet_id,
        sheetName="provinces_location.csv",
        to_dataframe=True)
)

var.to_csv('provinces_location.csv', index = False)

s3.upload_file('provinces_location.csv',
"DATA/ECON/LOOKUP_DATA/CHINESE_PROVINCE_LOCATION")
os.remove('provinces_location.csv')

schema = [
{
    "Name": "prov2013",
    "Type": "string",
    "Comment": "Province name,  in Chinese"
},
{
    "Name": "province_en",
    "Type": "string",
    "Comment": "Province name,  in English"
},
{
    "Name": "lower_location",
    "Type": "string",
    "Comment": "Location city. one of Coastal, Central, Northwest, Northeast, Southwest"
},
{
    "Name": "larger_location",
    "Type": "string",
    "Comment": "Location city. one of Eastern, Central, Western"
},
{
    "Name": "coastal",
    "Type": "string",
    "Comment": "City is bordered by sea or not"
},

]

glue = service_glue.connect_glue(client=client)
target_S3URI = "s3://datalake-datascience/DATA/ECON/LOOKUP_DATA/CHINESE_PROVINCE_LOCATION"
name_crawler = "crawl-pollution"
Role = 'arn:aws:iam::468786073381:role/AWSGlueServiceRole-crawler-datalake'
DatabaseName = "chinese_lookup"
TablePrefix = 'geo_'


glue.create_table_glue(
    target_S3URI,
    name_crawler,
    Role,
    DatabaseName,
    TablePrefix,
    from_athena=False,
    update_schema=schema,
)

# Add tp ETL parameter files
json_etl = {
    'description': 'Create Control zone policy city',
    'schema': schema,
    'partition_keys': [],
    'metadata': {
        'DatabaseName': DatabaseName,
        'TablePrefix': TablePrefix,
        'target_S3URI': target_S3URI,
        'from_athena': 'False'
    }
}

path_to_etl = os.path.join(str(Path(path).parent.parent),
                           'parameters_ETL_Financial_dependency_pollution.json')
with open(path_to_etl) as json_file:
    parameters = json.load(json_file)

#parameters['TABLES']['CREATION']['ALL_SCHEMA'].pop(0)

parameters['TABLES']['CREATION']['ALL_SCHEMA'].append(json_etl)

with open(path_to_etl, "w")as outfile:
    json.dump(parameters, outfile)
