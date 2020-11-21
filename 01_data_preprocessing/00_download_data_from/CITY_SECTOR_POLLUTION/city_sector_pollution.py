import pandas as pd
from awsPy.aws_s3 import service_s3
from awsPy.aws_glue import service_glue
from awsPy.aws_authorization import aws_connector
from GoogleDrivePy.google_platform import connect_cloud_platform
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
gd_auth = authorization_service.get_authorization(
    path_credential_gcp="{}/creds/service.json".format(parent_path),
    path_credential_drive="{}/creds".format(parent_path),
    verbose=False
)

gcp_auth = auth.authorization_gcp()
gcp = connect_cloud_platform.connect_console(project = project,
                                                 service_account = gcp_auth)

gcp.download_blob(
bucket_name = "chinese_data",
destination_blob_name = "Environmental_Statistics_china/Processed_",
source_file_name = 'China_city_pollution_98_2007.gz')

### Make sure of the content
#pd.read_csv('China_city_pollution_98_2007.gz').head()

### Save S3
s3.upload_file('China_city_pollution_98_2007.gz', "DATA/ENVIRONMENT/CHINA/CITY_SECTOR_POLLUTION")
os.remove('China_city_pollution_98_2007.gz')

# Craw the table
### Schema

schema = [
{
    "Name": "year",
    "Type": "string",
    "Comment": ""
},
{
    "Name": "prov2013",
    "Type": "string",
    "Comment": "Province name in Chinese"
},
{
    "Name": "Provinces",
    "Type": "string",
    "Comment": "Province name in English"
},
{
    "Name": "citycode",
    "Type": "string",
    "Comment": "citycode refers to geocode4corr"
},
{
    "Name": "citycn",
    "Type": "string",
    "Comment": "City name in Chinese"
},
{
    "Name": "cityen",
    "Type": "string",
    "Comment": "City name in English"
},
{
    "Name": "indus_code",
    "Type": "string",
    "Comment": "4 digits industry code"
},
{
    "Name": "ind2",
    "Type": "string",
    "Comment": "2 digits industry code"
},
{
    "Name": "ttoutput",
    "Type": "float",
    "Comment": "Total output city sector"
},
{
    "Name": "twaste_water",
    "Type": "int",
    "Comment": "Total waste water city sector"
},
{
    "Name": "tCOD",
    "Type": "float",
    "Comment": "Total COD city sector"
},
{
    "Name": "tAmmonia_Nitrogen",
    "Type": "float",
    "Comment": "Total Ammonia Nitrogen city sector"
},
{
    "Name": "twaste_gas",
    "Type": "int",
    "Comment": "Total Waste gas city sector"
},
{
    "Name": "tso2",
    "Type": "int",
    "Comment": "Total so2 city sector"
},
{
    "Name": "tNOx",
    "Type": "int",
    "Comment": "Total NOx city sector"
},
{
    "Name": "tsmoke_dust",
    "Type": "int",
    "Comment": "Total smoke dust city sector"
},
{
    "Name": "tsoot",
    "Type": "int",
    "Comment": "Total soot city sector"
},
{
    "Name": "Lower_location",
    "Type": "string",
    "Comment": "Location city. one of Coastal, Central, Northwest, Northeast, Southwest"
},
{
    "Name": "Larger_location",
    "Type": "string",
    "Comment": "Location city. one of Eastern, Central, Western"
},
{
    "Name": "Coastal",
    "Type": "string",
    "Comment": "City is bordered by sea or not"
},

]



glue = service_glue.connect_glue(client=client)
target_S3URI = "s3://datalake-datascience/DATA/ENVIRONMENT/CHINA/CITY_SECTOR_POLLUTION"
name_crawler = "crawl-pollution"
Role = 'arn:aws:iam::468786073381:role/AWSGlueServiceRole-crawler-datalake'
DatabaseName = "environment"
TablePrefix = 'china_'


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
    'description': 'Create China city sector pollution',
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
