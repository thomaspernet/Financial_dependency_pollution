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

# DATA/ECON/FIRM_SURVEY/ASIF_CHINA/UNZIP_DATA_STATA
s3.download_file("DATA/ECON/FIRM_SURVEY/ASIF_CHINA/UNZIP_DATA_STATA/all9807finalruiliyong14.dta",
                 path_local=os.path.join(parent_path, "00_data_catalogue/temporary_local_data"))

# Remove previous files in folder if exist
s3.remove_all_bucket(
    path_remove='DATA/ECON/FIRM_SURVEY/ASIF_CHINA/UNZIP_DATA_CSV')

# read file in chunk -> Take about 10 minutes

filename = os.path.join(
    parent_path, "00_data_catalogue/temporary_local_data/all9807finalruiliyong14.dta")

# List to remove
list_to_remove = [

    "vaaddtax",
    "vanew",
    "dq1",
    # "citycode",
    # "prov",
    "age",
    "region",
    "ownership_new",
    "cic_adj",
    "cpi",
    "ippi",
    "rawmppi",
    "fassetpi",
    "ind_two",
    "OutputDefl",
    "InputDefl",
    "merge_ind",
    "inputdefl",
    "outputdefl",
    "BR_deflator",
    "citycode_asifad",
    "citycodenew",
    "_merge",
    "lenth",
    "indus2",
    "exportdum"
]


itr = pd.read_stata(filename, chunksize=25000)
i = 0
for chunk in tqdm(itr):
    # Upload to S3
    chunk = (
        chunk
        .drop(columns=list_to_remove)
        .assign(

            firm=lambda x: x['firm'].astype('str').str.split('.').str[0],
            year=lambda x: x['year'].astype('str').str.split('.').str[0],
            type=lambda x: x['type'].astype('str').str.split('.').str[0],
            setup=lambda x: x['setup'].astype('str').str.split('.').str[0],
            cic=lambda x: x['cic'].astype('str')
        )
    )
    chunk.to_csv('ASIF_9807_chunk_{}.csv'.format(i), index=False)
    s3.upload_file('ASIF_9807_chunk_{}.csv'.format(
        i), "DATA/ECON/FIRM_SURVEY/ASIF_CHINA/UNZIP_DATA_CSV")
    os.remove('ASIF_9807_chunk_{}.csv'.format(i))
    i += 1


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

spreadsheet_id = drive.find_file_id('var_name02-07', to_print=False)
var = (
    drive.upload_data_from_spreadsheet(
        sheetID=spreadsheet_id,
        sheetName="var_name02-07.csv",
        to_dataframe=True)
    .loc[lambda x:  ~x['Var_name'].isin(list_to_remove)]
)

schema = []
for i in chunk.columns:

    temp = var.loc[lambda x: x['Var_name'].isin([i])]
    temp

    type = temp['type'].values[0]
    comment = temp['comments'].values[0]

    if type == None:
        type = ''

    if comment == None:
        comment = ''

    dic = {
        "Name": i,
        "Type": type,
        "Comment": comment
    }

    schema.append(dic)

# Craw the table
glue = service_glue.connect_glue(client=client)
target_S3URI = "s3://datalake-datascience/DATA/ECON/FIRM_SURVEY/ASIF_CHINA/UNZIP_DATA_CSV"
name_crawler = "crawl-ASIF"
Role = 'arn:aws:iam::468786073381:role/AWSGlueServiceRole-crawler-datalake'
DatabaseName = "firms_survey"
TablePrefix = 'asif_'


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
    'description': 'Create Firms survey ASIF panel data from STATA',
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
