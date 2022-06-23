import pandas as pd
from awsPy.aws_s3 import service_s3
from awsPy.aws_glue import service_glue
from awsPy.aws_authorization import aws_connector
from GoogleDrivePy.google_drive import connect_drive
from GoogleDrivePy.google_authorization import authorization_service
from pathlib import Path
import os, re
import json
from tqdm import tqdm
# Download stata file
path = os.getcwd()
parent_path = str(Path(path).parent.parent.parent)
name_credential = 'financial_dep_SO2_accessKeys.csv'
region = 'eu-west-2'
bucket = 'datalake-london'
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

# DOWNLOAD DATA: pollution data
# if time out, download it manually

FILENAME_SPREADSHEET = "pollution_city_cic4_china"
spreadsheet_id = drive.find_file_id(FILENAME_SPREADSHEET, to_print=False)
sheetName = 'pollution_city_cic4_china'
var1 = (
    drive.upload_data_from_spreadsheet(
        sheetID=spreadsheet_id,
        sheetName=sheetName,
        to_dataframe=True)
)
input_path_1 = os.path.join(parent_path, "00_data_catalogue",
                            "temporary_local_data", "pollution_city_cic4_china - pollution_city_cic4_china.csv")
# DOWNLOAD DATA: equipment data
FILENAME_DRIVE = 'thomasusepollution.dta'
FILEID = drive.find_file_id(FILENAME_DRIVE, to_print=False)
var = (
    drive.download_file(
        filename=FILENAME_DRIVE,
        file_id=FILEID,
        local_path=os.path.join(parent_path, "00_data_catalogue", "temporary_local_data"))
)
input_path_2 = os.path.join(parent_path, "00_data_catalogue",
                            "temporary_local_data", FILENAME_DRIVE)
# DOWNLOAD DATA: Count of firm
FILENAME_DRIVE1 = 'thomasusepollutionfirmn.dta'
FILEID = drive.find_file_id(FILENAME_DRIVE1, to_print=False)
var = (
    drive.download_file(
        filename=FILENAME_DRIVE1,
        file_id=FILEID,
        local_path=os.path.join(parent_path, "00_data_catalogue", "temporary_local_data"))
)
input_path_3 = os.path.join(parent_path, "00_data_catalogue",
                            "temporary_local_data", FILENAME_DRIVE1)

#### DOWNLOAD WASTE WATER equipment
FILENAME_DRIVE2 = 'thomasusepollution_062022.dta'
FILEID = drive.find_file_id(FILENAME_DRIVE2, to_print=False)
var = (
    drive.download_file(
        filename=FILENAME_DRIVE2,
        file_id=FILEID,
        local_path=os.path.join(parent_path, "00_data_catalogue", "temporary_local_data"))
)
input_path_4 = os.path.join(parent_path, "00_data_catalogue",
                            "temporary_local_data", FILENAME_DRIVE2)
# Make sure of the content
pd.read_csv(input_path_1).shape
pd.read_stata(input_path_2).shape
pd.read_stata(input_path_3).shape
pd.read_stata(input_path_4).shape
# Previous file
pd.io.json.build_table_schema(pd.read_csv(input_path_1))


var = (
    pd.read_csv(input_path_1,
                dtype={
                    "citycode": 'string',
                    "year": 'string',
                    "indus_code": 'string',
                    "ind2": 'string'
                }
                )
    .dropna(subset=['citycode'])
    .merge((
        pd.read_stata(input_path_2)
        .dropna(subset=['citycode'])
        .assign(
            year=lambda x: x['year'].round().astype('int').astype('str'),
            citycode=lambda x: x['citycode'].round().astype(
                'int').astype('str'),
        )
        .reindex(columns=[
            'year',
            'indus_code',
            'citycode',
            'tlssnl',
            'tdwastegas_equip',
            'tdso2_equip',
            'tfqzlssnl',
            'ttlssnl'
        ])
    ))
    .merge(
        (
            pd.read_stata(input_path_3)
            .dropna(subset=['citycode'])
            .assign(
                year=lambda x: x['year'].round().astype('int').astype('str'),
                citycode=lambda x: x['citycode'].round().astype(
                    'int').astype('str'),
            )
            .reindex(columns=[
                'year',
                'citycode',
                'indus_code',
                'firmdum',
                'tfirm'
            ])
        ), how='left')
    .assign(
    tlssnl_output = lambda x: x['tlssnl']/x['ttoutput'],
    tdwastegas_equip_output = lambda x: x['tdwastegas_equip']/x['ttoutput'],
    tdso2_equip_output = lambda x: x['tdso2_equip']/x['ttoutput'],
    )
    .merge(
    (
        pd.read_stata(input_path_4)
        .dropna(subset=['citycode'])
        .assign(
            year=lambda x: x['year'].round().astype('int').astype('str'),
            citycode=lambda x: x['citycode'].round().astype(
                'int').astype('str'),
        )
        .reindex(columns=[
            'year', 'indus_code', 'citycode',
           'total_industrialwater_used', 'total_freshwater_used',
           'total_repeatedwater_used', 'total_coal_used', 'trlmxf', 'tylmxf',
           'clean_gas_used', 'dwastewater_equip', 'tfszlssnl', 'tfszlssfee'
        ])
    ), how = 'left'
    )
)

# Brief information ...
var.to_csv('China_city_pollution_98_2007.csv', index=False)
s3.remove_file(
    key="DATA/ENVIRONMENT/CHINA/CITY_SECTOR_POLLUTION/China_city_pollution_98_2007.gz")
# Save S3
s3.upload_file('China_city_pollution_98_2007.csv',
               "DATA/ENVIRONMENT/CHINA/CITY_SECTOR_POLLUTION")

os.remove('China_city_pollution_98_2007.csv')
[os.remove(i) for i in [input_path_1, input_path_2, input_path_3, input_path_4]]

pd.io.json.build_table_schema(var)
# Craw the table
# Schema
#for i in pd.io.json.build_table_schema(var)['fields']:
#    if i['type'] in ['number', 'integer']:
#        i['type'] = 'int'
#    print("{},".format({'Name':i['name'], 'Type':i['type'],'Comment':''}))

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
        "Name": "provinces",
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
        "Name": "tcod",
        "Type": "float",
        "Comment": "Total COD city sector"
    },
    {
        "Name": "tammonia_nitrogen",
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
        "Name": "tnox",
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
    {'Name': 'tlssnl', 'Type': 'int', "Comment": ""},
    {'Name': 'tdwastegas_equip', 'Type': 'int',
        "Comment": "the number of equipment of removing wasted gas"},
    {'Name': 'tdso2_equip', 'Type': 'int',
        "Comment": "the number of equipment of removing so2"},
    {'Name': 'tfqzlssnl', 'Type': 'int',
        "Comment": "the capacity to remove wasted gas cube meter/hour"},
    {'Name': 'ttlssnl', 'Type': 'int',
        "Comment": "the capacity to remove so2 kilogram/hour"},
    {'Name': 'firmdum', 'Type': 'int',
        "Comment": "the number of firms ith equipment in city-industry-year"},
    {'Name': 'tfirm', 'Type': 'int',
        "Comment": "the number of firms ith equipment in city-year"},

    {'Name': 'tlssnl_output', 'Type': 'float',
        "Comment": "the number of firms ith equipment in city-year"},
    {'Name': 'tdwastegas_equip_output', 'Type': 'float',
        "Comment": "the number of equipment of removing wasted gas per unit of output"},
    {'Name': 'tdso2_equip_output', 'Type': 'float',
        "Comment": "the number of equipment of removing so2 per unit of output"},
    {'Name': 'tdwastegas_equip_output', 'Type': 'int', 'Comment': ''},
{'Name': 'tdso2_equip_output', 'Type': 'int', 'Comment': ''},
{'Name': 'total_industrialwater_used', 'Type': 'int', 'Comment': 'total industrial water used'},
{'Name': 'total_freshwater_used', 'Type': 'int', 'Comment': 'total fresh water used'},
{'Name': 'total_repeatedwater_used', 'Type': 'int', 'Comment': 'total repeated water used'},
{'Name': 'total_coal_used', 'Type': 'int', 'Comment': 'total coal used'},
{'Name': 'trlmxf', 'Type': 'int', 'Comment': 'the consumption of fuel coal'},
{'Name': 'tylmxf', 'Type': 'int', 'Comment': 'the consumption of raw coal'},
{'Name': 'clean_gas_used', 'Type': 'int', 'Comment': 'clean gas used'},
{'Name': 'dwastewater_equip', 'Type': 'int', 'Comment': 'waste water equipment'},
{'Name': 'tfszlssnl', 'Type': 'int', 'Comment': 'the capacity to remove wasted water ton/day'},
{'Name': 'tfszlssfee', 'Type': 'int', 'Comment': 'the expense to remove wasted water ton/day'}
]
#pd.read_stata(input_path_4, iterator = True).variable_labels()

glue = service_glue.connect_glue(client=client)
target_S3URI = "s3://datalake-london/DATA/ENVIRONMENT/CHINA/CITY_SECTOR_POLLUTION"
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
filename = 'city_sector_pollution.py'
path_to_etl = os.path.join(
    str(Path(path).parent.parent.parent), 'utils', 'parameters_ETL_Financial_dependency_pollution.json')
with open(path_to_etl) as json_file:
    parameters = json.load(json_file)
github_url = os.path.join(
    "https://github.com/",
    parameters['GLOBAL']['GITHUB']['owner'],
    parameters['GLOBAL']['GITHUB']['repo_name'],
    re.sub(parameters['GLOBAL']['GITHUB']['repo_name'],
           '', re.sub(
               r".*(?={})".format(parameters['GLOBAL']['GITHUB']['repo_name']), '', path))[1:],
    filename
)
table_name = '{}{}'.format(TablePrefix, os.path.basename(target_S3URI).lower())
description = "construct city-industry pollution china"
json_etl = {
    'description': description,
    'schema': schema,
    'partition_keys': [],
    'metadata': {
        'DatabaseName': DatabaseName,
        'TablePrefix': TablePrefix,
        'TableName': table_name,
        'target_S3URI': target_S3URI,
        'from_athena': 'False',
        'filename': filename,
        'github_url': github_url
    }
}


with open(path_to_etl) as json_file:
    parameters = json.load(json_file)

# parameters['TABLES']['CREATION']['ALL_SCHEMA'].pop(0)

index_to_remove = next(
    (
        index
        for (index, d) in enumerate(parameters['TABLES']['CREATION']['ALL_SCHEMA'])
        if d['metadata']['filename'] == filename
    ),
    None,
)
if index_to_remove != None:
    parameters['TABLES']['CREATION']['ALL_SCHEMA'].pop(index_to_remove)

parameters['TABLES']['CREATION']['ALL_SCHEMA'].append(json_etl)

with open(path_to_etl, "w")as outfile:
    json.dump(parameters, outfile)
