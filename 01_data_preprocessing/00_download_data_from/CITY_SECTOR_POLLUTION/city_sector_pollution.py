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
