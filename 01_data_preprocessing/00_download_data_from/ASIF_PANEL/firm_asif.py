import pandas as pd
from awsPy.aws_s3 import service_s3
from awsPy.aws_authorization import aws_connector
from pathlib import Path
import os
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
s3.download_file("DATA/ECON/FIRM_SURVEY/ASIF_CHINA/UNZIP_DATA_STATA",
                 path_local=os.path.join(parent_path, "00_data_catalogue/temporary_local_data"))

# read file in chunk -> Take about 10 minutes

filename = os.path.join(
    parent_path, "00_data_catalogue/temporary_local_data/all9807finalruiliyong14.dta")

itr = pd.read_stata(filename, chunksize=25000)
i = 0
for chunk in tqdm(itr):
    # Upload to S3
    chunk.to_csv('ASIF_9807_chunk_{}.csv'.format(i))
    s3.upload_file('ASIF_9807_chunk_{}.csv'.format(
        i), "DATA/ECON/FIRM_SURVEY/ASIF_CHINA/UNZIP_DATA_CSV")
    os.remove('ASIF_9807_chunk_{}.csv'.format(i))
    i += 1
