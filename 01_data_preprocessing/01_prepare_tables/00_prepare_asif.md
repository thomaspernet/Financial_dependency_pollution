---
jupyter:
  jupytext:
    formats: ipynb,md
    text_representation:
      extension: .md
      format_name: markdown
      format_version: '1.2'
      jupytext_version: 1.4.2
  kernel_info:
    name: python3
  kernelspec:
    display_name: Python 3
    language: python
    name: python3
---

<!-- #region -->
# Prepare ASIF data to S3

# Objective(s)

## Business needs 

Prepare (cleaning  & removing unwanted rows) ASIF data using Athena and save output to S3 + Glue. 

## Description

**Objective**

Raw data is not 100% cleaned, as described by the analytical HTML table Template_analysis_from_lambda-2020-11-22-08-12-20.html (analysis of the raw table). 

**Steps**

We will clean the table by doing the following steps:
1. Keeping year 1998 to 2007
2. Clean citycode → Only 4 digits
3. Clean setup → replace to Null all values with a length lower/higher than 4 
4. Remove rows when cic is unknown


## Target

* The file is saved in S3: 
  * bucket: datalake-datascience 
  * path: DATA/ECON/FIRM_SURVEY/ASIF_CHINA/PREPARED 
* Glue data catalog should be updated
  * database: firms_survey 
  * table prefix: asif_firms 
    * table name (prefix + last folder S3 path): asif_firms_prepared 

# Metadata

* Key: fzt56oqnn52261m
* Parent key (for update parent):  
* Notebook US Parent (i.e the one to update): 
* https://github.com/thomaspernet/Financial_dependency_pollution/blob/master/01_data_preprocessing/02_prepare_tables_model/00_prepare_asif.md
* Epic: Epic 1
* US: US 3
* Date Begin: 11/22/2020
* Duration Task: 0
* Description: Prepare (cleaning  & removing unwanted rows) ASIF data using Athena and save output to S3 + Glue. 
* Step type: Prepare table
* Status: Active
* Source URL: US 03 Prepare ASIF
* Task type: Jupyter Notebook
* Users: Thomas Pernet
* Watchers: Thomas Pernet
* User Account: https://468786073381.signin.aws.amazon.com/console
* Estimated Log points: 6
* Task tag: #asif,#athena
* Toggl Tag: #data-preparation
* current nb commits: 
 * Meetings:  
* Presentation:  
* Email Information:  
  * thread: Number of threads: 0(Default 0, to avoid display email)
  *  

# Input Cloud Storage [AWS/GCP]

## Table/file

* Origin: 
* Athena
* Name: 
* china_asif
* Github: 
  * https://github.com/thomaspernet/Financial_dependency_pollution/blob/master/01_data_preprocessing/00_download_data_from/ASIF_PANEL/firm_asif.py

# Destination Output/Delivery

## Table/file

* Origin: 
* S3
* Athena
* Name:
* DATA/ECON/FIRM_SURVEY/ASIF_CHINA/PREPARED
* asif_firms_prepared
* GitHub:
* https://github.com/thomaspernet/Financial_dependency_pollution/blob/master/01_data_preprocessing/02_prepare_tables_model/00_prepare_asif.md
* URL: 
  * datalake-datascience/DATA/ECON/FIRM_SURVEY/ASIF_CHINA/PREPARED
* 

# Knowledge

## List of candidates

* [Analytical raw dataset (HTML)](https://s3.console.aws.amazon.com/s3/buckets/datalake-datascience?region=eu-west-3&prefix=ANALYTICS/HTML_OUTPUT/ASIF_UNZIP_DATA_CSV/)
<!-- #endregion -->
```python inputHidden=false outputHidden=false jupyter={"outputs_hidden": false}
from awsPy.aws_authorization import aws_connector
from awsPy.aws_s3 import service_s3
from awsPy.aws_glue import service_glue
from pathlib import Path
import pandas as pd
import numpy as np
import seaborn as sns
import os, shutil, json

path = os.getcwd()
parent_path = str(Path(path).parent.parent)


name_credential = 'financial_dep_SO2_accessKeys.csv'
region = 'eu-west-3'
bucket = 'datalake-datascience'
path_cred = "{0}/creds/{1}".format(parent_path, name_credential)
```

```python inputHidden=false outputHidden=false jupyter={"outputs_hidden": false}
con = aws_connector.aws_instantiate(credential = path_cred,
                                       region = region)
client= con.client_boto()
s3 = service_s3.connect_S3(client = client,
                      bucket = bucket, verbose = True) 
glue = service_glue.connect_glue(client = client) 
```

```python
pandas_setting = True
if pandas_setting:
    cm = sns.light_palette("green", as_cmap=True)
    pd.set_option('display.max_columns', None)
    pd.set_option('display.max_colwidth', None)
```

# Prepare query 

Write query and save the CSV back in the S3 bucket `datalake-datascience` 


# Steps


# Table `XX`

- Table name: `XX`


Choose a location in S3 to save the CSV. It is recommended to save in it the `datalake-datascience` bucket. Locate an appropriate folder in the bucket, and make sure all output have the same format

```python
db = 'firms_survey'
s3_output = 'SQL_OUTPUT_ATHENA'
```

### Brief analysis

- Count size of:
    - `year` in 1998 to 2007
    - `firms` has no digit
    - `citycode` 
    - `cic`
    - `setup`

```python
query = """
SELECT year, COUNT(*) as CNT
FROM "firms_survey"."asif_unzip_data_csv"
WHERE year in ('1998', '1999', '2000', '2001', '2002', '2003', '2004', '2005', '2006', '2007')
  GROUP BY  year
  ORDER BY year
"""
output = s3.run_query(
                    query=query,
                    database=db,
                    s3_output=s3_output,
    filename = 'count_year'
                )
output
```

```python
query = """
WITH test AS (
SELECT digit, COUNT(digit) AS CNT
FROM (
SELECT regexp_like(firm, '\d+') as digit
FROM "firms_survey"."asif_unzip_data_csv"
WHERE year in ('1998', '1999', '2000', '2001', '2002', '2003', '2004', '2005', '2006', '2007')
)
GROUP BY digit
)
SELECT CNT, COUNT(CNT)
FROM test
GROUP BY CNT

"""
output = s3.run_query(
                    query=query,
                    database=db,
                    s3_output=s3_output,
    filename = 'count_digit'
                )
output
```

```python
query = """
SELECT len, COUNT(*) as CNT
FROM (
SELECT LENGTH(citycode) as len
FROM "firms_survey"."asif_unzip_data_csv"
  )
  GROUP BY len
  ORDER BY CNT
"""
output = s3.run_query(
                    query=query,
                    database=db,
                    s3_output=s3_output,
    filename = 'count_citycode'
                )
output
```

```python
query = """
SELECT len, COUNT(*) as CNT
FROM (
SELECT LENGTH(cic) as len
FROM "firms_survey"."asif_unzip_data_csv"
  )
  GROUP BY len
  ORDER BY CNT
"""
output = s3.run_query(
                    query=query,
                    database=db,
                    s3_output=s3_output,
    filename = 'count_cic'
                )
output
```

```python
query = """
SELECT len, COUNT(*) as CNT
FROM (
SELECT LENGTH(setup) as len
FROM "firms_survey"."asif_unzip_data_csv"
  )
  GROUP BY len
  ORDER BY CNT
"""
output = s3.run_query(
                    query=query,
                    database=db,
                    s3_output=s3_output,
    filename = 'count_setup'
                )
output
```

Clean up the folder with the previous csv file. Be careful, it will erase all files inside the folder

```python
s3_output = 'DATA/ECON/FIRM_SURVEY/ASIF_CHINA/PREPARED'
```

```python
s3.remove_all_bucket(path_remove = s3_output)
```

```python
%%time
query = """

SELECT *
FROM "firms_survey"."asif_unzip_data_csv"
WHERE 
 (
  year in ('1998', '1999', '2000', '2001', '2002', '2003', '2004', '2005', '2006', '2007')
  )
  AND 
  (
  LENGTH(citycode) = 4
  AND 
  LENGTH(setup) = 4
  AND 
  LENGTH(cic) <= 4
  AND 
  regexp_like(firm, '\d+') = TRUE
    )
  
"""
output = s3.run_query(
                    query=query,
                    database=db,
                    s3_output=s3_output
                )
```

```python
output
```

Need to remove the metadata generated by Athena. We remove it to avoid parsing incorrect value with the crawler

```python
s3.remove_file(key = os.path.join(s3_output, output['QueryID'] + ".csv.metadata"))
```

# Validate query

This step is mandatory to validate the query in the ETL. If you are not sure about the quality of the query, go to the next step.


To validate the query, please fillin the json below. Don't forget to change the schema so that the crawler can use it.

1. Add a partition key:
    - Inform if there is group in the table so that, the parser can compute duplicate
2. Add the steps number -> Not automtic yet. Start at 0
3. Change the schema if needed. It is highly recommanded to add comment to the fields
4. Provide a description -> detail the steps 


1. Add a partition key

```python
partition_keys = ['firm', 'year']
```

2. Add the steps number

```python
step = 0
```

3. Change the schema

We load the schema from the raw data

```python
DatabaseName = 'firms_survey'
table_name = 'asif_unzip_data_csv'
```

```python
schema = glue.get_table_information(
    database = DatabaseName,
    table = table_name
)['Table']['StorageDescriptor']['Columns']
schema
```

4. Provide a description

```python
description = """
Prepare ASIF raw data by removing unconsistent year format, industry and birth year
"""
```

5. provide metadata

- DatabaseName
- TablePrefix
- 

```python
TablePrefix = 'asif_firms_'
```

```python
json_etl = {
    'step': step,
    'description':description,
    'query':query,
    'schema': schema,
    'partition_keys':partition_keys,
    'metadata':{
    'DatabaseName' : DatabaseName,
    'TablePrefix' : TablePrefix,
    'target_S3URI' : os.path.join('s3://',bucket, s3_output),
    'from_athena': 'True'    
    }
}
```

```python
with open(os.path.join(str(Path(path).parent), 'parameters_ETL_Financial_dependency_pollution.json')) as json_file:
    parameters = json.load(json_file)
```

```python
#parameters['TABLES']['PREPARATION']['STEPS'].pop(0)
```

```python
parameters['TABLES']['PREPARATION']['STEPS'].append(json_etl)
```

Save JSON

```python
with open(os.path.join(str(Path(path).parent), 'parameters_ETL_Financial_dependency_pollution.json'), "w")as outfile:
    json.dump(parameters, outfile)
```

# Create or update the data catalog

The query is saved in the S3 (bucket `datalake-datascience`) but the table is not available yet in the Data Catalog. Use the function `create_table_glue` to generate the table and update the catalog.

Few parameters are required:

- name_crawler: Name of the crawler
- Role: Role to temporary provide an access tho the service
- DatabaseName: Name of the database to create the table
- TablePrefix: Prefix of the table. Full name of the table will be `TablePrefix` + folder name

To update the schema, please use the following structure

```
schema = [
    {
        "Name": "VAR1",
        "Type": "",
        "Comment": ""
    },
    {
        "Name": "VAR2",
        "Type": "",
        "Comment": ""
    }
]
```

```python
name_crawler = 'crawl-ASIF-prepared'
Role = 'arn:aws:iam::468786073381:role/AWSGlueServiceRole-crawler-datalake'
```

```python
target_S3URI = os.path.join('s3://',bucket, s3_output)
table_name = '{}{}'.format(TablePrefix, os.path.basename(target_S3URI).lower())
```

```python
glue.create_table_glue(
    target_S3URI,
    name_crawler,
    Role,
    DatabaseName,
    TablePrefix,
    from_athena=True,
    update_schema=schema,
)
```

## Check Duplicates

One of the most important step when creating a table is to check if the table contains duplicates. The cell below checks if the table generated before is empty of duplicates. The code uses the JSON file to create the query parsed in Athena. 

You are required to define the group(s) that Athena will use to compute the duplicate. For instance, your table can be grouped by COL1 and COL2 (need to be string or varchar), then pass the list ['COL1', 'COL2'] 

```python
partition_keys = ['firm', 'year']

with open(os.path.join(str(Path(path).parent), 'parameters_ETL_Financial_dependency_pollution.json')) as json_file:
    parameters = json.load(json_file)
```

```python
### COUNT DUPLICATES
if len(partition_keys) > 0:
    groups = ' , '.join(partition_keys)

    query_duplicates = parameters["ANALYSIS"]['COUNT_DUPLICATES']['query'].format(
                                DatabaseName,table_name,groups
                                )
    dup = s3.run_query(
                                query=query_duplicates,
                                database=DatabaseName,
                                s3_output="SQL_OUTPUT_ATHENA",
                                filename="duplicates_{}".format(table_name))
    display(dup)

```

# Analytics

In this part, we are providing basic summary statistic. Since we have created the tables, we can parse the schema in Glue and use our json file to automatically generates the analysis.

The cells below execute the job in the key `ANALYSIS`. You need to change the `primary_key` and `secondary_key` 


For a full analysis of the table, please use the following Lambda function. Be patient, it can takes between 5 to 30 minutes. Times varies according to the number of columns in your dataset.

Use the function as follow:

- `output_prefix`:  s3://datalake-datascience/ANALYTICS/OUTPUT/TABLE_NAME/
- `region`: region where the table is stored
- `bucket`: Name of the bucket
- `DatabaseName`: Name of the database
- `table_name`: Name of the table
- `group`: variables name to group to count the duplicates
- `primary_key`: Variable name to perform the grouping -> Only one variable for now
- `secondary_key`: Variable name to perform the secondary grouping -> Only one variable for now
- `proba`: Chi-square analysis probabilitity
- `y_var`: Continuous target variables

Check the job processing in Sagemaker: https://eu-west-3.console.aws.amazon.com/sagemaker/home?region=eu-west-3#/processing-jobs

The notebook is available: https://s3.console.aws.amazon.com/s3/buckets/datalake-datascience?region=eu-west-3&prefix=ANALYTICS/OUTPUT/&showversions=false

Please, download the notebook on your local machine, and convert it to HTML:

```
cd "/Users/thomas/Downloads/Notebook"
aws s3 cp s3://datalake-datascience/ANALYTICS/OUTPUT/asif_unzip_data_csv/Template_analysis_from_lambda-2020-11-22-08-12-20.ipynb .

## convert HTML no code
jupyter nbconvert --no-input --to html Template_analysis_from_lambda-2020-11-21-14-30-45.ipynb
jupyter nbconvert --to html Template_analysis_from_lambda-2020-11-22-08-12-20.ipynb
```

Then upload the HTML to: https://s3.console.aws.amazon.com/s3/buckets/datalake-datascience?region=eu-west-3&prefix=ANALYTICS/HTML_OUTPUT/

Add a new folder with the table name in upper case

```python
import boto3

key, secret_ = con.load_credential()
client_lambda = boto3.client(
    'lambda',
    aws_access_key_id=key,
    aws_secret_access_key=secret_,
    region_name = region)
```

```python
primary_key = 'year'
secondary_key = 'citycode'
y_var = 'output'
```

```python
payload = {
    "input_path": "s3://datalake-datascience/ANALYTICS/TEMPLATE_NOTEBOOKS/Template_analysis_from_lambda.ipynb",
    "output_prefix": "s3://datalake-datascience/ANALYTICS/OUTPUT/{}/".format(table_name.upper()),
    "parameters": {
        "region": "{}".format(region),
        "bucket": "{}".format(bucket),
        "DatabaseName": "{}".format(DatabaseName),
        "table_name": "{}".format(table_name),
        "group": "{}".format(','.join(partition_keys)),
        "primary_key": "{}".format(primary_key),
        "secondary_key": "{}".format(secondary_key),
        "y_var": "{}".format(y_var),
    },
}
```

```python
response = client_lambda.invoke(
    FunctionName='RunNotebook',
    InvocationType='RequestResponse',
    LogType='Tail',
    Payload=json.dumps(payload),
)
response
```

For a partial analysis, run the cells below


# Generation report

```python
import os, time, shutil, urllib, ipykernel, json
from pathlib import Path
from notebook import notebookapp
```

```python
def create_report(extension = "html", keep_code = False):
    """
    Create a report from the current notebook and save it in the 
    Report folder (Parent-> child directory)
    
    1. Exctract the current notbook name
    2. Convert the Notebook 
    3. Move the newly created report
    
    Args:
    extension: string. Can be "html", "pdf", "md"
    
    
    """
    
    ### Get notebook name
    connection_file = os.path.basename(ipykernel.get_connection_file())
    kernel_id = connection_file.split('-', 1)[0].split('.')[0]

    for srv in notebookapp.list_running_servers():
        try:
            if srv['token']=='' and not srv['password']:  
                req = urllib.request.urlopen(srv['url']+'api/sessions')
            else:
                req = urllib.request.urlopen(srv['url']+ \
                                             'api/sessions?token=' + \
                                             srv['token'])
            sessions = json.load(req)
            notebookname = sessions[0]['name']
        except:
            pass  
    
    sep = '.'
    path = os.getcwd()
    #parent_path = str(Path(path).parent)
    
    ### Path report
    #path_report = "{}/Reports".format(parent_path)
    #path_report = "{}/Reports".format(path)
    
    ### Path destination
    name_no_extension = notebookname.split(sep, 1)[0]
    source_to_move = name_no_extension +'.{}'.format(extension)
    dest = os.path.join(path,'Reports', source_to_move)
    
    ### Generate notebook
    if keep_code:
        os.system('jupyter nbconvert --to {} {}'.format(
    extension,notebookname))
    else:
        os.system('jupyter nbconvert --no-input --to {} {}'.format(
    extension,notebookname))
    
    ### Move notebook to report folder
    #time.sleep(5)
    shutil.move(source_to_move, dest)
    print("Report Available at this adress:\n {}".format(dest))
```

```python
create_report(extension = "html")
```
