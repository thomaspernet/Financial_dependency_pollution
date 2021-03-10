---
jupyter:
  jupytext:
    formats: ipynb,md
    text_representation:
      extension: .md
      format_name: markdown
      format_version: '1.2'
      jupytext_version: 1.8.0
  kernel_info:
    name: python3
  kernelspec:
    display_name: Python 3
    language: python
    name: python3
---

<!-- #region -->
# US Name

Transform china city sector pollution and others data by merging china city code normalised data by constructing polluted sectors city-industry-year (Create polluted sectors at city-industry-year) to china city sector year pollution threshold 

# Business needs 

Transform china city sector pollution and others data by merging china city code normalised data by constructing polluted sectors city-industry-year (Compute polluted sectors at decile .5, .75, .80, .85, .90 and .95 at the city-industry-year level) to china city sector year pollution threshold 

## Description
### Objective 

Use existing tables china city sector pollution, china city code normalised to constructing a bunch of variables listed below

# Construction variables 

- polluted sectors city-industry-year

### Steps 




**Cautious**
Make sure there is no duplicates

# Target

- The file is saved in S3:
- bucket: datalake-datascience
- path: DATA/ENVIRONMENT/CHINA/CITY_SECTOR_YEAR_POLLUTION_THRESHOLD
- Glue data catalog should be updated
- database: environment
- Table prefix: china_
- table name: china_city_sector_year_pollution_threshold
- Analytics
- HTML: ANALYTICS/HTML_OUTPUT/china_city_sector_year_pollution_threshold
- Notebook: ANALYTICS/OUTPUT/china_city_sector_year_pollution_threshold

# Metadata

- Key: 256_Financial_dependency_pollution
- Epic: Dataset transformation
- US: Polluted sectors
- Task tag: #polluted-sector, #city-industry-year
- Analytics reports: https://htmlpreview.github.io/?https://github.com/thomaspernet/Financial_dependency_pollution/blob/master/00_data_catalogue/HTML_ANALYSIS/CHINA_CITY_SECTOR_YEAR_POLLUTION_THRESHOLD.html

# Input Cloud Storage

## Table/file

**Name** 

- DATA/ENVIRONMENT/CHINA/CITY_SECTOR_POLLUTION
- DATA/ECON/LOOKUP_DATA/CITY_CODE_NORMALISED

**Github**

- https://github.com/thomaspernet/Financial_dependency_pollution/blob/master/01_data_preprocessing/00_download_data_from/CITY_SECTOR_POLLUTION/city_sector_pollution.py
- https://github.com/thomaspernet/Financial_dependency_pollution/blob/master/01_data_preprocessing/00_download_data_from/CITY_CODE_CORRESPONDANCE/city_code_correspondance.py

# Destination Output/Delivery

## Table/file

**Name**

china_city_sector_year_pollution_threshold

**GitHub**

- https://github.com/thomaspernet/Financial_dependency_pollution/blob/master/01_data_preprocessing/02_transform_tables/10_city_sector_year_polluted_sectors.md
<!-- #endregion -->
```python inputHidden=false jupyter={"outputs_hidden": false} outputHidden=false
from awsPy.aws_authorization import aws_connector
from awsPy.aws_s3 import service_s3
from awsPy.aws_glue import service_glue
from pathlib import Path
import pandas as pd
import numpy as np
import seaborn as sns
import os, shutil, json, re

path = os.getcwd()
parent_path = str(Path(path).parent.parent)


name_credential = 'financial_dep_SO2_accessKeys.csv'
region = 'eu-west-3'
bucket = 'datalake-datascience'
path_cred = "{0}/creds/{1}".format(parent_path, name_credential)
```

```python inputHidden=false jupyter={"outputs_hidden": false} outputHidden=false
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

```python
DatabaseName = 'environment'
s3_output_example = 'SQL_OUTPUT_ATHENA'
```

# Table `china_city_sector_year_pollution_threshold`

Since the table to create has missing value, please use the following at the top of the query

```
CREATE TABLE database.table_name WITH (format = 'PARQUET') AS
```


Choose a location in S3 to save the CSV. It is recommended to save in it the `datalake-datascience` bucket. Locate an appropriate folder in the bucket, and make sure all output have the same format

```python
s3_output = 'DATA/ENVIRONMENT/CHINA/CITY_SECTOR_YEAR_POLLUTION_THRESHOLD'
table_name = 'china_city_sector_year_pollution_threshold'
```

First, we need to delete the table (if exist)

```python
try:
    response = glue.delete_table(
        database=DatabaseName,
        table=table_name
    )
    print(response)
except Exception as e:
    print(e)
```

Clean up the folder with the previous csv file. Be careful, it will erase all files inside the folder

```python
s3.remove_all_bucket(path_remove = s3_output)
```

```python
%%time
query = """
CREATE TABLE {0}.{1} WITH (format = 'PARQUET') AS
WITH agg_ind2 AS (
  SELECT 
    year, 
    geocode4_corr, 
    ind2, 
    SUM(tso2) as tso2 
  FROM 
    (
      SELECT 
        year, 
        province_en, 
        citycode, 
        geocode4_corr, 
        china_city_sector_pollution.cityen, 
        ind2, 
        tso2 
      FROM 
        environment.china_city_sector_pollution 
        INNER JOIN (
          SELECT 
            extra_code, 
            geocode4_corr, 
            province_en 
          FROM 
            chinese_lookup.china_city_code_normalised 
          GROUP BY 
            extra_code, 
            geocode4_corr, 
            province_en
        ) as no_dup_citycode ON china_city_sector_pollution.citycode = no_dup_citycode.extra_code
    ) 
  GROUP BY 
    year, 
    geocode4_corr, 
    ind2
) 
SELECT 
  agg_ind2.year,
  agg_ind2.geocode4_corr,
  ind2, 
  tso2, 
  pct_50_tso2_cit,
  pct_75_tso2_cit, 
  pct_80_tso2_cit, 
  pct_85_tso2_cit, 
  pct_90_tso2_cit,
  pct_95_tso2_cit,
  avg_tso2_cit, 
  CASE WHEN tso2 > pct_50_tso2_cit THEN 'ABOVE' ELSE 'BELOW' END AS polluted_d50_cit,
  CASE WHEN tso2 > pct_75_tso2_cit THEN 'ABOVE' ELSE 'BELOW' END AS polluted_d75_cit,
  CASE WHEN tso2 > pct_80_tso2_cit THEN 'ABOVE' ELSE 'BELOW' END AS polluted_d80_cit,
  CASE WHEN tso2 > pct_85_tso2_cit THEN 'ABOVE' ELSE 'BELOW' END AS polluted_d85_cit,
  CASE WHEN tso2 > pct_90_tso2_cit THEN 'ABOVE' ELSE 'BELOW' END AS polluted_d90_cit,
  CASE WHEN tso2 > pct_95_tso2_cit THEN 'ABOVE' ELSE 'BELOW' END AS polluted_d95_cit,
  CASE WHEN tso2 > avg_tso2_cit THEN 'ABOVE' ELSE 'BELOW' END AS polluted_m_cit
FROM 
  agg_ind2 
  LEFT JOIN (
    SELECT 
      year,
      geocode4_corr,
      approx_percentile(tso2,.50) AS pct_50_tso2_cit,
      approx_percentile(tso2,.75) AS pct_75_tso2_cit, 
      approx_percentile(tso2,.80) AS pct_80_tso2_cit, 
      approx_percentile(tso2,.85) AS pct_85_tso2_cit, 
      approx_percentile(tso2,.90) AS pct_90_tso2_cit,
      approx_percentile(tso2,.95) AS pct_95_tso2_cit,
      AVG(tso2) AS avg_tso2_cit
    FROM 
      agg_ind2 
    WHERE 
      tso2 > 0 
    GROUP BY 
      year, geocode4_corr 
    ORDER BY 
      year
  ) as threshold ON agg_ind2.year = threshold.year 
  and agg_ind2.geocode4_corr = threshold.geocode4_corr 
  ORDER BY year, ind2, geocode4_corr

""".format(DatabaseName, table_name)
output = s3.run_query(
                    query=query,
                    database=DatabaseName,
                    s3_output=s3_output,
                )
output
```

```python
query_count = """
SELECT COUNT(*) AS CNT
FROM {}.{} 
""".format(DatabaseName, table_name)
output = s3.run_query(
                    query=query_count,
                    database=DatabaseName,
                    s3_output=s3_output_example,
    filename = 'count_{}'.format(table_name)
                )
output
```

# Validate query

This step is mandatory to validate the query in the ETL. If you are not sure about the quality of the query, go to the next step.


To validate the query, please fillin the json below. Don't forget to change the schema so that the crawler can use it.

1. Change the schema if needed. It is highly recommanded to add comment to the fields
2. Add a partition key:
    - Inform if there is group in the table so that, the parser can compute duplicate
3. Provide a description -> detail the steps 


1. Change the schema

Bear in mind that CSV SerDe (OpenCSVSerDe) does not support empty fields in columns defined as a numeric data type. All columns with missing values should be saved as string. 

```python
glue.get_table_information(
    database = DatabaseName,
    table = table_name)['Table']['StorageDescriptor']['Columns']
```

```python
schema = [{'Name': 'year', 'Type': 'string', 'Comment': ''},
          {'Name': 'geocode4_corr', 'Type': 'string', 'Comment': 'city code'},
 {'Name': 'ind2', 'Type': 'string', 'Comment': ''},
 {'Name': 'tso2', 'Type': 'bigint', 'Comment': ''},
 {'Name': 'pct_50_tso2_cit', 'Type': 'bigint', 'Comment': 'Yearly 50th percentile of SO2'},
 {'Name': 'pct_75_tso2_cit', 'Type': 'bigint', 'Comment': 'Yearly 75th percentile of SO2'},
 {'Name': 'pct_80_tso2_cit', 'Type': 'bigint', 'Comment': 'Yearly 80th percentile of SO2'},
 {'Name': 'pct_85_tso2_cit', 'Type': 'bigint', 'Comment': 'Yearly 85th percentile of SO2'},
 {'Name': 'pct_90_tso2_cit', 'Type': 'bigint', 'Comment': 'Yearly 90th percentile of SO2'},
 {'Name': 'pct_95_tso2_cit', 'Type': 'bigint', 'Comment': 'Yearly 95th percentile of SO2'},
 {'Name': 'avg_tso2_cit', 'Type': 'double', 'Comment': 'Yearly average of SO2'},
 {'Name': 'polluted_d50_cit', 'Type': 'varchar(5)', 'Comment': 'Sectors with values above Yearly 50th percentile of SO2 label as ABOVE else BELOW'},
 {'Name': 'polluted_d80_cit', 'Type': 'varchar(5)', 'Comment': 'Sectors with values above Yearly 80th percentile of SO2 label as ABOVE else BELOW'},
 {'Name': 'polluted_d85_cit', 'Type': 'varchar(5)', 'Comment': 'Sectors with values above Yearly 85th percentile of SO2 label as ABOVE else BELOW'}, 
 {'Name': 'polluted_d90_cit', 'Type': 'varchar(5)', 'Comment': 'Sectors with values above Yearly 90th percentile of SO2 label as ABOVE else BELOW'},
 {'Name': 'polluted_d95_cit', 'Type': 'varchar(5)', 'Comment': 'Sectors with values above Yearly 95th percentile of SO2 label as ABOVE else BELOW'},         
 {'Name': 'polluted_m_cit', 'Type': 'varchar(5)', 'Comment': 'Sectors with values above Yearly average of SO2 label as ABOVE else BELOW'}]
```

2. Provide a description

```python
description = """
Transform china city sector pollution and others data by merging china city code normalised 
data by constructing polluted sectors city-industry-year (Compute polluted sectors at 
decile .5, .75, .80, .85, .90 and .95 at the city-industry-year level) 
to china city sector year pollution threshold
"""
```

3. provide metadata

- DatabaseName:
- TablePrefix:
- input: 
- filename: Name of the notebook or Python script: to indicate
- Task ID: from Coda
- index_final_table: a list to indicate if the current table is used to prepare the final table(s). If more than one, pass the index. Start at 0
- if_final: A boolean. Indicates if the current table is the final table -> the one the model will be used to be trained

```python
name_json = 'parameters_ETL_Financial_dependency_pollution.json'
path_json = os.path.join(str(Path(path).parent.parent), 'utils',name_json)
```

```python
with open(path_json) as json_file:
    parameters = json.load(json_file)
```

```python
partition_keys = ['year', 'geocode4_corr','ind2']
notebookname =  "10_city_sector_year_polluted_sectors.ipynb"
index_final_table = [0]
if_final = 'False'
```

```python
github_url = os.path.join(
    "https://github.com/",
    parameters['GLOBAL']['GITHUB']['owner'],
    parameters['GLOBAL']['GITHUB']['repo_name'],
    "blob/master",
    re.sub(parameters['GLOBAL']['GITHUB']['repo_name'],
           '', re.sub(
               r".*(?={})".format(parameters['GLOBAL']['GITHUB']['repo_name'])
               , '', path))[1:],
    re.sub('.ipynb','.md',notebookname)
)
```

Grab the input name from query

```python
list_input = []
tables = glue.get_tables(full_output = False)
regex_matches = re.findall(r'(?=\.).*?(?=\s)|(?=\.\").*?(?=\")', query)
for i in regex_matches:
    cleaning = i.lstrip().rstrip().replace('.', '').replace('"', '')
    if cleaning in tables and cleaning != table_name:
        list_input.append(cleaning)
```

```python
json_etl = {
    'description': description,
    'query': query,
    'schema': schema,
    'partition_keys': partition_keys,
    'metadata': {
        'DatabaseName': DatabaseName,
        'TableName': table_name,
        'input': list_input,
        'target_S3URI': os.path.join('s3://', bucket, s3_output),
        'from_athena': 'True',
        'filename': notebookname,
        'index_final_table' : index_final_table,
        'if_final': if_final,
         'github_url':github_url
    }
}
json_etl['metadata']
```

Remove the table name from the current file (if exist)


**Chose carefully PREPARATION or TRANSFORMATION**

```python
index_to_remove = next(
                (
                    index
                    for (index, d) in enumerate(parameters['TABLES']['TRANSFORMATION']['STEPS'])
                    if d['metadata']['TableName'] == table_name
                ),
                None,
            )
if index_to_remove != None:
    parameters['TABLES']['TRANSFORMATION']['STEPS'].pop(index_to_remove)

parameters['TABLES']['TRANSFORMATION']['STEPS'].append(json_etl)
```

```python
print("Currently, the ETL has {} tables".format(len(parameters['TABLES']['TRANSFORMATION']['STEPS'])))
```

Save JSON

```python
with open(path_json, "w") as json_file:
    json.dump(parameters, json_file)
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
glue.update_schema_table(
    database = DatabaseName,
    table = table_name,
    schema= schema)
```

## Check Duplicates

One of the most important step when creating a table is to check if the table contains duplicates. The cell below checks if the table generated before is empty of duplicates. The code uses the JSON file to create the query parsed in Athena. 

You are required to define the group(s) that Athena will use to compute the duplicate. For instance, your table can be grouped by COL1 and COL2 (need to be string or varchar), then pass the list ['COL1', 'COL2'] 

```python
with open(path_json) as json_file:
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

## Count missing values

```python
#table = 'XX'
schema = glue.get_table_information(
    database = DatabaseName,
    table = table_name
)['Table']
```

```python
from datetime import date
today = date.today().strftime('%Y%M%d')
```

```python
table_top = parameters["ANALYSIS"]["COUNT_MISSING"]["top"]
table_middle = ""
table_bottom = parameters["ANALYSIS"]["COUNT_MISSING"]["bottom"].format(
    DatabaseName, table_name
)

for key, value in enumerate(schema["StorageDescriptor"]["Columns"]):
    if key == len(schema["StorageDescriptor"]["Columns"]) - 1:

        table_middle += "{} ".format(
            parameters["ANALYSIS"]["COUNT_MISSING"]["middle"].format(value["Name"])
        )
    else:
        table_middle += "{} ,".format(
            parameters["ANALYSIS"]["COUNT_MISSING"]["middle"].format(value["Name"])
        )
query = table_top + table_middle + table_bottom
output = s3.run_query(
    query=query,
    database=DatabaseName,
    s3_output="SQL_OUTPUT_ATHENA",
    filename="count_missing",  ## Add filename to print dataframe
    destination_key=None,  ### Add destination key if need to copy output
)
display(
    output.T.rename(columns={0: "total_missing"})
    .assign(total_missing_pct=lambda x: x["total_missing"] / x.iloc[0, 0])
    .sort_values(by=["total_missing"], ascending=False)
    .style.format("{0:,.2%}", subset=["total_missing_pct"])
    .bar(subset="total_missing_pct", color=["#d65f5f"])
)
```

# Update Github Data catalog

The data catalog is available in Glue. Although, we might want to get a quick access to the tables in Github. In this part, we are generating a `README.md` in the folder `00_data_catalogue`. All tables used in the project will be added to the catalog. We use the ETL parameter file and the schema in Glue to create the README. 

Bear in mind the code will erase the previous README. 

```python
README = """
# Data Catalogue

{}

    """

top_readme = """

## Table of Content

    """

template = """

## Table {0}

- Database: {1}
- S3uri: `{2}`
- Partitition: {3}
- Script: {5}

{4}

    """
github_link = os.path.join("https://github.com/", parameters['GLOBAL']['GITHUB']['owner'],
                           parameters['GLOBAL']['GITHUB']['repo_name'], "tree/master/00_data_catalogue#table-")
for key, value in parameters['TABLES'].items():
    if key == 'CREATION':
        param = 'ALL_SCHEMA'
    else:
        param = 'STEPS'
        
    for schema in parameters['TABLES'][key][param]:
        description = schema['description']
        DatabaseName = schema['metadata']['DatabaseName']
        target_S3URI = schema['metadata']['target_S3URI']
        partition = schema['partition_keys']
        script = schema['metadata']['github_url']
        
        if param =='ALL_SCHEMA':
            table_name_git = '{}{}'.format(
                schema['metadata']['TablePrefix'],
                os.path.basename(schema['metadata']['target_S3URI']).lower()
            )
        else:
            try:
                table_name_git = schema['metadata']['TableName']
            except:
                table_name_git = '{}{}'.format(
                schema['metadata']['TablePrefix'],
                os.path.basename(schema['metadata']['target_S3URI']).lower()
            )
        
        tb = pd.json_normalize(schema['schema']).to_markdown()
        toc = "{}{}".format(github_link, table_name_git)
        top_readme += '\n- [{0}]({1})'.format(table_name_git, toc)

        README += template.format(table_name_git,
                                  DatabaseName,
                                  target_S3URI,
                                  partition,
                                  tb,
                                  script
                                  )
README = README.format(top_readme)
with open(os.path.join(str(Path(path).parent.parent), '00_data_catalogue/README.md'), "w") as outfile:
    outfile.write(README)
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
secondary_key = 'polluted_d85_cit'
y_var = 'tso2'
```

```python
payload = {
    "input_path": "s3://datalake-datascience/ANALYTICS/TEMPLATE_NOTEBOOKS/template_analysis_from_lambda.ipynb",
    "output_prefix": "s3://datalake-datascience/ANALYTICS/OUTPUT/{}/".format(table_name.upper()),
    "parameters": {
        "region": "{}".format(region),
        "bucket": "{}".format(bucket),
        "DatabaseName": "{}".format(DatabaseName),
        "table_name": "{}".format(table_name),
        "group": "{}".format(','.join(partition_keys)),
        "keys": "{},{}".format(primary_key,secondary_key),
        "y_var": "{}".format(y_var),
        "threshold":0
    },
}
payload
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

# Generation report

```python
import os, time, shutil, urllib, ipykernel, json
from pathlib import Path
from notebook import notebookapp
import sys
sys.path.append(os.path.join(parent_path, 'utils'))
import make_toc
import create_schema
```

```python
def create_report(extension = "html", keep_code = False, notebookname = None):
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
            notebookname = notebookname  
    
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
create_report(extension = "html", keep_code = True, notebookname =  notebookname)
```

```python
create_schema.create_schema(path_json, path_save_image = os.path.join(parent_path, 'utils'))
```

```python
### Update TOC in Github
for p in [parent_path,
          str(Path(path).parent),
          os.path.join(str(Path(path).parent), "00_download_data_from"),
          #os.path.join(str(Path(path).parent.parent), "02_data_analysis"),
          #os.path.join(str(Path(path).parent.parent), "02_data_analysis", "00_statistical_exploration"),
          #os.path.join(str(Path(path).parent.parent), "02_data_analysis", "01_model_estimation"),
         ]:
    try:
        os.remove(os.path.join(p, 'README.md'))
    except:
        pass
    path_parameter = os.path.join(parent_path,'utils', name_json)
    md_lines =  make_toc.create_index(cwd = p, path_parameter = path_parameter)
    md_out_fn = os.path.join(p,'README.md')
    
    if p == parent_path:
    
        make_toc.replace_index(md_out_fn, md_lines, Header = os.path.basename(p).replace('_', ' '), add_description = True, path_parameter = path_parameter)
    else:
        make_toc.replace_index(md_out_fn, md_lines, Header = os.path.basename(p).replace('_', ' '), add_description = False)
```
