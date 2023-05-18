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

# US Name
Data preparation Prepare firm financial ratio and TFP with pollution information

# Description
None
## Merge
**Main table** 
None
Merged with:
None
# Target
- The file is saved in S3:
- bucket: datalake-datascience
- path: DATA/ENVIRONMENT/CHINA/FINANCIAL_DEPENDENCY_POLLUTION
- Glue data catalog should be updated
- database: environment
- Table prefix: china_
- table name: china_financial_dependency_pollution
- Analytics
- HTML: ANALYTICS/HTML_OUTPUT/china_financial_dependency_pollution
- Notebook: ANALYTICS/OUTPUT/china_financial_dependency_pollution
# Metadata
- Key: 515_Financial_dependency_pollution
- Epic: Dataset transformation
- US: Prepare ASIF firm level data
- Task tag: #etl-Financial-dependency-pollution, #financial-data, #financial-ratio, #pollution-china, #prepare-table-Financial-dependency-pollution
- Analytics reports: https://htmlpreview.github.io/?https://github.com/thomaspernet/Financial_dependency_pollution/blob/master/00_data_catalog/HTML_ANALYSIS/china_FINANCIAL_DEPENDENCY_POLLUTION.html
# Input
## Table/file
**Name**
None
**Github**
- https://github.com/thomaspernet/Financial_dependency_pollution/blob/master/01_data_preprocessing/02_transform_tables/11_firm_pollution_financial_ratio_tfp.md
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
region = 'eu-west-2'
bucket = 'datalake-london'
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


# Steps

```python
DatabaseName = 'environment'
s3_output_example = 'SQL_OUTPUT_ATHENA'
```

# Table `XX`

Since the table to create has missing value, please use the following at the top of the query

```
CREATE TABLE database.table_name WITH (format = 'PARQUET') AS
```


Choose a location in S3 to save the CSV. It is recommended to save in it the `datalake-datascience` bucket. Locate an appropriate folder in the bucket, and make sure all output have the same format

```python
s3_output = 'DATA/ENVIRONMENT/CHINA/FINANCIAL_DEPENDENCY_POLLUTION'
table_name = 'china_financial_dependency_pollution'
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
WITH test AS (
  SELECT 
    *, 
    CASE WHEN LENGTH(cic) = 4 THEN substr(cic, 1, 2) ELSE concat(
      '0', 
      substr(cic, 1, 1)
    ) END AS indu_2, 
    c80 + c81 + c82 + c79 as current_asset, 
    c91 + c92 AS intangible, 
    tofixed - cudepre AS tangible, 
    tofixed - cudepre + (c91 + c92) AS net_non_current, 
    (
      c80 + c81 + c82 + c79 + tofixed - cudepre + (c91 + c92)
    ) - (c95 + c97 + c99) AS error, 
    c95 + c97 as total_liabilities, 
    CASE WHEN (
      c80 + c81 + c82 + c79 + tofixed - cudepre + (c91 + c92)
    ) - (c95 + c97 + c99) > 0 THEN (c95 + c97 + c99) + ABS(
      (
        c80 + c81 + c82 + c79 + tofixed - cudepre + (c91 + c92)
      ) - (c95 + c97 + c99)
    ) ELSE (c95 + c97 + c99) END AS total_right, 
    CASE WHEN (
      c80 + c81 + c82 + c79 + tofixed - cudepre + (c91 + c92)
    ) - (c95 + c97 + c99) < 0 THEN (
      c80 + c81 + c82 + c79 + tofixed - cudepre + (c91 + c92)
    ) + ABS(
      (
        c80 + c81 + c82 + c79 + tofixed - cudepre + (c91 + c92)
      ) - (c95 + c97 + c99)
    ) ELSE (
      c80 + c81 + c82 + c79 + tofixed - cudepre + (c91 + c92)
    ) END AS total_asset, 
    (c131 - c134) + cudepre as cashflow 
  FROM 
    environment.china_firm_pollution_data 
    INNER JOIN (
      SELECT 
        extra_code, 
        geocode4_corr, 
        province_en 
      FROM 
        chinese_lookup.china_city_code_normalised 
      GROUP BY 
        extra_code, 
        province_en, 
        geocode4_corr
    ) as no_dup_citycode ON china_firm_pollution_data.citycode = no_dup_citycode.extra_code 
  WHERE 
    c95 > 0 -- current liabilities
    AND c97 > 0 -- long term liabilities
    AND c98 > 0 -- total liabilities
    AND c99 > 0 -- equity
    AND c80 + c81 + c82 + c79 > 0 
    AND tofixed > 0 
    AND output > 0 
    and employ > 0
) 
SELECT 
  test.year, 
  test.firm, 
  name, 
  test.geocode4_corr, 
  province_en, 
  cic_adj, 
  "cic03", 
  indu_2, 
  ownership_new, 
  "age", 
  "tfp_op", 
  "tfp_lp", 
  CAST(
    (output) AS DECIMAL(16, 5)
  ) AS output, 
  CAST(
    (outputdefl) AS DECIMAL(16, 5)
  ) AS outputdefl, 
  CAST(
    (sales) AS DECIMAL(16, 5)
  ) AS sales, 
  CAST(
    (employ) AS DECIMAL(16, 5)
  ) AS employment, 
  CAST(
    (captal) AS DECIMAL(16, 5)
  ) AS capital, 
  (current_asset) AS current_asset, 
  (tofixed) AS tofixed, 
  (error) AS error, 
  (total_liabilities) AS total_liabilities, 
  (total_asset) AS total_asset, 
  (total_right) AS total_right, 
  (intangible) AS intangible, 
  (tangible) AS tangible, 
  (net_non_current) AS net_non_current, 
  (cashflow) AS cashflow, 
  CAST(
    (c80 + c81 + c82 + c79) AS DECIMAL(16, 5)
  ) / NULLIF(
    CAST(
      (c95) AS DECIMAL(16, 5)
    ), 
    0
  ) AS current_ratio, 
  CAST(
    (c80 + c81 + c82 + c79 - c80 - c81) AS DECIMAL(16, 5)
  ) / NULLIF(
    CAST(
      (c95) AS DECIMAL(16, 5)
    ), 
    0
  ) AS quick_ratio, 
  CAST(
    (c98) AS DECIMAL(16, 5)
  ) / NULLIF(
    CAST(
      (total_asset) AS DECIMAL(16, 5)
    ), 
    0
  ) AS liabilities_tot_asset, 
  CAST(
    (sales) AS DECIMAL(16, 5)
  ) / NULLIF(
    CAST(
      (total_asset) AS DECIMAL(16, 5)
    ), 
    0
  ) AS sales_tot_asset, 
  CAST(
    (c84) AS DECIMAL(16, 5)
  ) / NULLIF(
    CAST(
      (total_asset) AS DECIMAL(16, 5)
    ), 
    0
  ) AS investment_tot_asset, 
  CAST(
    (rdfee) AS DECIMAL(16, 5)
  ) / NULLIF(
    CAST(
      (total_asset) AS DECIMAL(16, 5)
    ), 
    0
  ) AS rd_tot_asset, 
  CAST(
    (tangible) AS DECIMAL(16, 5)
  ) / NULLIF(
    CAST(
      (total_asset) AS DECIMAL(16, 5)
    ), 
    0
  ) asset_tangibility_tot_asset, 
  CAST(
    (cashflow) AS DECIMAL(16, 5)
  ) / NULLIF(
    CAST(
      (total_asset) AS DECIMAL(16, 5)
    ), 
    0
  ) AS cashflow_tot_asset, 
  CAST(
    (cashflow) AS DECIMAL(16, 5)
  ) / NULLIF(
    CAST(
      (tangible) AS DECIMAL(16, 5)
    ), 
    0
  ) AS cashflow_to_tangible, 
  -- update
  CAST(
    (c131) AS DECIMAL(16, 5)
  ) / NULLIF(
    CAST(
      (sales) AS DECIMAL(16, 5)
    ), 
    0
  ) AS return_to_sale, 
  CAST(
    (c131) AS DECIMAL(16, 5)
  ) / NULLIF(
    CAST(
      (c125) AS DECIMAL(16, 5)
    ), 
    0
  ) AS coverage_ratio, 
  CAST(
    (current_asset - c95) AS DECIMAL(16, 5)
  ) / NULLIF(
    CAST(
      (tangible) AS DECIMAL(16, 5)
    ), 
    0
  ) AS liquidity, 
  "total_industrialwater_used", 
  "total_freshwater_used", 
  "gyqs", 
  "total_repeatedwater_used", 
  "total_coal_used", 
  "rlmxf", 
  "ylmxf", 
  "rlmpjlf", 
  "rlyxf", 
  "zyxf", 
  "cyxf", 
  "rlypjlf", 
  "zypjlf", 
  "clean_gas_used", 
  "waste_water", 
  "cod", 
  "ad", 
  "waste_gas", 
  "so2", 
  "nox", 
  "smoke_dust", 
  "soot", 
  "yfc", 
  "gyfscll", 
  "hxxyqcl", 
  "xzssqcl", 
  "adqcl", 
  "eyhlqcl", 
  "dyhwqcl", 
  "ycqcl", 
  "gyfcqcl", 
  "dwastewater_equip", 
  "fszlssnl", 
  "fszlssfee", 
  "dwastegas_equip", 
  "dso2_equip", 
  "fqzlssnl", 
  "tlssnl", 
  "hxxycsl", 
  "adcsl", 
  "eyhlcsl", 
  "dyhwcsl", 
  "yfccsl" 
FROM 
  test 
  INNER JOIN (
    SELECT 
      "firm", 
      "year", 
      "citycode_asifad" as geocode4_corr, 
      "tfp_op", 
      "tfp_lp" 
    FROM 
      firms_survey.firm_tfp_china
  ) as tfp_table on test.firm = tfp_table.firm 
  and test.geocode4_corr = tfp_table.geocode4_corr 
  and test.year = tfp_table.year 
WHERE 
  --test.year in (
  --  '2000', '2001', '2002', '2003', '2004', 
  --  '2005', '2006', '2007'
  --) AND
  total_asset > 0 
  AND tangible > 0
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

```python
query_count = """
SELECT year, COUNT(*) AS CNT
FROM {}.{} 
GROUP BY year
ORDER BY year
""".format(DatabaseName, table_name)
output = s3.run_query(
                    query=query_count,
                    database=DatabaseName,
                    s3_output=s3_output_example,
    filename = 'count_{}'.format(table_name)
                )
output
```

```python
query_count = """
SELECT year, COUNT(DISTINCT(firm)) AS CNT
FROM {}.{} 
GROUP BY year
ORDER BY year
""".format(DatabaseName, table_name)
output = s3.run_query(
                    query=query_count,
                    database=DatabaseName,
                    s3_output=s3_output_example,
    filename = 'count_{}'.format(table_name)
                )
output
```

# Update Glue catalogue and Github

This step is mandatory to validate the query in the ETL.


## Create or update the data catalog

The query is saved in the S3 (bucket `datalake-london`), but the comments are not available. Use the functions below to update the catalogue and Github


Update the dictionary

- DatabaseName:
- TableName:
- ~TablePrefix:~
- input: 
- filename: Name of the notebook or Python script: to indicate
- Task ID: from Coda
- index_final_table: a list to indicate if the current table is used to prepare the final table(s). If more than one, pass the index. Start at 0
- if_final: A boolean. Indicates if the current table is the final table -> the one the model will be used to be trained
- schema: glue schema with comment
- description: details query objective

**Update schema**

If `automatic = False` in `automatic_update`, then the function returns only the variables to update the comments. Manually add the comment, **then**, pass the new schema (only the missing comment) to the argument `new_schema`. 

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
%load_ext autoreload
%autoreload 2
import sys
sys.path.append(os.path.join(parent_path, 'utils'))
import make_toc
import create_schema
import create_report
import update_glue_github
```

<!-- #region nteract={"transient": {"deleting": false}} -->
The function below manages everything automatically. If the final table comes from more than one query, then pass a list of table in `list_tables` instead of `automatic`
<!-- #endregion -->

```python nteract={"transient": {"deleting": false}} tags=[]
list_input,  schema = update_glue_github.automatic_update(
    list_tables = 'automatic',
    automatic= True,
    new_schema = None, ### override schema
    client = client,
    TableName = table_name,
    query = query)
```

```python
description = """

"""
name_json = 'parameters_ETL_Template.json'
partition_keys = ["XX"]
notebookname = "XX.ipynb"
dic_information = {
    "client":client,
    'bucket':bucket,
    's3_output':s3_output,
    'DatabaseName':DatabaseName,
    'TableName':table_name,
    'name_json':name_json,
    'partition_keys':partition_keys,
    'notebookname':notebookname,
    'index_final_table':[0],
    'if_final': 'True',
    'schema':schema,
    'description':description,
    'query':query,
    "list_input":list_input,
    'list_input_automatic':True
}
```

```python
update_glue_github.update_glue_github(client = client,dic_information = dic_information)
```

## Check Duplicates

One of the most important step when creating a table is to check if the table contains duplicates. The cell below checks if the table generated before is empty of duplicates. The code uses the JSON file to create the query parsed in Athena. 

You are required to define the group(s) that Athena will use to compute the duplicate. For instance, your table can be grouped by COL1 and COL2 (need to be string or varchar), then pass the list ['COL1', 'COL2'] 

```python
update_glue_github.find_duplicates(
    client = client,
    bucket = bucket,
    name_json = name_json,
    partition_keys = partition_keys,
    TableName= table_name
)
```

## Count missing values

```python
update_glue_github.count_missing(client = client, name_json = name_json, bucket = bucket,TableName = table_name)
```

# Update Github Data catalog

The data catalog is available in Glue. Although, we might want to get a quick access to the tables in Github. In this part, we are generating a `README.md` in the folder `00_data_catalogue`. All tables used in the project will be added to the catalog. We use the ETL parameter file and the schema in Glue to create the README. 

Bear in mind the code will erase the previous README. 

```python
create_schema.make_data_schema_github(name_json = name_json)
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
primary_key = ''
secondary_key = ''
y_var = ''
```

```python
payload = {
    "input_path": "s3://datalake-london/ANALYTICS/TEMPLATE_NOTEBOOKS/template_analysis_from_lambda.ipynb",
    "output_prefix": "s3://datalake-london/ANALYTICS/OUTPUT/{}/".format(table_name.upper()),
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
```

```python
create_report.create_report(extension = "html", keep_code = True, notebookname =  notebookname)
```

```python
create_schema.create_schema(path_json, path_save_image = os.path.join(parent_path, 'utils'))
```

```python
### Update TOC in Github
for p in [parent_path,
          str(Path(path).parent),
          os.path.join(str(Path(path).parent), "00_download_data"),
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
