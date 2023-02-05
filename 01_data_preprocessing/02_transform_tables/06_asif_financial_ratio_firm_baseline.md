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

Transform asif firms prepared and others data by merging china tcz spz and others data by constructing financial ratio and others (add firms ownership, keep firm size variables) to asif financial ratio baseline firm 

# Business needs 

Transform asif firms prepared and others data by merging china tcz spz, china city reduction mandate, china city code normalised, china credit constraint, ind cic 2 name data by constructing financial ratio, city-industry FE, city-year FE, industry-year FE, soe_vs_private, foreign_vs_domestic (add firms ownership soe and private and domestic and foreign, keep capital, output, sales and employment) to asif financial ratio baseline firm 

## Description
### Objective 

Use existing tables asif firms prepared, china credit constraint, ind cic 2 name, china city code normalised, china tcz spz, china city reduction mandate to constructing a bunch of variables listed below

# Construction variables 

- financial ratio
- city-industry FE
- city-year FE
- industry-year FE
- soe_vs_private
- foreign_vs_domestic

### Steps 




**Cautious**
Make sure there is no duplicates

# Target

- The file is saved in S3:
- bucket: datalake-datascience
- path: DATA/ECON/FIRM_SURVEY/ASIF_CHINA/TRANSFORMED/FINANCIAL_RATIO/FIRM
- Glue data catalog should be updated
- database: firms_survey
- Table prefix: asif_financial_ratio_baseline_
- table name: asif_financial_ratio_baseline_firm
- Analytics
- HTML: ANALYTICS/HTML_OUTPUT/asif_financial_ratio_baseline_firm
- Notebook: ANALYTICS/OUTPUT/asif_financial_ratio_baseline_firm

# Metadata

- Key: jxt46fwem20583a
- Epic: Dataset transformation
- US: Baseline
- Task tag: #financial-ratio, #firm-level
- Analytics reports: https://htmlpreview.github.io/?https://github.com/thomaspernet/Financial_dependency_pollution/blob/master/00_data_catalogue/HTML_ANALYSIS/ASIF_FINANCIAL_RATIO_BASELINE_FIRM.html

# Input Cloud Storage

## Table/file

**Name** 

- DATA/ECON/FIRM_SURVEY/ASIF_CHINA/PREPARED
- DATA/ECON/INDUSTRY/ADDITIONAL_DATA/CHINA/CIC/CREDIT_CONSTRAINT
- DATA/ECON/LOOKUP_DATA/CIC_2_NAME
- DATA/ECON/LOOKUP_DATA/CITY_CODE_NORMALISED
- DATA/ECON/POLICY/CHINA/STRUCTURAL_TRANSFORMATION/CITY_TARGET/TCZ_SPZ
- DATA/ENVIRONMENT/CHINA/FYP/CITY_REDUCTION_MANDATE

**Github**

- https://github.com/thomaspernet/Financial_dependency_pollution/blob/master/01_data_preprocessing/01_prepare_tables/00_prepare_asif.md
- https://github.com/thomaspernet/Financial_dependency_pollution/blob/master/01_data_preprocessing/00_download_data_from/CIC_CREDIT_CONSTRAINT/financial_dependency.py
- https://github.com/thomaspernet/Financial_dependency_pollution/blob/master/01_data_preprocessing/00_download_data_from/CIC_NAME/cic_industry_name.py
- https://github.com/thomaspernet/Financial_dependency_pollution/blob/master/01_data_preprocessing/00_download_data_from/CITY_CODE_CORRESPONDANCE/city_code_correspondance.py
- https://github.com/thomaspernet/Financial_dependency_pollution/blob/master/01_data_preprocessing/00_download_data_from/TCZ_SPZ/tcz_spz_policy.py
- https://github.com/thomaspernet/Financial_dependency_pollution/blob/master/01_data_preprocessing/00_download_data_from/CITY_REDUCTION_MANDATE/city_reduction_mandate.py

# Destination Output/Delivery

## Table/file

**Name**: 

asif_financial_ratio_baseline_firm

**GitHub**: 

- https://github.com/thomaspernet/Financial_dependency_pollution/blob/master/01_data_preprocessing/02_transform_tables/06_asif_financial_ratio_firm_baseline.md
<!-- #endregion -->
```python inputHidden=false jupyter={"outputs_hidden": false} outputHidden=false
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

1. construct the following financial ratio
  1. asset tangibility
  2. current ratio
  3. cash over total asset
    1. don’t use variable c79, missing year before 2004
  4. liabilities asset
  5. sales over total asset
2. Construct firms ownership
3. merge city characteristic (tcz, cpz), policy mandate and normalize city code


In the above table:

- avg_asset_tangibility_f: Average asset tangibility of the firm across all year
- avg_asset_tangibility: Average of `avg_asset_tangibility_f` across all firms
    - Vary across city and industry
- avg_large_f: condition if `avg_asset_tangibility_f` > `avg_asset_tangibility`
- sequence_asset: Array `avg_asset_tangibility_f`
- pct_asset_tangibility: Percentile of `avg_asset_tangibility_f` from all available firms
- large_f: condition if `avg_asset_tangibility_f` > `pct_asset_tangibility`



# Table `asif_financial_ratio_baseline_firm`

Since the table to create has missing value, please use the following at the top of the query

```
CREATE TABLE database.table_name WITH (format = 'PARQUET') AS
```


Choose a location in S3 to save the CSV. It is recommended to save in it the `datalake-datascience` bucket. Locate an appropriate folder in the bucket, and make sure all output have the same format

```python
DatabaseName = 'firms_survey'
s3_output_example = 'SQL_OUTPUT_ATHENA'
```

```python
s3_output = 'DATA/ECON/FIRM_SURVEY/ASIF_CHINA/TRANSFORMED/FINANCIAL_RATIO/FIRM'
table_name = 'asif_financial_ratio_baseline_firm'
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
    tofixed - cudepre  AS tangible, 
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
    firms_survey.asif_firms_prepared 
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
    ) as no_dup_citycode ON asif_firms_prepared.citycode = no_dup_citycode.extra_code 
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
  * 
FROM 
  (
    WITH ratio AS (
      SELECT 
        firm,
        year, 
        -- cic, 
        indu_2, 
        geocode4_corr, 
        province_en, 
        CAST(
          output AS DECIMAL(16, 5)
        ) AS output, 
        CAST(
          sales AS DECIMAL(16, 5)
        ) AS sales, 
        CAST(
          employ AS DECIMAL(16, 5)
        ) AS employment, 
        CAST(
          captal AS DECIMAL(16, 5)
        ) AS capital, 
        current_asset AS current_asset, 
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
        ) AS liquidity 
      FROM 
        test 
      WHERE 
        year in (
          '2000','2001', '2002', '2003', '2004', '2005', 
          '2006', '2007'
        ) 
        AND total_asset > 0 
        AND tangible > 0 
    ) 
    SELECT 
      firm,
      year, 
      indu_2, 
      geocode4_corr, 
      province_en, 
      output, 
      sales, 
      employment, 
      capital, 
      current_asset, 
      tofixed, 
      error, 
      total_liabilities, 
      total_asset, 
      total_right, 
      intangible, 
      tangible, 
      net_non_current, 
      cashflow, 
      current_ratio,
      LAG(current_ratio, 1) OVER (
        PARTITION BY firm,geocode4_corr, indu_2 
        ORDER BY 
          firm, year
      ) as lag_current_ratio,
      quick_ratio, 
      LAG(quick_ratio, 1) OVER (
        PARTITION BY firm, geocode4_corr, indu_2 
        ORDER BY 
          firm, year
      ) as lag_quick_ratio,
      liabilities_tot_asset, 
      LAG(liabilities_tot_asset, 1) OVER (
        PARTITION BY firm, geocode4_corr, indu_2 
        ORDER BY 
          firm, year
      ) as lag_liabilities_tot_asset,
      sales_tot_asset,
      LAG(sales_tot_asset, 1) OVER (
        PARTITION BY firm, geocode4_corr, indu_2 
        ORDER BY 
          firm, year
      ) as lag_sales_tot_asset,
      investment_tot_asset, 
      rd_tot_asset, 
      asset_tangibility_tot_asset, 
      cashflow_tot_asset,
      LAG(cashflow_tot_asset, 1) OVER (
        PARTITION BY firm, geocode4_corr, indu_2 
        ORDER BY 
          firm, year
      ) as lag_cashflow_tot_asset,
      cashflow_to_tangible, 
      LAG(cashflow_to_tangible, 1) OVER (
        PARTITION BY firm, geocode4_corr, indu_2 
        ORDER BY 
          firm, year
      ) as lag_cashflow_to_tangible,
      return_to_sale, 
      LAG(return_to_sale, 1) OVER (
        PARTITION BY firm, geocode4_corr, indu_2 
        ORDER BY 
         firm, year
      ) as lag_return_to_sale,
      coverage_ratio, 
      liquidity 
    FROM 
      ratio
  )
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

Test if the methodology works -> One firm one status


Number of observations per size


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
partition_keys = ["firm","year","cic","geocode4_corr"]
```

3. Change the schema

Bear in mind that CSV SerDe (OpenCSVSerDe) does not support empty fields in columns defined as a numeric data type. All columns with missing values should be saved as string. 

```python
glue.get_table_information(
    database = DatabaseName,
    table = table_name)['Table']['StorageDescriptor']['Columns']
```

```python
schema = [{'Name': 'output', 'Type': 'decimal(16,5)', 'Comment': 'Output'},
          {'Name': 'employment',
              'Type': 'decimal(16,5)', 'Comment': 'employment'},
          {'Name': 'capital', 'Type': 'decimal(16,5)', 'Comment': 'capital'},
          {'Name': 'current_asset', 'Type': 'int', 'Comment': 'current asset'},
          {'Name': 'net_non_current', 'Type': 'int', 'Comment': 'total net non current asset'},
          {'Name': 'error', 'Type': 'int',
           'Comment': 'difference between cuasset+tofixed and total liabilities +equity. Error makes the balance sheet equation right'},
          {'Name': 'total_liabilities', 'Type': 'int',
           'Comment': 'total adjusted liabilities'},
          {'Name': 'total_asset', 'Type': 'int',
              'Comment': 'total adjusted asset'},
          {'Name': 'current_liabilities', 'Type': 'int', 'Comment': 'current liabilities'},
 {'Name': 'lt_liabilities', 'Type': 'int', 'Comment': 'long term liabilities'},
 {'Name': 'from_asif_tot_liabilities', 'Type': 'int', 'Comment': 'total liabilities from asif not constructed'},
          {'Name': 'total_right', 'Type': 'int', 'Comment': 'Adjusted right part balance sheet'},
          {'Name': 'intangible', 'Type': 'int',
           'Comment': 'intangible asset measured as the sum of intangibles variables'},
          {'Name': 'tangible', 'Type': 'int',
           'Comment': 'tangible asset measured as the difference between total fixed asset minus intangible asset'},
          {'Name': 'cashflow', 'Type': 'int', 'Comment': 'cash flow'},
          {'Name': 'sales', 'Type': 'decimal(16,5)', 'Comment': 'sales'},
          {'Name': 'current_ratio',
           'Type': 'decimal(21,5)', 'Comment': 'current ratio cuasset/流动负债合计 (c95)'},
          {'Name': 'lag_current_ratio', 'Type': 'decimal(21,5)', 'Comment': 'lag value of current ratio'},
          {'Name': 'quick_ratio',
           'Type': 'decimal(21,5)', 'Comment': 'quick ratio (cuasset-存货 (c81) ) / 流动负债合计 (c95)'},
          {'Name': 'lag_quick_ratio', 'Type': 'decimal(21,5)', 'Comment': 'lag value of quick ratio'},
          {'Name': 'liabilities_tot_asset',
           'Type': 'decimal(21,5)', 'Comment': 'liabilities to total asset'},
          {'Name': 'lag_liabilities_tot_asset', 'Type': 'decimal(21,5)', 'Comment': 'lag value of liabilities to asset'},
          {'Name': 'sales_tot_asset',
           'Type': 'decimal(21,5)', 'Comment': 'sales to total asset'},
          {'Name': 'lag_sales_tot_asset', 'Type': 'decimal(21,5)', 'Comment': 'lag value of sales to asset'},
          #{'Name': 'cash_tot_asset',
          # 'Type': 'decimal(21,5)', 'Comment': 'cash to total asset'},
          {'Name': 'investment_tot_asset',
           'Type': 'decimal(21,5)', 'Comment': 'investment to total asset'},
          {'Name': 'rd_tot_asset',
           'Type': 'decimal(21,5)', 'Comment': 'rd to total asset'},
          {'Name': 'asset_tangibility_tot_asset',
           'Type': 'decimal(21,5)',
           'Comment': 'asset tangibility to total asset'},
          {'Name': 'cashflow_tot_asset',
           'Type': 'decimal(21,5)', 'Comment': 'cashflow to total asset'},
          {'Name': 'lag_cashflow_tot_asset', 'Type': 'decimal(21,5)', 'Comment': 'lag value of cashflow to total asset'},
          {'Name': 'cashflow_to_tangible',
           'Type': 'decimal(21,5)', 'Comment': 'cashflow to tangible asset'},
          {'Name': 'lag_cashflow_to_tangible', 'Type': 'decimal(21,5)', 'Comment': 'lag value of cashflow to tangible asset'},
          {'Name': 'return_to_sale', 'Type': 'decimal(21,5)', 'Comment': ''},
          {'Name': 'lag_return_to_sale', 'Type': 'decimal(21,5)', 'Comment': 'lag value of return to sale'},
          {'Name': 'coverage_ratio', 'Type': 'decimal(21,5)', 'Comment': 'net income(c131) /total interest payments'},
          {'Name': 'liquidity', 'Type': 'decimal(21,5)', 'Comment': 'current assets-current liabilities/total assets'}]
```

4. Provide a description

```python
description = """
Transform asif firms prepared to asif financial ratio baseline firm
"""
```

3. provide metadata

- DatabaseName:
- TablePrefix:
- input: 
- notebook name: to indicate
- Task ID: from Coda
- index_final_table: a list to indicate if the current table is used to prepare the final table(s). If more than one, pass the index. Start at 0
- if_final: A boolean. Indicates if the current table is the final table -> the one the model will be used to be trained

```python
import re
```

```python
with open(os.path.join(str(Path(path).parent.parent), 'utils','parameters_ETL_Financial_dependency_pollution.json')) as json_file:
    parameters = json.load(json_file)
```

```python
filename =  "06_asif_financial_ratio_firm_baseline.ipynb"
index_final_table = []
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
    re.sub('.ipynb','.md',filename)
)
```

Grab the input name from query

```python
import re
```

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
```

```python
parameters['TABLES']['TRANSFORMATION']['STEPS'].append(json_etl)
```

```python
print("Currently, the ETL has {} tables".format(len(parameters['TABLES']['TRANSFORMATION']['STEPS'])))
```

Save JSON

```python
with open(os.path.join(str(Path(path).parent.parent), 'utils','parameters_ETL_Financial_dependency_pollution.json'), "w") as json_file:
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
partition_keys = ["firm","year","cic","geocode4_corr"]

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

## Count missing values

```python
#table = 'XX'
schema = glue.get_table_information(
    database = DatabaseName,
    table = table_name
)['Table']
schema
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
secondary_key = 'short'
y_var = 'asset_tangibility_fcit'
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
create_report(extension = "html", keep_code = True, notebookname = "06_asif_financial_ratio_firm_baseline.ipynb")
```

```python
create_schema.create_schema(path_json, path_save_image = os.path.join(parent_path, 'utils'))
```

```python
### Update TOC in Github
for p in [parent_path,
          str(Path(path).parent),
          os.path.join(str(Path(path).parent), "00_download_data_from"),
          os.path.join(str(Path(path).parent.parent), "02_data_analysis"),
          os.path.join(str(Path(path).parent.parent), "02_data_analysis", "00_statistical_exploration"),
          os.path.join(str(Path(path).parent.parent), "02_data_analysis", "01_model_estimation"),
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
