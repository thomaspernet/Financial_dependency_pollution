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


# Steps

<!-- #region -->
# Table `XX`

- Table name: `XX`


Since the table to create has missing value, please use the following at the top of the query

```
CREATE TABLE database.table_name WITH (format = 'PARQUET') AS
```
<!-- #endregion -->

Choose a location in S3 to save the CSV. It is recommended to save in it the `datalake-datascience` bucket. Locate an appropriate folder in the bucket, and make sure all output have the same format

```python
DatabaseName = 'firms_survey'
table_name = 'asif_firms_prepared'
s3_output = 'SQL_OUTPUT_ATHENA'
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

<!-- #region -->
### Brief analysis



- Count size of:
    - `year` in 1998 to 2007
    - `firms` has no digit
    - `citycode` 
    - `cic`
    - `setup`
    - `type` 
    
**Type**

use "type" variable, classify ownership into five types:

- 1=state -> 110 141 143 151
- 2=collective -> 120 130 142 149
- 3=private -171 172 173 174 190
- 4=foreign- 210 220 230 240
- 5=Hong Kong, Macau and Taiwan (4 and 5 can be combined into a single "foreign" category - 310 320 330 340

There are two labels "159" and "160", which are joint stock and stock shareholding, that cannot be assigned straight away. We identify these firms' ownership using the information of other variables about firm equity structure. The firm's ownership for "159" and "160" is the group with the largest share (or value)
    
- Count nb of observations when `type`is broken down:
    
<!-- #endregion -->

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
                    database=DatabaseName,
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
SELECT CNT, COUNT(CNT) AS CNT_DIGIT
FROM test
GROUP BY CNT

"""
output = s3.run_query(
                    query=query,
                    database=DatabaseName,
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
                    database=DatabaseName,
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
                    database=DatabaseName,
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
                    database=DatabaseName,
                    s3_output=s3_output,
    filename = 'count_setup'
                )
output
```

```python
query = """
SELECT type, COUNT(*) as CNT
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
  GROUP BY type
  ORDER BY type
"""
output = s3.run_query(
                    query=query,
                    database=DatabaseName,
                    s3_output=s3_output,
    filename = 'count_type'
                )
output
```

```python
query = """
WITH own AS(
SELECT CASE 
WHEN type in ('110','141','143','151') THEN 'SOE' 
WHEN type in ('120','130','142','149') THEN 'COLLECTIVE' 
WHEN type in ('171','172','173','174','190') THEN 'PRIVATE' 
WHEN type in ('210','220','230','240') THEN 'FOREIGN' 
WHEN type in ( '310','320','330','340') THEN 'HTM'
ELSE 'NOT_ASSIGNED' END AS ownership
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
  )
  SELECT ownership, COUNT(*) AS CNT
  FROM own
  GROUP BY ownership
  ORDER BY CNT
"""
output = s3.run_query(
                    query=query,
                    database=DatabaseName,
                    s3_output=s3_output,
    filename = 'count_ownership'
                )
output
```

In the cell below, we show how we retrive the ownership when type is equal to 159 or 160.  The rule is:

If the maximum equity is :

- 'e_collective': 'Collective',
- 'e_state': 'SOE',
- 'e_individual': 'Private',
- 'e_legal_person': 'Private',
- 'e_HMT': 'HTM',
- 'e_foreign': 'Foreign'

The strategy consists to get the maximum key of the array containing the columns `e_state`, `e_collective`, `e_legal_person`, `e_individual`, `e_hmt`, `e_foreign`. Then assigned an ownership knowing the key, i.e. 1 is `COLLECTIVE`, 2 is `SOE` and so on.

If the maximum of the array is equal to zero, we leave it blank and remove it in the final table. It can happens when there is no known value for any of the equity columns or when one of the equity is negative and all the other equals to 0.

```python
query = """
WITH arrays AS (
SELECT 
  type, 
  ARRAY[e_state, e_collective, e_legal_person, e_individual, e_hmt, e_foreign] AS array_equity, 
  array_max(
    ARRAY[e_state, e_collective, e_legal_person, e_individual, e_hmt, e_foreign]
    ) AS max_array_equity,
  MAP(
    ARRAY[1,2,3,4,5, 6],
    ARRAY[e_state, e_collective, e_legal_person, e_individual, e_hmt, e_foreign]
    ) AS map_array_equity,
  map_filter(
  map(
  ARRAY[1,2,3,4,5, 6],
  ARRAY[e_state, e_collective, e_legal_person, e_individual, e_hmt, e_foreign]
  ),
  (k, v) -> v = array_max(
  ARRAY[e_state, e_collective, e_legal_person, e_individual, e_hmt, e_foreign]
  )) AS max_map_array_equity,
  map_keys(
  map_filter(
  map(
  ARRAY[1,2,3,4,5, 6],
  ARRAY[e_state, e_collective, e_legal_person, e_individual, e_hmt, e_foreign]
  ),
  (k, v) -> v = array_max(
  ARRAY[e_state, e_collective, e_legal_person, e_individual, e_hmt, e_foreign]
  )
  )
  ) AS max_key_array_equity
FROM asif_unzip_data_csv 

  )
  SELECT *,
  CASE 
 
  WHEN type in ('159','160') AND max_key_array_equity = ARRAY[1] THEN 'COLLECTIVE'
  WHEN type in ('159','160') AND max_key_array_equity = ARRAY[2] THEN 'SOE'
  WHEN type in ('159','160') AND max_key_array_equity = ARRAY[3] THEN 'PRIVATE'
  WHEN type in ('159','160') AND max_key_array_equity = ARRAY[4] THEN 'PRIVATE'
  WHEN type in ('159','160') AND max_key_array_equity = ARRAY[5] THEN 'HTM'
  WHEN type in ('159','160') AND max_key_array_equity = ARRAY[6] THEN 'FOREIGN'
  WHEN type in ('110','141','143','151') THEN 'SOE' 
  WHEN type in ('120','130','142','149') THEN 'COLLECTIVE' 
  WHEN type in ('171','172','173','174','190') THEN 'PRIVATE' 
  WHEN type in ('210','220','230','240') THEN 'FOREIGN' 
  WHEN type in ( '310','320','330','340') THEN 'HTM'
  END AS ownership
  FROM arrays
  WHERE type in ('159', '160') OR max_array_equity = 0 
  LIMIT 10
"""
output = s3.run_query(
                    query=query,
                    database=DatabaseName,
                    s3_output=s3_output,
    filename = 'ex_ownership'
                )
output
```

Count the ownership with the new rule. Not that, we exclude the `NULL` from `ownership`

```python
query = """
WITH own AS(
SELECT 
type,
array_max(
    ARRAY[e_state, e_collective, e_legal_person, e_individual, e_hmt, e_foreign]
    ) AS max_array_equity,
map_keys(
  map_filter(
  map(
  ARRAY[1,2,3,4,5, 6],
  ARRAY[e_state, e_collective, e_legal_person, e_individual, e_hmt, e_foreign]
  ),
  (k, v) -> v = array_max(
  ARRAY[e_state, e_collective, e_legal_person, e_individual, e_hmt, e_foreign]
  )
  )
  ) AS max_key_array_equity
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
  )
  SELECT ownership, COUNT(*) AS CNT
  FROM (
  SELECT 
  CASE 
  WHEN type in ('159','160') AND max_key_array_equity = ARRAY[1] THEN 'COLLECTIVE'
  WHEN type in ('159','160') AND max_key_array_equity = ARRAY[2] THEN 'SOE'
  WHEN type in ('159','160') AND max_key_array_equity = ARRAY[3] THEN 'PRIVATE'
  WHEN type in ('159','160') AND max_key_array_equity = ARRAY[4] THEN 'PRIVATE'
  WHEN type in ('159','160') AND max_key_array_equity = ARRAY[5] THEN 'HTM'
  WHEN type in ('159','160') AND max_key_array_equity = ARRAY[6] THEN 'FOREIGN'
  WHEN type in ('110','141','143','151') THEN 'SOE' 
  WHEN type in ('120','130','142','149') THEN 'COLLECTIVE' 
  WHEN type in ('171','172','173','174','190') THEN 'PRIVATE' 
  WHEN type in ('210','220','230','240') THEN 'FOREIGN' 
  WHEN type in ( '310','320','330','340') THEN 'HTM'
  END AS ownership
  FROM own
  )
  WHERE ownership in ('COLLECTIVE', 'SOE', 'PRIVATE', 'HTM', 'FOREIGN')
  GROUP BY ownership
  ORDER BY CNT
  
"""
output = s3.run_query(
                    query=query,
                    database=DatabaseName,
                    s3_output=s3_output,
    filename = 'count_ownership'
                )
output
```

Method cleaning dataset 

| Step cleaning | method                                                                                                  | Variables                                                      |
|---------------|---------------------------------------------------------------------------------------------------------|----------------------------------------------------------------|
| 1             | remove row if  total assets, net value of fixed assets, sales, gross value of industrial output missing | (负债合计 (c98) + 所有者权益合计 (c99), tofixed, sales, output |
| 2             | ~number of employees hired by a firm must not be less than 10~                                            | c62 (年末从业人员合计_总计)                                    |
| 3             | total assets must be higher than the liquid assets                                                      | (负债合计 (c98) + 所有者权益合计 (c99) >                       |
| 4             | total assets must be larger than the total fixed assets                                                 | (负债合计 (c98) + 所有者权益合计 (c99) > totfixed              |
| 5             | total assets must be larger than the net value of the fixed assets                                      | (负债合计 (c98) + 所有者权益合计 (c99) > netfixed              |


Clean up the folder with the previous csv file. Be careful, it will erase all files inside the folder

```python
s3_output = 'DATA/ECON/FIRM_SURVEY/ASIF_CHINA/PREPARED'
```

```python
s3.remove_all_bucket(path_remove = s3_output)
```

Just for the record, it took 17 seconds to manipulate 1.7gib of data

```python
%%time
query = """
CREATE TABLE firms_survey.asif_firms_prepared WITH (format = 'PARQUET') AS WITH own AS(
  SELECT 
    *, 
    array_max(
      ARRAY[e_state, e_collective, e_legal_person, 
      e_individual, e_hmt, e_foreign]
    ) AS max_array_equity, 
    map_keys(
      map_filter(
        map(
          ARRAY[1, 2, 3, 4, 5, 6], ARRAY[e_state, 
          e_collective, e_legal_person, e_individual, 
          e_hmt, e_foreign]
        ), 
        (k, v) -> v = array_max(
          ARRAY[e_state, e_collective, e_legal_person, 
          e_individual, e_hmt, e_foreign]
        )
      )
    ) AS max_key_array_equity 
  FROM 
    "firms_survey"."asif_unzip_data_csv" 
  WHERE 
    (
      year in (
        '1998', '1999', '2000', '2001', '2002', 
        '2003', '2004', '2005', '2006', '2007'
      )
    ) 
    AND (
      LENGTH(citycode) = 4 
      AND LENGTH(setup) = 4 
      AND LENGTH(cic) <= 4 
      AND regexp_like(firm, '\d+') = TRUE
    )
) 
SELECT 
  firm, 
  year, 
  export, 
  dq, 
  name, 
  town, 
  village, 
  street, 
  c15, 
  zip, 
  product1_, 
  c26, 
  c27, 
  cic, 
  type, 
  c44, 
  c45, 
  setup, 
  c47, 
  c60, 
  c61, 
  employ, 
  c69, 
  output, 
  new_product, 
  c74, 
  addval, 
  cuasset, 
  c80, 
  c81, 
  c82, 
  c83, 
  c84, 
  c85, 
  tofixed, 
  c87, 
  todepre, 
  cudepre, 
  netfixed, 
  CASE WHEN c91 IS NULL THEN 0 ELSE c91 END AS c91, 
  CASE WHEN c92 IS NULL THEN 0 ELSE c92 END AS c92, 
  toasset, 
  c95, 
  c97, 
  c98, 
  c99, 
  captal, 
  e_state, 
  e_collective, 
  e_legal_person, 
  e_individual, 
  e_hmt, 
  e_foreign, 
  sales, 
  c108, 
  c113, 
  c110, 
  c111, 
  c114, 
  c115, 
  c116, 
  c118, 
  c124, 
  c125, 
  profit, 
  c128, 
  c131, 
  c132, 
  c133, 
  c134, 
  c136, 
  wage, 
  c140, 
  c141, 
  c142, 
  c143, 
  c144, 
  c145, 
  midput, 
  c62, 
  c147, 
  c64, 
  c65, 
  c93, 
  c16, 
  c9, 
  c10, 
  c11, 
  c17, 
  c29, 
  c167, 
  c168, 
  v90, 
  CASE WHEN c79 IS NULL THEN 0 ELSE c79 END AS c79, 
  c96, 
  c117, 
  c119, 
  c121, 
  trainfee, 
  c123, 
  c127, 
  c135, 
  c120, 
  c138, 
  c148, 
  c149, 
  c150, 
  c151, 
  rdfee, 
  c156, 
  c157, 
  citycode, 
  prov, 
  ownership 
FROM 
  (
    SELECT 
      *, 
      CASE WHEN type in ('159', '160') 
      AND max_key_array_equity = ARRAY[1] THEN 'COLLECTIVE' WHEN type in ('159', '160') 
      AND max_key_array_equity = ARRAY[2] THEN 'SOE' WHEN type in ('159', '160') 
      AND max_key_array_equity = ARRAY[3] THEN 'PRIVATE' WHEN type in ('159', '160') 
      AND max_key_array_equity = ARRAY[4] THEN 'PRIVATE' WHEN type in ('159', '160') 
      AND max_key_array_equity = ARRAY[5] THEN 'HTM' WHEN type in ('159', '160') 
      AND max_key_array_equity = ARRAY[6] THEN 'FOREIGN' WHEN type in ('110', '141', '143', '151') THEN 'SOE' WHEN type in ('120', '130', '142', '149') THEN 'COLLECTIVE' WHEN type in ('171', '172', '173', '174', '190') THEN 'PRIVATE' WHEN type in ('210', '220', '230', '240') THEN 'FOREIGN' WHEN type in ('310', '320', '330', '340') THEN 'HTM' END AS ownership 
    FROM 
      own
  ) 
WHERE 
  (
    ownership in (
      'COLLECTIVE', 'SOE', 'PRIVATE', 'HTM', 
      'FOREIGN'
    ) 
    AND c98 > 0 
    AND c99 > 0 
    AND tofixed > 0 
    AND sales > 0 
    AND output > 0 -- AND
    -- c62 > 10 
    AND (c98 + c99) > tofixed 
    AND (c98 + c99) > netfixed
  )

"""
output = s3.run_query(
                    query=query,
                    database=DatabaseName,
                    s3_output=s3_output
                )
```

```python
output
```

Need to remove the metadata generated by Athena. We remove it to avoid parsing incorrect value with the crawler


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
schema = glue.get_table_information(
    database = DatabaseName,
    table = 'asif_unzip_data_csv'
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
json_etl = {
    'step': step,
    'description':description,
    'query':query,
    'schema': schema,
    'partition_keys':partition_keys,
    'metadata':{
    'DatabaseName' : DatabaseName,
    'TableName' : table_name,
    'target_S3URI' : os.path.join('s3://',bucket, s3_output),
    'from_athena': 'True'    
    }
}
```

```python
with open(os.path.join(str(Path(path).parent), 'parameters_ETL_Financial_dependency_pollution.json')) as json_file:
    parameters = json.load(json_file)
```

Remove the step number from the current file (if exist)

```python
index_to_remove = next(
                (
                    index
                    for (index, d) in enumerate(parameters['TABLES']['PREPARATION']['STEPS'])
                    if d["step"] == step
                ),
                None,
            )
if index_to_remove != None:
    parameters['TABLES']['PREPARATION']['STEPS'].pop(index_to_remove)
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
to_add = {
        "Name": "ownership",
        "Type": "string",
        "Comment": "One of COLLECTIVE, SOE, PRIVATE, HTM, FOREIGN. Cf https://github.com/thomaspernet/Financial_dependency_pollution/blob/master/01_data_preprocessing/01_prepare_tables/00_prepare_asif.md"
    }
```

```python
schema.append(to_add)
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
secondary_key = 'ownership'
y_var = 'output'
```

```python
table_name.upper()
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
        "threshold":.5
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
create_report(extension = "html", keep_code = True)
```
