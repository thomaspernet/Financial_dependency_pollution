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

# Transform ASIF data by constructing financial variables raw data data to S3

# Objective(s)

## Business needs 

Transform (creating financial variables) ASIF data using Athena and save output to S3 + Glue. 

## Description

### Objective 

Construct the financial ratio variables by aggregating the data (not anymore at the firm level)

The asif_financial_ratio  has the following levels:

* year
* city
* industry -> 2 digits compared with 4 digits with the parent task

**Construction variables**

* ~Rescale output, fa_net, employment~
* construct the following ratio:
    * If possible compute by:
      1. industry level
      2. city-industry level
      3. city-industry-year level
  * Working capital = Current Assets - Current Liabilities
  * Asset Tangibility
  * Current Ratio: 
    * Cash = non-cash assets -  total current assets
      * non-cash assets = short-term investments, accounts receivable, inventory and supplies 
  * Liabilities/Assets (Total-Debt-to-Total-Assets)
  * Sales/Assets
  * Return on Asset
* Fixed effect:
  * city-industry
  * year-industry
  * city-year

**Steps** 

We will clean the table by doing the following steps:

1. Compute the financial ratio by aggregating the data

**Cautious**

* Make sure there is no duplicates when merging ratio from different level

**Target**

* The file is saved in S3: 
  * bucket: datalake-datascience 
  * path: DATA/ECON/FIRM_SURVEY/ASIF_CHINA/TRANSFORMED/FINANCIAL_RATIO 
* Glue data catalog should be updated
  * database: firms_survey 
  * table prefix: asif_city_industry 
    * table name (prefix + last folder S3 path): asif_city_industry_financial_ratio 

# Metadata

* Key: spr04tlko02392a
* Parent key (for update parent):  
* Notebook US Parent (i.e the one to update): 
* https://github.com/thomaspernet/Financial_dependency_pollution/blob/master/01_data_preprocessing/02_transform_tables/00_asif_financial_ratio.md
* Epic: Epic 2
* US: US 1
* Date Begin: 11/23/2020
* Duration Task: 1
* Description: Transform (creating financial variables) ASIF data using Athena and save output to S3 + Glue. 
* Step type: Transform table
* Status: Active
* Source URL: Create Task and Epics
* Task type: Jupyter Notebook
* Users: Thomas Pernet
* Watchers: Thomas Pernet
* User Account: https://468786073381.signin.aws.amazon.com/console
* Estimated Log points: 10
* Task tag: #athena,#glue,#crawler,#financial-ratio
* Toggl Tag: #data-transformation
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
* asif_firms_prepared
* Github: 
  * https://github.com/thomaspernet/Financial_dependency_pollution/blob/master/01_data_preprocessing/01_prepare_tables/00_prepare_asif.md

# Destination Output/Delivery

## Table/file

* Origin: 
* S3
* Athena
* Name:
* DATA/ECON/FIRM_SURVEY/ASIF_CHINA/TRANSFORMED/FINANCIAL_RATIO
* asif_city_industry_financial_ratio
* GitHub:
* https://github.com/thomaspernet/Financial_dependency_pollution/blob/master/01_data_preprocessing/02_transform_tables/00_asif_financial_ratio.md
* URL: 
  * datalake-datascience/DATA/ECON/FIRM_SURVEY/ASIF_CHINA/TRANSFORMED/FINANCIAL_RATIO
* 

# Knowledge

## List of candidates

* [List of financial ratios that can be computed with ASIF panel data](https://roamresearch.com/#/app/thomas_db/page/PS3o9Z3VA)
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

<!-- #region -->
# Steps

Detail computation:

1. `working capital`:
    - Inventory [存货 (c81)] + Accounts receivable [应收帐款 (c80)] - Accounts payable [应付帐款  (c96)]
2. `Asset Tangibility: 
    - Total fixed assets [固定资产合计 (c85)] - Intangible assets [无形资产 (c91)]
3. `Current Ratio`:
    - Current asset [cuasset] / Current liabilities [c95]
4. `Cash/Assets`:
    - non-cash assets -  total current assets / non-cash assets
        - Cash [( 其中：短期投资 (c79) + 应收帐款 (c80) + 存货 (c81)) - cuasset)] /  Assets [其中：短期投资 (c79) + 应收帐款 (c80) + 存货 (c81)]
5. `Liabilities/Assets` (Total-Debt-to-Total-Assets)
    - (Total current liabilities + Total long-term liabilities)/ Total assets
        - Liabilities [(流动负债合计 (c95) + 长期负债合计 (c97))] /  Total assets [资产总计318 (c93)]
        - Total Liabilities [负债合计 (c98)]  /  Total assets [资产总计318 (c93)]
6. `Sales/Assets`:
    - Total annual revenue [全年营业收入合计 (c64) ] / ($\Delta$ Total assets 318 [$\Delta$ 资产总计318 (c98)]/2)
7. `Return on Asset`
    - (Total annual revenue - Income tax payable) [(全年营业收入合计 (c64) - 应交所得税 (c134))] / Total assets [资产总计318 (c98)]
    
    
**pct missing**

![](https://drive.google.com/uc?export=view&id=1LPNhZIPkJgx0-ZsM6NLNAB6dGH9h7ELo)
<!-- #endregion -->

## Example step by step



```python
DatabaseName = 'firms_survey'
s3_output_example = 'SQL_OUTPUT_ATHENA'
```

1. Count the number of digit by industry

We want to keep only the fist two digit

```python
query = """
SELECT len, COUNT(len) as CNT
FROM (
SELECT length(cic) AS len
FROM asif_firms_prepared 
)
GROUP BY len
ORDER BY CNT
"""
output = s3.run_query(
                    query=query,
                    database=DatabaseName,
                    s3_output=s3_output_example,
    filename = 'example_1'
                )
output
```

Count when substring 1 or 2 digits

```python
query = """
WITH test AS (
SELECT
CASE 
WHEN LENGTH(cic) = 4 THEN substr(cic,1, 2) 
ELSE substr(cic,1, 1) END AS indu_2
FROM asif_firms_prepared 
)

SELECT len, COUNT(len) as CNT
FROM (
SELECT length(indu_2) AS len
FROM test 
)
GROUP BY len
ORDER BY CNT
"""
output = s3.run_query(
                    query=query,
                    database=DatabaseName,
                    s3_output=s3_output_example,
    filename = 'example_1'
                )
output
```

1. Add consistent city code


There is a need to remove the duplicates in `china_city_code_normalised` because it is possible to have the same code but different Chinese name link Chongqing

```python
query = """
SELECT *
FROM chinese_lookup.china_city_code_normalised 
WHERE extra_code = '5001'
"""
output = s3.run_query(
                    query=query,
                    database=DatabaseName,
                    s3_output=s3_output_example,
    filename = 'example_1'
                )
output
```

```python
query = """
WITH test AS (
SELECT firm, year, citycode, geocode4_corr, CASE 
WHEN LENGTH(cic) = 4 THEN substr(cic,1, 2) 
ELSE substr(cic,1, 1) END AS indu_2
  FROM firms_survey.asif_firms_prepared 
INNER JOIN 
  (
  SELECT extra_code, geocode4_corr
  FROM chinese_lookup.china_city_code_normalised 
  GROUP BY extra_code, geocode4_corr
  ) as no_dup_citycode
ON asif_firms_prepared.citycode = no_dup_citycode.extra_code
  )
  SELECT CNT, COUNT(*) 
  FROM(
  SELECT firm, year, geocode4_corr, indu_2, COUNT(*) AS CNT
  FROM test
  GROUP BY firm, year, geocode4_corr, indu_2
    )
    GROUP BY CNT
"""
output = s3.run_query(
                    query=query,
                    database=DatabaseName,
                    s3_output=s3_output_example,
    filename = 'example_1'
                )
output
```

Make sure the output is the same before and after the use of city consistent code

```python
query = """
WITH test AS (
SELECT 
  year, 
  geocode4_corr,
  cic, 
  SUM(output) as sum_output,
  SUM(c81) + SUM(c80) - SUM(c96) AS working_capital_cit, 
  SUM(c85) - SUM(c91) AS asset_tangibility_cit, 
  CAST(
    SUM(cuasset) AS DECIMAL(16, 5)
  ) / NULLIF(
    CAST(
      SUM(c95) AS DECIMAL(16, 5)
    ), 
    0
  ) AS current_ratio_cit, 
  CAST(
    (SUM(c79) + SUM(c80) + SUM(c81)) - SUM(cuasset) AS DECIMAL(16, 5)
  ) / NULLIF(CAST(
    SUM(c93) AS DECIMAL(16, 5)
  ), 
    0
  ) AS cash_assets_cit, 
  CAST(
    SUM(c95) + SUM(c97) AS DECIMAL(16, 5)
  )/ NULLIF(
    CAST(
      SUM(c93) AS DECIMAL(16, 5)
    ), 
    0
  ) AS liabilities_assets_cit, 
  CAST(
    SUM(c64) - SUM(c134) AS DECIMAL(16, 5)
  )/ NULLIF(
    CAST(
      SUM(c98) AS DECIMAL(16, 5)
    ), 
    0
  ) AS return_on_asset_cit, 
  CAST(
    SUM(cuasset) AS DECIMAL(16, 5)
  )/ NULLIF(
    CAST(
      (
        SUM(c98) - lag(
          SUM(c98), 
          1
        ) over(
          partition by geocode4_corr, 
          cic 
          order by 
            geocode4_corr, 
            cic, 
            year
        )
      )/ 2 AS DECIMAL(16, 5)
    ), 
    0
  ) AS sales_assets_cit 
FROM firms_survey.asif_firms_prepared 
INNER JOIN 
  (
  SELECT extra_code, geocode4_corr
  FROM chinese_lookup.china_city_code_normalised 
  GROUP BY extra_code, geocode4_corr
  ) as no_dup_citycode
  
ON asif_firms_prepared.citycode = no_dup_citycode.extra_code
GROUP BY 
  geocode4_corr, 
  cic, 
  year 
)
SELECT SUM(sum_output) as sum_output
FROM test

"""
output_1 = s3.run_query(
                    query=query,
                    database=DatabaseName,
                    s3_output=s3_output_example,
    filename = 'example_1'
                )
output_1
```

```python
query = """
SELECT SUM(output) as sum_output
FROM firms_survey.asif_firms_prepared 

"""
output_2 = s3.run_query(
                    query=query,
                    database=DatabaseName,
                    s3_output=s3_output_example,
    filename = 'example_1'
                )
output_2
```

```python
output_1 > output_2
```

2. Computation ratio by city-industry-year

```python
query = """
WITH test AS (
SELECT *, CASE 
WHEN LENGTH(cic) = 4 THEN substr(cic,1, 2) 
ELSE substr(cic,1, 1) END AS indu_2
FROM firms_survey.asif_firms_prepared 
)

SELECT 
  year, 
  geocode4_corr,
  indu_2, 
  SUM(c81) + SUM(c80) - SUM(c96) AS working_capital_cit, 
  SUM(c85) - SUM(c91) AS asset_tangibility_cit, 
  CAST(
    SUM(cuasset) AS DECIMAL(16, 5)
  ) / NULLIF(
    CAST(
      SUM(c95) AS DECIMAL(16, 5)
    ), 
    0
  ) AS current_ratio_cit, 
  CAST(
    (SUM(c79) + SUM(c80) + SUM(c81)) - SUM(cuasset) AS DECIMAL(16, 5)
  ) / NULLIF(CAST(
    SUM(c93) AS DECIMAL(16, 5)
  ), 
    0
  ) AS cash_assets_cit, 
  CAST(
    SUM(c95) + SUM(c97) AS DECIMAL(16, 5)
  )/ NULLIF(
    CAST(
      SUM(c93) AS DECIMAL(16, 5)
    ), 
    0
  ) AS liabilities_assets_cit, 
  CAST(
    SUM(c64) - SUM(c134) AS DECIMAL(16, 5)
  )/ NULLIF(
    CAST(
      SUM(c98) AS DECIMAL(16, 5)
    ), 
    0
  ) AS return_on_asset_cit, 
  CAST(
    SUM(cuasset) AS DECIMAL(16, 5)
  )/ NULLIF(
    CAST(
      (
        SUM(c98) - lag(
          SUM(c98), 
          1
        ) over(
          partition by geocode4_corr, 
          indu_2 
          order by 
            geocode4_corr, 
            indu_2, 
            year
        )
      )/ 2 AS DECIMAL(16, 5)
    ), 
    0
  ) AS sales_assets_cit 
FROM test 
INNER JOIN 
  (
  SELECT extra_code, geocode4_corr
  FROM chinese_lookup.china_city_code_normalised 
  GROUP BY extra_code, geocode4_corr
  ) as no_dup_citycode
  
ON test.citycode = no_dup_citycode.extra_code
WHERE year in ('2001', '2002', '2003', '2004', '2005', '2006', '2007') 
GROUP BY 
  geocode4_corr, 
  indu_2, 
  year 
LIMIT 
  10

"""
output = s3.run_query(
                    query=query,
                    database=DatabaseName,
                    s3_output=s3_output_example,
    filename = 'example_1'
                )
output
```

2. Computation ratio by city-industry

As an average over year 2002 to 2005

```python
query = """
WITH test AS (
SELECT *, CASE 
WHEN LENGTH(cic) = 4 THEN substr(cic,1, 2) 
ELSE substr(cic,1, 1) END AS indu_2
FROM firms_survey.asif_firms_prepared 
)
SELECT * 
FROM (
WITH ratio AS (
SELECT 
  year, 
  geocode4_corr,
  indu_2, 
  SUM(c81) + SUM(c80) - SUM(c96) AS working_capital_cit, 
  SUM(c85) - SUM(c91) AS asset_tangibility_cit, 
  CAST(
    SUM(cuasset) AS DECIMAL(16, 5)
  ) / NULLIF(
    CAST(
      SUM(c95) AS DECIMAL(16, 5)
    ), 
    0
  ) AS current_ratio_cit, 
  CAST(
    (SUM(c79) + SUM(c80) + SUM(c81)) - SUM(cuasset) AS DECIMAL(16, 5)
  ) / NULLIF(CAST(
    SUM(c93) AS DECIMAL(16, 5)
  ), 
    0
  ) AS cash_assets_cit, 
  CAST(
    SUM(c95) + SUM(c97) AS DECIMAL(16, 5)
  )/ NULLIF(
    CAST(
      SUM(c93) AS DECIMAL(16, 5)
    ), 
    0
  ) AS liabilities_assets_cit, 
  CAST(
    SUM(c64) - SUM(c134) AS DECIMAL(16, 5)
  )/ NULLIF(
    CAST(
      SUM(c98) AS DECIMAL(16, 5)
    ), 
    0
  ) AS return_on_asset_cit, 
  CAST(
    SUM(cuasset) AS DECIMAL(16, 5)
  )/ NULLIF(
    CAST(
      (
        SUM(c98) - lag(
          SUM(c98), 
          1
        ) over(
          partition by geocode4_corr, 
          indu_2 
          order by 
            geocode4_corr, 
            indu_2, 
            year
        )
      )/ 2 AS DECIMAL(16, 5)
    ), 
    0
  ) AS sales_assets_cit 
FROM test
INNER JOIN 
  (
  SELECT extra_code, geocode4_corr
  FROM chinese_lookup.china_city_code_normalised 
  GROUP BY extra_code, geocode4_corr
  ) as no_dup_citycode
  
ON test.citycode = no_dup_citycode.extra_code
WHERE year in ('2001', '2002', '2003', '2004', '2005') 
GROUP BY 
  geocode4_corr, 
  indu_2, 
  year 
  )
  SELECT
  geocode4_corr, 
  indu_2,
  AVG(working_capital_cit) AS working_capital_ci,
  AVG(asset_tangibility_cit) AS asset_tangibility_ci,
  AVG(current_ratio_cit) AS current_ratio_ci,
  AVG(cash_assets_cit) AS cash_assets_ci,
  AVG(liabilities_assets_cit) AS liabilities_assets_ci,
  AVG(return_on_asset_cit) AS return_on_asset_ci,
  AVG(sales_assets_cit) AS sales_assets_ci
  FROM ratio
  GROUP BY geocode4_corr, indu_2
  LIMIT 10
  )
"""
output = s3.run_query(
                    query=query,
                    database=DatabaseName,
                    s3_output=s3_output_example,
    filename = 'example_2'
                )
output
```

3. Computation ratio by industry

As an average over year 2002 to 2005

```python
query = """
WITH test AS (
SELECT *, CASE 
WHEN LENGTH(cic) = 4 THEN substr(cic,1, 2) 
ELSE substr(cic,1, 1) END AS indu_2
FROM firms_survey.asif_firms_prepared 
)
SELECT * 
FROM (
WITH ratio AS (
SELECT 
  year, 
  geocode4_corr,
  indu_2, 
  SUM(c81) + SUM(c80) - SUM(c96) AS working_capital_cit, 
  SUM(c85) - SUM(c91) AS asset_tangibility_cit, 
  CAST(
    SUM(cuasset) AS DECIMAL(16, 5)
  ) / NULLIF(
    CAST(
      SUM(c95) AS DECIMAL(16, 5)
    ), 
    0
  ) AS current_ratio_cit, 
  CAST(
    (SUM(c79) + SUM(c80) + SUM(c81)) - SUM(cuasset) AS DECIMAL(16, 5)
  ) / NULLIF(CAST(
    SUM(c93) AS DECIMAL(16, 5)
  ), 
    0
  ) AS cash_assets_cit, 
  CAST(
    SUM(c95) + SUM(c97) AS DECIMAL(16, 5)
  )/ NULLIF(
    CAST(
      SUM(c93) AS DECIMAL(16, 5)
    ), 
    0
  ) AS liabilities_assets_cit, 
  CAST(
    SUM(c64) - SUM(c134) AS DECIMAL(16, 5)
  )/ NULLIF(
    CAST(
      SUM(c98) AS DECIMAL(16, 5)
    ), 
    0
  ) AS return_on_asset_cit, 
  CAST(
    SUM(cuasset) AS DECIMAL(16, 5)
  )/ NULLIF(
    CAST(
      (
        SUM(c98) - lag(
          SUM(c98), 
          1
        ) over(
          partition by geocode4_corr, 
          indu_2 
          order by 
            geocode4_corr, 
            indu_2, 
            year
        )
      )/ 2 AS DECIMAL(16, 5)
    ), 
    0
  ) AS sales_assets_cit 
FROM test
INNER JOIN 
  (
  SELECT extra_code, geocode4_corr
  FROM chinese_lookup.china_city_code_normalised 
  GROUP BY extra_code, geocode4_corr
  ) as no_dup_citycode
  
ON test.citycode = no_dup_citycode.extra_code
WHERE year in ('2001', '2002', '2003', '2004', '2005') 
GROUP BY 
  geocode4_corr, 
  indu_2, 
  year 
  )
  SELECT
  indu_2,
  AVG(working_capital_cit) AS working_capital_i,
  AVG(asset_tangibility_cit) AS asset_tangibility_i,
  AVG(current_ratio_cit) AS current_ratio_i,
  AVG(cash_assets_cit) AS cash_assets_i,
  AVG(liabilities_assets_cit) AS liabilities_assets_i,
  AVG(return_on_asset_cit) AS return_on_asset_i,
  AVG(sales_assets_cit) AS sales_assets_it
  FROM ratio
  GROUP BY indu_2
  LIMIT 10
  )
"""
output = s3.run_query(
                    query=query,
                    database=DatabaseName,
                    s3_output=s3_output_example,
    filename = 'example_3'
                )
output
```

# Table `asif_city_industry_financial_ratio`


<!-- #region -->
Since the table to create has missing value, please use the following at the top of the query

```
CREATE TABLE database.table_name WITH (format = 'PARQUET') AS
```


Choose a location in S3 to save the CSV. It is recommended to save in it the `datalake-datascience` bucket. Locate an appropriate folder in the bucket, and make sure all output have the same format
<!-- #endregion -->

First, we need to delete the table (if exist)

```python
table_name = 'asif_city_industry_financial_ratio'
s3_output = 'DATA/ECON/FIRM_SURVEY/ASIF_CHINA/TRANSFORMED/FINANCIAL_RATIO'
```

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
SELECT *, CASE 
WHEN LENGTH(cic) = 4 THEN substr(cic,1, 2) 
ELSE concat('0',substr(cic,1, 1)) END AS indu_2

FROM firms_survey.asif_firms_prepared 
)
SELECT * 
FROM (
WITH ratio AS (
SELECT 
  year, 
  geocode4_corr,
  indu_2, 
  SUM(output) AS output,
  SUM(employ) AS employment,  
  SUM(c64) AS sales,  
  SUM(c81) + SUM(c80) - SUM(c96) AS working_capital_cit, 
  SUM(c85) - SUM(c91) AS asset_tangibility_cit, 
  CAST(
    SUM(cuasset) AS DECIMAL(16, 5)
  ) / NULLIF(
    CAST(
      SUM(c95) AS DECIMAL(16, 5)
    ), 
    0
  ) AS current_ratio_cit, 
  CAST(
    (SUM(c79) + SUM(c80) + SUM(c81)) - SUM(cuasset) AS DECIMAL(16, 5)
  ) / NULLIF(CAST(
    SUM(c93) AS DECIMAL(16, 5)
  ), 
    0
  ) AS cash_assets_cit, 
  CAST(
    SUM(c95) + SUM(c97) AS DECIMAL(16, 5)
  )/ NULLIF(
    CAST(
      SUM(c93) AS DECIMAL(16, 5)
    ), 
    0
  ) AS liabilities_assets_cit, 
  CAST(
    SUM(c64) - SUM(c134) AS DECIMAL(16, 5)
  )/ NULLIF(
    CAST(
      SUM(c98) AS DECIMAL(16, 5)
    ), 
    0
  ) AS return_on_asset_cit, 
  CAST(
    SUM(cuasset) AS DECIMAL(16, 5)
  )/ NULLIF(
    CAST(
      (
        SUM(c98) - lag(
          SUM(c98), 
          1
        ) over(
          partition by geocode4_corr, 
          indu_2 
          order by 
            geocode4_corr, 
            indu_2, 
            year
        )
      )/ 2 AS DECIMAL(16, 5)
    ), 
    0
  ) AS sales_assets_cit 
FROM test
INNER JOIN 
  (
  SELECT extra_code, geocode4_corr
  FROM chinese_lookup.china_city_code_normalised 
  GROUP BY extra_code, geocode4_corr
  ) as no_dup_citycode
  
ON test.citycode = no_dup_citycode.extra_code
WHERE year in ('2001', '2002', '2003', '2004', '2005', '2006', '2007') 
GROUP BY 
  geocode4_corr, 
  indu_2, 
  year 
) 
SELECT 
  ratio.geocode4_corr, 
  ratio.indu_2, 
  ratio.year,
  output, 
  employment,
  sales,
  working_capital_cit, 
  working_capital_ci, 
  working_capital_i, 
  asset_tangibility_cit, 
  asset_tangibility_ci, 
  asset_tangibility_i, 
  current_ratio_cit, 
  current_ratio_ci, 
  current_ratio_i, 
  cash_assets_cit, 
  cash_assets_ci, 
  cash_assets_i, 
  liabilities_assets_cit,
  liabilities_assets_ci, 
  liabilities_assets_i, 
  return_on_asset_cit, 
  return_on_asset_ci, 
  return_on_asset_i, 
  sales_assets_cit,
  sales_assets_ci,
  sales_assets_i
  FROM ratio
  LEFT JOIN (
    SELECT
  geocode4_corr, 
  indu_2,
  AVG(working_capital_cit) AS working_capital_ci,
  AVG(asset_tangibility_cit) AS asset_tangibility_ci,
  AVG(current_ratio_cit) AS current_ratio_ci,
  AVG(cash_assets_cit) AS cash_assets_ci,
  AVG(liabilities_assets_cit) AS liabilities_assets_ci,
  AVG(return_on_asset_cit) AS return_on_asset_ci,
  AVG(sales_assets_cit) AS sales_assets_ci
  FROM ratio
  WHERE year in ('2001', '2002', '2003', '2004', '2005') 
  GROUP BY geocode4_corr, indu_2
  
    ) as ratio_ci
    ON ratio.geocode4_corr = ratio_ci.geocode4_corr AND
    ratio.indu_2 = ratio_ci.indu_2
  LEFT JOIN (
    SELECT
  indu_2,
  AVG(working_capital_cit) AS working_capital_i,
  AVG(asset_tangibility_cit) AS asset_tangibility_i,
  AVG(current_ratio_cit) AS current_ratio_i,
  AVG(cash_assets_cit) AS cash_assets_i,
  AVG(liabilities_assets_cit) AS liabilities_assets_i,
  AVG(return_on_asset_cit) AS return_on_asset_i,
  AVG(sales_assets_cit) AS sales_assets_i
  FROM ratio
  WHERE year in ('2001', '2002', '2003', '2004', '2005') 
  GROUP BY indu_2
    ) as ratio_i
    ON ratio.indu_2 = ratio_i.indu_2    
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
query = """
SELECT COUNT(*) AS CNT
FROM {}.{} 
""".format(DatabaseName, table_name)
output = s3.run_query(
                    query=query,
                    database=DatabaseName,
                    s3_output=s3_output_example,
    filename = 'count_{}'.format(table_name)
                )
output
```

```python
query = """
SELECT len, COUNT(len) as CNT
FROM (
SELECT length(indu_2) AS len
FROM {}.{} 
)
GROUP BY len
ORDER BY CNT
""".format(DatabaseName, table_name)
output = s3.run_query(
                    query=query,
                    database=DatabaseName,
                    s3_output=s3_output_example,
    filename = 'example_1'
                )
output
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
partition_keys = ["geocode4_corr", "indu_2", "year"]
```

2. Add the steps number

```python
step = 0
```

3. Change the schema

Bear in mind that CSV SerDe (OpenCSVSerDe) does not support empty fields in columns defined as a numeric data type. All columns with missing values should be saved as string. 

```python
glue.get_table_information(
    database=DatabaseName,
    table=table_name)['Table']['StorageDescriptor']['Columns']
```

```python
schema = [{'Name': 'geocode4_corr', 'Type': 'string', 'Comment': ''},
          {'Name': 'indu_2', 'Type': 'string', 'Comment': 'Two digits industry. If length cic equals to 3, then add 0 to indu_2 '},
          {'Name': 'year', 'Type': 'string', 'Comment': ''},
          {'Name': 'output', 'Type': 'bigint', 'Comment': ''},
          {'Name': 'employment', 'Type': 'bigint', 'Comment': ''},
          {'Name': 'sales', 'Type': 'bigint', 'Comment': ''},
          {'Name': 'working_capital_cit', 'Type': 'bigint', 'Comment': 'Inventory [存货 (c81)] + Accounts receivable [应收帐款 (c80)] - Accounts payable [应付帐款  (c96)] city industry year'},
          {'Name': 'working_capital_ci', 'Type': 'double', 'Comment': 'Inventory [存货 (c81)] + Accounts receivable [应收帐款 (c80)] - Accounts payable [应付帐款  (c96)] city industry'},
          {'Name': 'working_capital_i', 'Type': 'double', 'Comment': 'Inventory [存货 (c81)] + Accounts receivable [应收帐款 (c80)] - Accounts payable [应付帐款  (c96)] industry'},
          {'Name': 'asset_tangibility_cit', 'Type': 'bigint', 'Comment': 'Total fixed assets [固定资产合计 (c85)] - Intangible assets [无形资产 (c91)] city industry year'},
          {'Name': 'asset_tangibility_ci', 'Type': 'double', 'Comment': 'Total fixed assets [固定资产合计 (c85)] - Intangible assets [无形资产 (c91)] city industry'},
          {'Name': 'asset_tangibility_i', 'Type': 'double', 'Comment': 'Total fixed assets [固定资产合计 (c85)] - Intangible assets [无形资产 (c91)] industry'},
          {'Name': 'current_ratio_cit',
              'Type': 'decimal(21,5)', 'Comment': 'Current asset [cuasset] / Current liabilities [c95]  city industry year'},
          {'Name': 'current_ratio_ci', 'Type': 'decimal(21,5)', 'Comment': 'Current asset [cuasset] / Current liabilities [c95] city industry'},
          {'Name': 'current_ratio_i', 'Type': 'decimal(21,5)', 'Comment': 'Current asset [cuasset] / Current liabilities [c95] industry'},
          {'Name': 'cash_assets_cit', 'Type': 'decimal(21,5)', 'Comment': 'Cash [( 其中：短期投资 (c79) + 应收帐款 (c80) + 存货 (c81)) - cuasset)] /  Assets [其中：短期投资 (c79) + 应收帐款 (c80) + 存货 (c81)]  city industry year'},
          {'Name': 'cash_assets_ci', 'Type': 'decimal(21,5)', 'Comment': 'Cash [( 其中：短期投资 (c79) + 应收帐款 (c80) + 存货 (c81)) - cuasset)] /  Assets [其中：短期投资 (c79) + 应收帐款 (c80) + 存货 (c81)] city industry'},
          {'Name': 'cash_assets_i', 'Type': 'decimal(21,5)', 'Comment': 'Cash [( 其中：短期投资 (c79) + 应收帐款 (c80) + 存货 (c81)) - cuasset)] /  Assets [其中：短期投资 (c79) + 应收帐款 (c80) + 存货 (c81)] industry'},
          {'Name': 'liabilities_assets_cit',
              'Type': 'decimal(21,5)', 'Comment': 'Liabilities [(流动负债合计 (c95) + 长期负债合计 (c97))] /  Total assets [资产总计318 (c93)]  city industry year'},
          {'Name': 'liabilities_assets_ci',
              'Type': 'decimal(21,5)', 'Comment': 'Liabilities [(流动负债合计 (c95) + 长期负债合计 (c97))] /  Total assets [资产总计318 (c93)] city industry'},
          {'Name': 'liabilities_assets_i',
              'Type': 'decimal(21,5)', 'Comment': 'Liabilities [(流动负债合计 (c95) + 长期负债合计 (c97))] /  Total assets [资产总计318 (c93)] industry'},
          {'Name': 'return_on_asset_cit',
              'Type': 'decimal(21,5)', 'Comment': 'Total annual revenue [全年营业收入合计 (c64) ] / (Delta Total assets 318 [$\Delta$ 资产总计318 (c98)]/2)  city industry year'},
          {'Name': 'return_on_asset_ci',
              'Type': 'decimal(21,5)', 'Comment': 'Total annual revenue [全年营业收入合计 (c64) ] / (Delta Total assets 318 [$\Delta$ 资产总计318 (c98)]/2) city industry'},
          {'Name': 'return_on_asset_i',
              'Type': 'decimal(21,5)', 'Comment': 'Total annual revenue [全年营业收入合计 (c64) ] / (Delta Total assets 318 [$\Delta$ 资产总计318 (c98)]/2) industry'},
          {'Name': 'sales_assets_cit', 'Type': 'decimal(21,5)', 'Comment': '(Total annual revenue - Income tax payable) [(全年营业收入合计 (c64) - 应交所得税 (c134))] / Total assets [资产总计318 (c98)]  city industry year'},
          {'Name': 'sales_assets_ci', 'Type': 'decimal(21,5)', 'Comment': '(Total annual revenue - Income tax payable) [(全年营业收入合计 (c64) - 应交所得税 (c134))] / Total assets [资产总计318 (c98)] city industry'},
          {'Name': 'sales_assets_i', 'Type': 'decimal(21,5)', 'Comment': '(Total annual revenue - Income tax payable) [(全年营业收入合计 (c64) - 应交所得税 (c134))] / Total assets [资产总计318 (c98)] industry'}]
```

4. Provide a description

```python
description = """
Compute the financial ratio by city industry year, city industry and industry
"""
```

5. provide metadata

- DatabaseName
- TablePrefix
- 

```python
DatabaseName = 'firms_survey'
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
    'TableName' : table_name,
    'target_S3URI' : os.path.join('s3://',bucket, s3_output),
    'from_athena': 'True'    
    }
}
json_etl
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
                    for (index, d) in enumerate(parameters['TABLES']['TRANSFORMATION']['STEPS'])
                    if d["step"] == step
                ),
                None,
            )
if index_to_remove != None:
    parameters['TABLES']['TRANSFORMATION']['STEPS'].pop(index_to_remove)
```

```python
parameters['TABLES']['TRANSFORMATION']['STEPS'].append(json_etl)
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
glue.update_schema_table(
    database = DatabaseName,
    table = table_name,
    schema= schema)
```

## Check Duplicates

One of the most important step when creating a table is to check if the table contains duplicates. The cell below checks if the table generated before is empty of duplicates. The code uses the JSON file to create the query parsed in Athena. 

You are required to define the group(s) that Athena will use to compute the duplicate. For instance, your table can be grouped by COL1 and COL2 (need to be string or varchar), then pass the list ['COL1', 'COL2'] 

```python
partition_keys = ["geocode4_corr", "indu_2", "year"]

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
- `keys`: Variable name to perform the grouping -> Only one variable for now, Variable name to perform the secondary grouping -> Only one variable for now
    - format: 'A,B'
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
secondary_key = 'indu_2'
y_var = 'working_capital_cit'
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
        "threshold":0.5
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
