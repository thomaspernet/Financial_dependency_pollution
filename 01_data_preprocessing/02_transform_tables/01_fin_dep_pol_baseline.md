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
# Transform (creating time break variables) financial ration data and merging pollution tables to S3

# Objective(s)

## Business needs 

Transform (creating time-break variables and fixed effect) asif_financial_ratio data and merging pollution, industry and city mandate tables using Athena and save output to S3 + Glue. 

## Description

**Objective**

Construct the time-break and fixed effect variables

**Construction variables**

* time-break: If year prior to 2006, then False else True
* Fixed effect:
  * city-industry: FE_c_i
  * time-industry: FE_t_i
  * city-time: FE_c_t

**Steps**

We will clean the table by doing the following steps:

1. merge tables asif_city_industry_financial_ratio  with
  1. china_city_sector_pollution 
  2. china_city_tcz_spz
  3. china_city_reduction_mandate
  4. china_code_normalised
  5. ind_cic_2_name

**Cautious**

* Make sure there is no duplicates

![](https://drive.google.com/uc?export=view&id=1QyMI8wkI7SNGbQ-aHR-Ci_rrKwE9bGit)


Target
* The file is saved in S3: 
  * bucket: datalake-datascience 
  * path: DATA/ENVIRONMENT/CHINA/FYP/FINANCIAL_CONTRAINT/PAPER_FYP_FINANCE_POL/BASELINE 
* Glue data catalog should be updated
  * database: environment 
  * table prefix: fin_dep_pollution 
    * table name (prefix + last folder S3 path): fin_dep_pollution_baseline 
* Analytics
  * HTML:  ANALYTICS/HTML_OUTPUT/FIN_DEP_POLLUTION_BASELINE 
  * Notebook:  ANALYTICS/OUTPUT/FIN_DEP_POLLUTION_BASELINE 

# Metadata

* Key: jfl55vdtx66109y
* Parent key (for update parent):  
* Notebook US Parent (i.e the one to update): 
* https://github.com/thomaspernet/Financial_dependency_pollution/blob/master/01_data_preprocessing/02_transform_tables/01_fin_dep_pol_baseline.md
* Epic: Epic 2
* US: US 2
* Date Begin: 11/24/2020
* Duration Task: 1
* Description: Transform (creating time-break variables and fixed effect) asif_financial_ratio data and merging pollution, industry and city mandate tables using Athena and save output to S3 + Glue. 
* Step type: Transform table
* Status: Active
* Source URL: US 02 Baseline
* Task type: Jupyter Notebook
* Users: Thomas Pernet
* Watchers: Thomas Pernet
* User Account: https://468786073381.signin.aws.amazon.com/console
* Estimated Log points: 8
* Task tag: #baseline-table,#financial-ratio,#policy,#pollution
* Toggl Tag: #data-transformation
* current nb commits: 0
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
* asif_city_industry_financial_ratio
* china_city_reduction_mandate
* china_city_sector_pollution 
* china_city_tcz_spz
* china_code_normalised
* ind_cic_2_name
* Github: 
  * https://github.com/thomaspernet/Financial_dependency_pollution/blob/master/01_data_preprocessing/02_transform_tables/00_asif_financial_ratio.md
  * https://github.com/thomaspernet/Financial_dependency_pollution/blob/master/01_data_preprocessing/00_download_data_from/CITY_REDUCTION_MANDATE/city_reduction_mandate.py
  * https://github.com/thomaspernet/Financial_dependency_pollution/blob/master/01_data_preprocessing/00_download_data_from/CITY_SECTOR_POLLUTION/city_sector_pollution.py
  * https://github.com/thomaspernet/Financial_dependency_pollution/blob/master/01_data_preprocessing/00_download_data_from/TCZ_SPZ/tcz_spz_policy.py
  * https://github.com/thomaspernet/Financial_dependency_pollution/blob/master/01_data_preprocessing/00_download_data_from/CITY_CODE_CORRESPONDANCE/city_code_correspondance.py
  * https://github.com/thomaspernet/Financial_dependency_pollution/blob/master/01_data_preprocessing/00_download_data_from/CIC_NAME/cic_industry_name.py

# Destination Output/Delivery

## Table/file

* Origin: 
* S3
* Athena
* Name:
* DATA/ENVIRONMENT/CHINA/FYP/FINANCIAL_CONTRAINT/PAPER_FYP_FINANCE_POL/BASELINE
* fin_dep_pollution_baseline
* GitHub:
* https://github.com/thomaspernet/Financial_dependency_pollution/blob/master/01_data_preprocessing/02_transform_tables/01_fin_dep_pol_baseline.md
* URL: 
  * datalake-datascience/DATA/ENVIRONMENT/CHINA/FYP/FINANCIAL_CONTRAINT/PAPER_FYP_FINANCE_POL/BASELINE
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

1. Load the pollution data: china_city_sector_pollution
2. Merge with:
    - asif_city_industry_financial_ratio
    - china_city_reduction_mandate
    - china_city_tcz_spz
    - china_code_normalised
    - ind_cic_2_name
3. Scale working capital and tangible asset by a factor of 1000000
4. Keep year 2001 to 2007 and keep when SO2 emission are strickly possitive


## Example step by step

```python
DatabaseName = 'environment'
s3_output_example = 'SQL_OUTPUT_ATHENA'
```

```python
query= """
SELECT *
FROM environment.china_city_sector_pollution
LIMIT 10
"""
output = s3.run_query(
                    query=query,
                    database=DatabaseName,
                    s3_output=s3_output_example,
    filename = 'example_1'
                )
output
```

Count number of observation in `china_city_sector_pollution`

```python
query= """
SELECT COUNT(*) AS CNT
FROM environment.china_city_sector_pollution
"""
output = s3.run_query(
                    query=query,
                    database=DatabaseName,
                    s3_output=s3_output_example,
    filename = 'count_1'
                )
output
```

Need to tackle duplicates in the table

![](https://drive.google.com/uc?export=view&id=1QyMI8wkI7SNGbQ-aHR-Ci_rrKwE9bGit)

```python
query = """
SELECT year, citycode, geocode4_corr, china_city_sector_pollution.cityen, indus_code, ind2, tso2, lower_location, larger_location, coastal
FROM environment.china_city_sector_pollution 
INNER JOIN (
  SELECT extra_code, geocode4_corr
  FROM chinese_lookup.china_city_code_normalised 
  GROUP BY extra_code, geocode4_corr
  ) as no_dup_citycode
ON china_city_sector_pollution.citycode = no_dup_citycode.extra_code
WHERE citycode in ('1101', '1102') and year = '2001' and indus_code = '3123'
"""
output = s3.run_query(
                    query=query,
                    database=DatabaseName,
                    s3_output=s3_output_example,
    filename = 'count_1'
                )
output
```

We can group by `geocode4_corr` 

```python
query = """
SELECT year, geocode4_corr, cityen, indus_code, ind2, SUM(tso2), lower_location, larger_location, coastal

FROM (
  SELECT 
year, citycode, geocode4_corr, china_city_sector_pollution.cityen, indus_code, ind2, tso2, lower_location, larger_location, coastal
FROM environment.china_city_sector_pollution 
INNER JOIN (
  SELECT extra_code, geocode4_corr
  FROM chinese_lookup.china_city_code_normalised 
  GROUP BY extra_code, geocode4_corr
  ) as no_dup_citycode
ON china_city_sector_pollution.citycode = no_dup_citycode.extra_code
WHERE citycode in ('1101', '1102') and year = '2001' and indus_code = '3123'
  )
  GROUP BY year, geocode4_corr, cityen, indus_code, ind2, lower_location, larger_location, coastal
"""
output = s3.run_query(
                    query=query,
                    database=DatabaseName,
                    s3_output=s3_output_example,
    filename = 'count_1'
                )
output
```

Make sure the total amount of `tso2` is the same before and after, or at least lower because of no match with the city

```python
query = """
WITH test AS (
SELECT year, geocode4_corr, cityen, indus_code, ind2, SUM(tso2) as tso2, lower_location, larger_location, coastal

FROM (
  SELECT 
year, citycode, geocode4_corr, china_city_sector_pollution.cityen, indus_code, ind2, tso2, lower_location, larger_location, coastal
FROM environment.china_city_sector_pollution 
INNER JOIN (
  SELECT extra_code, geocode4_corr
  FROM chinese_lookup.china_city_code_normalised 
  GROUP BY extra_code, geocode4_corr
  ) as no_dup_citycode
ON china_city_sector_pollution.citycode = no_dup_citycode.extra_code
  )
  GROUP BY year, geocode4_corr, cityen, indus_code, ind2, lower_location, larger_location, coastal
  )
  SELECT SUM(tso2) as total_so2
  FROM test
"""
output_1 = s3.run_query(
                    query=query,
                    database=DatabaseName,
                    s3_output=s3_output_example,
    filename = 'count_1'
                )
output_1
```

```python
query = """
SELECT SUM(tso2) as total_so2
  FROM environment.china_city_sector_pollution
"""
output_2 = s3.run_query(
                    query=query,
                    database=DatabaseName,
                    s3_output=s3_output_example,
    filename = 'count_1'
                )
output_2
```

After grouping, the total amount of SO2 is lower, indicating that some cities are not matched with our list. 

```python
output_1 == output_2
```

```python
query= """
SELECT *
FROM firms_survey.asif_city_industry_financial_ratio
LIMIT 10
"""
output = s3.run_query(
                    query=query,
                    database=DatabaseName,
                    s3_output=s3_output_example,
    filename = 'example_2'
                )
output
```

```python
query= """
SELECT *
FROM policy.china_city_reduction_mandate
LIMIT 10
"""
output = s3.run_query(
                    query=query,
                    database=DatabaseName,
                    s3_output=s3_output_example,
    filename = 'example_3'
                )
output
```

Check if duplicated in `china_city_reduction_mandate` with `citycn` or `cityen`

```python
query ="""
SELECT CNT, COUNT(*)
FROM (
SELECT citycn, COUNT(*) AS CNT
FROM policy.china_city_reduction_mandate
GROUP BY citycn
  ) 
  GROUP BY CNT
"""
output = s3.run_query(
                    query=query,
                    database=DatabaseName,
                    s3_output=s3_output_example,
    filename = 'example_3'
                )
output
```

```python
query ="""
SELECT CNT, COUNT(*)
FROM (
SELECT cityen, COUNT(*) AS CNT
FROM policy.china_city_reduction_mandate
GROUP BY cityen
  ) 
  GROUP BY CNT
"""
output = s3.run_query(
                    query=query,
                    database=DatabaseName,
                    s3_output=s3_output_example,
    filename = 'example_3'
                )
output
```

```python
query = """
SELECT cityen, COUNT(*) AS CNT
FROM policy.china_city_reduction_mandate
GROUP BY cityen
HAVING COUNT(*) > 1
"""
output = s3.run_query(
                    query=query,
                    database=DatabaseName,
                    s3_output=s3_output_example,
    filename = 'example_3'
                )
output
```

In this case, we should ignore the english, because same spelling in English but different in Chinese

```python
query ="""
SELECT *
FROM policy.china_city_reduction_mandate
WHERE cityen = 'Suzhou'
"""
output = s3.run_query(
                    query=query,
                    database=DatabaseName,
                    s3_output=s3_output_example,
    filename = 'example_3'
                )
output
```

Count number of observation in `china_city_reduction_mandate`

```python
query= """
SELECT COUNT(*) AS CNT
FROM policy.china_city_reduction_mandate
"""
output = s3.run_query(
                    query=query,
                    database=DatabaseName,
                    s3_output=s3_output_example,
    filename = 'count_2'
                )
output
```

```python
query= """
SELECT *
FROM policy.china_city_tcz_spz
LIMIT 10
"""
output = s3.run_query(
                    query=query,
                    database=DatabaseName,
                    s3_output=s3_output_example,
    filename = 'example_4'
                )
output
```

```python
query= """
SELECT *
FROM chinese_lookup.china_city_code_normalised
LIMIT 10
"""
output = s3.run_query(
                    query=query,
                    database=DatabaseName,
                    s3_output=s3_output_example,
    filename = 'example_5'
                )
output
```

```python
query= """
SELECT *
FROM chinese_lookup.ind_cic_2_name
LIMIT 10
"""
output = s3.run_query(
                    query=query,
                    database=DatabaseName,
                    s3_output=s3_output_example,
    filename = 'example_6'
                )
output
```

Test merge on `china_city_reduction_mandate` and `china_city_code_normalised`

Shanghai for instance has more than 3 codes.  Note that, in the final merge, we keep when `extra_code` is equal to `geocode4_corr` because we have already matched the other table on `geocode4_corr` 

```python
query = """
SELECT china_city_code_normalised.citycn, extra_code, geocode4_corr, tso2_mandate_c, in_10_000_tonnes
FROM policy.china_city_reduction_mandate
INNER JOIN chinese_lookup.china_city_code_normalised 
ON china_city_reduction_mandate.citycn = china_city_code_normalised.citycn
-- WHERE extra_code = geocode4_corr
LIMIT 10
"""
output = s3.run_query(
                    query=query,
                    database=DatabaseName,
                    s3_output=s3_output_example,
    filename = 'example_7'
                )
output
```

## Check the table with many city code

A city can have more than one codes over time. Below, we show which tables have more than one code so we know on which columns we need to merge `extra_code` or `geocode4_corr` 


**china_city_sector_pollution**

The table `china_city_sector_pollution` needs to be matched using `extra_code`

```python
query = """
SELECT DISTINCT(citycode)
FROM environment.china_city_sector_pollution
WHERE citycode in ('3100','3101','3102')
"""
output = s3.run_query(
                    query=query,
                    database=DatabaseName,
                    s3_output=s3_output_example,
    filename = 'example_8'
                )
output
```

**asif_city_industry_financial_ratio**

The table `asif_city_industry_financial_ratio` needs to be matched using `extra_code`

```python
query = """
SELECT DISTINCT(geocode4_corr)
FROM firms_survey.asif_city_industry_financial_ratio
WHERE geocode4_corr in ('3100','3101','3102')
"""
output = s3.run_query(
                    query=query,
                    database=DatabaseName,
                    s3_output=s3_output_example,
    filename = 'example_8'
                )
output
```

**china_city_tcz_spz**

The table `china_city_tcz_spz` needs to be matched using `geocode4_corr`

```python
query = """
SELECT DISTINCT(geocode4_corr)
FROM policy.china_city_tcz_spz
WHERE geocode4_corr in ('3100','3101','3102')
"""
output = s3.run_query(
                    query=query,
                    database=DatabaseName,
                    s3_output=s3_output_example,
    filename = 'example_8'
                )
output
```

### Test merge

Let's test the merge with the tables on `citycode` with  `china_city_code_normalised` to be sure there is no duplicates


Test by adding the aggregated pollution table with the financial ratio


```python
query = """
WITH aggregate_pol AS (
  SELECT 
    year, 
    geocode4_corr, 
    cityen, 
    indus_code, 
    ind2, 
    SUM(tso2) as tso2, 
    lower_location, 
    larger_location, 
    coastal 
  FROM 
    (
      SELECT 
        year, 
        citycode, 
        geocode4_corr, 
        china_city_sector_pollution.cityen, 
        indus_code, 
        ind2, 
        tso2, 
        lower_location, 
        larger_location, 
        coastal 
      FROM 
        environment.china_city_sector_pollution 
        INNER JOIN (
          SELECT 
            extra_code, 
            geocode4_corr 
          FROM 
            chinese_lookup.china_city_code_normalised 
          GROUP BY 
            extra_code, 
            geocode4_corr
        ) as no_dup_citycode ON china_city_sector_pollution.citycode = no_dup_citycode.extra_code
    ) 
  GROUP BY 
    year, 
    geocode4_corr, 
    cityen, 
    indus_code, 
    ind2, 
    lower_location, 
    larger_location, 
    coastal
) 
SELECT 
  * 
FROM 
  (
    WITH merge_asif AS (
      SELECT 
        asif_city_industry_financial_ratio.year, 
        asif_city_industry_financial_ratio.geocode4_corr, 
        cityen, 
        cic, 
        ind2, 
        tso2, 
        lower_location, 
        larger_location, 
        coastal 
      FROM 
        aggregate_pol 
        INNER JOIN firms_survey.asif_city_industry_financial_ratio ON aggregate_pol.geocode4_corr = asif_city_industry_financial_ratio.geocode4_corr 
        AND aggregate_pol.year = asif_city_industry_financial_ratio.year 
        AND aggregate_pol.indus_code = asif_city_industry_financial_ratio.cic
    ) 
    SELECT 
      CNT, 
      COUNT(CNT) AS dup 
    FROM 
      (
        SELECT 
          geocode4_corr, 
          year, 
          cic, 
          COUNT(*) AS CNT 
        FROM 
          merge_asif 
        GROUP BY 
          geocode4_corr, 
          year, 
          cic
      ) AS count_dup 
    GROUP BY 
      CNT
  )
"""
output = s3.run_query(
                    query=query,
                    database=DatabaseName,
                    s3_output=s3_output_example,
    filename = 'example_9'
                )
output
```

Test by adding the aggregated pollution table with the financial ratio and reduction mandate

```python
query = """
WITH aggregate_pol AS (
  SELECT 
    year, 
    geocode4_corr, 
    cityen, 
    indus_code, 
    ind2, 
    SUM(tso2) as tso2, 
    lower_location, 
    larger_location, 
    coastal 
  FROM 
    (
      SELECT 
        year, 
        citycode, 
        geocode4_corr, 
        china_city_sector_pollution.cityen, 
        indus_code, 
        ind2, 
        tso2, 
        lower_location, 
        larger_location, 
        coastal 
      FROM 
        environment.china_city_sector_pollution 
        INNER JOIN (
          SELECT 
            extra_code, 
            geocode4_corr 
          FROM 
            chinese_lookup.china_city_code_normalised 
          GROUP BY 
            extra_code, 
            geocode4_corr
        ) as no_dup_citycode ON china_city_sector_pollution.citycode = no_dup_citycode.extra_code
    ) 
  GROUP BY 
    year, 
    geocode4_corr, 
    cityen, 
    indus_code, 
    ind2, 
    lower_location, 
    larger_location, 
    coastal
) 
SELECT 
  * 
FROM 
  (
    WITH merge_asif AS (
      SELECT 
        asif_city_industry_financial_ratio.year, 
        asif_city_industry_financial_ratio.geocode4_corr, 
        cityen, 
        cic, 
        ind2, 
        tso2, 
        tso2_mandate_c,
        in_10_000_tonnes
        lower_location, 
        larger_location, 
        coastal 
      FROM 
        aggregate_pol 
        INNER JOIN firms_survey.asif_city_industry_financial_ratio ON aggregate_pol.geocode4_corr = asif_city_industry_financial_ratio.geocode4_corr 
        AND aggregate_pol.year = asif_city_industry_financial_ratio.year 
        AND aggregate_pol.indus_code = asif_city_industry_financial_ratio.cic
        INNER JOIN (
        
        SELECT geocode4_corr, tso2_mandate_c, in_10_000_tonnes
        FROM policy.china_city_reduction_mandate
        INNER JOIN chinese_lookup.china_city_code_normalised 
        ON china_city_reduction_mandate.citycn = china_city_code_normalised.citycn
        WHERE extra_code = geocode4_corr
        ) as city_mandate
        ON aggregate_pol.geocode4_corr = city_mandate.geocode4_corr
    ) 
    SELECT 
      CNT, 
      COUNT(CNT) AS dup 
    FROM 
      (
        SELECT 
          geocode4_corr, 
          year, 
          cic, 
          COUNT(*) AS CNT 
        FROM 
          merge_asif 
        GROUP BY 
          geocode4_corr, 
          year, 
          cic
      ) AS count_dup 
    GROUP BY 
      CNT
  )
"""
output = s3.run_query(
                    query=query,
                    database=DatabaseName,
                    s3_output=s3_output_example,
    filename = 'example_9'
                )
output
```

Test by adding the aggregated pollution table with the financial ratio, reduction mandate and TCZ policy

```python
query = """
WITH aggregate_pol AS (
  SELECT 
    year, 
    geocode4_corr, 
    cityen, 
    indus_code, 
    ind2, 
    SUM(tso2) as tso2, 
    lower_location, 
    larger_location, 
    coastal 
  FROM 
    (
      SELECT 
        year, 
        citycode, 
        geocode4_corr, 
        china_city_sector_pollution.cityen, 
        indus_code, 
        ind2, 
        tso2, 
        lower_location, 
        larger_location, 
        coastal 
      FROM 
        environment.china_city_sector_pollution 
        INNER JOIN (
          SELECT 
            extra_code, 
            geocode4_corr 
          FROM 
            chinese_lookup.china_city_code_normalised 
          GROUP BY 
            extra_code, 
            geocode4_corr
        ) as no_dup_citycode ON china_city_sector_pollution.citycode = no_dup_citycode.extra_code
    ) 
  GROUP BY 
    year, 
    geocode4_corr, 
    cityen, 
    indus_code, 
    ind2, 
    lower_location, 
    larger_location, 
    coastal
) 
SELECT 
  * 
FROM 
  (
    WITH merge_asif AS (
      SELECT 
        asif_city_industry_financial_ratio.year, 
        asif_city_industry_financial_ratio.geocode4_corr, 
        cityen, 
        cic, 
        ind2, 
        tso2, 
        tso2_mandate_c,
        in_10_000_tonnes
        lower_location, 
        larger_location, 
        coastal 
      FROM 
        aggregate_pol 
        INNER JOIN firms_survey.asif_city_industry_financial_ratio ON aggregate_pol.geocode4_corr = asif_city_industry_financial_ratio.geocode4_corr 
        AND aggregate_pol.year = asif_city_industry_financial_ratio.year 
        AND aggregate_pol.indus_code = asif_city_industry_financial_ratio.cic
        INNER JOIN (
        
        SELECT geocode4_corr, tso2_mandate_c, in_10_000_tonnes
        FROM policy.china_city_reduction_mandate
        INNER JOIN chinese_lookup.china_city_code_normalised 
        ON china_city_reduction_mandate.citycn = china_city_code_normalised.citycn
        WHERE extra_code = geocode4_corr
        ) as city_mandate
        ON aggregate_pol.geocode4_corr = city_mandate.geocode4_corr
        LEFT JOIN policy.china_city_tcz_spz
        ON aggregate_pol.geocode4_corr = china_city_tcz_spz.geocode4_corr
    ) 
    SELECT 
      CNT, 
      COUNT(CNT) AS dup 
    FROM 
      (
        SELECT 
          geocode4_corr, 
          year, 
          cic, 
          COUNT(*) AS CNT 
        FROM 
          merge_asif 
        GROUP BY 
          geocode4_corr, 
          year, 
          cic
      ) AS count_dup 
    GROUP BY 
      CNT
  )
"""
output = s3.run_query(
                    query=query,
                    database=DatabaseName,
                    s3_output=s3_output_example,
    filename = 'example_9'
                )
output
```

# Table `fin_dep_pollution_baseline`

Since the table to create has missing value, please use the following at the top of the query

```
CREATE TABLE database.table_name WITH (format = 'PARQUET') AS
```


Choose a location in S3 to save the CSV. It is recommended to save in it the `datalake-datascience` bucket. Locate an appropriate folder in the bucket, and make sure all output have the same format

```python
s3_output = 'DATA/ENVIRONMENT/CHINA/FYP/FINANCIAL_CONTRAINT/PAPER_FYP_FINANCE_POL/BASELINE'
table_name = 'fin_dep_pollution_baseline'
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

WITH aggregate_pol AS (
  SELECT 
    year, 
    geocode4_corr,
    provinces,
    cityen, 
    indus_code, 
    ind2, 
    SUM(tso2) as tso2, 
    lower_location, 
    larger_location, 
    coastal 
  FROM 
    (
      SELECT 
        year, 
        provinces,
        citycode, 
        geocode4_corr, 
        china_city_sector_pollution.cityen, 
        indus_code, 
        ind2, 
        tso2, 
        lower_location, 
        larger_location, 
        coastal 
      FROM 
        environment.china_city_sector_pollution 
        INNER JOIN (
          SELECT 
            extra_code, 
            geocode4_corr 
          FROM 
            chinese_lookup.china_city_code_normalised 
          GROUP BY 
            extra_code, 
            geocode4_corr
        ) as no_dup_citycode ON china_city_sector_pollution.citycode = no_dup_citycode.extra_code
    ) 
  GROUP BY 
    year, 
    provinces,
    geocode4_corr, 
    cityen, 
    indus_code, 
    ind2, 
    lower_location, 
    larger_location, 
    coastal
) 
SELECT 
  asif_city_industry_financial_ratio.year, 
  CASE WHEN aggregate_pol.year in (
    '2001', '2002', '2003', '2004', '2005'
  ) THEN 'FALSE' WHEN aggregate_pol.year in ('2006', '2007') THEN 'TRUE' END AS period, 
  provinces, 
  cityen, 
  asif_city_industry_financial_ratio.geocode4_corr, 
  CASE WHEN tcz IS NULL THEN '0' ELSE tcz END AS tcz,
  CASE WHEN spz IS NULL THEN '0' ELSE spz END AS spz,
  asif_city_industry_financial_ratio.cic, 
  ind2, 
  CASE WHEN short IS NULL THEN 'Unknown' ELSE short END AS short,
  tso2, 
  tso2_mandate_c, 
  in_10_000_tonnes, 
  working_capital_cit/1000000 AS working_capital_cit, 
  working_capital_ci/1000000 AS working_capital_ci, 
  working_capital_i/1000000 AS working_capital_i, 
  asset_tangibility_cit/1000000 AS asset_tangibility_cit, 
  asset_tangibility_ci/1000000 AS asset_tangibility_ci, 
  asset_tangibility_i/1000000 AS asset_tangibility_i, 
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
  sales_assets_i, 
  lower_location, 
  larger_location, 
  coastal, 
  DENSE_RANK() OVER (
    ORDER BY 
      city_mandate.geocode4_corr, 
      asif_city_industry_financial_ratio.cic
  ) AS fe_c_i, 
  DENSE_RANK() OVER (
    ORDER BY 
      asif_city_industry_financial_ratio.year, 
      asif_city_industry_financial_ratio.cic
  ) AS fe_t_i, 
  DENSE_RANK() OVER (
    ORDER BY 
      city_mandate.geocode4_corr, 
      asif_city_industry_financial_ratio.year
  ) AS fe_c_t 
      FROM 
        aggregate_pol 
        INNER JOIN firms_survey.asif_city_industry_financial_ratio ON aggregate_pol.geocode4_corr = asif_city_industry_financial_ratio.geocode4_corr 
        AND aggregate_pol.year = asif_city_industry_financial_ratio.year 
        AND aggregate_pol.indus_code = asif_city_industry_financial_ratio.cic
        INNER JOIN (
        
        SELECT geocode4_corr, tso2_mandate_c, in_10_000_tonnes
        FROM policy.china_city_reduction_mandate
        INNER JOIN chinese_lookup.china_city_code_normalised 
        ON china_city_reduction_mandate.citycn = china_city_code_normalised.citycn
        WHERE extra_code = geocode4_corr
        ) as city_mandate
        ON aggregate_pol.geocode4_corr = city_mandate.geocode4_corr
        LEFT JOIN policy.china_city_tcz_spz
        ON aggregate_pol.geocode4_corr = china_city_tcz_spz.geocode4_corr
        LEFT JOIN chinese_lookup.ind_cic_2_name ON aggregate_pol.ind2 = ind_cic_2_name.cic 

WHERE 
  asif_city_industry_financial_ratio.year in (
    '2001', '2002', '2003', '2004', '2005', 
    '2006', '2007'
  ) AND tso2 > 0
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

1. Add a partition key:
    - Inform if there is group in the table so that, the parser can compute duplicate
2. Add the steps number -> Not automtic yet. Start at 0
3. Change the schema if needed. It is highly recommanded to add comment to the fields
4. Provide a description -> detail the steps 


1. Add a partition key

```python
partition_keys = ["geocode4_corr", "year", "cic"]
```

2. Add the steps number

```python
step = 1
```

3. Change the schema

Bear in mind that CSV SerDe (OpenCSVSerDe) does not support empty fields in columns defined as a numeric data type. All columns with missing values should be saved as string. 

```python
glue.get_table_information(
    database = DatabaseName,
    table = table_name)['Table']['StorageDescriptor']['Columns']
```

```python
schema = [{'Name': 'year', 'Type': 'string', 'Comment': 'year from 2001 to 2007'},
 {'Name': 'period', 'Type': 'string', 'Comment': 'False if year before 2005 included, True if year 2006 and 2007'},
 {'Name': 'province', 'Type': 'string', 'Comment': ''},
 {'Name': 'city', 'Type': 'string', 'Comment': ''},
 {'Name': 'geocode4_corr', 'Type': 'string', 'Comment': ''},
 {'Name': 'tcz', 'Type': 'string', 'Comment': 'Two control zone policy city'},
 {'Name': 'spz', 'Type': 'string', 'Comment': 'Special policy zone policy city'},
 {'Name': 'cic', 'Type': 'string', 'Comment': '4 digits industry'},
 {'Name': 'ind2', 'Type': 'string', 'Comment': ''},
 {'Name': 'short', 'Type': 'string', 'Comment': ''},
 {'Name': 'tso2', 'Type': 'int', 'Comment': 'Total so2 city sector'},
 {'Name': 'tso2_mandate_c', 'Type': 'float', 'Comment': 'city reduction mandate in tonnes'},
 {'Name': 'in_10_000_tonnes', 'Type': 'float', 'Comment': 'city reduction mandate in 10k tonnes'},
 {
   "Name": "working_capital_cit",
   "Type": "bigint",
   "Comment": "Inventory [存货 (c81)] + Accounts receivable [应收帐款 (c80)] - Accounts payable [应付帐款  (c96)] city industry year. Scaled by a factor of 1000000"
},
   {
   "Name": "working_capital_ci",
   "Type": "double",
   "Comment": "Inventory [存货 (c81)] + Accounts receivable [应收帐款 (c80)] - Accounts payable [应付帐款  (c96)] city industry. Scaled by a factor of 1000000"
},
   {
   "Name": "working_capital_i",
   "Type": "double",
   "Comment": "Inventory [存货 (c81)] + Accounts receivable [应收帐款 (c80)] - Accounts payable [应付帐款  (c96)] industry. Scaled by a factor of 1000000"
},
   {
   "Name": "asset_tangibility_cit",
   "Type": "bigint",
   "Comment": "Total fixed assets [固定资产合计 (c85)] - Intangible assets [无形资产 (c91)] city industry year. Scaled by a factor of 1000000"
},
   {
   "Name": "asset_tangibility_ci",
   "Type": "double",
   "Comment": "Total fixed assets [固定资产合计 (c85)] - Intangible assets [无形资产 (c91)] city industry. Scaled by a factor of 1000000"
},
   {
   "Name": "asset_tangibility_i",
   "Type": "double",
   "Comment": "Total fixed assets [固定资产合计 (c85)] - Intangible assets [无形资产 (c91)] industry. Scaled by a factor of 1000000"
},
   {
   "Name": "current_ratio_cit",
   "Type": "decimal(21,5)",
   "Comment": "Current asset [cuasset] / Current liabilities [c95]  city industry year"
},
   {
   "Name": "current_ratio_ci",
   "Type": "decimal(21,5)",
   "Comment": "Current asset [cuasset] / Current liabilities [c95] city industry"
},
   {
   "Name": "current_ratio_i",
   "Type": "decimal(21,5)",
   "Comment": "Current asset [cuasset] / Current liabilities [c95] industry"
},
   {
   "Name": "cash_assets_cit",
   "Type": "decimal(21,5)",
   "Comment": "Cash [( 其中：短期投资 (c79) + 应收帐款 (c80) + 存货 (c81)) - cuasset)] /  Assets [其中：短期投资 (c79) + 应收帐款 (c80) + 存货 (c81)]  city industry year"
},
   {
   "Name": "cash_assets_ci",
   "Type": "decimal(21,5)",
   "Comment": "Cash [( 其中：短期投资 (c79) + 应收帐款 (c80) + 存货 (c81)) - cuasset)] /  Assets [其中：短期投资 (c79) + 应收帐款 (c80) + 存货 (c81)] city industry"
},
   {
   "Name": "cash_assets_i",
   "Type": "decimal(21,5)",
   "Comment": "Cash [( 其中：短期投资 (c79) + 应收帐款 (c80) + 存货 (c81)) - cuasset)] /  Assets [其中：短期投资 (c79) + 应收帐款 (c80) + 存货 (c81)] industry"
},
   {
   "Name": "liabilities_assets_cit",
   "Type": "decimal(21,5)",
   "Comment": "Liabilities [(流动负债合计 (c95) + 长期负债合计 (c97))] /  Total assets [资产总计318 (c93)]  city industry year"
},
   {
   "Name": "liabilities_assets_ci",
   "Type": "decimal(21,5)",
   "Comment": "Liabilities [(流动负债合计 (c95) + 长期负债合计 (c97))] /  Total assets [资产总计318 (c93)] city industry"
},
   {
   "Name": "liabilities_assets_i",
   "Type": "decimal(21,5)",
   "Comment": "Liabilities [(流动负债合计 (c95) + 长期负债合计 (c97))] /  Total assets [资产总计318 (c93)] industry"
},
   {
   "Name": "return_on_asset_cit",
   "Type": "decimal(21,5)",
   "Comment": "Total annual revenue [全年营业收入合计 (c64) ] / (Delta Total assets 318 [$\\Delta$ 资产总计318 (c98)]/2)  city industry year"
},
   {
   "Name": "return_on_asset_ci",
   "Type": "decimal(21,5)",
   "Comment": "Total annual revenue [全年营业收入合计 (c64) ] / (Delta Total assets 318 [$\\Delta$ 资产总计318 (c98)]/2) city industry"
},
   {
   "Name": "return_on_asset_i",
   "Type": "decimal(21,5)",
   "Comment": "Total annual revenue [全年营业收入合计 (c64) ] / (Delta Total assets 318 [$\\Delta$ 资产总计318 (c98)]/2) industry"
},
   {
   "Name": "sales_assets_cit",
   "Type": "decimal(21,5)",
   "Comment": "(Total annual revenue - Income tax payable) [(全年营业收入合计 (c64) - 应交所得税 (c134))] / Total assets [资产总计318 (c98)]  city industry year"
},
   {
   "Name": "sales_assets_ci",
   "Type": "decimal(21,5)",
   "Comment": "(Total annual revenue - Income tax payable) [(全年营业收入合计 (c64) - 应交所得税 (c134))] / Total assets [资产总计318 (c98)] city industry"
},
   {
   "Name": "sales_assets_i",
   "Type": "decimal(21,5)",
   "Comment": "(Total annual revenue - Income tax payable) [(全年营业收入合计 (c64) - 应交所得税 (c134))] / Total assets [资产总计318 (c98)] industry"
},
    {'Name': 'lower_location', 'Type': 'string', 'Comment': 'Location city. one of Coastal, Central, Northwest, Northeast, Southwest'},
    {'Name': 'larger_location', 'Type': 'string', 'Comment': 'Location city. one of Eastern, Central, Western'},
    {'Name': 'coastal', 'Type': 'string', 'Comment': 'City is bordered by sea or not'},
    {'Name': 'fe_c_i', 'Type': 'int', 'Comment': 'City industry fixed effect'},
    {'Name': 'fe_t_i', 'Type': 'int', 'Comment': 'year industry fixed effect'},
    {'Name': 'fe_c_t', 'Type': 'int', 'Comment': 'city industry fixed effect'}]
```

4. Provide a description

```python
description = """
Transform (creating time-break variables and fixed effect) asif_financial_ratio data and merging pollution, industry and city mandate tables
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
json_etl
```

```python
with open(os.path.join(str(Path(path).parent), 'parameters_ETL_Financial_dependency_pollution.json')) as json_file:
    parameters = json.load(json_file)
```

index_to_removeRemove the step number from the current file (if exist)

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
partition_keys = ["geocode4_corr", "year", "cic"]

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
        
        if param =='ALL_SCHEMA':
            table_name = '{}{}'.format(
                schema['metadata']['TablePrefix'],
                os.path.basename(schema['metadata']['target_S3URI']).lower()
            )
        else:
            table_name = schema['metadata']['TableName']
        
        tb = pd.json_normalize(schema['schema']).to_markdown()
        toc = "{}{}".format(github_link, table_name)
        top_readme += '\n- [{0}]({1})'.format(table_name, toc)

        README += template.format(table_name,
                                  DatabaseName,
                                  target_S3URI,
                                  partition,
                                  tb
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
secondary_key = 'period'
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
