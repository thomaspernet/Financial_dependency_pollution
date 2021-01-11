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

Transform asif firms prepared data by merging china city code normalised data by constructing foreign_vs_domestic and others (create dominated industry) to asif industry characteristics ownership 

# Business needs 

Transform asif firms prepared data by merging china city code normalised data by constructing foreign_vs_domestic, foreign_size, domestic_size, private_size, public_size, soe_vs_private (create dominated industry by ownership (public-private, foreign-domestic) using industry size) to asif industry characteristics ownership 

## Description
### Objective 

Use existing tables asif firms prepared to constructing a bunch of variables listed below

# Construction variables 

- foreign_vs_domestic
- foreign_size
- domestic_size
- private_size
- public_size
- soe_vs_private

### Steps 




**Cautious**
Make sure there is no duplicates

# Target

- The file is saved in S3:
- bucket: datalake-datascience
- path: DATA/ECON/FIRM_SURVEY/ASIF_CHINA/TRANSFORMED/INDUSTRY_CHARACTERISTICS/OWNERSHIP
- Glue data catalog should be updated
- database: firms_survey
- Table prefix: asif_industry_characteristics_
- table name: asif_industry_characteristics_ownership
- Analytics
- HTML: ANALYTICS/HTML_OUTPUT/asif_industry_characteristics_ownership
- Notebook: ANALYTICS/OUTPUT/asif_industry_characteristics_ownership

# Metadata

- Key: uvy96muto62979g
- Epic: Dataset transformation
- US: city and sector characteristics
- Task tag: #dominated-domestic, #dominated-foreign, #dominated-private, #dominated-public, #industry
- Analytics reports: https://htmlpreview.github.io/?https://github.com/thomaspernet/Financial_dependency_pollution/blob/master/00_data_catalogue/HTML_ANALYSIS/ASIF_INDUSTRY_CHARACTERISTICS_OWNERSHIP.html

# Input Cloud Storage

## Table/file

**Name** 

- DATA/ECON/FIRM_SURVEY/ASIF_CHINA/PREPARED

**Github**

- https://github.com/thomaspernet/Financial_dependency_pollution/blob/master/01_data_preprocessing/01_prepare_tables/00_prepare_asif.md

# Destination Output/Delivery

## Table/file

**Name**

asif_industry_characteristics_ownership

**GitHub**

- https://github.com/thomaspernet/Financial_dependency_pollution/blob/master/01_data_preprocessing/02_transform_tables/08_dominated_industry_ownership.md
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

1. Aggregate output, employment, capital and sales by industry (use 2000)
2. Compute the percentile .5, .75, .90,.95
3. Compute dominated city for each percentile
  1. If public > private then public else private
  2. If foreign > domestic then foreign else domestic

The notebook reference is the following https://github.com/thomaspernet/Financial_dependency_pollution/blob/master/01_data_preprocessing/02_transform_tables/07_dominated_city_ownership.md#steps-1

A dominated sector is defined as positive when the average output of the firms is above the cross secteur average
* Compute the firm’s industrial output average
* Compute the firm’s national average


## Example step by step

```python
DatabaseName = 'firms_survey'
s3_output_example = 'SQL_OUTPUT_ATHENA'
```

### Example with National Average

Compute mean and percentile for year 2001. Bear in mind that we get the average and percentile from the firm level of output, employment, capital and sales

```python
query= """
WITH test as (
  SELECT 
    year, 
    firm, 
    cic, 
    output, 
    employ, 
    sales, 
    captal, 
    CASE WHEN LENGTH(cic) = 4 THEN substr(cic, 1, 2) ELSE concat(
      '0', 
      substr(cic, 1, 1)
    ) END AS indu_2, 
    CASE WHEN ownership = 'SOE' THEN 'SOE' ELSE 'PRIVATE' END AS soe_vs_pri, 
    CASE WHEN ownership in ('HTM', 'FOREIGN') THEN 'FOREIGN' ELSE 'DOMESTIC' END AS for_vs_dom 
  FROM 
    firms_survey.asif_firms_prepared 
  WHERE 
    year = '2001'
) 
SELECT 
  indu_2, 
  industry_pct.year, 
  output_pct,
  n_avg_output
FROM 
  (
    (
      SELECT 
        indu_2, 
        year, 
        approx_percentile(output, ARRAY[.5,.75,.90,.95]) AS output_pct 
      FROM 
        test 
      GROUP BY 
        year, 
        indu_2 
      ORDER BY 
        indu_2
    ) as industry_pct 
    LEFT JOIN (
      SELECT 
        year, 
        AVG(output) as n_avg_output 
      FROM 
        test 
      GROUP BY 
        year
    ) as national_avg ON industry_pct.year = national_avg.year
  ) 
LIMIT 
  5

"""
output = s3.run_query(
                    query=query,
                    database=DatabaseName,
                    s3_output=s3_output_example,
    filename = 'example_1'
                )
output
```

Compute the condition -> True if percentile is above national average

```python
query= """
WITH test as (
  SELECT 
    year, 
    firm, 
    cic, 
    output, 
    employ, 
    sales, 
    captal, 
    CASE WHEN LENGTH(cic) = 4 THEN substr(cic, 1, 2) ELSE concat(
      '0', 
      substr(cic, 1, 1)
    ) END AS indu_2, 
    CASE WHEN ownership = 'SOE' THEN 'SOE' ELSE 'PRIVATE' END AS soe_vs_pri, 
    CASE WHEN ownership in ('HTM', 'FOREIGN') THEN 'FOREIGN' ELSE 'DOMESTIC' END AS for_vs_dom 
  FROM 
    firms_survey.asif_firms_prepared 
  WHERE 
    year = '2001'
) 
SELECT 
  indu_2, 
  industry_pct.year, 
  output_pct,
  n_avg_output,
  zip_with(
      transform(
        sequence(
          1, 
          4
        ), 
        x -> n_avg_output
      ),
        output_pct, (x, y) -> x < y) AS dominated_output
FROM 
  (
    (
      SELECT 
        indu_2, 
        year, 
        approx_percentile(output, ARRAY[.5,.75,.90,.95]) AS output_pct 
      FROM 
        test 
      GROUP BY 
        year, 
        indu_2 
      ORDER BY 
        indu_2
    ) as industry_pct 
    LEFT JOIN (
      SELECT 
        year, 
        AVG(output) as n_avg_output 
      FROM 
        test 
      GROUP BY 
        year
    ) as national_avg ON industry_pct.year = national_avg.year
  ) 
LIMIT 
  5

"""
output = s3.run_query(
                    query=query,
                    database=DatabaseName,
                    s3_output=s3_output_example,
    filename = 'example_3'
                )
output
```

Last, we need to reconstruct the map 

```python
query = """
WITH test as (
  SELECT 
    year, 
    firm, 
    cic, 
    output, 
    employ, 
    sales, 
    captal, 
    CASE WHEN LENGTH(cic) = 4 THEN substr(cic, 1, 2) ELSE concat(
      '0', 
      substr(cic, 1, 1)
    ) END AS indu_2, 
    CASE WHEN ownership = 'SOE' THEN 'SOE' ELSE 'PRIVATE' END AS soe_vs_pri, 
    CASE WHEN ownership in ('HTM', 'FOREIGN') THEN 'FOREIGN' ELSE 'DOMESTIC' END AS for_vs_dom 
  FROM 
    firms_survey.asif_firms_prepared 
  WHERE 
    year = '2001'
) 
SELECT 
  indu_2, 
  industry_pct.year, 
  output_pct,
  n_avg_output,
  MAP(
        ARRAY[
          .5, 
          .75, 
          .90, 
          .95
          ],
  zip_with(
      transform(
        sequence(
          1, 
          4
        ), 
        x -> n_avg_output
      ),
        output_pct, (x, y) -> x < y)
        ) AS dominated_output
FROM 
  (
    (
      SELECT 
        indu_2, 
        year, 
        approx_percentile(output, ARRAY[.5,.75,.90,.95]) AS output_pct 
      FROM 
        test 
      GROUP BY 
        year, 
        indu_2 
      ORDER BY 
        indu_2
    ) as industry_pct 
    LEFT JOIN (
      SELECT 
        year, 
        AVG(output) as n_avg_output 
      FROM 
        test 
      GROUP BY 
        year
    ) as national_avg ON industry_pct.year = national_avg.year
  ) 
LIMIT 
  5
"""
output = s3.run_query(
                    query=query,
                    database=DatabaseName,
                    s3_output=s3_output_example,
    filename = 'example_4'
                )
output
```

### Example with SOE vs private

Compute mean and percentile for year 2001 by firm's ownership. Bear in mind that we get the average and percentile from the firm level of output, employment, capital and sales.

If percentile of SOE above Private, then sector dominated by SOE

```python
query = """
WITH test as (
  SELECT 
    year, 
    firm, 
    cic, 
    output, 
    employ, 
    sales, 
    captal, 
    CASE WHEN LENGTH(cic) = 4 THEN substr(cic, 1, 2) ELSE concat(
      '0', 
      substr(cic, 1, 1)
    ) END AS indu_2, 
    CASE WHEN ownership = 'SOE' THEN 'SOE' ELSE 'PRIVATE' END AS soe_vs_pri, 
    CASE WHEN ownership in ('HTM', 'FOREIGN') THEN 'FOREIGN' ELSE 'DOMESTIC' END AS for_vs_dom 
  FROM 
    firms_survey.asif_firms_prepared 
  WHERE 
    year = '2001'
) 
SELECT 
  * 
FROM 
  (
    WITH mapping AS (
      SELECT 
        indu_2, 
        map_agg(soe_vs_pri, output_pct) AS output_pct 
      FROM 
        (
          SELECT 
            indu_2, 
            year, 
            soe_vs_pri, 
            approx_percentile(output, ARRAY[.5,.75,.90,.95]) AS output_pct 
          FROM 
            test 
          GROUP BY 
            year, 
            indu_2, 
            soe_vs_pri 
          ORDER BY 
            indu_2
        ) 
      GROUP BY 
        indu_2
    ) 
    SELECT 
      * 
    FROM 
      mapping
    LIMIT 10
  )

"""
output = s3.run_query(
                    query=query,
                    database=DatabaseName,
                    s3_output=s3_output_example,
    filename = 'example_5'
                )
output
```

Last, we need to compute the condition and reconstruct the map

```python
query = """
WITH test as (
  SELECT 
    year, 
    firm, 
    cic, 
    output, 
    employ, 
    sales, 
    captal, 
    CASE WHEN LENGTH(cic) = 4 THEN substr(cic, 1, 2) ELSE concat(
      '0', 
      substr(cic, 1, 1)
    ) END AS indu_2, 
    CASE WHEN ownership = 'SOE' THEN 'SOE' ELSE 'PRIVATE' END AS soe_vs_pri, 
    CASE WHEN ownership in ('HTM', 'FOREIGN') THEN 'FOREIGN' ELSE 'DOMESTIC' END AS for_vs_dom 
  FROM 
    firms_survey.asif_firms_prepared 
  WHERE 
    year = '2001'
) 
SELECT *
FROM 
  (
    WITH mapping AS (
SELECT 
  indu_2, 
  map_agg(soe_vs_pri, output_pct) AS output_pct
FROM 
  (
      SELECT 
        indu_2, 
        year, 
      soe_vs_pri,
        approx_percentile(output, ARRAY[.5,.75,.90,.95]) AS output_pct 
      FROM 
        test 
      GROUP BY 
        year, 
        indu_2,
      soe_vs_pri
      ORDER BY 
        indu_2
      )
 GROUP BY indu_2
  )
    SELECT 
    indu_2,
    map(
        ARRAY[
          .5, 
          .75, 
          .90, 
          .95
          ], 
        map_values(
          transform_values(
            MAP(
              output_pct[ 'SOE' ], output_pct[ 'PRIVATE' ]
            ), 
            (k, v) -> k > v
          )
        )
      ) AS dominated_output_soe
    FROM mapping
    LIMIT 10
    )
"""
output = s3.run_query(
                    query=query,
                    database=DatabaseName,
                    s3_output=s3_output_example,
    filename = 'example_5'
                )
output
```

# Table `asif_industry_characteristics_ownership`

Since the table to create has missing value, please use the following at the top of the query

```
CREATE TABLE database.table_name WITH (format = 'PARQUET') AS
```


Choose a location in S3 to save the CSV. It is recommended to save in it the `datalake-datascience` bucket. Locate an appropriate folder in the bucket, and make sure all output have the same format

```python
s3_output = 'DATA/ECON/FIRM_SURVEY/ASIF_CHINA/TRANSFORMED/INDUSTRY_CHARACTERISTICS/OWNERSHIP'
table_name = 'asif_industry_characteristics_ownership'
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
WITH test as (
  SELECT 
    year, 
    firm, 
    cic, 
    output, 
    employ, 
    sales, 
    captal, 
    CASE WHEN LENGTH(cic) = 4 THEN substr(cic, 1, 2) ELSE concat(
      '0', 
      substr(cic, 1, 1)
    ) END AS indu_2, 
    CASE WHEN ownership = 'SOE' THEN 'SOE' ELSE 'PRIVATE' END AS soe_vs_pri, 
    CASE WHEN ownership in ('HTM', 'FOREIGN') THEN 'FOREIGN' ELSE 'DOMESTIC' END AS for_vs_dom 
  FROM 
    firms_survey.asif_firms_prepared 
  WHERE 
    year = '2001'
) 
SELECT 
  national.indu_2, 
  MAP(
    ARRAY[.5, 
    .75, 
    .90, 
    .95 ], 
    zip_with(
      transform(
        sequence(1, 4), 
        x -> n_avg_output
      ), 
      output_pct, 
      (x, y) -> x < y
    )
  ) AS dominated_output, 
  MAP(
    ARRAY[.5, 
    .75, 
    .90, 
    .95 ], 
    zip_with(
      transform(
        sequence(1, 4), 
        x -> n_avg_employ
      ), 
      employ_pct, 
      (x, y) -> x < y
    )
  ) AS dominated_employment, 
  MAP(
    ARRAY[.5, 
    .75, 
    .90, 
    .95 ], 
    zip_with(
      transform(
        sequence(1, 4), 
        x -> n_avg_capital
      ), 
      captal_pct, 
      (x, y) -> x < y
    )
  ) AS dominated_capital, 
  MAP(
    ARRAY[.5, 
    .75, 
    .90, 
    .95 ], 
    zip_with(
      transform(
        sequence(1, 4), 
        x -> n_avg_sales
      ), 
      sales_pct, 
      (x, y) -> x < y
    )
  ) AS dominated_sales, 
  dominated_output_soe, 
  dominated_employment_soe, 
  dominated_sales_soe, 
  dominated_capital_soe, 
  dominated_output_for, 
  dominated_employment_for, 
  dominated_sales_for, 
  dominated_capital_for 
FROM 
  (
    (
      SELECT 
        indu_2, 
        year, 
        approx_percentile(output, ARRAY[.5,.75,.90,.95]) AS output_pct, 
        approx_percentile(employ, ARRAY[.5,.75,.90,.95]) AS employ_pct, 
        approx_percentile(sales, ARRAY[.5,.75,.90,.95]) AS sales_pct, 
        approx_percentile(captal, ARRAY[.5,.75,.90,.95]) AS captal_pct 
      FROM 
        test 
      GROUP BY 
        year, 
        indu_2 
      ORDER BY 
        indu_2
    ) as industry_pct 
    LEFT JOIN (
      SELECT 
        year, 
        AVG(output) as n_avg_output, 
        AVG(employ) as n_avg_employ, 
        AVG(sales) as n_avg_sales, 
        AVG(captal) as n_avg_capital 
      FROM 
        test 
      GROUP BY 
        year
    ) as national_avg ON industry_pct.year = national_avg.year
  ) as national 
  LEFT JOIN (
    SELECT 
      * 
    FROM 
      (
        WITH mapping AS (
          SELECT 
            indu_2, 
            map_agg(soe_vs_pri, output_pct) AS output_pct, 
            map_agg(soe_vs_pri, employ_pct) AS employ_pct, 
            map_agg(soe_vs_pri, sales_pct) AS sales_pct, 
            map_agg(soe_vs_pri, captal_pct) AS captal_pct 
          FROM 
            (
              SELECT 
                indu_2, 
                year, 
                soe_vs_pri, 
                approx_percentile(output, ARRAY[.5,.75,.90,.95]) AS output_pct, 
                approx_percentile(employ, ARRAY[.5,.75,.90,.95]) AS employ_pct, 
                approx_percentile(sales, ARRAY[.5,.75,.90,.95]) AS sales_pct, 
                approx_percentile(captal, ARRAY[.5,.75,.90,.95]) AS captal_pct 
              FROM 
                test 
              GROUP BY 
                year, 
                indu_2, 
                soe_vs_pri 
              ORDER BY 
                indu_2
            ) 
          GROUP BY 
            indu_2
        ) 
        SELECT 
          indu_2, 
          map(
            ARRAY[.5, 
            .75, 
            .90, 
            .95 ], 
            map_values(
              transform_values(
                MAP(
                  output_pct[ 'SOE' ], output_pct[ 'PRIVATE' ]
                ), 
                (k, v) -> k > v
              )
            )
          ) AS dominated_output_soe, 
          map(
            ARRAY[.5, 
            .75, 
            .90, 
            .95 ], 
            map_values(
              transform_values(
                MAP(
                  employ_pct[ 'SOE' ], employ_pct[ 'PRIVATE' ]
                ), 
                (k, v) -> k > v
              )
            )
          ) AS dominated_employment_soe, 
          map(
            ARRAY[.5, 
            .75, 
            .90, 
            .95 ], 
            map_values(
              transform_values(
                MAP(
                  sales_pct[ 'SOE' ], sales_pct[ 'PRIVATE' ]
                ), 
                (k, v) -> k > v
              )
            )
          ) AS dominated_sales_soe, 
          map(
            ARRAY[.5, 
            .75, 
            .90, 
            .95 ], 
            map_values(
              transform_values(
                MAP(
                  captal_pct[ 'SOE' ], captal_pct[ 'PRIVATE' ]
                ), 
                (k, v) -> k > v
              )
            )
          ) AS dominated_capital_soe 
        FROM 
          mapping
      )
  ) AS soe_private ON national.indu_2 = soe_private.indu_2 
  LEFT JOIN (
    SELECT 
      * 
    FROM 
      (
        WITH mapping AS (
          SELECT 
            indu_2, 
            map_agg(for_vs_dom, output_pct) AS output_pct, 
            map_agg(for_vs_dom, employ_pct) AS employ_pct, 
            map_agg(for_vs_dom, sales_pct) AS sales_pct, 
            map_agg(for_vs_dom, captal_pct) AS captal_pct 
          FROM 
            (
              SELECT 
                indu_2, 
                year, 
                for_vs_dom, 
                approx_percentile(output, ARRAY[.5,.75,.90,.95]) AS output_pct, 
                approx_percentile(employ, ARRAY[.5,.75,.90,.95]) AS employ_pct, 
                approx_percentile(sales, ARRAY[.5,.75,.90,.95]) AS sales_pct, 
                approx_percentile(captal, ARRAY[.5,.75,.90,.95]) AS captal_pct 
              FROM 
                test 
              GROUP BY 
                year, 
                indu_2, 
                for_vs_dom 
              ORDER BY 
                indu_2
            ) 
          GROUP BY 
            indu_2
        ) 
        SELECT 
          indu_2, 
          map(
            ARRAY[.5, 
            .75, 
            .90, 
            .95 ], 
            map_values(
              transform_values(
                MAP(
                  output_pct[ 'FOREIGN' ], output_pct[ 'DOMESTIC' ]
                ), 
                (k, v) -> k > v
              )
            )
          ) AS dominated_output_for, 
          map(
            ARRAY[.5, 
            .75, 
            .90, 
            .95 ], 
            map_values(
              transform_values(
                MAP(
                  employ_pct[ 'FOREIGN' ], employ_pct[ 'DOMESTIC' ]
                ), 
                (k, v) -> k > v
              )
            )
          ) AS dominated_employment_for, 
          map(
            ARRAY[.5, 
            .75, 
            .90, 
            .95 ], 
            map_values(
              transform_values(
                MAP(
                  sales_pct[ 'FOREIGN' ], sales_pct[ 'DOMESTIC' ]
                ), 
                (k, v) -> k > v
              )
            )
          ) AS dominated_sales_for, 
          map(
            ARRAY[.5, 
            .75, 
            .90, 
            .95 ], 
            map_values(
              transform_values(
                MAP(
                  captal_pct[ 'FOREIGN' ], captal_pct[ 'DOMESTIC' ]
                ), 
                (k, v) -> k > v
              )
            )
          ) AS dominated_capital_for 
        FROM 
          mapping
      )
  ) AS foreign_dom ON national.indu_2 = foreign_dom.indu_2

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
partition_keys = ['indu_2']
```

2. Add the steps number

```python
step = 7
```

3. Change the schema

Bear in mind that CSV SerDe (OpenCSVSerDe) does not support empty fields in columns defined as a numeric data type. All columns with missing values should be saved as string. 

```python
glue.get_table_information(
    database = DatabaseName,
    table = table_name)['Table']['StorageDescriptor']['Columns']
```

```python
schema = [
    {"Name": "indu_2", "Type": "string", "Comment": ""},
    {
        "Name": "dominated_output",
        "Type": "map<double,boolean>",
        "Comment": "map with information dominated industry knowing percentile .5, .75, .9, .95 of output",
    },
    {"Name": "dominated_employment", "Type": "map<double,boolean>", "Comment": "map with information on dominated industry knowing percentile .5, .75, .9, .95 of employment"},
    {"Name": "dominated_capital", "Type": "map<double,boolean>", "Comment": "map with information on dominated industry knowing percentile .5, .75, .9, .95 of capital"},
    {"Name": "dominated_sales", "Type": "map<double,boolean>", "Comment": "map with information on SOE dominated industry knowing percentile .5, .75, .9, .95 of sales"},
    {
        "Name": "dominated_output_soe",
        "Type": "map<double,boolean>",
        "Comment": "map with information on SOE dominated industry knowing percentile .5, .75, .9, .95 of output",
    },
    {
        "Name": "dominated_employment_soe",
        "Type": "map<double,boolean>",
        "Comment": "map with information on SOE dominated industry knowing percentile .5, .75, .9, .95 of employment",
    },
    {
        "Name": "dominated_sales_soe",
        "Type": "map<double,boolean>",
        "Comment": "map with information on SOE dominated industry knowing percentile .5, .75, .9, .95 of sales",
    },
    {
        "Name": "dominated_capital_soe",
        "Type": "map<double,boolean>",
        "Comment": "map with information on SOE dominated industry knowing percentile .5, .75, .9, .95 of capital",
    },
    {
        "Name": "dominated_output_for",
        "Type": "map<double,boolean>",
        "Comment": "map with information on foreign dominated industry knowing percentile .5, .75, .9, .95 of output",
    },
    {
        "Name": "dominated_employment_for",
        "Type": "map<double,boolean>",
        "Comment": "map with information on foreign dominated industry knowing percentile .5, .75, .9, .95 of employment",
    },
    {
        "Name": "dominated_sales_for",
        "Type": "map<double,boolean>",
        "Comment": "map with information on foreign dominated industry knowing percentile .5, .75, .9, .95 of sales",
    },
    {
        "Name": "dominated_capital_for",
        "Type": "map<double,boolean>",
        "Comment": "map with information on foreign dominated industry knowing percentile .5, .75, .9, .95 of capital",
    },
]
```

4. Provide a description

```python
description = """
Transform asif firms prepared data by merging china city code normalised data by constructing foreign_vs_domestic, foreign_size, domestic_size, private_size, public_size, soe_vs_private (create dominated industry by ownership (public-private, foreign-domestic) using industry size) to asif industry characteristics  ownership
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
partition_keys = ['indu_2']

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
                                  tb
                                  )
README = README.format(top_readme)
with open(os.path.join(str(Path(path).parent.parent), '00_data_catalogue/README.md'), "w") as outfile:
    outfile.write(README)
```

# Generation report

```python
import os, time, shutil, urllib, ipykernel, json
from pathlib import Path
from notebook import notebookapp
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
create_report(extension = "html", keep_code = True, notebookname =  '08_dominated_industry_ownership.ipynb')
```
