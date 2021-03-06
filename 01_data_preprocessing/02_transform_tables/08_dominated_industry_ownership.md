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

* Industrial size effect
  * Change computation large vs small industry
    * Compute the median (percentile) within a city taking all firms
    * Compute the median (percentile) within a city-industry taking all firms within the industry
  *  For instance, Shanghai has 3 sectors, compute the median for Shanghai, and 3 median for each sector

The notebook reference is the following https://github.com/thomaspernet/Financial_dependency_pollution/blob/master/01_data_preprocessing/02_transform_tables/07_dominated_city_ownership.md#steps-1

A dominated sector is defined as positive when the average output of the firms is above the cross secteur average
* Compute the firm’s industrial output average
* Compute the firm’s national median


## Example step by step

```python
DatabaseName = 'firms_survey'
s3_output_example = 'SQL_OUTPUT_ATHENA'
```

### Example with National Average

Compute percentile for year 2001. 

Bear in mind that we get the median and percentile from the city-industry level of output, employment, capital and sales.

- `ci_med_output`: Percentile output by city-industry
- `c_med_output`:Percentile output by city

```python
query= """
WITH test AS (
  SELECT 
    *, 
    CASE WHEN LENGTH(cic) = 4 THEN substr(cic, 1, 2) ELSE concat(
      '0', 
      substr(cic, 1, 1)
    ) END AS indu_2, 
    CASE WHEN ownership = 'SOE' THEN 'SOE' ELSE 'PRIVATE' END AS soe_vs_pri, 
    CASE WHEN ownership in ('HTM', 'FOREIGN') THEN 'FOREIGN' ELSE 'DOMESTIC' END AS for_vs_dom 
  FROM 
    firms_survey.asif_firms_prepared 
    INNER JOIN (
      SELECT 
        extra_code, 
        geocode4_corr 
      FROM 
        chinese_lookup.china_city_code_normalised 
      GROUP BY 
        extra_code, 
        geocode4_corr
    ) as no_dup_citycode ON asif_firms_prepared.citycode = no_dup_citycode.extra_code
-- WHERE year = '2001'
) 
SELECT 
  indu_2, 
  industry_pct.year, 
  industry_pct.geocode4_corr,
  ci_med_output,
  c_med_output
FROM 
  (
    (
      SELECT 
        indu_2, 
--        year, 
        geocode4_corr,
        approx_percentile(output, ARRAY[.5,.75,.90,.95]) AS ci_med_output 
      FROM 
        test 
      GROUP BY 
--        year, 
        indu_2,
        geocode4_corr
      ORDER BY 
        indu_2
    ) as industry_pct 
    LEFT JOIN (
      SELECT 
--        year, 
        geocode4_corr,
        approx_percentile(output, ARRAY[.5,.75,.90,.95]) as c_med_output 
      FROM 
        test 
      GROUP BY 
        -- year,
        geocode4_corr
    ) as national_avg
    ON industry_pct.geocode4_corr = national_avg.geocode4_corr
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

Compute the condition -> True if percentile is above national median

We cannot use average because the data are too much skewed so median is never above the average

```python
query= """
WITH test AS (
  SELECT 
    *, 
    CASE WHEN LENGTH(cic) = 4 THEN substr(cic, 1, 2) ELSE concat(
      '0', 
      substr(cic, 1, 1)
    ) END AS indu_2, 
    CASE WHEN ownership = 'SOE' THEN 'SOE' ELSE 'PRIVATE' END AS soe_vs_pri, 
    CASE WHEN ownership in ('HTM', 'FOREIGN') THEN 'FOREIGN' ELSE 'DOMESTIC' END AS for_vs_dom 
  FROM 
    firms_survey.asif_firms_prepared 
    INNER JOIN (
      SELECT 
        extra_code, 
        geocode4_corr 
      FROM 
        chinese_lookup.china_city_code_normalised 
      GROUP BY 
        extra_code, 
        geocode4_corr
    ) as no_dup_citycode ON asif_firms_prepared.citycode = no_dup_citycode.extra_code
-- WHERE year = '2001'
) 
SELECT 
  indu_2, 
--  industry_pct.year, 
  industry_pct.geocode4_corr,
  c_med_output,
  ci_med_output,
  zip_with(
  c_med_output,
  ci_med_output, (x, y) -> x < y) AS dominated_output
FROM 
  (
    (
      SELECT 
        indu_2, 
--        year, 
        geocode4_corr,
        approx_percentile(output, ARRAY[.5,.75,.90,.95]) AS ci_med_output 
      FROM 
        test 
      GROUP BY 
--        year, 
        indu_2, geocode4_corr 
      ORDER BY 
        indu_2
    ) as industry_pct 
    LEFT JOIN (
      SELECT 
--        year, 
        geocode4_corr,
        approx_percentile(output, ARRAY[.5,.75,.90,.95]) as c_med_output  
      FROM 
        test 
      GROUP BY 
--        year, 
        geocode4_corr
    ) as national_avg 
    ON 
    -- industry_pct.year = national_avg.year
    industry_pct.geocode4_corr = national_avg.geocode4_corr
  ) 
  ORDER BY geocode4_corr
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
WITH test AS (
  SELECT 
    *, 
    CASE WHEN LENGTH(cic) = 4 THEN substr(cic, 1, 2) ELSE concat(
      '0', 
      substr(cic, 1, 1)
    ) END AS indu_2, 
    CASE WHEN ownership = 'SOE' THEN 'SOE' ELSE 'PRIVATE' END AS soe_vs_pri, 
    CASE WHEN ownership in ('HTM', 'FOREIGN') THEN 'FOREIGN' ELSE 'DOMESTIC' END AS for_vs_dom 
  FROM 
    firms_survey.asif_firms_prepared 
    INNER JOIN (
      SELECT 
        extra_code, 
        geocode4_corr 
      FROM 
        chinese_lookup.china_city_code_normalised 
      GROUP BY 
        extra_code, 
        geocode4_corr
    ) as no_dup_citycode ON asif_firms_prepared.citycode = no_dup_citycode.extra_code
-- WHERE year = '2001'
)
SELECT 
  indu_2, 
--  industry_pct.year, 
  industry_pct.geocode4_corr,
  c_med_output,
  ci_med_output,
  MAP(
        ARRAY[
          .5, 
          .75, 
          .90, 
          .95
          ],
  zip_with(
  c_med_output,
  ci_med_output, (x, y) -> x < y)) AS dominated_output_i
FROM 
  (
    (
      SELECT 
        indu_2, 
--        year, 
        geocode4_corr,
        approx_percentile(output, ARRAY[.5,.75,.90,.95]) AS ci_med_output 
      FROM 
        test 
      GROUP BY 
--        year, 
        indu_2,
        geocode4_corr
      ORDER BY 
        indu_2
    ) as industry_pct 
    LEFT JOIN (
      SELECT 
--        year, 
        geocode4_corr,
        approx_percentile(output, ARRAY[.5,.75,.90,.95]) as c_med_output  
      FROM 
        test 
      GROUP BY 
--        year,
        geocode4_corr
    ) as national_avg ON --industry_pct.year = national_avg.year
    industry_pct.geocode4_corr = national_avg.geocode4_corr
  ) 
  ORDER BY geocode4_corr
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

## Test with ownership private SOE

1. Compute the percentile by city-industry-ownership 
2. If the percentile of SOE above Private, then sector dominated by SOE 

```python
query = """
WITH test AS (
  SELECT 
    *, 
    CASE WHEN LENGTH(cic) = 4 THEN substr(cic, 1, 2) ELSE concat(
      '0', 
      substr(cic, 1, 1)
    ) END AS indu_2, 
    CASE WHEN ownership = 'SOE' THEN 'SOE' ELSE 'PRIVATE' END AS soe_vs_pri, 
    CASE WHEN ownership in ('HTM', 'FOREIGN') THEN 'FOREIGN' ELSE 'DOMESTIC' END AS for_vs_dom 
  FROM 
    firms_survey.asif_firms_prepared 
    INNER JOIN (
      SELECT 
        extra_code, 
        geocode4_corr 
      FROM 
        chinese_lookup.china_city_code_normalised 
      GROUP BY 
        extra_code, 
        geocode4_corr
    ) as no_dup_citycode ON asif_firms_prepared.citycode = no_dup_citycode.extra_code
) 
SELECT 
  * 
FROM 
  (
    WITH mapping AS (
      SELECT 
        indu_2,
        geocode4_corr,
        -- soe_vs_pri,
        -- output,
        map_agg(soe_vs_pri, output) AS output
        -- map_agg(soe_vs_pri, employ) AS employ, 
        -- map_agg(soe_vs_pri, sales) AS sales, 
        -- map_agg(soe_vs_pri, captal) AS captal 
      FROM 
        (
          SELECT 
            geocode4_corr, 
            indu_2,
            soe_vs_pri, 
            approx_percentile(output, ARRAY[.5,.75,.90,.95]) as output
          FROM 
            test 
          GROUP BY 
            geocode4_corr, 
            indu_2,
            soe_vs_pri 
        ) 
      GROUP BY 
      geocode4_corr,
      indu_2
    ) 
    SELECT 
      *
    FROM 
      mapping
    -- WHERE geocode4_corr = '1101' AND indu_2 = '13'
    ORDER BY geocode4_corr, indu_2
    LIMIT 10
  )
"""
output = s3.run_query(
                    query=query,
                    database=DatabaseName,
                    s3_output=s3_output_example,
    filename = 'example_4'
                )
output
```

Compute the size and get the map

```python
query = """
WITH test AS (
  SELECT 
    *, 
    CASE WHEN LENGTH(cic) = 4 THEN substr(cic, 1, 2) ELSE concat(
      '0', 
      substr(cic, 1, 1)
    ) END AS indu_2, 
    CASE WHEN ownership = 'SOE' THEN 'SOE' ELSE 'PRIVATE' END AS soe_vs_pri, 
    CASE WHEN ownership in ('HTM', 'FOREIGN') THEN 'FOREIGN' ELSE 'DOMESTIC' END AS for_vs_dom 
  FROM 
    firms_survey.asif_firms_prepared 
    INNER JOIN (
      SELECT 
        extra_code, 
        geocode4_corr 
      FROM 
        chinese_lookup.china_city_code_normalised 
      GROUP BY 
        extra_code, 
        geocode4_corr
    ) as no_dup_citycode ON asif_firms_prepared.citycode = no_dup_citycode.extra_code
) 
SELECT 
  * 
FROM 
  (
    WITH mapping AS (
      SELECT 
        indu_2,
        geocode4_corr,
        -- soe_vs_pri,
        -- output,
        map_agg(soe_vs_pri, output) AS output
        -- map_agg(soe_vs_pri, employ) AS employ, 
        -- map_agg(soe_vs_pri, sales) AS sales, 
        -- map_agg(soe_vs_pri, captal) AS captal 
      FROM 
        (
          SELECT 
            geocode4_corr, 
            indu_2,
            soe_vs_pri, 
            approx_percentile(output, ARRAY[.5,.75,.90,.95]) as output
          FROM 
            test 
          GROUP BY 
            geocode4_corr, 
            indu_2,
            soe_vs_pri 
        ) 
      GROUP BY 
      geocode4_corr,
      indu_2
    ) 
    SELECT 
      *,
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
              output[ 'SOE' ], output[ 'PRIVATE' ]
            ), 
            (k, v) -> k > v
          )
        )
      ) AS dominated_output_soe
    FROM 
      mapping
    ORDER BY geocode4_corr, indu_2
    LIMIT 10
  )
"""
output = s3.run_query(
                    query=query,
                    database=DatabaseName,
                    s3_output=s3_output_example,
    filename = 'example_4'
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
WITH test AS (
  SELECT 
    *, 
    CASE WHEN LENGTH(cic) = 4 THEN substr(cic, 1, 2) ELSE concat(
      '0', 
      substr(cic, 1, 1)
    ) END AS indu_2, 
    CASE WHEN ownership = 'SOE' THEN 'SOE' ELSE 'PRIVATE' END AS soe_vs_pri, 
    CASE WHEN ownership in ('HTM', 'FOREIGN') THEN 'FOREIGN' ELSE 'DOMESTIC' END AS for_vs_dom 
  FROM 
    firms_survey.asif_firms_prepared 
    INNER JOIN (
      SELECT 
        extra_code, 
        geocode4_corr 
      FROM 
        chinese_lookup.china_city_code_normalised 
      GROUP BY 
        extra_code, 
        geocode4_corr
    ) as no_dup_citycode ON asif_firms_prepared.citycode = no_dup_citycode.extra_code 
    WHERE year in ('2001', '2002', '2003','2004', '2005', '2006', '2007')
) 
SELECT 
  national.indu_2, 
  national.geocode4_corr, 
  MAP(
    ARRAY[.5, 
    .75, 
    .90, 
    .95 ], 
    zip_with(
      c_med_output, 
      ci_med_output, 
      (x, y) -> x < y
    )
  ) AS dominated_output_i, 
  MAP(
    ARRAY[.5, 
    .75, 
    .90, 
    .95 ], 
    zip_with(
      c_med_employ, 
      ci_med_employ, 
      (x, y) -> x < y
    )
  ) AS dominated_employ_i, 
  MAP(
    ARRAY[.5, 
    .75, 
    .90, 
    .95 ], 
    zip_with(
      c_med_sales, 
      ci_med_sales, 
      (x, y) -> x < y
    )
  ) AS dominated_sales_i, 
  MAP(
    ARRAY[.5, 
    .75, 
    .90, 
    .95 ], 
    zip_with(
      c_med_captal, 
      ci_med_captal, 
      (x, y) -> x < y
    )
  ) AS dominated_capital_i, 
  dominated_output_soe AS dominated_output_soe_i, 
  dominated_employment_soe AS dominated_employment_soe_i, 
  dominated_sales_soe AS dominated_sales_soe_i, 
  dominated_capital_soe AS dominated_capital_soe_i, 
  dominated_output_for AS dominated_output_for_i, 
  dominated_employment_for AS dominated_employment_for_i, 
  dominated_sales_for AS dominated_sales_for_i, 
  dominated_capital_for AS dominated_capital_for_i 
FROM 
  (
    (
      SELECT 
        indu_2, 
--        year, 
        geocode4_corr AS geo, 
        -- rename to avoid ambiguous name
        approx_percentile(output, ARRAY[.5,.75,.90,.95]) AS ci_med_output, 
        approx_percentile(employ, ARRAY[.5,.75,.90,.95]) AS ci_med_employ, 
        approx_percentile(sales, ARRAY[.5,.75,.90,.95]) AS ci_med_sales, 
        approx_percentile(captal, ARRAY[.5,.75,.90,.95]) AS ci_med_captal 
      FROM 
        test 
      GROUP BY 
--        year, 
        indu_2, 
        geocode4_corr
    ) as industry_pct 
    LEFT JOIN (
      SELECT 
--        year, 
        geocode4_corr, 
        approx_percentile(output, ARRAY[.5,.75,.90,.95]) as c_med_output, 
        approx_percentile(employ, ARRAY[.5,.75,.90,.95]) as c_med_employ, 
        approx_percentile(sales, ARRAY[.5,.75,.90,.95]) as c_med_sales, 
        approx_percentile(captal, ARRAY[.5,.75,.90,.95]) as c_med_captal 
      FROM 
        test 
      GROUP BY 
--        year, 
        geocode4_corr
    ) as national_avg ON --industry_pct.year = national_avg.year 
--    AND 
    industry_pct.geo = national_avg.geocode4_corr
  ) AS national 
  LEFT JOIN (
    SELECT 
      * 
    FROM 
      (
        WITH mapping AS (
          SELECT 
            indu_2, 
            geocode4_corr, 
            map_agg(soe_vs_pri, output) AS output, 
            map_agg(soe_vs_pri, employ) AS employ, 
            map_agg(soe_vs_pri, sales) AS sales, 
            map_agg(soe_vs_pri, captal) AS captal 
          FROM 
            (
              SELECT 
                geocode4_corr, 
                indu_2, 
                soe_vs_pri, 
                approx_percentile(output, ARRAY[.5,.75,.90,.95]) AS output, 
                approx_percentile(employ, ARRAY[.5,.75,.90,.95]) AS employ, 
                approx_percentile(sales, ARRAY[.5,.75,.90,.95]) AS sales, 
                approx_percentile(captal, ARRAY[.5,.75,.90,.95]) AS captal 
              FROM 
                test 
              GROUP BY 
                geocode4_corr, 
                indu_2, 
                soe_vs_pri
            ) 
          GROUP BY 
            geocode4_corr, 
            indu_2
        ) 
        SELECT 
          *, 
          map(
            ARRAY[.5, 
            .75, 
            .90, 
            .95 ], 
            map_values(
              transform_values(
                MAP(
                  output[ 'SOE' ], output[ 'PRIVATE' ]
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
                  employ[ 'SOE' ], employ[ 'PRIVATE' ]
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
                MAP(sales[ 'SOE' ], sales[ 'PRIVATE' ]), 
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
                  captal[ 'SOE' ], captal[ 'PRIVATE' ]
                ), 
                (k, v) -> k > v
              )
            )
          ) AS dominated_capital_soe 
        FROM 
          mapping
      )
  ) AS soe_private ON national.indu_2 = soe_private.indu_2 
  AND national.geocode4_corr = soe_private.geocode4_corr 
  LEFT JOIN (
    SELECT 
      * 
    FROM 
      (
        WITH mapping AS (
          SELECT 
            indu_2, 
            geocode4_corr, 
            map_agg(for_vs_dom, output) AS output, 
            map_agg(for_vs_dom, employ) AS employ, 
            map_agg(for_vs_dom, sales) AS sales, 
            map_agg(for_vs_dom, captal) AS captal 
          FROM 
            (
              SELECT 
                geocode4_corr, 
                indu_2, 
                for_vs_dom, 
                approx_percentile(output, ARRAY[.5,.75,.90,.95]) as output, 
                approx_percentile(employ, ARRAY[.5,.75,.90,.95]) AS employ, 
                approx_percentile(sales, ARRAY[.5,.75,.90,.95]) AS sales, 
                approx_percentile(captal, ARRAY[.5,.75,.90,.95]) AS captal 
              FROM 
                test 
              GROUP BY 
                geocode4_corr, 
                indu_2, 
                for_vs_dom
            ) 
          GROUP BY 
            geocode4_corr, 
            indu_2
        ) 
        SELECT 
          *, 
          map(
            ARRAY[.5, 
            .75, 
            .90, 
            .95 ], 
            map_values(
              transform_values(
                MAP(
                  output[ 'FOREIGN' ], output[ 'DOMESTIC' ]
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
                  employ[ 'FOREIGN' ], employ[ 'DOMESTIC' ]
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
                  sales[ 'FOREIGN' ], sales[ 'DOMESTIC' ]
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
                  captal[ 'FOREIGN' ], captal[ 'DOMESTIC' ]
                ), 
                (k, v) -> k > v
              )
            )
          ) AS dominated_capital_for 
        FROM 
          mapping
      )
  ) AS foreign_dom ON national.indu_2 = foreign_dom.indu_2 
  AND national.geocode4_corr = foreign_dom.geocode4_corr 
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

Example query map in Athena

```python
query_filter = """
SELECT 
dominated_median, COUNT(dominated_median) as count

FROM (
  SELECT 
indu_2, element_at(dominated_output_i, .5) as dominated_median
FROM asif_industry_characteristics_ownership  
  )
GROUP BY dominated_median
"""
output = s3.run_query(
                    query=query_filter,
                    database=DatabaseName,
                    s3_output=s3_output_example,
    filename = 'filter_{}'.format(table_name)
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
partition_keys = ['geocode4_corr','indu_2']
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
    {'Name': 'geocode4_corr', 'Type': 'string', 'Comment': 'city code'},
    {
        "Name": "dominated_output_i",
        "Type": "map<double,boolean>",
        "Comment": "map with information dominated industry knowing percentile .5, .75, .9, .95 of output",
    },
    {"Name": "dominated_employment_i", "Type": "map<double,boolean>", "Comment": "map with information on dominated industry knowing percentile .5, .75, .9, .95 of employment"},
    {"Name": "dominated_capital_i", "Type": "map<double,boolean>", "Comment": "map with information on dominated industry knowing percentile .5, .75, .9, .95 of capital"},
    {"Name": "dominated_sales_i", "Type": "map<double,boolean>", "Comment": "map with information on SOE dominated industry knowing percentile .5, .75, .9, .95 of sales"},
    {
        "Name": "dominated_output_soe_i",
        "Type": "map<double,boolean>",
        "Comment": "map with information on SOE dominated industry knowing percentile .5, .75, .9, .95 of output",
    },
    {
        "Name": "dominated_employment_soe_i",
        "Type": "map<double,boolean>",
        "Comment": "map with information on SOE dominated industry knowing percentile .5, .75, .9, .95 of employment",
    },
    {
        "Name": "dominated_sales_soe_i",
        "Type": "map<double,boolean>",
        "Comment": "map with information on SOE dominated industry knowing percentile .5, .75, .9, .95 of sales",
    },
    {
        "Name": "dominated_capital_soe_i",
        "Type": "map<double,boolean>",
        "Comment": "map with information on SOE dominated industry knowing percentile .5, .75, .9, .95 of capital",
    },
    {
        "Name": "dominated_output_for_i",
        "Type": "map<double,boolean>",
        "Comment": "map with information on foreign dominated industry knowing percentile .5, .75, .9, .95 of output",
    },
    {
        "Name": "dominated_employment_for_i",
        "Type": "map<double,boolean>",
        "Comment": "map with information on foreign dominated industry knowing percentile .5, .75, .9, .95 of employment",
    },
    {
        "Name": "dominated_sales_for_i",
        "Type": "map<double,boolean>",
        "Comment": "map with information on foreign dominated industry knowing percentile .5, .75, .9, .95 of sales",
    },
    {
        "Name": "dominated_capital_for_i",
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

3. provide metadata

- DatabaseName:
- TablePrefix:
- input: 
- notebook name: to indicate
- Task ID: from Coda
- index_final_table: a list to indicate if the current table is used to prepare the final table(s). If more than one, pass the index. Start at 0
- if_final: A boolean. Indicates if the current table is the final table -> the one the model will be used to be trained

```python
with open(os.path.join(str(Path(path).parent.parent), 'utils','parameters_ETL_Financial_dependency_pollution.json')) as json_file:
    parameters = json.load(json_file)
```

```python
filename =  "08_dominated_industry_ownership.ipynb"
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
        'filename': filename,
        'index_final_table' : index_final_table,
        'if_final': if_final,
        'github_url':github_url
    }
}
json_etl['metadata']
```

Remove the step number from the current file (if exist)

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
partition_keys = ['geocode4_corr', 'indu_2']

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
create_report(extension = "html", keep_code = True, notebookname =  '08_dominated_industry_ownership.ipynb')
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
