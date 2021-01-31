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
# Transform asif firms prepared data by constructing working capital requirement and others (update formula financial ratio) to asif industry city

# US Name

Transform asif firms prepared data by constructing working capital requirement and others (update formula financial ratio) to asif industry city 

# Business needs 

Transform asif firms prepared data by constructing working capital requirement, tangible asset, working capital, liabilities, account receivable, account payable (Use more accurate formula to compute financial ratio) to asif industry city 

## Description
### Objective 

Use existing table asif firms prepared to construct working capital requirement and others

# Construction variables 

- asset_tangibility
- cash_over_total_asset
- sales over asset Andersen method
- current ratio
- investment to total asset
- RD to total asset
- tangible asset to total asset
- cash flow to total tangible asset
- cash_over_total_asset [UPDATE] → add the following variables
- return on sale: net income (c111)/sales
- coverage ratio: net income(c111) /total interest payments

### Steps 

1. Create balance sheet variables at the firm level
2. Aggregate at the city-industry-year from balance sheet variables


**Cautious**

Make sure there is no duplicates

# Target

* The file is saved in S3:
  * bucket: datalake-datascience
  * path: DATA/ECON/FIRM_SURVEY/ASIF_CHINA/TRANSFORMED/FINANCIAL_RATIO/CITY
* Glue data catalog should be updated
  * database:firms_survey
  * Table prefix:asif_industry_
  * table name:asif_industry_city
* Analytics
  * HTML: ANALYTICS/HTML_OUTPUT/asif_industry_city
  * Notebook: ANALYTICS/OUTPUT/asif_industry_city

# Metadata
* Key: spo81olgr86055h
* Parent key (for update parent):  
* Epic: Dataset transformation
* US: Financial ratio
* Task tag: ,#credit-constraint,#financial-dependency,#financial-ratio,#asif
* Notebook US Parent (i.e the one to update): 
https://github.com/thomaspernet/Financial_dependency_pollution/blob/master/01_data_preprocessing/02_transform_tables/03_asif_financial_ratio_city.md
* Reports: https://htmlpreview.github.io/?https://github.com/thomaspernet/Financial_dependency_pollution/blob/master/01_data_preprocessing/02_transform_tables/Reports/03_asif_financial_ratio_city.html
* Analytics reports:
https://htmlpreview.github.io/?https://github.com/thomaspernet/Financial_dependency_pollution/blob/master/00_data_catalogue/HTML_ANALYSIS/ASIF_INDUSTRY_CITY.html

# Input Cloud Storage [AWS/GCP]

## Table/file
* Name: 
* asif_firms_prepared
* Github: 
  * https://github.com/thomaspernet/Financial_dependency_pollution/blob/master/01_data_preprocessing/01_prepare_tables/00_prepare_asif.md

# Destination Output/Delivery
## Table/file
* Name:
* asif_industry_city
* GitHub:
* https://github.com/thomaspernet/Financial_dependency_pollution/blob/master/01_data_preprocessing/02_transform_tables/03_asif_financial_ratio_city.md


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

Detail computation:

| Origin                  | Variable                    | construction                                                                                                                                                                       | Roam               |
|-------------------------|-----------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|--------------------|
| Balance sheet variables | current asset               | c80 + c81 + c82 + c79                                                                                                                                                              | #current-asset     |
| Balance sheet variables | intangible                  | c91 + c92                                                                                                                                                                          | #intangible-asset  |
| Balance sheet variables | tangible                    | tofixed - cudepre - (c91 + c92)                                                                                                                                                    | #tangible-asset    |
| Balance sheet variables | net fixed asset             | tofixed - cudepre + (c91 + c92)                                                                                                                                                    | #net-fixed-asset   |
| Balance sheet variables | error                       | (c80 + c81 + c82 + c79 +  tofixed - cudepre + (c91 + c92)) - (c95 + c97 + c99)                                                                                                     |                    |
| Balance sheet variables | total_liabilities           | if (c80 + c81 + c82 + c79 +  tofixed - cudepre + (c91 + c92)) - (c95 + c97 + c99). > 0 then allocate error to liabilities else c95 + c97 + c99                                     | #total-liabilities |
| Balance sheet variables | total_asset                 | if (c80 + c81 + c82 + c79 +  tofixed - cudepre + (c91 + c92)) - (c95 + c97 + c99). <  0 then allocate error to asset else c80 + c81 + c82 + c79 + tofixed - cudepre + (c91 + c92)  |                    |
| Financial metric        | cashflow                    | (c131 - c134) + cudepre                                                                                                                                                            | #cashflow          |
| Financial metric        | current_ratio               |  c80 + c81 + c82 + c79 / c95                                                                                                                                                       | #current-ratio     |
| Financial metric        | quick ratio                 |  c80 + c81 + c82 + c79 - c80 - c81 / c95                                                                                                                                           | #quick-ratio       |
| Financial metric        | liabilities_tot_asset       | c98 / total_asset                                                                                                                                                                  | #leverage          |
| Financial metric        | sales_tot_asset             | sales / total_asset                                                                                                                                                                | #sales-over-asset  |
| Financial metric        | investment_tot_asset        | c84 / total_asset                                                                                                                                                                  |                    |
| Financial metric        | rd_tot_asset                | rdfee / total_asset                                                                                                                                                                |                    |
| Financial metric        | asset_tangibility_tot_asset |  tangible / total_asset                                                                                                                                                            | #collateral        |
| Financial metric        | cashflow_tot_asset          | cashflow / total_asset                                                                                                                                                             |                    |
| Financial metric        | cashflow_to_tangible        | cashflow / tangible                                                                                                                                                                |                    |
| Financial metric        | return_to_sale              | c131 / sales                                                                                                                                                                       | #return-on-sales   |
| Financial metric        | coverage_ratio              | c131 / c125                                                                                                                                                                        | #coverage-ratio    |
| Financial metric        | liquidity                   | cuasset - c95 / total_asset                                                                                                                                                        | #liquidity         |
| Other variables         | labor_productivity          | sales/employ                                                                                                                                                                       |                    |
| Other variables         | labor_capital               | employ / tangible                                                                                                                                                                  |                    |
| Other variables         | age                         | year - setup                                                                                                                                                                       |                    |
| Other variables         | export_to_sale              |  export / sale                                                                                                                                                                     |                    |


## Example step by step

1. Computation ratio by industry

As an average over year 2002 to 2006. As in Fan, compute directly at the industry, then get the average

- Computed using the Chinese data
    - The ExtFin based on Chinese data is calculated at the 2-digit Chinese Industrial Classification (CIC) level
    - Data available in year 2004–2006 in the NBSC Database. We calculate the aggregate rather than the median external finance dependence at 2-digit industry level, because the median firm in Chinese database often has no capital expenditure
    - In our sample, approximately 68.1% firms have zero capital expenditure

4. General Accepted Accounting Principles to discard observations for which one of the following criteria is violated
   
    - (1) the key financial variables (such as total assets, net value of fixed assets, sales, gross value of industrial output) cannot be missing
    - (2) the number of employees hired by a firm must not be less than 10
    - (3) the total assets must be higher than the liquid assets
    - (4) the total assets must be larger than the total fixed assets
    - (5) the total assets must be larger than the net value of the fixed assets
    - (6) a firm’s identification number cannot be missing and must be unique
    - (7) the established time must be valid (e.g., the opening month cannot be later than December or earlier than January)

![](https://cdn.corporatefinanceinstitute.com/assets/A-Balance-Sheet.png)

To satisfy the equation, we compute the left hand side and the right and side. IF the equation is not satisfied, we add the difference to either the right or left part according to the following rules:

- total asset (toasset) - total liabilities (c98) + total equity (c99) < 0 then add the difference to total asset (left part)
- total asset (toasset) - total liabilities (c98) + total equity (c99) > 0 then add the difference to total liabilities and equity (right part)

```python
DatabaseName = 'firms_survey'
s3_output_example = 'SQL_OUTPUT_ATHENA'
```

```python
query= """
WITH test AS (
  SELECT 
    *, 
    CASE WHEN LENGTH(cic) = 4 THEN substr(cic, 1, 2) ELSE concat(
      '0', 
      substr(cic, 1, 1)
    ) END AS indu_2, 
    c80 + c81 + c82 + c79 as current_asset, 
    c91 + c92 AS intangible, 
    tofixed - cudepre - (c91 + c92) AS tangible, 
    tofixed - cudepre + (c91 + c92) AS net_fixed_asset, 
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
        year, 
        -- cic, 
        indu_2, 
        geocode4_corr, 
        province_en, 
        CAST(
          SUM(output) AS DECIMAL(16, 5)
        ) AS output, 
        CAST(
          SUM(sales) AS DECIMAL(16, 5)
        ) AS sales, 
        CAST(
          SUM(employ) AS DECIMAL(16, 5)
        ) AS employment, 
        CAST(
          SUM(captal) AS DECIMAL(16, 5)
        ) AS capital, 
        SUM(current_asset) AS current_asset, 
        SUM(tofixed) AS tofixed, 
        SUM(error) AS error, 
        SUM(total_liabilities) AS total_liabilities, 
        SUM(total_asset) AS total_asset, 
        SUM(total_right) AS total_right, 
        SUM(intangible) AS intangible, 
        SUM(tangible) AS tangible, 
        SUM(net_fixed_asset) AS net_fixed_asset, 
        SUM(cashflow) AS cashflow, 
        CAST(
          SUM(c80 + c81 + c82 + c79) AS DECIMAL(16, 5)
        ) / NULLIF(
          CAST(
            SUM(c95) AS DECIMAL(16, 5)
          ), 
          0
        ) AS current_ratio, 
        CAST(
          SUM(c80 + c81 + c82 + c79 - c80 - c81) AS DECIMAL(16, 5)
        ) / NULLIF(
          CAST(
            SUM(c95) AS DECIMAL(16, 5)
          ), 
          0
        ) AS quick_ratio, 
        CAST(
          SUM(c98) AS DECIMAL(16, 5)
        ) / NULLIF(
          CAST(
            SUM(total_asset) AS DECIMAL(16, 5)
          ), 
          0
        ) AS liabilities_tot_asset, 
        CAST(
          SUM(sales) AS DECIMAL(16, 5)
        ) / NULLIF(
          CAST(
            SUM(total_asset) AS DECIMAL(16, 5)
          ), 
          0
        ) AS sales_tot_asset, 
        CAST(
          SUM(c84) AS DECIMAL(16, 5)
        ) / NULLIF(
          CAST(
            SUM(total_asset) AS DECIMAL(16, 5)
          ), 
          0
        ) AS investment_tot_asset, 
        CAST(
          SUM(rdfee) AS DECIMAL(16, 5)
        ) / NULLIF(
          CAST(
            SUM(total_asset) AS DECIMAL(16, 5)
          ), 
          0
        ) AS rd_tot_asset, 
        CAST(
          SUM(tangible) AS DECIMAL(16, 5)
        ) / NULLIF(
          CAST(
            SUM(total_asset) AS DECIMAL(16, 5)
          ), 
          0
        ) asset_tangibility_tot_asset, 
        CAST(
          SUM(cashflow) AS DECIMAL(16, 5)
        ) / NULLIF(
          CAST(
            SUM(total_asset) AS DECIMAL(16, 5)
          ), 
          0
        ) AS cashflow_tot_asset, 
        CAST(
          SUM(cashflow) AS DECIMAL(16, 5)
        ) / NULLIF(
          CAST(
            SUM(tangible) AS DECIMAL(16, 5)
          ), 
          0
        ) AS cashflow_to_tangible, 
        -- update
        CAST(
          SUM(c131) AS DECIMAL(16, 5)
        ) / NULLIF(
          CAST(
            SUM(sales) AS DECIMAL(16, 5)
          ), 
          0
        ) AS return_to_sale, 
        CAST(
          SUM(c131) AS DECIMAL(16, 5)
        ) / NULLIF(
          CAST(
            SUM(c125) AS DECIMAL(16, 5)
          ), 
          0
        ) AS coverage_ratio, 
        CAST(
          SUM(current_asset - c95) AS DECIMAL(16, 5)
        ) / NULLIF(
          CAST(
            SUM(tangible) AS DECIMAL(16, 5)
          ), 
          0
        ) AS liquidity 
      FROM 
        test 
      WHERE 
        year in (
          '2001', '2002', '2003', '2004', '2005', 
          '2006', '2007'
        ) 
        AND total_asset > 0 
        AND tangible > 0 
      GROUP BY 
        province_en, 
        geocode4_corr, 
        -- cic,
        indu_2, 
        year 
      
    ) 
    SELECT 
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
      net_fixed_asset, 
      cashflow, 
      current_ratio,
      LAG(current_ratio, 1) OVER (
        PARTITION BY geocode4_corr, indu_2 
        ORDER BY 
          year
      ) as lag_current_ratio,
      quick_ratio, 
      LAG(quick_ratio, 1) OVER (
        PARTITION BY geocode4_corr, indu_2 
        ORDER BY 
          year
      ) as lag_quick_ratio,
      liabilities_tot_asset, 
      LAG(liabilities_tot_asset, 1) OVER (
        PARTITION BY geocode4_corr, indu_2 
        ORDER BY 
          year
      ) as lag_liabilities_tot_asset,
      sales_tot_asset,
      LAG(sales_tot_asset, 1) OVER (
        PARTITION BY geocode4_corr, indu_2 
        ORDER BY 
          year
      ) as lag_sales_tot_asset,
      investment_tot_asset, 
      rd_tot_asset, 
      asset_tangibility_tot_asset, 
      cashflow_tot_asset,
      LAG(cashflow_tot_asset, 1) OVER (
        PARTITION BY geocode4_corr, indu_2 
        ORDER BY 
          year
      ) as lag_cashflow_tot_asset,
      cashflow_to_tangible, 
      LAG(cashflow_to_tangible, 1) OVER (
        PARTITION BY geocode4_corr, indu_2 
        ORDER BY 
          year
      ) as lag_cashflow_to_tangible,
      return_to_sale, 
      LAG(return_to_sale, 1) OVER (
        PARTITION BY geocode4_corr, indu_2 
        ORDER BY 
          year
      ) as lag_return_to_sale,
      coverage_ratio, 
      liquidity 
    FROM 
      ratio
    LIMIT 
        10
  )
"""
output = s3.run_query(
                    query=query,
                    database=DatabaseName,
                    s3_output=s3_output_example,
    filename = 'example_1'
                )
output
```

# Table `asif_industry_financial_ratio_city`

Since the table to create has missing value, please use the following at the top of the query

```
CREATE TABLE database.table_name WITH (format = 'PARQUET') AS
```


Choose a location in S3 to save the CSV. It is recommended to save in it the `datalake-datascience` bucket. Locate an appropriate folder in the bucket, and make sure all output have the same format

```python
s3_output = 'DATA/ECON/FIRM_SURVEY/ASIF_CHINA/TRANSFORMED/FINANCIAL_RATIO/CITY'
table_name = 'asif_industry_financial_ratio_city'
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
    tofixed - cudepre - (c91 + c92) AS tangible, 
    tofixed - cudepre + (c91 + c92) AS net_fixed_asset, 
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
        year, 
        -- cic, 
        indu_2, 
        geocode4_corr, 
        province_en, 
        CAST(
          SUM(output) AS DECIMAL(16, 5)
        ) AS output, 
        CAST(
          SUM(sales) AS DECIMAL(16, 5)
        ) AS sales, 
        CAST(
          SUM(employ) AS DECIMAL(16, 5)
        ) AS employment, 
        CAST(
          SUM(captal) AS DECIMAL(16, 5)
        ) AS capital, 
        SUM(current_asset) AS current_asset, 
        SUM(tofixed) AS tofixed, 
        SUM(error) AS error, 
        SUM(total_liabilities) AS total_liabilities, 
        SUM(total_asset) AS total_asset, 
        SUM(total_right) AS total_right, 
        SUM(intangible) AS intangible, 
        SUM(tangible) AS tangible, 
        SUM(net_fixed_asset) AS net_fixed_asset, 
        SUM(cashflow) AS cashflow, 
        CAST(
          SUM(c80 + c81 + c82 + c79) AS DECIMAL(16, 5)
        ) / NULLIF(
          CAST(
            SUM(c95) AS DECIMAL(16, 5)
          ), 
          0
        ) AS current_ratio, 
        CAST(
          SUM(c80 + c81 + c82 + c79 - c80 - c81) AS DECIMAL(16, 5)
        ) / NULLIF(
          CAST(
            SUM(c95) AS DECIMAL(16, 5)
          ), 
          0
        ) AS quick_ratio, 
        CAST(
          SUM(c98) AS DECIMAL(16, 5)
        ) / NULLIF(
          CAST(
            SUM(total_asset) AS DECIMAL(16, 5)
          ), 
          0
        ) AS liabilities_tot_asset, 
        CAST(
          SUM(sales) AS DECIMAL(16, 5)
        ) / NULLIF(
          CAST(
            SUM(total_asset) AS DECIMAL(16, 5)
          ), 
          0
        ) AS sales_tot_asset, 
        CAST(
          SUM(c84) AS DECIMAL(16, 5)
        ) / NULLIF(
          CAST(
            SUM(total_asset) AS DECIMAL(16, 5)
          ), 
          0
        ) AS investment_tot_asset, 
        CAST(
          SUM(rdfee) AS DECIMAL(16, 5)
        ) / NULLIF(
          CAST(
            SUM(total_asset) AS DECIMAL(16, 5)
          ), 
          0
        ) AS rd_tot_asset, 
        CAST(
          SUM(tangible) AS DECIMAL(16, 5)
        ) / NULLIF(
          CAST(
            SUM(total_asset) AS DECIMAL(16, 5)
          ), 
          0
        ) asset_tangibility_tot_asset, 
        CAST(
          SUM(cashflow) AS DECIMAL(16, 5)
        ) / NULLIF(
          CAST(
            SUM(total_asset) AS DECIMAL(16, 5)
          ), 
          0
        ) AS cashflow_tot_asset, 
        CAST(
          SUM(cashflow) AS DECIMAL(16, 5)
        ) / NULLIF(
          CAST(
            SUM(tangible) AS DECIMAL(16, 5)
          ), 
          0
        ) AS cashflow_to_tangible, 
        -- update
        CAST(
          SUM(c131) AS DECIMAL(16, 5)
        ) / NULLIF(
          CAST(
            SUM(sales) AS DECIMAL(16, 5)
          ), 
          0
        ) AS return_to_sale, 
        CAST(
          SUM(c131) AS DECIMAL(16, 5)
        ) / NULLIF(
          CAST(
            SUM(c125) AS DECIMAL(16, 5)
          ), 
          0
        ) AS coverage_ratio, 
        CAST(
          SUM(current_asset - c95) AS DECIMAL(16, 5)
        ) / NULLIF(
          CAST(
            SUM(tangible) AS DECIMAL(16, 5)
          ), 
          0
        ) AS liquidity 
      FROM 
        test 
      WHERE 
        year in (
          '2001', '2002', '2003', '2004', '2005', 
          '2006', '2007'
        ) 
        AND total_asset > 0 
        AND tangible > 0 
      GROUP BY 
        province_en, 
        geocode4_corr, 
        -- cic,
        indu_2, 
        year 
      
    ) 
    SELECT 
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
      net_fixed_asset, 
      cashflow, 
      current_ratio,
      LAG(current_ratio, 1) OVER (
        PARTITION BY geocode4_corr, indu_2 
        ORDER BY 
          year
      ) as lag_current_ratio,
      quick_ratio, 
      LAG(quick_ratio, 1) OVER (
        PARTITION BY geocode4_corr, indu_2 
        ORDER BY 
          year
      ) as lag_quick_ratio,
      liabilities_tot_asset, 
      LAG(liabilities_tot_asset, 1) OVER (
        PARTITION BY geocode4_corr, indu_2 
        ORDER BY 
          year
      ) as lag_liabilities_tot_asset,
      sales_tot_asset,
      LAG(sales_tot_asset, 1) OVER (
        PARTITION BY geocode4_corr, indu_2 
        ORDER BY 
          year
      ) as lag_sales_tot_asset,
      investment_tot_asset, 
      rd_tot_asset, 
      asset_tangibility_tot_asset, 
      cashflow_tot_asset,
      LAG(cashflow_tot_asset, 1) OVER (
        PARTITION BY geocode4_corr, indu_2 
        ORDER BY 
          year
      ) as lag_cashflow_tot_asset,
      cashflow_to_tangible, 
      LAG(cashflow_to_tangible, 1) OVER (
        PARTITION BY geocode4_corr, indu_2 
        ORDER BY 
          year
      ) as lag_cashflow_to_tangible,
      return_to_sale, 
      LAG(return_to_sale, 1) OVER (
        PARTITION BY geocode4_corr, indu_2 
        ORDER BY 
          year
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

```python
query_test = """
SELECT  
AVG(tangible) AS avg_tangible, 
AVG(intangible) AS avg_intangible, 
AVG(current_asset) AS avg_current_asset, 
AVG(return_to_sale)AS avg_return_to_sale, 
AVG(liabilities_tot_asset) AS avg_liabilities_tot_asset, 
AVG(cashflow_tot_asset) AS avg_cashflow_tot_asset,
AVG(cashflow_to_tangible) AS avg_cashflow_to_tangible,
AVG(liquidity) AS avg_liquidity,
AVG(sales_tot_asset) AS avg_sales_tot_asset,
AVG(coverage_ratio) AS avg_coverage_ratio
FROM asif_industry_financial_ratio_city  

"""
output = s3.run_query(
                    query=query_test,
                    database=DatabaseName,
                    s3_output=s3_output_example,
    filename = 'count_{}'.format(table_name)
                )
output.T
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
partition_keys = ["province_en", "geocode4_corr","indu_2", "year" ]
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
schema = [{'Name': 'output', 'Type': 'decimal(16,5)', 'Comment': 'Output'},
          {'Name': 'employment',
              'Type': 'decimal(16,5)', 'Comment': 'employment'},
          {'Name': 'capital', 'Type': 'decimal(16,5)', 'Comment': 'capital'},
          {'Name': 'current_asset', 'Type': 'int', 'Comment': 'current asset'},
          {'Name': 'net_fixed_asset', 'Type': 'int', 'Comment': 'total net fixed asset'},
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
Compute the financial ratio by city-industry
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
glue.update_schema_table(
    database = DatabaseName,
    table = table_name,
    schema= schema)
```

## Check Duplicates

One of the most important step when creating a table is to check if the table contains duplicates. The cell below checks if the table generated before is empty of duplicates. The code uses the JSON file to create the query parsed in Athena. 

You are required to define the group(s) that Athena will use to compute the duplicate. For instance, your table can be grouped by COL1 and COL2 (need to be string or varchar), then pass the list ['COL1', 'COL2'] 

```python
partition_keys = ["province_en", "geocode4_corr","indu_2", "year" ]

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
secondary_key = 'indu_2'
y_var = 'tangible'
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
create_report(extension = "html", keep_code = True, notebookname = "03_asif_financial_ratio_city.ipynb")
```
