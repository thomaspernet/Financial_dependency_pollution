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

Transform asif tfp firm level and others data by merging asif firms prepared and others data by constructing asset_tangibility and others () to asif tfp credit constraint 

# Business needs 

Transform asif tfp firm level and others data by merging asif firms prepared, china credit constraint, ind cic 2 name, china city code normalised, china tcz spz, china city reduction mandate data by constructing asset_tangibility, cash_over_total_asset, sales over asset Andersen method, current ratio (Compute proxy for credit constraint , Construct main variable asset tangibility) to asif tfp credit constraint 

## Description
### Objective 

Use existing tables asif tfp firm level, asif firms prepared, china credit constraint, ind cic 2 name, china city code normalised, china tcz spz, china city reduction mandate to constructing a bunch of variables listed below

# Construction variables 

- asset_tangibility
- cash_over_total_asset
- sales over asset Andersen method
- current ratio
* investment to total asset
* RD to total asset
* tangible asset to total asset
* cash flow to total tangible asset
* cash_over_total_asset
[UPDATE] → add the following variables
* all credits to gdp → from "chinese_lookup"."province_credit_constraint"
  * from US yrq43zfhl56557i
* long term loan to gdp → from "chinese_lookup"."province_credit_constraint"
* return on sale: net income (c111)/sales
* coverage ratio: net income(c111) /total interest payments
* liquidity: (current assets − current liabilities)/total assets.
* labor productivity: real sales/number of employees.
* age: current year – firm’s year of establishment.
* export to sale: overseas turnover / sales

### Steps 




**Cautious**
Make sure there is no duplicates

# Target

- The file is saved in S3:
- bucket: datalake-datascience
- path: DATA/ECON/FIRM_SURVEY/ASIF_CHINA/TRANSFORMED/TFP/CREDIT_CONSTRAINT
- Glue data catalog should be updated
- database: firms_survey
- Table prefix: asif_tfp_
- table name: asif_tfp_credit_constraint
- Analytics
- HTML: ANALYTICS/HTML_OUTPUT/asif_tfp_credit_constraint
- Notebook: ANALYTICS/OUTPUT/asif_tfp_credit_constraint

# Metadata

- Key: skh99aptd66739g
- Epic: Dataset transformation
- US: TFP firm table
- Task tag: #tfp, #firm-level, #financial-ratio
- Analytics reports: https://htmlpreview.github.io/?https://github.com/thomaspernet/Financial_dependency_pollution/blob/master/00_data_catalogue/HTML_ANALYSIS/ASIF_TFP_CREDIT_CONSTRAINT.html

# Input Cloud Storage

## Table/file

**Name** 

- DATA/ECON/FIRM_SURVEY/ASIF_CHINA/TRANSFORMED/TFP/FIRM_LEVEL
- DATA/ECON/FIRM_SURVEY/ASIF_CHINA/PREPARED
- DATA/ECON/INDUSTRY/ADDITIONAL_DATA/CHINA/CIC/CREDIT_CONSTRAINT
- DATA/ECON/LOOKUP_DATA/CIC_2_NAME
- DATA/ECON/LOOKUP_DATA/CITY_CODE_NORMALISED
- DATA/ECON/POLICY/CHINA/STRUCTURAL_TRANSFORMATION/CITY_TARGET/TCZ_SPZ
- DATA/ENVIRONMENT/CHINA/FYP/CITY_REDUCTION_MANDATE
- DATA/ECON/INDUSTRY/ADDITIONAL_DATA/CHINA/PROVINCES/CREDIT_CONSTRAINT

**Github**

- https://github.com/thomaspernet/Financial_dependency_pollution/blob/master/01_data_preprocessing/02_transform_tables/05_tfp_computation.md
- https://github.com/thomaspernet/Financial_dependency_pollution/blob/master/01_data_preprocessing/01_prepare_tables/00_prepare_asif.md
- https://github.com/thomaspernet/Financial_dependency_pollution/blob/master/01_data_preprocessing/00_download_data_from/CIC_CREDIT_CONSTRAINT/financial_dependency.py
- https://github.com/thomaspernet/Financial_dependency_pollution/blob/master/01_data_preprocessing/00_download_data_from/CIC_NAME/cic_industry_name.py
- https://github.com/thomaspernet/Financial_dependency_pollution/blob/master/01_data_preprocessing/00_download_data_from/CITY_CODE_CORRESPONDANCE/city_code_correspondance.py
- https://github.com/thomaspernet/Financial_dependency_pollution/blob/master/01_data_preprocessing/00_download_data_from/TCZ_SPZ/tcz_spz_policy.py
- https://github.com/thomaspernet/Financial_dependency_pollution/blob/master/01_data_preprocessing/00_download_data_from/CITY_REDUCTION_MANDATE/city_reduction_mandate.py
- https://github.com/thomaspernet/Financial_dependency_pollution/blob/master/01_data_preprocessing/00_download_data_from/PROVINCE_CREDIT_CONSTRAINT/supply_credit.py

# Destination Output/Delivery

## Table/file

**Name**

asif_tfp_credit_constraint

**GitHub**

- https://github.com/thomaspernet/Financial_dependency_pollution/blob/master/01_data_preprocessing/02_transform_tables/09_asif_tfp_firm_baseline.md
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


## Example step by step

- total asset = fixed asset + current assets
    - fixed assets = tangible + intangible + other fixed assets
    - current asset = inventories + accounts receivable + other current assets
        - other current assets = cash and equivalents + prepaid expenses and advances +other current assets + deferred charges+ and short term investments

- total asset = (tangible + intangible + other fixed assets) + (inventories + accounts receivable + other current assets
    - fixed asset (tofixed):
        - tangible: tofixed - cudepre 
        - intangible: c91 (无形及递延) + c92 (无形资产)
        - other fixed asset: Missing in the dataset
   - current assets (cuasset):
       - account receivable: c80 (应收帐款)
       - inventories: c81 (存货)
       - other current asset: cuasset - c80 - c81
            - cash and equivalents
            - prepaid expenses and advances
            - other current assets
            - deferred charges
            - short term investments: c79 (其中：短期投资)

| Origin                  | Variable                    | construction                                                                                                                                                                 | Roam               |
|-------------------------|-----------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|--------------------|
| Balance sheet variables | current asset               | c80 + c81 + c82 + c79                                                                                                                                                        | #current-asset     |
| Balance sheet variables | intangible                  | c91 + c92                                                                                                                                                                    | #intangible-asset  |
| Balance sheet variables | tangible                    | tofixed - cudepre                                                                                                                                               | #tangible-asset    |
| Balance sheet variables | total net non current             | tofixed - cudepre + (c91 + c92)                                                                                                                                              | #net-fixed-asset   |
| Balance sheet variables | error                       | (c80 + c81 + c82 + c79 +  tofixed - cudepre + (c91 + c92)) - (c95 + c97  + c99)                                                                                                     |                    |
| Balance sheet variables | total_liabilities           | if (c80 + c81 + c82 + c79 +  tofixed - cudepre + (c91 + c92)) - (c95 + c97  + c99). > 0 then allocate error to liabilities else c98 + c99                                           | #total-liabilities |
| Balance sheet variables | total_asset                 | if (c80 + c81 + c82 + c79 +  tofixed - cudepre + (c91 + c92)) - (c95 + c97  + c99). <  0 then allocate error to asset else c80 + c81 + c82 + c79 + tofixed - cudepre + (c91 + c92)  |                    |
| Financial metric        | cashflow                    | (c131 - c134) + cudepre                                                                                                                                                      | #cashflow          |
| Financial metric        | current_ratio               |  c80 + c81 + c82 + c79 / c95                                                                                                                                                 | #current-ratio     |
| Financial metric        | quick ratio                 |  c80 + c81 + c82 + c79 - c80 - c81 / c95                                                                                                                                     | #quick-ratio       |
| Financial metric        | liabilities_tot_asset       | c98 / total_asset                                                                                                                                                            | #leverage          |
| Financial metric        | sales_tot_asset             | sales / total_asset                                                                                                                                                          | #sales-over-asset  |
| Financial metric        | investment_tot_asset        | c84 / total_asset                                                                                                                                                            |                    |
| Financial metric        | rd_tot_asset                | rdfee / total_asset                                                                                                                                                          |                    |
| Financial metric        | asset_tangibility_tot_asset |  tangible / total_asset                                                                                                                                                      | #collateral        |
| Financial metric        | cashflow_tot_asset          | cashflow / total_asset                                                                                                                                                       |                    |
| Financial metric        | cashflow_to_tangible        | cashflow / tangible                                                                                                                                                          |                    |
| Financial metric        | return_to_sale              | c131 / sales                                                                                                                                                                 | #return-on-sales   |
| Financial metric        | coverage_ratio              | c131 / c125                                                                                                                                                                  | #coverage-ratio    |
| Financial metric        | liquidity                   | cuasset - c95 / total_asset                                                                                                                                                  | #liquidity         |
| Other variables         | labor_productivity          | sales/employ                                                                                                                                                                 |                    |
| Other variables         | labor_capital               | employ / tangible                                                                                                                                                            |                    |
| Other variables         | age                         | year - setup                                                                                                                                                                 |                    |
| Other variables         | export_to_sale              |  export / sale                                                                                                                                                               |                    |

- [[Internal finance and growth: Microeconometric evidence on Chinese firms]]
    - https://www.sciencedirect.com/science/article/pii/S0304387810000805
- [[Internal financial constraints and firm productivity in China: Do liquidity and export behavior make a difference?]]
    - https://www.sciencedirect.com/science/article/pii/S0147596713000760
- [[Credit constraints and firm productivity: Microeconomic evidence from China]]
    - https://www.sciencedirect.com/science/article/pii/S0275531917301745

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
    ) - (c95 + c97 + c99) < 0 THEN (c80 + c81 + c82 + c79 + tofixed - cudepre + (c91 + c92)) + ABS(
      (
        c80 + c81 + c82 + c79 + tofixed - cudepre + (c91 + c92)
      ) - (c95 + c97 + c99)
    ) ELSE (
      c80 + c81 + c82 + c79 + tofixed - cudepre + (c91 + c92)
    ) END AS total_asset, 
    (c131 - c134) + cudepre as cashflow, 
    CAST(
      c80 + c81 + c82 + c79 AS DECIMAL(16, 5)
    ) / NULLIF(
      CAST(
        c95 AS DECIMAL(16, 5)
      ), 
      0
    ) AS current_ratio, 
    CAST(
      c80 + c81 + c82 + c79 - c80 - c81 AS DECIMAL(16, 5)
    ) / NULLIF(
      CAST(
        c95 AS DECIMAL(16, 5)
      ), 
      0
    ) AS quick_ratio 
    
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
  AND c97 >0 -- long term liabilities
  AND c98 > 0 -- total liabilities
  AND c99 > 0 -- equity
  AND c80 + c81 + c82 + c79 >0
  AND tofixed >0
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
        cic, 
        indu_2, 
        geocode4_corr,
        province_en,
        ownership, 
        CASE WHEN ownership = 'SOE' THEN 'SOE' ELSE 'PRIVATE' END AS soe_vs_pri, 
        CASE WHEN ownership in ('HTM', 'FOREIGN') THEN 'FOREIGN' ELSE 'DOMESTIC' END AS for_vs_dom, 
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
        quick_ratio,
        CAST(
          c98 AS DECIMAL(16, 5)
        ) / NULLIF(
          CAST(
            total_asset AS DECIMAL(16, 5)
          ), 
          0
        ) AS liabilities_tot_asset, 
        CAST(
          sales AS DECIMAL(16, 5)
        ) / NULLIF(
          CAST(
            total_asset AS DECIMAL(16, 5)
          ), 
          0
        ) AS sales_tot_asset,         
        CAST(
          c84 AS DECIMAL(16, 5)
        ) / NULLIF(
          CAST(
            total_asset AS DECIMAL(16, 5)
          ), 
          0
        ) AS investment_tot_asset, 
        CAST(
          rdfee AS DECIMAL(16, 5)
        ) / NULLIF(
          CAST(
            total_asset AS DECIMAL(16, 5)
          ), 
          0
        ) AS rd_tot_asset, 
        CAST(
          tangible AS DECIMAL(16, 5)
        ) / NULLIF(
          CAST(
            total_asset AS DECIMAL(16, 5)
          ), 
          0
        ) asset_tangibility_tot_asset, 
        CAST(
          cashflow AS DECIMAL(16, 5)
        ) / NULLIF(
          CAST(
            total_asset AS DECIMAL(16, 5)
          ), 
          0
        ) AS cashflow_tot_asset, 
        CAST(
          cashflow AS DECIMAL(16, 5)
        ) / NULLIF(
          CAST(
            tangible AS DECIMAL(16, 5)
          ), 
          0
        ) AS cashflow_to_tangible,
       -- update
       CAST(
          c131 AS DECIMAL(16, 5)
        ) / NULLIF(
          CAST(
            sales AS DECIMAL(16, 5)
          ), 
          0
        ) AS return_to_sale,
       CAST(
          c131 AS DECIMAL(16, 5)
        ) / NULLIF(
          CAST(
            c125 AS DECIMAL(16, 5)
          ), 
          0
        ) AS coverage_ratio,
       CAST(
          current_asset - c95 AS DECIMAL(16, 5)
         ) / NULLIF(
           CAST(
            tangible AS DECIMAL(16, 5)
          ), 
          0
         ) AS liquidity,
         CAST(
          sales AS DECIMAL(16, 5)
         ) / NULLIF(
          CAST(
            employ AS DECIMAL(16, 5)
          ), 
          0
         ) AS labor_productivity,
         CAST(
          employ AS DECIMAL(16, 5)
         ) / NULLIF(
          CAST(
            tangible/10  AS DECIMAL(16, 5)
          ), 
          0
         ) AS labor_capital,
        CAST(year AS DECIMAL(16, 5)) - CAST(setup AS DECIMAL(16, 5)) AS age,
        CAST(
           export AS DECIMAL(16, 5)
         ) / NULLIF(
          CAST(
            sales AS DECIMAL(16, 5)
          ), 
          0
         ) AS export_to_sale
        
      FROM 
        test 
      WHERE 
        year in (
          '2001', '2002', '2003', '2004', '2005', 
          '2006', '2007'
        )
        AND total_asset > 0 
        AND tangible > 0
        AND quick_ratio > 0
        AND current_ratio > 0
        
    ) 
    SELECT 
      ratio.firm, 
      ratio.year, 
      CASE WHEN ratio.year in (
        '2001', '2002', '2003', '2004', '2005'
      ) THEN 'FALSE' WHEN ratio.year in ('2006', '2007') THEN 'TRUE' END AS period, 
      ratio.cic, 
      ratio.indu_2, 
      CASE WHEN short IS NULL THEN 'Unknown' ELSE short END AS short, 
      ratio.geocode4_corr,
      ratio. province_en,
      CASE WHEN tcz IS NULL THEN '0' ELSE tcz END AS tcz, 
      CASE WHEN spz IS NULL 
      OR spz = '#N/A' THEN '0' ELSE spz END AS spz, 
      ratio.ownership, 
      soe_vs_pri, 
      for_vs_dom, 
      count_ownership, 
      count_city, 
      count_industry, 
      tso2_mandate_c, 
      in_10_000_tonnes, 
      ratio.output, 
      ratio.employment, 
      ratio.capital, 
      current_asset, 
      tofixed, 
      error, 
      total_liabilities, 
      total_asset, 
      intangible, 
      tangible, 
      -- cash, 
      cashflow, 
      sales, 
      tfp_op, 
      credit_constraint,
      1/ supply_all_credit as supply_all_credit,
      1/ supply_long_term_credit as supply_long_term_credit,
      current_ratio,
      LAG(current_ratio, 1) OVER (
        PARTITION BY ratio.firm, ratio.geocode4_corr, ratio.cic 
        ORDER BY 
          ratio.year
      ) as lag_current_ratio,
      quick_ratio, 
      liabilities_tot_asset,
      LAG(liabilities_tot_asset, 1) OVER (
        PARTITION BY ratio.firm, ratio.geocode4_corr, ratio.cic 
        ORDER BY 
          ratio.year
      ) as lag_liabilities_tot_asset,
      sales_tot_asset, 
      LAG(sales_tot_asset, 1) OVER (
        PARTITION BY ratio.firm, ratio.geocode4_corr, ratio.cic 
        ORDER BY 
          ratio.year
      ) as lag_sales_tot_asset,
      -- cash_tot_asset, 
      investment_tot_asset, 
      rd_tot_asset, 
      asset_tangibility_tot_asset, 
      cashflow_tot_asset, 
      cashflow_to_tangible,
      LAG(cashflow_to_tangible, 1) OVER (
        PARTITION BY ratio.firm, ratio.geocode4_corr, ratio.cic 
        ORDER BY 
          ratio.year
      ) as lag_cashflow_to_tangible,
      return_to_sale,
      CASE WHEN coverage_ratio IS NULL THEN 0 ELSE coverage_ratio END AS coverage_ratio,
      liquidity,
      CASE WHEN export_to_sale IS NULL THEN 0 ELSE export_to_sale END AS export_to_sale,
      labor_productivity /1000 as labor_productivity,
      labor_capital,
      age,
      DENSE_RANK() OVER (
        ORDER BY 
          ratio.geocode4_corr, 
          ratio.cic
      ) AS fe_c_i, 
      DENSE_RANK() OVER (
        ORDER BY 
          ratio.year, 
          ratio.cic
      ) AS fe_t_i, 
      DENSE_RANK() OVER (
        ORDER BY 
          ratio.geocode4_corr, 
          ratio.year
      ) AS fe_c_t 
    FROM 
      ratio 
      INNER JOIN (
        SELECT 
          geocode4_corr, 
          tso2_mandate_c, 
          in_10_000_tonnes 
        FROM 
          policy.china_city_reduction_mandate 
          INNER JOIN chinese_lookup.china_city_code_normalised ON china_city_reduction_mandate.citycn = china_city_code_normalised.citycn 
        WHERE 
          extra_code = geocode4_corr
      ) as city_mandate ON ratio.geocode4_corr = city_mandate.geocode4_corr 
      LEFT JOIN policy.china_city_tcz_spz ON ratio.geocode4_corr = china_city_tcz_spz.geocode4_corr 
      LEFT JOIN chinese_lookup.ind_cic_2_name ON ratio.indu_2 = ind_cic_2_name.cic
      LEFT JOIN chinese_lookup.province_credit_constraint ON ratio.province_en = province_credit_constraint.Province
      LEFT JOIN (
        SELECT 
          cic, 
          financial_dep_china AS credit_constraint 
        FROM 
          industry.china_credit_constraint
      ) as cred_constraint ON ratio.indu_2 = cred_constraint.cic 
      INNER JOIN firms_survey.asif_tfp_firm_level on ratio.firm = asif_tfp_firm_level.firm 
      AND ratio.year = asif_tfp_firm_level.year 
      AND ratio.geocode4_corr = asif_tfp_firm_level.geocode4_corr 
      AND ratio.ownership = asif_tfp_firm_level.ownership 
      AND ratio.indu_2 = asif_tfp_firm_level.indu_2 
    WHERE 
    liabilities_tot_asset > 0
    AND sales_tot_asset > 0
    ORDER BY 
      year
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

# Table `asif_tfp_credit_constraint`

Since the table to create has missing value, please use the following at the top of the query

```
CREATE TABLE database.table_name WITH (format = 'PARQUET') AS
```


Choose a location in S3 to save the CSV. It is recommended to save in it the `datalake-datascience` bucket. Locate an appropriate folder in the bucket, and make sure all output have the same format

```python
s3_output = 'DATA/ECON/FIRM_SURVEY/ASIF_CHINA/TRANSFORMED/TFP/CREDIT_CONSTRAINT'
table_name = 'asif_tfp_credit_constraint'
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
    ) - (c95 + c97 + c99) < 0 THEN (c80 + c81 + c82 + c79 + tofixed - cudepre + (c91 + c92)) + ABS(
      (
        c80 + c81 + c82 + c79 + tofixed - cudepre + (c91 + c92)
      ) - (c95 + c97 + c99)
    ) ELSE (
      c80 + c81 + c82 + c79 + tofixed - cudepre + (c91 + c92)
    ) END AS total_asset, 
    (c131 - c134) + cudepre as cashflow, 
    CAST(
      c80 + c81 + c82 + c79 AS DECIMAL(16, 5)
    ) / NULLIF(
      CAST(
        c95 AS DECIMAL(16, 5)
      ), 
      0
    ) AS current_ratio, 
    CAST(
      c80 + c81 + c82 + c79 - c80 - c81 AS DECIMAL(16, 5)
    ) / NULLIF(
      CAST(
        c95 AS DECIMAL(16, 5)
      ), 
      0
    ) AS quick_ratio 
    
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
  AND c97 >0 -- long term liabilities
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
        cic, 
        indu_2, 
        geocode4_corr,
        province_en,
        ownership, 
        CASE WHEN ownership = 'SOE' THEN 'SOE' ELSE 'PRIVATE' END AS soe_vs_pri, 
        CASE WHEN ownership in ('HTM', 'FOREIGN') THEN 'FOREIGN' ELSE 'DOMESTIC' END AS for_vs_dom, 
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
        current_asset, 
        net_non_current, 
        total_liabilities, 
        total_asset,
        total_right, 
        error,
        c91,
        c92,
        c95,
        c97,
        c98,
        intangible, 
        tangible, 
        cashflow, 
        current_ratio,
        quick_ratio,
        CAST(
          c98 AS DECIMAL(16, 5)
        ) / NULLIF(
          CAST(
            total_asset AS DECIMAL(16, 5)
          ), 
          0
        ) AS liabilities_tot_asset, 
        CAST(
          sales AS DECIMAL(16, 5)
        ) / NULLIF(
          CAST(
            total_asset AS DECIMAL(16, 5)
          ), 
          0
        ) AS sales_tot_asset,         
        CAST(
          c84 AS DECIMAL(16, 5)
        ) / NULLIF(
          CAST(
            total_asset AS DECIMAL(16, 5)
          ), 
          0
        ) AS investment_tot_asset, 
        CAST(
          rdfee AS DECIMAL(16, 5)
        ) / NULLIF(
          CAST(
            total_asset AS DECIMAL(16, 5)
          ), 
          0
        ) AS rd_tot_asset, 
        CAST(
          tangible AS DECIMAL(16, 5)
        ) / NULLIF(
          CAST(
            total_asset AS DECIMAL(16, 5)
          ), 
          0
        ) asset_tangibility_tot_asset, 
        CAST(
          cashflow AS DECIMAL(16, 5)
        ) / NULLIF(
          CAST(
            total_asset AS DECIMAL(16, 5)
          ), 
          0
        ) AS cashflow_tot_asset, 
        CAST(
          cashflow AS DECIMAL(16, 5)
        ) / NULLIF(
          CAST(
            tangible AS DECIMAL(16, 5)
          ), 
          0
        ) AS cashflow_to_tangible,
       -- update
       CAST(
          c131 AS DECIMAL(16, 5)
        ) / NULLIF(
          CAST(
            sales AS DECIMAL(16, 5)
          ), 
          0
        ) AS return_to_sale,
       CAST(
          c131 AS DECIMAL(16, 5)
        ) / NULLIF(
          CAST(
            c125 AS DECIMAL(16, 5)
          ), 
          0
        ) AS coverage_ratio,
       CAST(
          current_asset - c95 AS DECIMAL(16, 5)
         ) /
           CAST(
            total_asset AS DECIMAL(16, 5)
          ) AS liquidity,
         CAST(
          sales AS DECIMAL(16, 5)
         ) / NULLIF(
          CAST(
            employ AS DECIMAL(16, 5)
          ), 
          0
         ) AS labor_productivity,
         CAST(
          employ AS DECIMAL(16, 5)
         ) / NULLIF(
          CAST(
            tangible/10  AS DECIMAL(16, 5)
          ), 
          0
         ) AS labor_capital,
        CAST(year AS DECIMAL(16, 5)) - CAST(setup AS DECIMAL(16, 5)) AS age,
        CAST(
           export AS DECIMAL(16, 5)
         ) / NULLIF(
          CAST(
            sales AS DECIMAL(16, 5)
          ), 
          0
         ) AS export_to_sale,
        'FAKE' AS fake 
      FROM 
        test 
      WHERE 
        year in (
          '2000', '2001', '2002', '2003', '2004', 
          '2005', '2006', '2007'
        )
        AND tangible > 0
    ) 
    SELECT 
      ratio.firm, 
      ratio.year, 
      CASE WHEN ratio.year in (
        '2001', '2002', '2003', '2004', '2005'
      ) THEN 'FALSE' WHEN ratio.year in ('2006', '2007') THEN 'TRUE' END AS period, 
      ratio.cic, 
      ratio.indu_2, 
      CASE WHEN short IS NULL THEN 'Unknown' ELSE short END AS short, 
      ratio.geocode4_corr,
      province_en,
      CASE WHEN tcz IS NULL THEN '0' ELSE tcz END AS tcz, 
      CASE WHEN spz IS NULL 
      OR spz = '#N/A' THEN '0' ELSE spz END AS spz, 
      ratio.ownership, 
      soe_vs_pri, 
      for_vs_dom, 
      tso2_mandate_c, 
      in_10_000_tonnes, 
      ratio.output, 
      ratio.employment, 
      ratio.capital, 
      current_asset,
      net_non_current,
      intangible, 
      tangible,
      total_asset,
      c95  AS current_liabilities, 
      c97 AS lt_liabilities,
      c98 AS from_asif_tot_liabilities,
      total_liabilities, 
      total_right, 
      error,
      cashflow, 
      sales, 
      tfp_op, 
      credit_constraint,
      d_credit_constraint,
      1/supply_all_credit as supply_all_credit,
      1/supply_long_term_credit as supply_long_term_credit,
      current_ratio,
      LAG(current_ratio, 1) OVER (
        PARTITION BY ratio.firm, ratio.geocode4_corr, ratio.cic 
        ORDER BY 
          ratio.year
      ) as lag_current_ratio,
      quick_ratio, 
      liabilities_tot_asset,
      LAG(liabilities_tot_asset, 1) OVER (
        PARTITION BY ratio.firm, ratio.geocode4_corr, ratio.cic 
        ORDER BY 
          ratio.year
      ) as lag_liabilities_tot_asset,
      sales_tot_asset,
      LAG(sales_tot_asset, 1) OVER (
        PARTITION BY ratio.firm, ratio.geocode4_corr, ratio.cic 
        ORDER BY 
          ratio.year
      ) as lag_sales_tot_asset,
      --cash_tot_asset, 
      investment_tot_asset, 
      rd_tot_asset, 
      asset_tangibility_tot_asset, 
      cashflow_tot_asset, 
      cashflow_to_tangible,
       LAG(cashflow_to_tangible, 1) OVER (
        PARTITION BY ratio.firm, ratio.geocode4_corr, ratio.cic 
        ORDER BY 
          ratio.year
      ) as lag_cashflow_to_tangible,
      return_to_sale,
      CASE WHEN coverage_ratio IS NULL THEN 0 ELSE coverage_ratio /100 END AS coverage_ratio,
      liquidity,
      CASE WHEN export_to_sale IS NULL THEN 0 ELSE export_to_sale END AS export_to_sale,
      labor_productivity /1000 as labor_productivity,
      labor_capital,
      age + 1 AS age,
      CASE WHEN avg_asset_tangibility_f > avg_asset_tangibility_ci THEN 'LARGE' ELSE 'SMALL' END AS avg_size_asset_fci, 
      CASE WHEN avg_output_f > avg_output_ci THEN 'LARGE' ELSE 'SMALL' END AS avg_size_output_fci, 
      CASE WHEN avg_employment_f > avg_employment_ci THEN 'LARGE' ELSE 'SMALL' END AS avg_employment_fci, 
      CASE WHEN avg_capital_f > avg_capital_ci THEN 'LARGE' ELSE 'SMALL' END AS avg_size_capital_fci, 
      CASE WHEN avg_sales_f > avg_sales_ci THEN 'LARGE' ELSE 'SMALL' END AS avg_sales_fci, 
      MAP(
        ARRAY[.5, 
        .75, 
        .90, 
        .95 ], 
        zip_with(
          transform(
            sequence(1, 4), 
            x -> avg_asset_tangibility_f
          ), 
          pct_asset_tangibility_ci, 
          (x, y) -> x > y
        )
      ) AS size_asset_fci,
      MAP(
        ARRAY[.5, 
        .75, 
        .90, 
        .95 ], 
        zip_with(
          transform(
            sequence(1, 4), 
            x -> avg_asset_tangibility_f
          ), 
          pct_asset_tangibility_c, 
          (x, y) -> x > y
        )
      ) AS size_asset_fc, 
      MAP(
        ARRAY[.5, 
        .75, 
        .90, 
        .95 ], 
        zip_with(
          transform(
            sequence(1, 4), 
            x -> avg_asset_tangibility_f
          ), 
          pct_asset_tangibility_i, 
          (x, y) -> x > y
        )
      ) AS size_asset_fi,
      MAP(
        ARRAY[.5, 
        .75, 
        .90, 
        .95 ], 
        zip_with(
          transform(
            sequence(1, 4), 
            x -> avg_output_f
          ), 
          pct_output_ci, 
          (x, y) -> x > y
        )
      ) AS size_output_fci,
      MAP(
        ARRAY[.5, 
        .75, 
        .90, 
        .95 ], 
        zip_with(
          transform(
            sequence(1, 4), 
            x -> avg_output_f
          ), 
          pct_output_c, 
          (x, y) -> x > y
        )
      ) AS size_output_fc,
      MAP(
        ARRAY[.5, 
        .75, 
        .90, 
        .95 ], 
        zip_with(
          transform(
            sequence(1, 4), 
            x -> avg_output_f
          ), 
          pct_output_i, 
          (x, y) -> x > y
        )
      ) AS size_output_fi,
      MAP(
        ARRAY[.5, 
        .75, 
        .90, 
        .95 ], 
        zip_with(
          transform(
            sequence(1, 4), 
            x -> avg_employment_f
          ), 
          pct_employment_ci, 
          (x, y) -> x > y
        )
      ) AS size_employment_fci, 
      MAP(
        ARRAY[.5, 
        .75, 
        .90, 
        .95 ], 
        zip_with(
          transform(
            sequence(1, 4), 
            x -> avg_employment_f
          ), 
          pct_employment_c, 
          (x, y) -> x > y
        )
      ) AS size_employment_fc, 
      MAP(
        ARRAY[.5, 
        .75, 
        .90, 
        .95 ], 
        zip_with(
          transform(
            sequence(1, 4), 
            x -> avg_employment_f
          ), 
          pct_employment_i, 
          (x, y) -> x > y
        )
      ) AS size_employment_fi, 
      MAP(
        ARRAY[.5, 
        .75, 
        .90, 
        .95 ], 
        zip_with(
          transform(
            sequence(1, 4), 
            x -> avg_capital_f
          ), 
          pct_capital_ci, 
          (x, y) -> x > y
        )
      ) AS size_capital_fci, 
      MAP(
        ARRAY[.5, 
        .75, 
        .90, 
        .95 ], 
        zip_with(
          transform(
            sequence(1, 4), 
            x -> avg_capital_f
          ), 
          pct_capital_c, 
          (x, y) -> x > y
        )
      ) AS size_capital_fc, 
      MAP(
        ARRAY[.5, 
        .75, 
        .90, 
        .95 ], 
        zip_with(
          transform(
            sequence(1, 4), 
            x -> avg_capital_f
          ), 
          pct_capital_i, 
          (x, y) -> x > y
        )
      ) AS size_capital_fi, 
      MAP(
        ARRAY[.5, 
        .75, 
        .90, 
        .95 ], 
        zip_with(
          transform(
            sequence(1, 4), 
            x -> avg_sales_f
          ), 
          pct_sales_ci, 
          (x, y) -> x > y
        )
      ) AS size_sales_fci, 
      MAP(
        ARRAY[.5, 
        .75, 
        .90, 
        .95 ], 
        zip_with(
          transform(
            sequence(1, 4), 
            x -> avg_sales_f
          ), 
          pct_sales_c, 
          (x, y) -> x > y
        )
      ) AS size_sales_fc,
      MAP(
        ARRAY[.5, 
        .75, 
        .90, 
        .95 ], 
        zip_with(
          transform(
            sequence(1, 4), 
            x -> avg_sales_f
          ), 
          pct_sales_i, 
          (x, y) -> x > y
        )
      ) AS size_sales_fi,
      count_ownership,
      count_city,
      count_industry,
      DENSE_RANK() OVER (
        ORDER BY 
          ratio.geocode4_corr, 
          ratio.cic
      ) AS fe_c_i, 
      DENSE_RANK() OVER (
        ORDER BY 
          ratio.year, 
          ratio.cic
      ) AS fe_t_i, 
      DENSE_RANK() OVER (
        ORDER BY 
          ratio.geocode4_corr, 
          ratio.year
      ) AS fe_c_t 
    FROM 
      ratio 
      INNER JOIN (
        SELECT 
          year, 
          geocode4_corr, 
          provinces, 
          cityen, 
          indus_code AS cic, 
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
          lower_location, 
          larger_location, 
          coastal
      ) as aggregate_pol ON ratio.year = aggregate_pol.year 
      AND ratio.geocode4_corr = aggregate_pol.geocode4_corr 
      AND ratio.cic = aggregate_pol.cic 
      INNER JOIN (
        SELECT 
          geocode4_corr, 
          tso2_mandate_c, 
          in_10_000_tonnes 
        FROM 
          policy.china_city_reduction_mandate 
          INNER JOIN chinese_lookup.china_city_code_normalised ON china_city_reduction_mandate.citycn = china_city_code_normalised.citycn 
        WHERE 
          extra_code = geocode4_corr
      ) as city_mandate ON ratio.geocode4_corr = city_mandate.geocode4_corr 
      LEFT JOIN policy.china_city_tcz_spz ON ratio.geocode4_corr = china_city_tcz_spz.geocode4_corr 
      LEFT JOIN chinese_lookup.ind_cic_2_name ON ratio.indu_2 = ind_cic_2_name.cic
      LEFT JOIN chinese_lookup.province_credit_constraint ON ratio.province_en = province_credit_constraint.Province
      LEFT JOIN (
        SELECT 
          cic, 
          financial_dep_china AS credit_constraint, 
          CASE WHEN financial_dep_china > -.47 THEN 'ABOVE' ELSE 'BELOW' END AS d_credit_constraint 
        FROM 
          industry.china_credit_constraint
      ) as cred_constraint ON ratio.indu_2 = cred_constraint.cic 
      LEFT JOIN (
        SELECT 
          -- fake, 
          geocode4_corr, 
          indu_2,
          approx_percentile(
            avg_asset_tangibility_f, ARRAY[.5, 
            .75,.90,.95]
          ) as pct_asset_tangibility_ci, 
          AVG(avg_asset_tangibility_f) AS avg_asset_tangibility_ci, 
          approx_percentile(
            avg_output_f, ARRAY[.5,.75,.90,.95]
          ) as pct_output_ci, 
          AVG(avg_output_f) AS avg_output_ci, 
          approx_percentile(
            avg_employment_f, ARRAY[.5,.75,.90, 
            .95]
          ) as pct_employment_ci, 
          AVG(avg_employment_f) AS avg_employment_ci, 
          approx_percentile(
            avg_capital_f, ARRAY[.5,.75,.90, 
            .95]
          ) as pct_capital_ci, 
          AVG(avg_capital_f) AS avg_capital_ci, 
          approx_percentile(
            avg_sales_f, ARRAY[.5,.75,.90,.95]
          ) as pct_sales_ci, 
          AVG(avg_sales_f) AS avg_sales_ci 
        FROM 
          (
            SELECT 
              firm, 
              geocode4_corr,
              indu_2,
              -- fake, 
              AVG(tangible) as avg_asset_tangibility_f, 
              AVG(output) as avg_output_f, 
              AVG(employment) as avg_employment_f, 
              AVG(capital) as avg_capital_f, 
              AVG(sales) as avg_sales_f 
            FROM 
              ratio 
            GROUP BY 
              -- fake, 
              firm,
              geocode4_corr,
              indu_2
          ) 
        GROUP BY 
          -- fake
          geocode4_corr, 
          indu_2
      ) as pct_ci ON ratio.geocode4_corr = pct_ci.geocode4_corr 
    AND ratio.indu_2 = pct_ci.indu_2
    LEFT JOIN (
        SELECT 
          -- fake, 
          geocode4_corr, 
          approx_percentile(
            avg_asset_tangibility_f, ARRAY[.5, 
            .75,.90,.95]
          ) as pct_asset_tangibility_c, 
          AVG(avg_asset_tangibility_f) AS avg_asset_tangibility_c, 
          approx_percentile(
            avg_output_f, ARRAY[.5,.75,.90,.95]
          ) as pct_output_c, 
          AVG(avg_output_f) AS avg_output_c, 
          approx_percentile(
            avg_employment_f, ARRAY[.5,.75,.90, 
            .95]
          ) as pct_employment_c, 
          AVG(avg_employment_f) AS avg_employment_c, 
          approx_percentile(
            avg_capital_f, ARRAY[.5,.75,.90, 
            .95]
          ) as pct_capital_c, 
          AVG(avg_capital_f) AS avg_capital_c, 
          approx_percentile(
            avg_sales_f, ARRAY[.5,.75,.90,.95]
          ) as pct_sales_c, 
          AVG(avg_sales_f) AS avg_sales_c
        FROM 
          (
            SELECT 
              firm, 
              geocode4_corr,
              -- fake, 
              AVG(tangible) as avg_asset_tangibility_f, 
              AVG(output) as avg_output_f, 
              AVG(employment) as avg_employment_f, 
              AVG(capital) as avg_capital_f, 
              AVG(sales) as avg_sales_f 
            FROM 
              ratio 
            GROUP BY 
              -- fake, 
              firm,
              geocode4_corr
          ) 
        GROUP BY 
          -- fake
          geocode4_corr
      ) as pct_c ON ratio.geocode4_corr = pct_c.geocode4_corr
      LEFT JOIN (
        SELECT 
          -- fake, 
          indu_2, 
          approx_percentile(
            avg_asset_tangibility_f, ARRAY[.5, 
            .75,.90,.95]
          ) as pct_asset_tangibility_i, 
          AVG(avg_asset_tangibility_f) AS avg_asset_tangibility_i, 
          approx_percentile(
            avg_output_f, ARRAY[.5,.75,.90,.95]
          ) as pct_output_i, 
          AVG(avg_output_f) AS avg_output_i, 
          approx_percentile(
            avg_employment_f, ARRAY[.5,.75,.90, 
            .95]
          ) as pct_employment_i, 
          AVG(avg_employment_f) AS avg_employment_i, 
          approx_percentile(
            avg_capital_f, ARRAY[.5,.75,.90, 
            .95]
          ) as pct_capital_i, 
          AVG(avg_capital_f) AS avg_capital_i, 
          approx_percentile(
            avg_sales_f, ARRAY[.5,.75,.90,.95]
          ) as pct_sales_i, 
          AVG(avg_sales_f) AS avg_sales_i
        FROM 
          (
            SELECT 
              firm, 
              indu_2,
              -- fake, 
              AVG(tangible) as avg_asset_tangibility_f, 
              AVG(output) as avg_output_f, 
              AVG(employment) as avg_employment_f, 
              AVG(capital) as avg_capital_f, 
              AVG(sales) as avg_sales_f 
            FROM 
              ratio 
            GROUP BY 
              -- fake, 
              firm,
              indu_2
          ) 
        GROUP BY 
          -- fake
          indu_2
      ) as pct_i ON ratio.indu_2 = pct_i.indu_2 
      INNER JOIN (
        SELECT 
          firm, 
          -- fake, 
          AVG(tangible) as avg_asset_tangibility_f,
          AVG(output) as avg_output_f, 
          AVG(employment) as avg_employment_f, 
          AVG(capital) as avg_capital_f, 
          AVG(sales) as avg_sales_f
        FROM 
          ratio 
        GROUP BY 
          -- fake, 
          firm
      ) as firm_avg ON ratio.firm = firm_avg.firm
    INNER JOIN firms_survey.asif_tfp_firm_level on 
      ratio.firm = asif_tfp_firm_level.firm 
      AND ratio.year = asif_tfp_firm_level.year
      AND ratio.geocode4_corr = asif_tfp_firm_level.geocode4_corr
      AND ratio.ownership = asif_tfp_firm_level.ownership
    ORDER BY 
      year 
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

```python
query_test = """
WITH test AS (
SELECT firm, COUNT(avg_size_asset_fci) AS count
FROM (
SELECT firm, avg_size_asset_fci,COUNT(*) AS count
FROM "firms_survey"."asif_financial_ratio_baseline_firm" 
GROUP BY firm, avg_size_asset_fci
  )
  GROUP BY firm
  ORDER BY count DESC
  )
  SELECT count, COUNT(*) AS count_m
  FROM test
  GROUP BY count
"""
output = s3.run_query(
                    query=query_test,
                    database=DatabaseName,
                    s3_output=s3_output_example,
    filename = 'count_{}'.format(table_name)
                )
output
```

Number of observations per size

```python
for i in [.5, .75, .9, .95]:
    query_test = """
    SELECT 
    dominated, COUNT(*) as count
    FROM (
      SELECT 
      element_at(size_asset_fci, {}) as dominated
    FROM asif_financial_ratio_baseline_firm   
      )
    GROUP BY dominated
    ORDER BY dominated
    """.format(i)
    output = s3.run_query(
                        query=query_test,
                        database=DatabaseName,
                        s3_output=s3_output_example,
        filename = 'count_{}'.format(table_name)
                    )
    print("Decile {}".format(i))
    display(output)
```

Check if the data roughly matches with the following paper: [Internal finance and growth: Microeconometric evidence on Chinese firms](https://www.sciencedirect.com/science/article/pii/S0304387810000805)

![](https://drive.google.com/uc?export=view&id=1mE8QEMpxtDsPT200DKmcWKmwY2qqSBpp)

![](https://drive.google.com/uc?export=view&id=1RDpaBEcgtXxe2muW7BqVvhtoIGGg2Itv)


Without filtering data

```python
query_test = """
SELECT ownership,
COUNT(*) as nb_obs,
AVG(tfp_op) AS avg_tfp_op, 
AVG(tangible) AS avg_tangible, 
AVG(intangible) AS avg_intangible, 
AVG(current_asset) AS avg_current_asset, 
AVG(return_to_sale)AS avg_return_to_sale, 
AVG(liabilities_tot_asset) AS avg_liabilities_tot_asset, 
AVG(cashflow_tot_asset) AS avg_cashflow_tot_asset,
AVG(cashflow_to_tangible) AS avg_cashflow_to_tangible,
AVG(liquidity) AS avg_liquidity,
AVG(age) AS avg_age,
AVG(labor_capital) AS avg_labor_capital,
AVG(coverage_ratio) AS avg_coverage_ratio,
AVG(export_to_sale) AS avg_export_to_sale

FROM asif_tfp_credit_constraint  
GROUP BY ownership
"""
output = s3.run_query(
                    query=query_test,
                    database=DatabaseName,
                    s3_output=s3_output_example,
    filename = 'count_{}'.format(table_name)
                )
output.set_index('ownership').T
```

After filtering data

```python
query_test = """
SELECT ownership,
COUNT(*) as nb_obs,
AVG(tfp_op) AS avg_tfp_op, 
AVG(tangible) AS avg_tangible, 
AVG(intangible) AS avg_intangible, 
AVG(current_asset) AS avg_current_asset, 
AVG(return_to_sale)AS avg_return_to_sale, 
AVG(liabilities_tot_asset) AS avg_liabilities_tot_asset, 
AVG(cashflow_tot_asset) AS avg_cashflow_tot_asset,
AVG(cashflow_to_tangible) AS avg_cashflow_to_tangible,
AVG(liquidity) AS avg_liquidity,
AVG(age) AS avg_age,
AVG(labor_capital) AS avg_labor_capital,
AVG(coverage_ratio) AS avg_coverage_ratio,
AVG(export_to_sale) AS avg_export_to_sale
FROM asif_tfp_credit_constraint  
WHERE 
cashflow_to_tangible > 0
AND current_ratio > 0
AND liabilities_tot_asset > 0
GROUP BY ownership
"""
output = s3.run_query(
                    query=query_test,
                    database=DatabaseName,
                    s3_output=s3_output_example,
    filename = 'count_{}'.format(table_name)
                )
output.set_index('ownership').T
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
schema = [{'Name': 'firm', 'Type': 'string', 'Comment': 'firm ID'},
          {'Name': 'year', 'Type': 'string', 'Comment': 'year'},
          {'Name': 'period',
              'Type': 'varchar(5)', 'Comment': 'if year prior to 2006 then False else true. Indicate break from 10 and 11 FYP'},
          {'Name': 'cic', 'Type': 'string', 'Comment': '4 digits industry code'},
          {'Name': 'indu_2', 'Type': 'string',
           'Comment': 'Two digits industry. If length cic equals to 3, then add 0 to indu_2'},
          {'Name': 'short', 'Type': 'string',
              'Comment': 'Industry short description'},
          {'Name': 'geocode4_corr', 'Type': 'string', 'Comment': 'city code'},
          {'Name': 'tcz', 'Type': 'string', 'Comment': 'Two control zone policy'},
          {'Name': 'spz', 'Type': 'string', 'Comment': 'Special policy zone'},
          {'Name': 'ownership', 'Type': 'string', 'Comment': 'Firms ownership'},
          {'Name': 'soe_vs_pri',
              'Type': 'varchar(7)', 'Comment': 'SOE vs PRIVATE'},
          {'Name': 'for_vs_dom',
           'Type': 'varchar(8)', 'Comment': 'FOREIGN vs DOMESTICT if ownership is HTM then FOREIGN'},
          {'Name': 'tso2_mandate_c', 'Type': 'float',
           'Comment': 'city reduction mandate in tonnes'},
          {'Name': 'in_10_000_tonnes', 'Type': 'float',
           'Comment': 'city reduction mandate in 10k tonnes'},
          {'Name': 'output', 'Type': 'decimal(16,5)', 'Comment': 'Output'},
          {'Name': 'employment',
              'Type': 'decimal(16,5)', 'Comment': 'employment'},
          {'Name': 'capital', 'Type': 'decimal(16,5)', 'Comment': 'capital'},
          {'Name': 'current_asset', 'Type': 'int', 'Comment': 'current asset'},
          {'Name': 'net_non_current', 'Type': 'int', 'Comment': 'total net current asset'},
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
          {'Name': 'c91', 'Type': 'int', 'Comment': 'Intangible and Deferred'},
          {'Name': 'c92', 'Type': 'int', 'Comment': 'Intangible assets'},
          #{'Name': 'cash', 'Type': 'int', 'Comment': 'cash '},
          {'Name': 'cashflow', 'Type': 'int', 'Comment': 'cash flow'},
          {'Name': 'sales', 'Type': 'decimal(16,5)', 'Comment': 'sales'},
          {'Name': 'tfp_op', 'Type': 'double', 'Comment': 'TFP. Computed from https://github.com/thomaspernet/Financial_dependency_pollution/blob/master/01_data_preprocessing/02_transform_tables/05_tfp_computation.md'},
          {'Name': 'credit_constraint', 'Type': 'float',
           'Comment': 'Financial dependency. From paper https://www.sciencedirect.com/science/article/pii/S0147596715000311'},
          {'Name': 'supply_all_credit', 'Type': 'double', 'Comment': 'total credit over gdp province'},
          {'Name': 'supply_long_term_credit', 'Type': 'float', 'Comment': 'total long term credit over gdp province'},
          {'Name': 'current_ratio',
           'Type': 'decimal(21,5)', 'Comment': 'current ratio cuasset/流动负债合计 (c95)'},
          {'Name': 'lag_current_ratio', 'Type': 'decimal(21,5)', 'Comment': 'lag current ratio'},
          {'Name': 'quick_ratio',
           'Type': 'decimal(21,5)', 'Comment': 'quick ratio (cuasset-存货 (c81) ) / 流动负债合计 (c95)'},
          {'Name': 'liabilities_tot_asset',
           'Type': 'decimal(21,5)', 'Comment': 'liabilities to total asset'},
          {'Name': 'lag_liabilities_tot_asset', 'Type': 'decimal(21,5)', 'Comment': 'lag liabilities to asset'},
          {'Name': 'sales_tot_asset',
           'Type': 'decimal(21,5)', 'Comment': 'sales to total asset'},
          {'Name': 'lag_sales_tot_asset', 'Type': 'decimal(21,5)', 'Comment': 'lag sales to asset'},
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
          {'Name': 'cashflow_to_tangible',
           'Type': 'decimal(21,5)', 'Comment': 'cashflow to tangible asset'},
          {'Name': 'lag_cashflow_to_tangible', 'Type': 'decimal(21,5)', 'Comment': 'lag cashflow to tangible asset'},
          {'Name': 'return_to_sale', 'Type': 'decimal(21,5)', 'Comment': ''},
 {'Name': 'export_to_sale', 'Type': 'decimal(21,5)', 'Comment': 'overseas turnover / sales'},
 {'Name': 'labor_productivity', 'Type': 'decimal(21,5)', 'Comment': 'real sales/number of employees.'},
 {'Name': 'labor_capital', 'Type': 'decimal(21,5)', 'Comment': 'Labor / tangible asset'},
 {'Name': 'age', 'Type': 'decimal(17,5)', 'Comment': 'current year – firms year of establishment'},
          {'Name': 'coverage_ratio', 'Type': 'decimal(21,5)', 'Comment': 'net income(c131) /total interest payments'},
          {'Name': 'liquidity', 'Type': 'decimal(21,5)', 'Comment': 'current assets-current liabilities/total assets'},
          {'Name': 'avg_size_asset_f',
           'Type': 'varchar(5)', 'Comment': 'if firm s asset tangibility average is above average of firm s average then firm is large'},
          {'Name': 'avg_size_output_f',
              'Type': 'varchar(5)', 'Comment': 'if firm s ouptut average is above average of firm s average then firm is large'},
          {'Name': 'avg_employment_f',
              'Type': 'varchar(5)', 'Comment': 'if firm s employment average is above average of firm s average then firm is large'},
          {'Name': 'avg_size_capital_f',
           'Type': 'varchar(5)', 'Comment': 'if firm s capital average is above average of firm s average then firm is large'},
          {'Name': 'avg_sales_f',
           'Type': 'varchar(5)', 'Comment': 'if firm s sale is above average of firm s average then firm is large'},
          {'Name': 'size_asset_fci', 'Type': 'map<double,boolean>',
           'Comment': 'if firm s asset tangibility average is above average of firm city industry s decile then firm is large'},
          {'Name': 'size_asset_fc', 'Type': 'map<double,boolean>',
           'Comment': 'if firm s asset tangibility average is above average of firm s city decile then firm is large'},
          {'Name': 'size_asset_fi', 'Type': 'map<double,boolean>',
           'Comment': 'if firm s asset tangibility average is above average of firm s industry decile then firm is large'},
          {'Name': 'size_output_fci', 'Type': 'map<double,boolean>',
           'Comment': 'if firm s ouptut average is above average of firm s city industry decile then firm is large'},
          {'Name': 'size_output_fc', 'Type': 'map<double,boolean>',
           'Comment': 'if firm s ouptut average is above average of firm s city decile then firm is large'},
          {'Name': 'size_output_fi', 'Type': 'map<double,boolean>',
           'Comment': 'if firm s ouptut average is above average of firm s industry decile then firm is large'},
          {'Name': 'size_employment_fci', 'Type': 'map<double,boolean>',
           'Comment': 'if firm s employment average is above average of firm s city industry decile then firm is large'},
          {'Name': 'size_employment_fc', 'Type': 'map<double,boolean>',
           'Comment': 'if firm s employment average is above average of firm s city decile then firm is large'},
          {'Name': 'size_employment_fi', 'Type': 'map<double,boolean>',
           'Comment': 'if firm s employment average is above average of firm s industry decile then firm is large'},
          {'Name': 'size_capital_fci', 'Type': 'map<double,boolean>',
           'Comment': 'if firm s capital average is above average of firm s city industry decile then firm is large'},
          {'Name': 'size_capital_fc', 'Type': 'map<double,boolean>',
           'Comment': 'if firm s capital average is above average of firm s city decile then firm is large'},
          {'Name': 'size_capital_fi', 'Type': 'map<double,boolean>',
           'Comment': 'if firm s capital average is above average of firm s industry decile then firm is large'},
          {'Name': 'size_sales_fci', 'Type': 'map<double,boolean>',
           'Comment': 'if firm s sale is above average of firm s city industry decile then firm is large'},
          {'Name': 'size_sales_fc', 'Type': 'map<double,boolean>',
           'Comment': 'if firm s sale is above average of firm s city decile then firm is large'},
          {'Name': 'size_sales_fi', 'Type': 'map<double,boolean>',
           'Comment': 'if firm s sale is above average of firm s industry decile then firm is large'},
          {'Name': 'fe_c_i', 'Type': 'bigint',
              'Comment': 'City industry fixed effect'},
          {'Name': 'fe_t_i', 'Type': 'bigint',
              'Comment': 'year industry fixed effect'},
          {'Name': 'fe_c_t', 'Type': 'bigint', 'Comment': 'city industry fixed effect'}]
```

4. Provide a description

```python
description = """
Transform asif tfp firm level and others data by merging asif firms prepared, china credit constraint, ind cic 2 name, china city code normalised, china tcz spz, china city reduction mandate data by constructing asset_tangibility, cash_over_total_asset, sales over asset Andersen method, current ratio (Compute proxy for credit constraint , Construct main variable asset tangibility) to asif tfp credit constraint
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
name_json = 'parameters_ETL_Financial_dependency_pollution.json'
path_json = os.path.join(str(Path(path).parent.parent), 'utils',name_json)
```

```python
with open(path_json) as json_file:
    parameters = json.load(json_file)
```

```python
filename =  "09_asif_tfp_firm_baseline.ipynb"
index_final_table = [1]
if_final = 'True'
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
partition_keys = ["firm","year","cic","geocode4_corr"]

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
secondary_key = 'short'
y_var = 'tfp_op'
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
create_report(extension = "html", keep_code = True, notebookname =  '09_asif_tfp_firm_baseline.ipynb')
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
