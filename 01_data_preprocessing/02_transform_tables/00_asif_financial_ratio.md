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

Rescale output, fa_net, employment
construct the following ratio:
* If possible compute by:
  * industry level
  * city-industry level
  * city-industry-year level
1. Working capital: cuasset- 流动负债合计 (c95)
2. Working capital requirement: 存货 (c81) + 应收帐款 (c80) - 应付帐款 (c96)
3. Current Ratio: cuasset/流动负债合计 (c95)
4. Cash ratio: (( 其中：短期投资 (c79) + 应收帐款 (c80) + 存货 (c81)) - cuasset)/ 流动负债合计 (c95)
5. Debt to total asset:
  1. (流动负债合计 (c95) + 长期负债合计 (c97)) / 资产总计318 (c93)
  2. 负债合计 (c98)/ 资产总计318 (c93)
6. Return on Asset: 全年营业收入合计 (c64) - (主营业务成本 (c108) + 营业费用 (c113) + 管理费用 (c114) + 财产保险费 (c116) + 劳动、失业保险费 (c118)+ 财务费用 (c124) + 本年应付工资总额 (wage)) /资产总计318 (c93)
7. Asset turnover ratio: 全年营业收入合计 (c64) /(ΔΔ 资产总计318 (c93)/2)
8. R&D intensity: rdfee/全年营业收入合计 (c64)
9. Inventory to sales: 存货 (c81) / 全年营业收入合计 (c64)
10. Asset Tangibility: 固定资产合计 (c85) - 无形资产 (c91)
11. Account payable to total asset: (Δ 应付帐款 (c96))/ (Δ资产总计318 (c93))

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

import plotly.express as px
import seaborn as sns
import matplotlib.pyplot as plt

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

Detail computation:

| index | Metrics                        | comments                                                                                                    | variables                                                                                                                                                           | Roam_link                                       | Exepected sign              | Comment                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
|-------|--------------------------------|-------------------------------------------------------------------------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------|-------------------------------------------------|-----------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| 1     | External finance dependence    | From #[[Fan et al. 2015 - Credit constraints, quality, and export prices - Theory and evidence from China]] |                                                                                                                                                                     | #external-finance-dependence                    | Negative                    | An industry’s external finance dependence (ExtFini) is defined as the share of capital expenditure not financed with cash flows from operations. If external finance dependence is high, the industry is more financially vulnerable and have higher credit needs                                                                                                                                                                                                                                                                              |
| 2     | R&D intensity                  | RD / Sales                                                                                                  | rdfee/sales                                                                                                                                                         | #rd-intensity                                   | Negative                    | Share of RD expenditure over sales. larger values indicates larger use of sales to spend on RD. Say differently, lower borrowing done toward RD                                                                                                                                                                                                                                                                                                                                                                                                |
| 3     | Inventory to sales             | Inventory / sales                                                                                           | 存货 (c81) / sales                                                                                                                                                  | #inventory-to-sales                             | Negative                    | Share of inventory over sales. Larger values indicates share of unsold or not consumed items. large values is a demonstration of tighter credit constraint                                                                                                                                                                                                                                                                                                                                                                                     |
| 4     | % receivable                   | receivable account / current asset                                                                          | 应收帐款 (c80) / cuasset                                                                                                                                            | #account-receivable #current-asset              | Negative                    | Share of receivable over current asset. Larger value indicates longer time before collecting the money from the customers                                                                                                                                                                                                                                                                                                                                                                                                                      |
| 5     | Liabilities over asset         | (Short-Tern Debt + Long-Term Debt)/total asset                                                              | (流动负债合计 (c95) + 长期负债合计 (c97)) / toasset                                                                                                                 | #total-debt-to-total-assets                     | Negative                    | Share of liabilities over total asset. Larger value indicates assets that are financed by external creditors                                                                                                                                                                                                                                                                                                                                                                                                                                   |
| 6     | working capital requirement    | Inventory + Accounts receivable - Accounts payable                                                          | 存货 (c81) + 应收帐款 (c80) - 应付帐款  (c96)                                                                                                                       | #working-capital                                | Negative                    | Working Capital Requirement is the amount of money needed to finance the gap between disbursements (payments to suppliers) and receipts (payments from customers). Larger values indicate the amount of money needed to meet the debt.                                                                                                                                                                                                                                                                                                         |
| 7     | % cash                         | Current asset - cash / current asset                                                                        | (cuasset- 其中：短期投资 (c79) - 应收帐款 (c80) - 存货 (c81) - 其中：产成品 (c82)) /current asset                                                                   | #current-asset #cash                            | Positive                    | Share of cash asset over current asset. Larger values indicate more cash in hand.                                                                                                                                                                                                                                                                                                                                                                                                                                                              |
| 8     | cash ratio                     | (Cash + marketable securities)/current liabilities                                                          | 1-(cuasset - 其中：短期投资 (c79) - 应收帐款 (c80) - 存货 (c81) - 其中：产成品 (c82))/ 流动负债合计 (c95)                                                           | #cash-asset #cash-ratio                         | Positive                    | Cash divided by liabilities. A portion of short-term debt that can be financed by cash. A larger value indicates the company generates enough cash to cope with the short term debt                                                                                                                                                                                                                                                                                                                                                            |
| 9     | Working capital                | Current asset - current liabilities                                                                         | cuasset- 流动负债合计 (c95)                                                                                                                                         | #working-capital-requirement                    | Positive                    | Difference between current asset and current liabilities. Larger value indicates that assets are enough to cope with the short term need                                                                                                                                                                                                                                                                                                                                                                                                       |
| 10    | current ratio                  | Current asset /current liabilities                                                                          | cuasset/流动负债合计 (c95)                                                                                                                                          | #current-ratio                                  | Ambiguous                   | Asset divided by liabilities. Values above 1 indicate there are more assets than liabilities. There are two effects on the liquidity constraint. Larger values imply the company has more liquidity, hence they may be less dependent on the formal financial market. By analogy, the financial market prefers to invest or provide money to the more liquid company (reduce the risk default)                                                                                                                                                 |
| 11    | Quick ratio                    | (Current asset - Inventory)/current liabilities                                                             | (cuasset -  其中：短期投资 (c79) - 应收帐款 (c80) - 存货 (c81)) / 流动负债合计 (c95)                                                                                | #quick-ratio                                    | Ambiguous                   | The quick ratio is a measure of liquidity. The higher the more liquid the company is. To improve the ratio, the company should reduce the account receivable (reduce payment time) and increase the account payable (negotiate payment term). There are two effects on the liquidity constraint. Larger values imply the company has more liquidity, hence they may be less dependent on the formal financial market. By analogy, the financial market prefers to invest or provide money to the more liquid company (reduce the risk default) |
| 12    | Return on Asset                | Net income / Total assets                                                                                   | sales - (主营业务成本 (c108) + 营业费用 (c113) + 管理费用 (c114) + 财产保险费 (c116) + 劳动、失业保险费 (c118)+ 财务费用 (c124) + 本年应付工资总额 (wage)) /toasset | #return-on-asset                                | Ambiguous                   | Net income over total asset. Capacity of an asset to generate income. Larger value indicates that asset are used in an efficiente way to generate income                                                                                                                                                                                                                                                                                                                                                                                       |
| 13    | Asset Turnover Ratio           | Total sales / ((delta total asset)/2)                                                                       | 全年营业收入合计 (c64) /($\Delta$ toasset/2)                                                                                                                        | #asset-turnover-ratio                           | Ambiguous                   | Sales divided by the average changes in total asset. Larger value indicates better efficiency at using asset to generate revenue                                                                                                                                                                                                                                                                                                                                                                                                               |
| 14    | Asset tangibility              | Total fixed assets - Intangible assets                                                                      | tofixed - 无形资产 (c92)                                                                                                                                            | #asset-tangibility                              | Ambiguous                   | Difference between fixed sset and intangible asset. Larger value indicates more collateral, hence higher borrowing capacity                                                                                                                                                                                                                                                                                                                                                                                                                    |
| 15    | Account payable to total asset | (delta account payable)/ (delta total asset)                                                                | ($\Delta$ 应付帐款  (c96))/ ($\Delta$$ (toasset))                                                                                                                   | #change-account-paybable-to-change-total-assets | Ambiguous (favour positive) | Variation of account payable over variation total asset. If the nominator larger than the denominator, it means the account payable grew larger than an asset, or more time is given to pay back the supplier relative to the total asset.  Say differently,  companies can more easily access buyer or supplier trade credit, they may be less dependent on the formal financial market                                                                                                                                                       |
    
**pct missing**

![](https://drive.google.com/uc?export=view&id=1LPNhZIPkJgx0-ZsM6NLNAB6dGH9h7ELo)


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
  SELECT CNT, COUNT(*) AS CNT_dup
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

Asif felt to meet the basic accounting equation that the left part should be equal to the right part

```python
query = """
WITH test AS (
SELECT *, CASE 
WHEN LENGTH(cic) = 4 THEN substr(cic,1, 2)
ELSE substr(cic,1, 1) END AS indu_2,
c98 + c99 as total_asset
FROM firms_survey.asif_firms_prepared 
  WHERE year in (-- '2004', '2005', 
    '2006') 
)
SELECT 
  year, 
  indu_2, 
  
  SUM(c98) as total_liabilities,
  SUM(c99) as equity,
  SUM(toasset) as left_part,
  SUM(c98) + SUM(c99) as right_part,
  SUM(toasset) - (SUM(c98) + SUM(c99)) as diff,
  
  CASE 
  WHEN (SUM(c98) + SUM(c99)) - SUM(toasset) < 0 THEN 
  CAST( (SUM(c98) + SUM(c99) - SUM(toasset)) AS DECIMAL(16, 5)) / SUM(toasset)
  WHEN (SUM(c98) + SUM(c99)) - SUM(toasset) > 0 THEN  
  CAST( (SUM(c98) + SUM(c99) - SUM(toasset)) AS DECIMAL(16, 5)) / (SUM(c98) + SUM(c99))
  END AS pct_missing,
  
  CASE 
  WHEN SUM(toasset) - (SUM(c98) + SUM(c99)) < 0 THEN 
  SUM(toasset) + ABS(SUM(toasset) - (SUM(c98) + SUM(c99))) - (SUM(c98) + SUM(c99))
  
  WHEN SUM(toasset) - (SUM(c98) + SUM(c99)) > 0 THEN 
  SUM(toasset) - (SUM(c98) + SUM(c99) + SUM(toasset) - (SUM(c98) + SUM(c99)) )
  
  ELSE (SUM(c98) + SUM(c99)) - SUM(toasset) END AS diff_adjusted
  
  
  FROM test
  GROUP BY year, indu_2
  ORDER BY year DESC, indu_2
"""

output = s3.run_query(
                    query=query,
                    database=DatabaseName,
                    s3_output=s3_output_example,
    filename = 'example_3'
                )
(
    output
    #.style
    #.format('{0:,.2f}')
)
```

3. Computation ratio by industry

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
pd.set_option('display.max_rows', None)
```

```python
query = """
WITH test AS (
SELECT *, CASE 
WHEN LENGTH(cic) = 4 THEN substr(cic,1, 2)
ELSE substr(cic,1, 1) END AS indu_2,
c98 + c99 as total_asset
FROM firms_survey.asif_firms_prepared 
)
SELECT * 
FROM (
WITH ratio AS (
SELECT 
  year, 
  indu_2, 
  SUM(cuasset)  as curr_asset,
  CASE 
  WHEN SUM(toasset) - (SUM(c98) + SUM(c99)) < 0 THEN
  SUM(toasset) + ABS(SUM(toasset) - (SUM(c98) + SUM(c99)))
  ELSE SUM(toasset) END AS left_part,
  
  SUM(c95) as curr_liabilities,
  SUM(c98) as tot_liabilites,
  SUM(c99) as tot_equity,
  
  CASE 
  WHEN SUM(toasset) - (SUM(c98) + SUM(c99)) > 0  THEN
  (SUM(c98) + SUM(c99) + SUM(toasset) - (SUM(c98) + SUM(c99)))
  ELSE (SUM(c98) + SUM(c99)) END AS right_part,
  
  SUM(toasset) - (SUM(c98) + SUM(c99)) as diff,  
  
  SUM(cuasset) - SUM(c79) - SUM(c80) - SUM(c81) - SUM(c82) as cash,
   CAST(
    SUM(c80) AS DECIMAL(16, 5)
  ) / NULLIF(
    CAST(
      SUM(cuasset) AS DECIMAL(16, 5)
    ), 
    0
  ) as pct_receivable_curasset,
  CAST(
    SUM(cuasset) - SUM(c79) - SUM(c80) - SUM(c81) - SUM(c82) AS DECIMAL(16, 5)
  ) / NULLIF(
    CAST(
      SUM(cuasset) AS DECIMAL(16, 5)
    ), 
    0
  ) as pct_cash_over_curasset,
  
  SUM(cuasset) - SUM(c95) as working_capital_it,
  SUM(c81) + SUM(c80) - SUM(c96) AS working_capital_requirement_it,   
  CAST(
    SUM(cuasset) AS DECIMAL(16, 5)
  ) / NULLIF(
    CAST(
      SUM(c95) AS DECIMAL(16, 5)
    ), 
    0
  ) AS current_ratio_it, 
  
  CAST(
    SUM(cuasset) - SUM(c79) - SUM(c80) - SUM(c81) AS DECIMAL(16, 5)
  ) / NULLIF(
    CAST(
      SUM(c95) AS DECIMAL(16, 5)
    ), 
    0
  ) AS quick_ratio_it, 

  1 - CAST(
    SUM(cuasset) - SUM(c79) - SUM(c80) - SUM(c81) - SUM(c82) AS DECIMAL(16, 5)
  ) / NULLIF(CAST(
    SUM(c95) AS DECIMAL(16, 5)
  ), 
    0
  ) AS cash_ratio_it,
  
  -- Need to add asset or debt when bs requirement not meet
  CASE 
  WHEN SUM(toasset) - (SUM(c98) + SUM(c99)) < 0 THEN
  CAST(
     SUM(c95) + SUM(c97) AS DECIMAL(16, 5)
  )/ NULLIF(
    CAST(
     SUM(toasset) + ABS(SUM(toasset) - (SUM(c98) + SUM(c99)))  AS DECIMAL(16, 5)
    ), 
    0
  ) 
  WHEN SUM(toasset) - (SUM(c98) + SUM(c99)) > 0 THEN
  CAST(
     SUM(c95) + SUM(c97) + SUM(toasset) - (SUM(c98) + SUM(c99)) AS DECIMAL(16, 5)
  )/ NULLIF(
    CAST(
     SUM(toasset)  AS DECIMAL(16, 5)
    ), 
    0
  )
  ELSE
  CAST(
     SUM(c95) + SUM(c97) AS DECIMAL(16, 5)
  )/ NULLIF(
    CAST(
     SUM(toasset)  AS DECIMAL(16, 5)
    ), 
    0
  )
  END AS liabilities_assets_it, 

  CASE 
  WHEN SUM(toasset) - (SUM(c98) + SUM(c99)) < 0 THEN
  CAST(SUM(sales) - (SUM(c108) + SUM(c113) + SUM(c114) + SUM(c116) + SUM(c118) + SUM(c124) + SUM(wage)) AS DECIMAL(16, 5))/ 
  NULLIF(CAST(SUM(toasset) + ABS(SUM(toasset) - (SUM(c98) + SUM(c99)))  AS DECIMAL(16, 5)), 0) 
  ELSE
  CAST(SUM(sales) - (SUM(c108) + SUM(c113) + SUM(c114) + SUM(c116) + SUM(c118) + SUM(c124) + SUM(wage)) AS DECIMAL(16, 5))/ 
  NULLIF(CAST(SUM(toasset)  AS DECIMAL(16, 5)), 0) 
  END AS return_on_asset_it,
  
  CASE 
  WHEN SUM(toasset) - (SUM(c98) + SUM(c99)) < 0 THEN  
  CAST(
    SUM(sales) AS DECIMAL(16, 5)
  )/ NULLIF(
    CAST(
      (
          SUM(toasset) + ABS(SUM(toasset) - (SUM(c98) + SUM(c99))) - lag(SUM(toasset) + ABS(SUM(toasset) - (SUM(c98) + SUM(c99))),
          1
        ) over(
          partition by indu_2 
          order by 
            indu_2, 
            year
        )
      )/ 2 AS DECIMAL(16, 5)
    ), 
    0
  )
  ELSE 
  CAST(
    SUM(sales) AS DECIMAL(16, 5)
  )/ NULLIF(
    CAST(
      (
        SUM(toasset) - lag(
          SUM(toasset), 
          1
        ) over(
          partition by indu_2 
          order by 
            indu_2, 
            year
        )
      )/ 2 AS DECIMAL(16, 5)
    ), 
    0
  )
  
  END AS sales_assets_it,
  
  CAST(SUM(rdfee) AS DECIMAL(16, 5))/ NULLIF(CAST(SUM(sales) AS DECIMAL(16, 5)),0) as rd_intensity_it,
  CAST(SUM(c81)  AS DECIMAL(16, 5))/ NULLIF(CAST(SUM(sales) AS DECIMAL(16, 5)),0) as inventory_to_sales_it,
  SUM(tofixed) - SUM(c92) AS asset_tangibility_it,
  
  CAST(
      (
        SUM(c96) - lag(
          SUM(c96), 
          1
        ) over(
          partition by indu_2 
          order by 
            indu_2, 
            year
        )
      ) AS DECIMAL(16, 5)
      )/
      NULLIF(
    CAST(
      (
        SUM(total_asset) - lag(
          SUM(total_asset), 
          1
        ) over(
          partition by indu_2 
          order by 
            indu_2, 
            year
        )
      )/ 2 AS DECIMAL(16, 5)
    ),0) as account_paybable_to_asset_it
    
  
FROM test
WHERE year in ( '2001', '2002', '2003', '2004', '2005', 
'2006') 
GROUP BY 
  indu_2, 
  year 
  )
  SELECT
  year,
  ratio.indu_2,
  short,
  curr_asset,
  curr_liabilities,
  tot_liabilites,
  tot_equity,
  left_part,
  right_part,
  diff,
  cash,
  pct_receivable_curasset,
  1 - pct_cash_over_curasset as pct_non_cash_over_curasset,
  working_capital_it AS working_capital_i,
  working_capital_requirement_it AS working_capital_requirement_i,
  current_ratio_it AS current_ratio_i,
  quick_ratio_it as quick_ratio_i,
  cash_ratio_it AS cash_ratio_i,
  liabilities_assets_it AS liabilities_assets_i,
  return_on_asset_it AS return_on_asset_i,
  sales_assets_it AS sales_assets_i,
  rd_intensity_it AS rd_intensity_i,
  inventory_to_sales_it AS inventory_to_sales_i,
  asset_tangibility_it AS asset_tangibility_i,
  account_paybable_to_asset_it AS account_paybable_to_asset_i
  FROM ratio
  INNER JOIN (
  SELECT cic as indu_2, short
  FROM chinese_lookup.ind_cic_2_name
  ) as ind
  ON ratio.indu_2 = ind.indu_2
  ORDER BY year, working_capital_i
  )
"""
output = s3.run_query(
                    query=query,
                    database=DatabaseName,
                    s3_output=s3_output_example,
    filename = 'example_3'
                )
(
    output
    #.style
    #.format('{0:,.2f}')
)
```

The table below shows the rank of variables group by three group:

1. Current asset
    - pct_receivable_curasset
    - pct_non_cash_over_curasset
2. Current Liabilities
    - working_capital_i
    - current_ratio_i
    - quick_ratio_i
    - cash_ratio_i
3. Total asset
    - liabilities_assets_i
    - return_on_asset_i
    - sales_assets_i
    - asset_tangibility_i
    - account_paybable_to_asset_i

```python
(
    output
    .loc[lambda x: x['year'].isin(['2005'])]
    .reindex(columns = 
    ['indu_2',
     'short',
     'curr_asset',
     'curr_liabilities',
     'pct_receivable_curasset',
     'pct_non_cash_over_curasset',
            'working_capital_i',
            'current_ratio_i',
     'quick_ratio_i',
            'cash_ratio_i',
            'liabilities_assets_i',
            'return_on_asset_i',
            'sales_assets_i',
     'asset_tangibility_i',
     'account_paybable_to_asset_i'
    ]
     )
    .sort_values(by = ['working_capital_i'])
    .set_index(['indu_2', 'short'])
    .assign(
        rank_rc = lambda x: x['pct_receivable_curasset'].rank().astype('int64'),
        rank_pct_non_cash = lambda x: x['pct_non_cash_over_curasset'].rank().astype('int64'),
        
        rank_w = lambda x: x['working_capital_i'].rank().astype('int64'),
        rank_c = lambda x: x['current_ratio_i'].rank().astype('int64'),
        rank_q = lambda x: x['quick_ratio_i'].rank().astype('int64'),
        rank_cash = lambda x: x['cash_ratio_i'].rank().astype('int64'),
        
        rank_li = lambda x: x['liabilities_assets_i'].rank().astype('int64'),
        rank_re = lambda x: x['return_on_asset_i'].rank().astype('int64'),
        rank_sa = lambda x: x['sales_assets_i'].rank().astype('int64'),
        rank_at = lambda x: x['asset_tangibility_i'].rank().astype('int64'),
        rank_ap = lambda x: x['account_paybable_to_asset_i'].rank().astype('int64'),
    )
    .style
    .background_gradient(cmap=sns.light_palette("green", as_cmap=True), subset = ["rank_rc",'rank_pct_non_cash'])
    .background_gradient(cmap=sns.light_palette("blue", as_cmap=True), subset = ["rank_w",'rank_c','rank_q', 'rank_cash'])
    .background_gradient(cmap=sns.light_palette("orange", as_cmap=True), subset = ["rank_li",'rank_re', 'rank_sa', 'rank_at', 'rank_ap'])
)
```

```python
(
    output
    .loc[lambda x: x['year'].isin(['2006'])]
    .reindex(columns = 
    ['indu_2',
     'short',
     'curr_asset',
     'curr_liabilities',
     'pct_receivable_curasset',
     'pct_non_cash_over_curasset',
            'working_capital_i',
            'current_ratio_i',
     'quick_ratio_i',
            'cash_ratio_i',
            'liabilities_assets_i',
            'return_on_asset_i',
            'sales_assets_i',
     'asset_tangibility_i',
     'account_paybable_to_asset_i'
    ]
     )
    .sort_values(by = ['working_capital_i'])
    .set_index(['indu_2', 'short'])
    .assign(
        rank_rc = lambda x: x['pct_receivable_curasset'].rank().astype('int64'),
        rank_pct_non_cash = lambda x: x['pct_non_cash_over_curasset'].rank().astype('int64'),
        
        rank_w = lambda x: x['working_capital_i'].rank().astype('int64'),
        rank_c = lambda x: x['current_ratio_i'].rank().astype('int64'),
        rank_q = lambda x: x['quick_ratio_i'].rank().astype('int64'),
        rank_cash = lambda x: x['cash_ratio_i'].rank().astype('int64'),
        
        rank_li = lambda x: x['liabilities_assets_i'].rank().astype('int64'),
        rank_re = lambda x: x['return_on_asset_i'].rank().astype('int64'),
        rank_sa = lambda x: x['sales_assets_i'].rank().astype('int64'),
        rank_at = lambda x: x['asset_tangibility_i'].rank().astype('int64'),
        rank_ap = lambda x: x['account_paybable_to_asset_i'].rank().astype('int64'),
    )
    .style
    .background_gradient(cmap=sns.light_palette("green", as_cmap=True), subset = ["rank_rc",'rank_pct_non_cash'])
    .background_gradient(cmap=sns.light_palette("blue", as_cmap=True), subset = ["rank_w",'rank_c','rank_q', 'rank_cash'])
    .background_gradient(cmap=sns.light_palette("orange", as_cmap=True), subset = ["rank_li",'rank_re', 'rank_sa', 'rank_at', 'rank_ap'])
)
```

The table shows that years prior to 2004 have large missing values, as emphasided by Fan et al. So we will filter the year after 2004 included. 

```python
query = """
WITH test AS (
  SELECT 
    *, 
    CASE WHEN LENGTH(cic) = 4 THEN substr(cic,1, 2) ELSE concat('0',substr(cic,1, 1)) END AS indu_2, 
    c98 + c99 as total_asset 
  FROM 
    firms_survey.asif_firms_prepared
) 
SELECT 
  * 
FROM 
  (
    WITH ratio AS (
      SELECT 
        year, 
        indu_2, 
        CAST(
    SUM(c80) AS DECIMAL(16, 5)
  ) / NULLIF(
    CAST(
      SUM(cuasset) AS DECIMAL(16, 5)
    ), 
    0
  ) as receivable_curasset_it,
  
  CAST(
    SUM(cuasset) - SUM(c79) - SUM(c80) - SUM(c81) - SUM(c82) AS DECIMAL(16, 5)
  ) / NULLIF(
    CAST(
      SUM(cuasset) AS DECIMAL(16, 5)
    ), 
    0
  ) as cash_over_curasset_it,
  
  SUM(cuasset) - SUM(c95) as working_capital_it,
  SUM(c81) + SUM(c80) - SUM(c96) AS working_capital_requirement_it,   
  CAST(
    SUM(cuasset) AS DECIMAL(16, 5)
  ) / NULLIF(
    CAST(
      SUM(c95) AS DECIMAL(16, 5)
    ), 
    0
  ) AS current_ratio_it, 
  
  CAST(
    SUM(cuasset) - SUM(c79) - SUM(c80) - SUM(c81) AS DECIMAL(16, 5)
  ) / NULLIF(
    CAST(
      SUM(c95) AS DECIMAL(16, 5)
    ), 
    0
  ) AS quick_ratio_it, 

  1- CAST(
    SUM(cuasset) - SUM(c79) - SUM(c80) - SUM(c81) - SUM(c82) AS DECIMAL(16, 5)
  ) / NULLIF(CAST(
    SUM(c95) AS DECIMAL(16, 5)
  ), 
    0
  ) AS cash_ratio_it,
  
  -- Need to add asset or debt when bs requirement not meet
  CASE 
  WHEN SUM(toasset) - (SUM(c98) + SUM(c99)) < 0 THEN
  CAST(
     SUM(c95) + SUM(c97) AS DECIMAL(16, 5)
  )/ NULLIF(
    CAST(
     SUM(toasset) + ABS(SUM(toasset) - (SUM(c98) + SUM(c99)))  AS DECIMAL(16, 5)
    ), 
    0
  ) 
  WHEN SUM(toasset) - (SUM(c98) + SUM(c99)) > 0 THEN
  CAST(
     SUM(c95) + SUM(c97) + SUM(toasset) - (SUM(c98) + SUM(c99)) AS DECIMAL(16, 5)
  )/ NULLIF(
    CAST(
     SUM(toasset)  AS DECIMAL(16, 5)
    ), 
    0
  )
  ELSE
  CAST(
     SUM(c95) + SUM(c97) AS DECIMAL(16, 5)
  )/ NULLIF(
    CAST(
     SUM(toasset)  AS DECIMAL(16, 5)
    ), 
    0
  )
  END AS liabilities_assets_it, 

  CASE 
  WHEN SUM(toasset) - (SUM(c98) + SUM(c99)) < 0 THEN
  CAST(SUM(sales) - (SUM(c108) + SUM(c113) + SUM(c114) + SUM(c116) + SUM(c118) + SUM(c124) + SUM(wage)) AS DECIMAL(16, 5))/ 
  NULLIF(CAST(SUM(toasset) + ABS(SUM(toasset) - (SUM(c98) + SUM(c99)))  AS DECIMAL(16, 5)), 0) 
  ELSE
  CAST(SUM(sales) - (SUM(c108) + SUM(c113) + SUM(c114) + SUM(c116) + SUM(c118) + SUM(c124) + SUM(wage)) AS DECIMAL(16, 5))/ 
  NULLIF(CAST(SUM(toasset)  AS DECIMAL(16, 5)), 0) 
  END AS return_on_asset_it,
  
  CASE 
  WHEN SUM(toasset) - (SUM(c98) + SUM(c99)) < 0 THEN  
  CAST(
    SUM(sales) AS DECIMAL(16, 5)
  )/ NULLIF(
    CAST(
      (
          SUM(toasset) + ABS(SUM(toasset) - (SUM(c98) + SUM(c99))) - lag(SUM(toasset) + ABS(SUM(toasset) - (SUM(c98) + SUM(c99))),
          1
        ) over(
          partition by indu_2 
          order by 
            indu_2, 
            year
        )
      )/ 2 AS DECIMAL(16, 5)
    ), 
    0
  )
  ELSE 
  CAST(
    SUM(sales) AS DECIMAL(16, 5)
  )/ NULLIF(
    CAST(
      (
        SUM(toasset) - lag(
          SUM(toasset), 
          1
        ) over(
          partition by indu_2 
          order by 
            indu_2, 
            year
        )
      )/ 2 AS DECIMAL(16, 5)
    ), 
    0
  )
  
  END AS sales_assets_it,
  
  CAST(SUM(rdfee) AS DECIMAL(16, 5))/ NULLIF(CAST(SUM(sales) AS DECIMAL(16, 5)),0) as rd_intensity_it,
  CAST(SUM(c81)  AS DECIMAL(16, 5))/ NULLIF(CAST(SUM(sales) AS DECIMAL(16, 5)),0) as inventory_to_sales_it,
  SUM(tofixed) - SUM(c92) AS asset_tangibility_it,
  
  CAST(
      (
        SUM(c96) - lag(
          SUM(c96), 
          1
        ) over(
          partition by indu_2 
          order by 
            indu_2, 
            year
        )
      ) AS DECIMAL(16, 5)
      )/
      NULLIF(
    CAST(
      (
        SUM(total_asset) - lag(
          SUM(total_asset), 
          1
        ) over(
          partition by indu_2 
          order by 
            indu_2, 
            year
        )
      )/ 2 AS DECIMAL(16, 5)
    ),0) as account_paybable_to_asset_it
    
FROM test
      WHERE 
        year in ('2004', '2005', '2006') 
      GROUP BY 
        indu_2, 
        year
    ) 
    SELECT 
      * 
    FROM 
      (
        WITH agg AS (
          SELECT 
            indu_2, 
            'FAKE_GROUP' as fake, 
            AVG(receivable_curasset_it) AS receivable_curasset_i, 
            AVG(cash_over_curasset_it) AS cash_over_curasset_i, 
            
            AVG(working_capital_it)/1000000 AS working_capital_i, 
            AVG(working_capital_requirement_it)/1000000 AS working_capital_requirement_i, 
            AVG(current_ratio_it) AS current_ratio_i, 
            AVG(quick_ratio_it) AS quick_ratio_i,
            AVG(cash_ratio_it) AS cash_ratio_i, 
            
            AVG(liabilities_assets_it) AS liabilities_assets_i, 
            AVG(return_on_asset_it) AS return_on_asset_i, 
            AVG(sales_assets_it) AS sales_assets_i, 
            AVG(account_paybable_to_asset_it) AS account_paybable_to_asset_i,
            AVG(asset_tangibility_it)/1000000 AS asset_tangibility_i, 
            
            AVG(rd_intensity_it) AS rd_intensity_i, 
            AVG(inventory_to_sales_it) AS inventory_to_sales_i

          FROM 
            ratio 
          GROUP BY 
            indu_2 
        ) 
        SELECT 
          field0 AS indu_2, 
          val_1[ 'receivable_curasset' ] AS receivable_curasset_i, 
          val_2[ 'receivable_curasset' ] AS std_receivable_curasset_i, 
          val_1[ 'cash_over_curasset' ] AS cash_over_curasset_i, 
          val_2[ 'cash_over_curasset' ] AS std_cash_over_curasset_i, 
          
          val_1[ 'workink_capital' ] AS working_capital_i, 
          val_2[ 'workink_capital' ] AS std_working_capital_i, 
          val_1[ 'working_capital_requirement' ] AS working_capital_requirement_i, 
          val_2[ 'working_capital_requirement' ] AS std_working_capital_requirement_i, 
          val_1[ 'current_ratio' ] AS current_ratio_i, 
          val_2[ 'current_ratio' ] AS std_current_ratio_i, 
          val_1[ 'quick_ratio' ] AS quick_ratio_i, 
          val_2[ 'quick_ratio' ] AS quick_ratio_i,
          val_1[ 'cash_ratio' ] AS cash_ratio_i, 
          val_2[ 'cash_ratio' ] AS std_cash_ratio_i, 
          
          val_1[ 'liabilities_assets' ] AS liabilities_assets_i, 
          val_2[ 'liabilities_assets' ] AS std_liabilities_assets_i, 
          val_1[ 'return_on_asset' ] AS return_on_asset_i, 
          val_2[ 'return_on_asset' ] AS std_return_on_asset_i, 
          val_1[ 'sales_assets' ] AS sales_assets_i, 
          val_2[ 'sales_assets' ] AS std_sales_assets_i, 
          val_1[ 'account_paybable_to_asset' ] AS account_paybable_to_asset_i, 
          val_2[ 'account_paybable_to_asset' ] AS std_account_paybable_to_asset_i,
          val_1[ 'asset_tangibility' ] AS asset_tangibility_i, 
          val_2[ 'asset_tangibility' ] AS std_asset_tangibility_i, 
          
          val_1[ 'rd_intensity' ] AS rd_intensity_i, 
          val_2[ 'rd_intensity' ] AS std_rd_intensity_i, 
          val_1[ 'inventory_to_sales' ] AS inventory_to_sales_i, 
          val_2[ 'inventory_to_sales' ] AS std_inventory_to_sales_i
         
        FROM 
          (
            SELECT 
              field0, 
              map_agg(w, field1) AS val_1, 
              map_agg(w, field2) AS val_2 
            FROM 
              (
                SELECT 
                  w, 
                  names.field0, 
                  names.field1, 
                  names.field2 
                FROM 
                  (
                    SELECT 
                      w, 
                      zip(
                        array_indu_2, 
                        array_w, 
                        transform(
                          array_w, 
                          x -> (x - avg)/ std_w
                        )
                      ) as zip_values 
                    FROM 
                      (
                        SELECT 
                          w, 
                          avg, 
                          array_w, 
                          array_indu_2, 
                          std_w 
                        FROM 
                        (
                            SELECT 
                              'receivable_curasset' as w, 
                              AVG(receivable_curasset_i) as avg, 
                              ARRAY_AGG(receivable_curasset_i) as array_w, 
                              ARRAY_AGG(indu_2) as array_indu_2, 
                              stddev(receivable_curasset_i) as std_w 
                            FROM 
                              agg 
                            GROUP BY 
                              fake
                          ) 
                        UNION 
                        (
                            SELECT 
                              'cash_over_curasset' as w, 
                              AVG(cash_over_curasset_i) as avg, 
                              ARRAY_AGG(cash_over_curasset_i) as array_w, 
                              ARRAY_AGG(indu_2) as array_indu_2, 
                              stddev(cash_over_curasset_i) as std_w 
                            FROM 
                              agg 
                            GROUP BY 
                              fake
                          ) 
                        UNION 
                          (
                            SELECT 
                              'workink_capital' as w, 
                              AVG(working_capital_i) as avg, 
                              ARRAY_AGG(working_capital_i) as array_w, 
                              ARRAY_AGG(indu_2) as array_indu_2, 
                              stddev(working_capital_i) as std_w 
                            FROM 
                              agg 
                            GROUP BY 
                              fake
                          ) 
                        UNION 
                          (
                            SELECT 
                              'working_capital_requirement' as w, 
                              AVG(working_capital_requirement_i) as avg, 
                              ARRAY_AGG(working_capital_requirement_i) as array_w, 
                              ARRAY_AGG(indu_2) as array_indu_2, 
                              stddev(working_capital_requirement_i) as std_w 
                            FROM 
                              agg 
                            GROUP BY 
                              fake
                          ) 
                        UNION 
                          (
                            SELECT 
                              'current_ratio' as w, 
                              AVG(current_ratio_i) as avg, 
                              ARRAY_AGG(current_ratio_i) as array_w, 
                              ARRAY_AGG(indu_2) as array_indu_2, 
                              stddev(current_ratio_i) as std_w 
                            FROM 
                              agg 
                            GROUP BY 
                              fake
                          ) 
                        UNION 
                          (
                            SELECT 
                              'quick_ratio' as w, 
                              AVG(quick_ratio_i) as avg, 
                              ARRAY_AGG(quick_ratio_i) as array_w, 
                              ARRAY_AGG(indu_2) as array_indu_2, 
                              stddev(quick_ratio_i) as std_w 
                            FROM 
                              agg 
                            GROUP BY 
                              fake
                          ) 
                        UNION 
                          (
                            SELECT 
                              'cash_ratio' as w, 
                              AVG(cash_ratio_i) as avg, 
                              ARRAY_AGG(cash_ratio_i) as array_w, 
                              ARRAY_AGG(indu_2) as array_indu_2, 
                              stddev(cash_ratio_i) as std_w 
                            FROM 
                              agg 
                            GROUP BY 
                              fake
                          ) 
                        UNION 
                          (
                            SELECT 
                              'liabilities_assets' as w, 
                              AVG(liabilities_assets_i) as avg, 
                              ARRAY_AGG(liabilities_assets_i) as array_w, 
                              ARRAY_AGG(indu_2) as array_indu_2, 
                              stddev(liabilities_assets_i) as std_w 
                            FROM 
                              agg 
                            GROUP BY 
                              fake
                          ) 
                        UNION 
                          (
                            SELECT 
                              'return_on_asset' as w, 
                              AVG(return_on_asset_i) as avg, 
                              ARRAY_AGG(return_on_asset_i) as array_w, 
                              ARRAY_AGG(indu_2) as array_indu_2, 
                              stddev(return_on_asset_i) as std_w 
                            FROM 
                              agg 
                            GROUP BY 
                              fake
                          ) 
                        UNION 
                          (
                            SELECT 
                              'sales_assets' as w, 
                              AVG(sales_assets_i) as avg, 
                              ARRAY_AGG(sales_assets_i) as array_w, 
                              ARRAY_AGG(indu_2) as array_indu_2, 
                              stddev(sales_assets_i) as std_w 
                            FROM 
                              agg 
                            GROUP BY 
                              fake
                          ) 
                        UNION 
                          (
                            SELECT 
                              'rd_intensity' as w, 
                              AVG(rd_intensity_i) as avg, 
                              ARRAY_AGG(rd_intensity_i) as array_w, 
                              ARRAY_AGG(indu_2) as array_indu_2, 
                              stddev(rd_intensity_i) as std_w 
                            FROM 
                              agg 
                            GROUP BY 
                              fake
                          ) 
                        UNION 
                          (
                            SELECT 
                              'inventory_to_sales' as w, 
                              AVG(inventory_to_sales_i) as avg, 
                              ARRAY_AGG(inventory_to_sales_i) as array_w, 
                              ARRAY_AGG(indu_2) as array_indu_2, 
                              stddev(inventory_to_sales_i) as std_w 
                            FROM 
                              agg 
                            GROUP BY 
                              fake
                          ) 
                        UNION 
                          (
                            SELECT 
                              'asset_tangibility' as w, 
                              AVG(asset_tangibility_i) as avg, 
                              ARRAY_AGG(asset_tangibility_i) as array_w, 
                              ARRAY_AGG(indu_2) as array_indu_2, 
                              stddev(asset_tangibility_i) as std_w 
                            FROM 
                              agg 
                            GROUP BY 
                              fake
                          ) 
                        UNION 
                          (
                            SELECT 
                              'account_paybable_to_asset' as w, 
                              AVG(account_paybable_to_asset_i) as avg, 
                              ARRAY_AGG(account_paybable_to_asset_i) as array_w, 
                              ARRAY_AGG(indu_2) as array_indu_2, 
                              stddev(account_paybable_to_asset_i) as std_w 
                            FROM 
                              agg 
                            GROUP BY 
                              fake
                          )
                      )
                  ) CROSS 
                  JOIN UNNEST(zip_values) as t(names)
              ) 
            GROUP BY 
              field0
          )
      )
      ORDER BY indu_2
  )
"""
output = s3.run_query(
                    query=query,
                    database=DatabaseName,
                    s3_output=s3_output_example,
    filename = 'example_3'
                )
(
    output
    #.style
    #.format('{0:,.2f}')
)
```

```python
(
    output
    #.loc[lambda x: x['year'].isin(['2006'])]
    .reindex(columns = 
    ['indu_2',
     'short',
     'receivable_curasset_i',
     'cash_over_curasset_i',
     'working_capital_i',
     'current_ratio_i',
     'quick_ratio_i',
     'cash_ratio_i',
     'liabilities_assets_i',
     'return_on_asset_i',
     'sales_assets_i',
     'account_paybable_to_asset_i',
     'asset_tangibility_i',
     'rd_intensity_i',
     'inventory_to_sales_i'
    ]
     )
    .sort_values(by = ['working_capital_i'])
    .set_index(['indu_2', 'short'])
    .assign(
        rank_rc = lambda x: x['receivable_curasset_i'].rank().astype('int64'),
        rank_pct_non_cash = lambda x: x['cash_over_curasset_i'].rank().astype('int64'),
        
        rank_w = lambda x: x['working_capital_i'].rank().astype('int64'),
        rank_c = lambda x: x['current_ratio_i'].rank().astype('int64'),
        rank_q = lambda x: x['quick_ratio_i'].rank().astype('int64'),
        rank_cash = lambda x: x['cash_ratio_i'].rank().astype('int64'),
        
        rank_li = lambda x: x['liabilities_assets_i'].rank().astype('int64'),
        rank_re = lambda x: x['return_on_asset_i'].rank().astype('int64'),
        rank_sa = lambda x: x['sales_assets_i'].rank().astype('int64'),
        rank_at = lambda x: x['asset_tangibility_i'].rank().astype('int64'),
        rank_ap = lambda x: x['account_paybable_to_asset_i'].rank().astype('int64'),
    )
    .style
    .background_gradient(cmap=sns.light_palette("green", as_cmap=True), subset = ["rank_rc",'rank_pct_non_cash'])
    .background_gradient(cmap=sns.light_palette("blue", as_cmap=True), subset = ["rank_w",'rank_c','rank_q', 'rank_cash'])
    .background_gradient(cmap=sns.light_palette("orange", as_cmap=True), subset = ["rank_li",'rank_re', 'rank_sa', 'rank_at', 'rank_ap'])
)
```

```python
fig = px.parallel_coordinates(
    output[['indu_2',
            'receivable_curasset_i',
     'cash_over_curasset_i',
     'working_capital_i',
     'current_ratio_i',
            'quick_ratio_i',
     'cash_ratio_i',
     'liabilities_assets_i',
     'return_on_asset_i',
     'sales_assets_i',
     'account_paybable_to_asset_i',
     'asset_tangibility_i',
     'rd_intensity_i',
     'inventory_to_sales_i']].rank(),
    labels={
        "receivable_curasset_i":"receivable_curasset_i",
        "cash_over_curasset_i":"cash_over_curasset_i",
        "working_capital_i": "working_capital_i",
        "working_capital_requirement_i": "working_capital_requirement_i",
        "current_ratio_i": "current_ratio_i",
        "quick_ratio_i": "quick_ratio_i",
        "cash_ratio_i": "cash_ratio_i",
        "liabilities_assets_i": "liabilities_assets_i",
        "return_on_asset_i": "return_on_asset_i",
        "sales_assets_i": "sales_assets_i",
        "rd_intensity_i": "rd_intensity_i",
        "inventory_to_sales_i": "inventory_to_sales_i",
        "asset_tangibility_i": "asset_tangibility_i",
        "account_paybable_to_asset_i": "account_paybable_to_asset_i",
    },
    color_continuous_scale=px.colors.diverging.Tealrose,
    color_continuous_midpoint=2,
)
fig
```

```python
sns.set_theme(style="white")

# Compute the correlation matrix
corr = output[['indu_2',
            'receivable_curasset_i',
     'cash_over_curasset_i',
     'working_capital_i',
     'current_ratio_i',
    'quick_ratio_i',
     'cash_ratio_i',
     'liabilities_assets_i',
     'return_on_asset_i',
     'sales_assets_i',
     'account_paybable_to_asset_i',
     'asset_tangibility_i',
     'rd_intensity_i',
     'inventory_to_sales_i']].set_index('indu_2').rank().corr()

# Generate a mask for the upper triangle
mask = np.triu(np.ones_like(corr, dtype=bool))

# Set up the matplotlib figure
f, ax = plt.subplots(figsize=(11, 9))

# Generate a custom diverging colormap
cmap = sns.diverging_palette(230, 20, as_cmap=True)

# Draw the heatmap with the mask and correct aspect ratio
sns.heatmap(corr, mask=mask, cmap=cmap, vmax=.3, center=0,
            square=True, linewidths=.5, cbar_kws={"shrink": .5})
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
table_name = 'asif_industry_financial_ratio_industry'
s3_output = 'DATA/ECON/FIRM_SURVEY/ASIF_CHINA/TRANSFORMED/FINANCIAL_RATIO/INDUSTRY'
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
  SELECT 
    *, 
    CASE WHEN LENGTH(cic) = 4 THEN substr(cic,1, 2) ELSE concat('0',substr(cic,1, 1)) END AS indu_2, 
    c98 + c99 as total_asset 
  FROM 
    firms_survey.asif_firms_prepared
) 
SELECT 
  * 
FROM 
  (
    WITH ratio AS (
      SELECT 
        year, 
        indu_2, 
        CAST(
    SUM(c80) AS DECIMAL(16, 5)
  ) / NULLIF(
    CAST(
      SUM(cuasset) AS DECIMAL(16, 5)
    ), 
    0
  ) as receivable_curasset_it,
  
  CAST(
    SUM(cuasset) - SUM(c79) - SUM(c80) - SUM(c81) - SUM(c82) AS DECIMAL(16, 5)
  ) / NULLIF(
    CAST(
      SUM(cuasset) AS DECIMAL(16, 5)
    ), 
    0
  ) as cash_over_curasset_it,
  
  SUM(cuasset) - SUM(c95) as working_capital_it,
  SUM(c81) + SUM(c80) - SUM(c96) AS working_capital_requirement_it,   
  CAST(
    SUM(cuasset) AS DECIMAL(16, 5)
  ) / NULLIF(
    CAST(
      SUM(c95) AS DECIMAL(16, 5)
    ), 
    0
  ) AS current_ratio_it, 
  
  CAST(
    SUM(cuasset) - SUM(c79) - SUM(c80) - SUM(c81) AS DECIMAL(16, 5)
  ) / NULLIF(
    CAST(
      SUM(c95) AS DECIMAL(16, 5)
    ), 
    0
  ) AS quick_ratio_it, 

  CAST(
    SUM(cuasset) - SUM(c79) - SUM(c80) - SUM(c81) - SUM(c82) AS DECIMAL(16, 5)
  ) / NULLIF(CAST(
    SUM(c95) AS DECIMAL(16, 5)
  ), 
    0
  ) AS cash_ratio_it,
  
  -- Need to add asset or debt when bs requirement not meet
  CASE 
  WHEN SUM(toasset) - (SUM(c98) + SUM(c99)) < 0 THEN
  CAST(
     SUM(c95) + SUM(c97) AS DECIMAL(16, 5)
  )/ NULLIF(
    CAST(
     SUM(toasset) + ABS(SUM(toasset) - (SUM(c98) + SUM(c99)))  AS DECIMAL(16, 5)
    ), 
    0
  ) 
  WHEN SUM(toasset) - (SUM(c98) + SUM(c99)) > 0 THEN
  CAST(
     SUM(c95) + SUM(c97) + SUM(toasset) - (SUM(c98) + SUM(c99)) AS DECIMAL(16, 5)
  )/ NULLIF(
    CAST(
     SUM(toasset)  AS DECIMAL(16, 5)
    ), 
    0
  )
  ELSE
  CAST(
     SUM(c95) + SUM(c97) AS DECIMAL(16, 5)
  )/ NULLIF(
    CAST(
     SUM(toasset)  AS DECIMAL(16, 5)
    ), 
    0
  )
  END AS liabilities_assets_it, 

  CASE 
  WHEN SUM(toasset) - (SUM(c98) + SUM(c99)) < 0 THEN
  CAST(SUM(sales) - (SUM(c108) + SUM(c113) + SUM(c114) + SUM(c116) + SUM(c118) + SUM(c124) + SUM(wage)) AS DECIMAL(16, 5))/ 
  NULLIF(CAST(SUM(toasset) + ABS(SUM(toasset) - (SUM(c98) + SUM(c99)))  AS DECIMAL(16, 5)), 0) 
  ELSE
  CAST(SUM(sales) - (SUM(c108) + SUM(c113) + SUM(c114) + SUM(c116) + SUM(c118) + SUM(c124) + SUM(wage)) AS DECIMAL(16, 5))/ 
  NULLIF(CAST(SUM(toasset)  AS DECIMAL(16, 5)), 0) 
  END AS return_on_asset_it,
  
  CASE 
  WHEN SUM(toasset) - (SUM(c98) + SUM(c99)) < 0 THEN  
  CAST(
    SUM(sales) AS DECIMAL(16, 5)
  )/ NULLIF(
    CAST(
      (
          SUM(toasset) + ABS(SUM(toasset) - (SUM(c98) + SUM(c99))) - lag(SUM(toasset) + ABS(SUM(toasset) - (SUM(c98) + SUM(c99))),
          1
        ) over(
          partition by indu_2 
          order by 
            indu_2, 
            year
        )
      )/ 2 AS DECIMAL(16, 5)
    ), 
    0
  )
  ELSE 
  CAST(
    SUM(sales) AS DECIMAL(16, 5)
  )/ NULLIF(
    CAST(
      (
        SUM(toasset) - lag(
          SUM(toasset), 
          1
        ) over(
          partition by indu_2 
          order by 
            indu_2, 
            year
        )
      )/ 2 AS DECIMAL(16, 5)
    ), 
    0
  )
  
  END AS sales_assets_it,
  
  CAST(SUM(rdfee) AS DECIMAL(16, 5))/ NULLIF(CAST(SUM(sales) AS DECIMAL(16, 5)),0) as rd_intensity_it,
  CAST(SUM(c81)  AS DECIMAL(16, 5))/ NULLIF(CAST(SUM(sales) AS DECIMAL(16, 5)),0) as inventory_to_sales_it,
  SUM(tofixed) - SUM(c92) AS asset_tangibility_it,
  
  CAST(
      (
        SUM(c96) - lag(
          SUM(c96), 
          1
        ) over(
          partition by indu_2 
          order by 
            indu_2, 
            year
        )
      ) AS DECIMAL(16, 5)
      )/
      NULLIF(
    CAST(
      (
        SUM(total_asset) - lag(
          SUM(total_asset), 
          1
        ) over(
          partition by indu_2 
          order by 
            indu_2, 
            year
        )
      )/ 2 AS DECIMAL(16, 5)
    ),0) as account_paybable_to_asset_it
    
FROM test
      WHERE 
        year in ('2004', '2005', '2006') 
      GROUP BY 
        indu_2, 
        year
    ) 
    SELECT 
      * 
    FROM 
      (
        WITH agg AS (
          SELECT 
            indu_2, 
            'FAKE_GROUP' as fake, 
            AVG(receivable_curasset_it) AS receivable_curasset_i, 
            AVG(cash_over_curasset_it) AS cash_over_curasset_i, 
            
            AVG(working_capital_it)/1000000 AS working_capital_i, 
            AVG(working_capital_requirement_it)/1000000 AS working_capital_requirement_i, 
            AVG(current_ratio_it) AS current_ratio_i, 
            AVG(quick_ratio_it) AS quick_ratio_i,
            AVG(cash_ratio_it) AS cash_ratio_i, 
            
            AVG(liabilities_assets_it) AS liabilities_assets_i, 
            AVG(return_on_asset_it) AS return_on_asset_i, 
            AVG(sales_assets_it) AS sales_assets_i, 
            AVG(account_paybable_to_asset_it) AS account_paybable_to_asset_i,
            AVG(asset_tangibility_it)/1000000 AS asset_tangibility_i, 
            
            AVG(rd_intensity_it) AS rd_intensity_i, 
            AVG(inventory_to_sales_it) AS inventory_to_sales_i

          FROM 
            ratio 
          GROUP BY 
            indu_2 
        ) 
        SELECT 
          field0 AS indu_2, 
          val_1[ 'receivable_curasset' ] AS receivable_curasset_i, 
          val_2[ 'receivable_curasset' ] AS std_receivable_curasset_i, 
          val_1[ 'cash_over_curasset' ] AS cash_over_curasset_i, 
          val_2[ 'cash_over_curasset' ] AS std_cash_over_curasset_i, 
          
          val_1[ 'workink_capital' ] AS working_capital_i, 
          val_2[ 'workink_capital' ] AS std_working_capital_i, 
          val_1[ 'working_capital_requirement' ] AS working_capital_requirement_i, 
          val_2[ 'working_capital_requirement' ] AS std_working_capital_requirement_i, 
          val_1[ 'current_ratio' ] AS current_ratio_i, 
          val_2[ 'current_ratio' ] AS std_current_ratio_i, 
          val_1[ 'quick_ratio' ] AS quick_ratio_i, 
          val_2[ 'quick_ratio' ] AS std_quick_ratio_i,
          1 - val_1[ 'cash_ratio' ] AS cash_ratio_i, 
          1 - val_2[ 'cash_ratio' ] AS std_cash_ratio_i, 
          
          val_1[ 'liabilities_assets' ] AS liabilities_assets_i, 
          val_2[ 'liabilities_assets' ] AS std_liabilities_assets_i, 
          val_1[ 'return_on_asset' ] AS return_on_asset_i, 
          val_2[ 'return_on_asset' ] AS std_return_on_asset_i, 
          val_1[ 'sales_assets' ] AS sales_assets_i, 
          val_2[ 'sales_assets' ] AS std_sales_assets_i, 
          val_1[ 'account_paybable_to_asset' ] AS account_paybable_to_asset_i, 
          val_2[ 'account_paybable_to_asset' ] AS std_account_paybable_to_asset_i,
          val_1[ 'asset_tangibility' ] AS asset_tangibility_i, 
          val_2[ 'asset_tangibility' ] AS std_asset_tangibility_i, 
          
          val_1[ 'rd_intensity' ] AS rd_intensity_i, 
          val_2[ 'rd_intensity' ] AS std_rd_intensity_i, 
          val_1[ 'inventory_to_sales' ] AS inventory_to_sales_i, 
          val_2[ 'inventory_to_sales' ] AS std_inventory_to_sales_i
         
        FROM 
          (
            SELECT 
              field0, 
              map_agg(w, field1) AS val_1, 
              map_agg(w, field2) AS val_2 
            FROM 
              (
                SELECT 
                  w, 
                  names.field0, 
                  names.field1, 
                  names.field2 
                FROM 
                  (
                    SELECT 
                      w, 
                      zip(
                        array_indu_2, 
                        array_w, 
                        transform(
                          array_w, 
                          x -> (x - avg)/ std_w
                        )
                      ) as zip_values 
                    FROM 
                      (
                        SELECT 
                          w, 
                          avg, 
                          array_w, 
                          array_indu_2, 
                          std_w 
                        FROM 
                        (
                            SELECT 
                              'receivable_curasset' as w, 
                              AVG(receivable_curasset_i) as avg, 
                              ARRAY_AGG(receivable_curasset_i) as array_w, 
                              ARRAY_AGG(indu_2) as array_indu_2, 
                              stddev(receivable_curasset_i) as std_w 
                            FROM 
                              agg 
                            GROUP BY 
                              fake
                          ) 
                        UNION 
                        (
                            SELECT 
                              'cash_over_curasset' as w, 
                              AVG(cash_over_curasset_i) as avg, 
                              ARRAY_AGG(cash_over_curasset_i) as array_w, 
                              ARRAY_AGG(indu_2) as array_indu_2, 
                              stddev(cash_over_curasset_i) as std_w 
                            FROM 
                              agg 
                            GROUP BY 
                              fake
                          ) 
                        UNION 
                          (
                            SELECT 
                              'workink_capital' as w, 
                              AVG(working_capital_i) as avg, 
                              ARRAY_AGG(working_capital_i) as array_w, 
                              ARRAY_AGG(indu_2) as array_indu_2, 
                              stddev(working_capital_i) as std_w 
                            FROM 
                              agg 
                            GROUP BY 
                              fake
                          ) 
                        UNION 
                          (
                            SELECT 
                              'working_capital_requirement' as w, 
                              AVG(working_capital_requirement_i) as avg, 
                              ARRAY_AGG(working_capital_requirement_i) as array_w, 
                              ARRAY_AGG(indu_2) as array_indu_2, 
                              stddev(working_capital_requirement_i) as std_w 
                            FROM 
                              agg 
                            GROUP BY 
                              fake
                          ) 
                        UNION 
                          (
                            SELECT 
                              'current_ratio' as w, 
                              AVG(current_ratio_i) as avg, 
                              ARRAY_AGG(current_ratio_i) as array_w, 
                              ARRAY_AGG(indu_2) as array_indu_2, 
                              stddev(current_ratio_i) as std_w 
                            FROM 
                              agg 
                            GROUP BY 
                              fake
                          ) 
                        UNION 
                          (
                            SELECT 
                              'quick_ratio' as w, 
                              AVG(quick_ratio_i) as avg, 
                              ARRAY_AGG(quick_ratio_i) as array_w, 
                              ARRAY_AGG(indu_2) as array_indu_2, 
                              stddev(quick_ratio_i) as std_w 
                            FROM 
                              agg 
                            GROUP BY 
                              fake
                          ) 
                        UNION 
                          (
                            SELECT 
                              'cash_ratio' as w, 
                              AVG(cash_ratio_i) as avg, 
                              ARRAY_AGG(cash_ratio_i) as array_w, 
                              ARRAY_AGG(indu_2) as array_indu_2, 
                              stddev(cash_ratio_i) as std_w 
                            FROM 
                              agg 
                            GROUP BY 
                              fake
                          ) 
                        UNION 
                          (
                            SELECT 
                              'liabilities_assets' as w, 
                              AVG(liabilities_assets_i) as avg, 
                              ARRAY_AGG(liabilities_assets_i) as array_w, 
                              ARRAY_AGG(indu_2) as array_indu_2, 
                              stddev(liabilities_assets_i) as std_w 
                            FROM 
                              agg 
                            GROUP BY 
                              fake
                          ) 
                        UNION 
                          (
                            SELECT 
                              'return_on_asset' as w, 
                              AVG(return_on_asset_i) as avg, 
                              ARRAY_AGG(return_on_asset_i) as array_w, 
                              ARRAY_AGG(indu_2) as array_indu_2, 
                              stddev(return_on_asset_i) as std_w 
                            FROM 
                              agg 
                            GROUP BY 
                              fake
                          ) 
                        UNION 
                          (
                            SELECT 
                              'sales_assets' as w, 
                              AVG(sales_assets_i) as avg, 
                              ARRAY_AGG(sales_assets_i) as array_w, 
                              ARRAY_AGG(indu_2) as array_indu_2, 
                              stddev(sales_assets_i) as std_w 
                            FROM 
                              agg 
                            GROUP BY 
                              fake
                          ) 
                        UNION 
                          (
                            SELECT 
                              'rd_intensity' as w, 
                              AVG(rd_intensity_i) as avg, 
                              ARRAY_AGG(rd_intensity_i) as array_w, 
                              ARRAY_AGG(indu_2) as array_indu_2, 
                              stddev(rd_intensity_i) as std_w 
                            FROM 
                              agg 
                            GROUP BY 
                              fake
                          ) 
                        UNION 
                          (
                            SELECT 
                              'inventory_to_sales' as w, 
                              AVG(inventory_to_sales_i) as avg, 
                              ARRAY_AGG(inventory_to_sales_i) as array_w, 
                              ARRAY_AGG(indu_2) as array_indu_2, 
                              stddev(inventory_to_sales_i) as std_w 
                            FROM 
                              agg 
                            GROUP BY 
                              fake
                          ) 
                        UNION 
                          (
                            SELECT 
                              'asset_tangibility' as w, 
                              AVG(asset_tangibility_i) as avg, 
                              ARRAY_AGG(asset_tangibility_i) as array_w, 
                              ARRAY_AGG(indu_2) as array_indu_2, 
                              stddev(asset_tangibility_i) as std_w 
                            FROM 
                              agg 
                            GROUP BY 
                              fake
                          ) 
                        UNION 
                          (
                            SELECT 
                              'account_paybable_to_asset' as w, 
                              AVG(account_paybable_to_asset_i) as avg, 
                              ARRAY_AGG(account_paybable_to_asset_i) as array_w, 
                              ARRAY_AGG(indu_2) as array_indu_2, 
                              stddev(account_paybable_to_asset_i) as std_w 
                            FROM 
                              agg 
                            GROUP BY 
                              fake
                          )
                      )
                  ) CROSS 
                  JOIN UNNEST(zip_values) as t(names)
              ) 
            GROUP BY 
              field0
          )
      )
      ORDER BY indu_2
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
query_ = """
SELECT COUNT(*) AS CNT
FROM {}.{} 
""".format(DatabaseName, table_name)
output = s3.run_query(
                    query=query_,
                    database=DatabaseName,
                    s3_output=s3_output_example,
    filename = 'count_{}'.format(table_name)
                )
output
```

```python
query_ = """
SELECT len, COUNT(len) as CNT
FROM (
SELECT length(indu_2) AS len
FROM {}.{} 
)
GROUP BY len
ORDER BY CNT
""".format(DatabaseName, table_name)
output = s3.run_query(
                    query=query_,
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
partition_keys = ["indu_2"]
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
schema = [{'Name': 'indu_2', 'Type': 'string', 'Comment': 'Two digits industry. If length cic equals to 3, then add 0 to indu_2'},
 {'Name': 'receivable_curasset_i', 'Type': 'double', 'Comment': '应收帐款 (c80) / cuasset'},
 {'Name': 'std_receivable_curasset_i', 'Type': 'double', 'Comment': 'standaridzed values (x - x mean) / std)'},
 {'Name': 'cash_over_curasset_i', 'Type': 'double', 'Comment': '(其中：短期投资 (c79) - 应收帐款 (c80) - 存货 (c81) - 其中：产成品 (c82)) /current asset'},
 {'Name': 'std_cash_over_curasset_i', 'Type': 'double', 'Comment': 'standaridzed values (x - x mean) / std)'},
 {'Name': 'working_capital_i', 'Type': 'double', 'Comment': 'cuasset- 流动负债合计 (c95)'},
 {'Name': 'std_working_capital_i', 'Type': 'double', 'Comment': 'standaridzed values (x - x mean) / std)'},
 {'Name': 'working_capital_requirement_i', 'Type': 'double', 'Comment': '存货 (c81) + 应收帐款 (c80) - 应付帐款  (c96)'},
 {'Name': 'std_working_capital_requirement_i',
  'Type': 'double',
  'Comment': 'standaridzed values (x - x mean) / std)'},
 {'Name': 'current_ratio_i', 'Type': 'double', 'Comment': 'cuasset/流动负债合计 (c95)'},
 {'Name': 'std_current_ratio_i', 'Type': 'double', 'Comment': 'standaridzed values (x - x mean) / std)'},
 {'Name': 'quick_ratio_i', 'Type': 'double', 'Comment': '(cuasset -  其中：短期投资 (c79) - 应收帐款 (c80) - 存货 (c81)) / 流动负债合计 (c95)'},
 {'Name': 'std_quick_ratio_i', 'Type': 'double', 'Comment': 'standaridzed values (x - x mean) / std)'},
 {'Name': 'cash_ratio_i', 'Type': 'double', 'Comment': '(1 - cuasset - 其中：短期投资 (c79) - 应收帐款 (c80) - 存货 (c81) - 其中：产成品 (c82))/ 流动负债合计 (c95)'},
 {'Name': 'std_cash_ratio_i', 'Type': 'double', 'Comment': 'standaridzed values (x - x mean) / std)'},
 {'Name': 'liabilities_assets_i', 'Type': 'double', 'Comment': '(流动负债合计 (c95) + 长期负债合计 (c97)) / toasset'},
 {'Name': 'std_liabilities_assets_i', 'Type': 'double', 'Comment': 'standaridzed values (x - x mean) / std)'},
 {'Name': 'return_on_asset_i', 'Type': 'double', 'Comment': 'sales - (主营业务成本 (c108) + 营业费用 (c113) + 管理费用 (c114) + 财产保险费 (c116) + 劳动、失业保险费 (c118)+ 财务费用 (c124) + 本年应付工资总额 (wage)) /toasset'},
 {'Name': 'std_return_on_asset_i', 'Type': 'double', 'Comment': 'standaridzed values (x - x mean) / std)'},
 {'Name': 'sales_assets_i', 'Type': 'double', 'Comment': '全年营业收入合计 (c64) /(\Delta toasset/2)'},
 {'Name': 'std_sales_assets_i', 'Type': 'double', 'Comment': 'standaridzed values (x - x mean) / std)'},
 {'Name': 'account_paybable_to_asset_i', 'Type': 'double', 'Comment': '(\Delta 应付帐款  (c96))/ (\Delta (toasset))'},
 {'Name': 'std_account_paybable_to_asset_i', 'Type': 'double', 'Comment': 'standaridzed values (x - x mean) / std)'},
 {'Name': 'asset_tangibility_i', 'Type': 'double', 'Comment': 'Total fixed assets - Intangible assets'},
 {'Name': 'std_asset_tangibility_i', 'Type': 'double', 'Comment': 'standaridzed values (x - x mean) / std)'},
 {'Name': 'rd_intensity_i', 'Type': 'double', 'Comment': 'rdfee/全年营业收入合计 (c64)'},
 {'Name': 'std_rd_intensity_i', 'Type': 'double', 'Comment': 'standaridzed values (x - x mean) / std)'},
 {'Name': 'inventory_to_sales_i', 'Type': 'double', 'Comment': '存货 (c81) / sales'},
 {'Name': 'std_inventory_to_sales_i', 'Type': 'double', 'Comment': 'standaridzed values (x - x mean) / std)'}]
```

4. Provide a description

```python
description = """
Compute the financial ratio by industry
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
#partition_keys = ["geocode4_corr", "indu_2", "year"]
#with open(os.path.join(str(Path(path).parent), 'parameters_ETL_Financial_dependency_pollution.json')) as json_file:
#    parameters = json.load(json_file)
```

```python
### COUNT DUPLICATES
#if len(partition_keys) > 0:
#    groups = ' , '.join(partition_keys)
#    query_duplicates = parameters["ANALYSIS"]['COUNT_DUPLICATES']['query'].format(
#                                DatabaseName,table_name,groups
#                                )
#    dup = s3.run_query(
#                                query=query_duplicates,
#                                database=DatabaseName,
#                                s3_output="SQL_OUTPUT_ATHENA",
#                                filename="duplicates_{}".format(table_name))
#    display(dup)

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
#import boto3

#key, secret_ = con.load_credential()
#client_lambda = boto3.client(
#    'lambda',
#    aws_access_key_id=key,
#    aws_secret_access_key=secret_,
#    region_name = region)
```

```python
#primary_key = 'year'
#secondary_key = 'indu_2'
#y_var = 'working_capital_cit'
```

```python
#payload = {
#    "input_path": "s3://datalake-datascience/ANALYTICS/TEMPLATE_NOTEBOOKS/template_analysis_from_lambda.ipynb",
#    "output_prefix": "s3://datalake-datascience/ANALYTICS/OUTPUT/{}/".format(table_name.upper()),
#    "parameters": {
#        "region": "{}".format(region),
#        "bucket": "{}".format(bucket),
#        "DatabaseName": "{}".format(DatabaseName),
#        "table_name": "{}".format(table_name),
#        "group": "{}".format(','.join(partition_keys)),
#        "keys": "{},{}".format(primary_key,secondary_key),
#        "y_var": "{}".format(y_var),
#        "threshold":0.5
#    },
#}
#payload
```

```python
#response = client_lambda.invoke(
#    FunctionName='RunNotebook',
#    InvocationType='RequestResponse',
#    LogType='Tail',
#    Payload=json.dumps(payload),
#)
#response
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
create_report(extension = "html", keep_code = True, notebookname ="00_asif_financial_ratio.ipynb")
```
