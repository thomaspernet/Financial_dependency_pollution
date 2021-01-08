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

* working capital requirement
* tangible asset
* working capital
* liabilities
* account receivable
* account payable

### Steps 

1. 


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
| 14    | Sale over asset                | Total sales /total asset                                                                                    | 全年营业收入合计 (c64) /(toasset)                                                                                                                                   | #sale-over-asset                                | Ambiguous                   | Sales divided by total asset. Larger value indicates better efficiency at using asset to generate revenue                                                                                                                                                                                                                                                                                                                                                                                                                                      |
| 15    | Asset tangibility              | Total fixed assets - Intangible assets                                                                      | tofixed - 无形资产 (c92)                                                                                                                                            | #asset-tangibility                              | Ambiguous                   | Difference between fixed sset and intangible asset. Larger value indicates more collateral, hence higher borrowing capacity                                                                                                                                                                                                                                                                                                                                                                                                                    |
| 16    | Account payable to total asset | (delta account payable)/ (delta total asset)                                                                | ($\Delta$ 应付帐款  (c96))/ ($\Delta$$ (toasset))                                                                                                                   | #change-account-paybable-to-change-total-assets | Ambiguous (favour positive) | Variation of account payable over variation total asset. If the nominator larger than the denominator, it means the account payable grew larger than an asset, or more time is given to pay back the supplier relative to the total asset.  Say differently,  companies can more easily access buyer or supplier trade credit, they may be less dependent on the formal financial market                                                                                                                                                       |


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
    CASE WHEN LENGTH(cic) = 4 THEN substr(cic,1, 2) ELSE concat('0',substr(cic,1, 1)) END AS indu_2, 
    c98 + c99 as total_asset 
  FROM 
    firms_survey.asif_firms_prepared
  INNER JOIN 
  (
  SELECT extra_code, geocode4_corr
  FROM chinese_lookup.china_city_code_normalised 
  GROUP BY extra_code, geocode4_corr
  ) as no_dup_citycode
ON asif_firms_prepared.citycode = no_dup_citycode.extra_code
) 
SELECT 
  * 
FROM 
  (
    WITH ratio AS (
      SELECT 
        year, 
        indu_2, 
        geocode4_corr,
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
          partition by indu_2, geocode4_corr 
          order by 
            geocode4_corr,
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
          partition by indu_2, geocode4_corr
          order by
            geocode4_corr,
            indu_2, 
            year
        )
      )/ 2 AS DECIMAL(16, 5)
    ), 
    0
  )
  
  END AS sales_assets_it,
  
  CASE 
  WHEN SUM(toasset) - (SUM(c98) + SUM(c99)) < 0 THEN  
  CAST(
    SUM(sales) AS DECIMAL(16, 5)
  )/ NULLIF(
    CAST(SUM(toasset) + ABS(SUM(toasset) - (SUM(c98) + SUM(c99))) AS DECIMAL(16, 5)), 
    0
  )
  ELSE 
  CAST(
    SUM(sales) AS DECIMAL(16, 5)
  )/ NULLIF(
    CAST(
    SUM(toasset) AS DECIMAL(16, 5)), 
    0
  )
  END AS sales_assets_andersen_it,
  
  CAST(SUM(rdfee) AS DECIMAL(16, 5))/ NULLIF(CAST(SUM(sales) AS DECIMAL(16, 5)),0) as rd_intensity_it,
  CAST(SUM(c81)  AS DECIMAL(16, 5))/ NULLIF(CAST(SUM(sales) AS DECIMAL(16, 5)),0) as inventory_to_sales_it,
  SUM(tofixed) - SUM(c92) AS asset_tangibility_it,
  
  CAST(
      (
        SUM(c96) - lag(
          SUM(c96), 
          1
        ) over(
          partition by indu_2, geocode4_corr 
          order by 
          geocode4_corr,
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
          partition by indu_2, geocode4_corr 
          order by 
          geocode4_corr,
            indu_2, 
            year
        )
      )/ 2 AS DECIMAL(16, 5)
    ),0) as account_paybable_to_asset_it
    
FROM test
      WHERE 
        year in ('2004', '2005', '2006') 
      GROUP BY 
      geocode4_corr,
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
            geocode4_corr,
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
            indu_2 ,
            geocode4_corr
        ) 
        SELECT 
          field0 AS indu_2, 
          field1 as geocode4_corr,
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
              field1,
              map_agg(w, field2) AS val_1, 
              map_agg(w, field3) AS val_2 
            FROM 
              (
                SELECT 
                  w, 
                  names.field0, 
                  names.field1, 
                  names.field2,
                  names.field3
                  
                FROM 
                  (
                    SELECT 
                      w, 
                      zip(
                        array_indu_2, 
                        array_geocode4_corr,
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
                          array_geocode4_corr,
                          std_w 
                        FROM 
                        (
                            SELECT 
                              'receivable_curasset' as w, 
                              AVG(receivable_curasset_i) as avg, 
                              ARRAY_AGG(receivable_curasset_i) as array_w, 
                              ARRAY_AGG(indu_2) as array_indu_2, 
                              ARRAY_AGG(geocode4_corr) as array_geocode4_corr, 
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
                              ARRAY_AGG(geocode4_corr) as array_geocode4_corr, 
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
                              ARRAY_AGG(geocode4_corr) as array_geocode4_corr, 
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
                              ARRAY_AGG(geocode4_corr) as array_geocode4_corr, 
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
                              ARRAY_AGG(geocode4_corr) as array_geocode4_corr, 
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
                              ARRAY_AGG(geocode4_corr) as array_geocode4_corr, 
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
                              ARRAY_AGG(geocode4_corr) as array_geocode4_corr, 
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
                              ARRAY_AGG(geocode4_corr) as array_geocode4_corr, 
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
                              ARRAY_AGG(geocode4_corr) as array_geocode4_corr, 
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
                              ARRAY_AGG(geocode4_corr) as array_geocode4_corr, 
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
                              ARRAY_AGG(geocode4_corr) as array_geocode4_corr, 
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
                              ARRAY_AGG(geocode4_corr) as array_geocode4_corr, 
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
                              ARRAY_AGG(geocode4_corr) as array_geocode4_corr, 
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
                              ARRAY_AGG(geocode4_corr) as array_geocode4_corr, 
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
              field0, field1
          )
      )
      ORDER BY indu_2, geocode4_corr
      LIMIT 10
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
    CASE WHEN LENGTH(cic) = 4 THEN substr(cic,1, 2) ELSE concat('0',substr(cic,1, 1)) END AS indu_2, 
    c98 + c99 as total_asset 
  FROM 
    firms_survey.asif_firms_prepared
  INNER JOIN 
  (
  SELECT extra_code, geocode4_corr
  FROM chinese_lookup.china_city_code_normalised 
  GROUP BY extra_code, geocode4_corr
  ) as no_dup_citycode
ON asif_firms_prepared.citycode = no_dup_citycode.extra_code
) 
SELECT 
  * 
FROM 
  (
    WITH ratio AS (
      SELECT 
        year, 
        indu_2, 
        geocode4_corr,
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
          partition by indu_2, geocode4_corr 
          order by 
            geocode4_corr,
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
          partition by indu_2, geocode4_corr
          order by
            geocode4_corr,
            indu_2, 
            year
        )
      )/ 2 AS DECIMAL(16, 5)
    ), 
    0
  )
  
  END AS sales_assets_it,
  
  CASE 
  WHEN SUM(toasset) - (SUM(c98) + SUM(c99)) < 0 THEN  
  CAST(
    SUM(sales) AS DECIMAL(16, 5)
  )/ NULLIF(
    CAST(SUM(toasset) + ABS(SUM(toasset) - (SUM(c98) + SUM(c99))) AS DECIMAL(16, 5)), 
    0
  )
  ELSE 
  CAST(
    SUM(sales) AS DECIMAL(16, 5)
  )/ NULLIF(
    CAST(
    SUM(toasset) AS DECIMAL(16, 5)), 
    0
  )
  END AS sales_assets_andersen_it,
  
  CASE 
  WHEN SUM(toasset) - (SUM(c98) + SUM(c99)) < 0 THEN  
  CAST(
    SUM(cuasset) - SUM(c79) - SUM(c80) - SUM(c81) - SUM(c82) AS DECIMAL(16, 5)
  )/ NULLIF(
    CAST(SUM(toasset) + ABS(SUM(toasset) - (SUM(c98) + SUM(c99))) AS DECIMAL(16, 5)), 
    0
  )
  ELSE 
  CAST(
    SUM(cuasset) - SUM(c79) - SUM(c80) - SUM(c81) - SUM(c82) AS DECIMAL(16, 5)
  )/ NULLIF(
    CAST(
    SUM(toasset) AS DECIMAL(16, 5)), 
    0
  )
  END AS cash_over_totasset_it,
  
  CAST(SUM(rdfee) AS DECIMAL(16, 5))/ NULLIF(CAST(SUM(sales) AS DECIMAL(16, 5)),0) as rd_intensity_it,
  CAST(SUM(c81)  AS DECIMAL(16, 5))/ NULLIF(CAST(SUM(sales) AS DECIMAL(16, 5)),0) as inventory_to_sales_it,
  SUM(tofixed) - SUM(c92) AS asset_tangibility_it,
  
  CAST(
      (
        SUM(c96) - lag(
          SUM(c96), 
          1
        ) over(
          partition by indu_2, geocode4_corr 
          order by 
          geocode4_corr,
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
          partition by indu_2, geocode4_corr 
          order by 
          geocode4_corr,
            indu_2, 
            year
        )
      )/ 2 AS DECIMAL(16, 5)
    ),0) as account_paybable_to_asset_it
    
FROM test
      WHERE 
        year in ('2004', '2005', '2006') 
      GROUP BY 
      geocode4_corr,
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
            geocode4_corr,
            'FAKE_GROUP' as fake, 
            AVG(receivable_curasset_it) AS receivable_curasset_i, 
            AVG(cash_over_curasset_it) AS cash_over_curasset_i, 
            AVG(cash_over_totasset_it) AS cash_over_totasset_i, 
            
            AVG(working_capital_it)/1000000 AS working_capital_i, 
            AVG(working_capital_requirement_it)/1000000 AS working_capital_requirement_i, 
            AVG(current_ratio_it) AS current_ratio_i, 
            AVG(quick_ratio_it) AS quick_ratio_i,
            AVG(cash_ratio_it) AS cash_ratio_i, 
            
            AVG(liabilities_assets_it) AS liabilities_assets_i, 
            AVG(return_on_asset_it) AS return_on_asset_i, 
            AVG(sales_assets_it) AS sales_assets_i,
            AVG(sales_assets_andersen_it) AS sales_assets_andersen_i,
            AVG(account_paybable_to_asset_it) AS account_paybable_to_asset_i,
            AVG(asset_tangibility_it)/1000000 AS asset_tangibility_i, 
            
            AVG(rd_intensity_it) AS rd_intensity_i, 
            AVG(inventory_to_sales_it) AS inventory_to_sales_i

          FROM 
            ratio 
          GROUP BY 
            indu_2 ,
            geocode4_corr
        ) 
        SELECT 
          field0 AS indu_2, 
          field1 as geocode4_corr,
          val_1[ 'receivable_curasset' ] AS receivable_curasset_ci, 
          val_2[ 'receivable_curasset' ] AS std_receivable_curasset_ci, 
          1 - val_1[ 'cash_over_curasset' ] AS cash_over_curasset_ci, 
          1 - val_2[ 'cash_over_curasset' ] AS std_cash_over_curasset_ci, 
          1 - val_1[ 'cash_over_totasset' ] AS cash_over_totasset_ci, 
          1 - val_2[ 'cash_over_totasset' ] AS std_cash_over_totasset_ci,
          
          val_1[ 'workink_capital' ] AS working_capital_ci, 
          val_2[ 'workink_capital' ] AS std_working_capital_ci, 
          val_1[ 'working_capital_requirement' ] AS working_capital_requirement_ci, 
          val_2[ 'working_capital_requirement' ] AS std_working_capital_requirement_ci, 
          val_1[ 'current_ratio' ] AS current_ratio_ci, 
          val_2[ 'current_ratio' ] AS std_current_ratio_ci, 
          val_1[ 'quick_ratio' ] AS quick_ratio_ci, 
          val_2[ 'quick_ratio' ] AS std_quick_ratio_ci,
          1-val_1[ 'cash_ratio' ] AS cash_ratio_ci, 
          1-val_2[ 'cash_ratio' ] AS std_cash_ratio_ci, 
          
          val_1[ 'liabilities_assets' ] AS liabilities_assets_ci, 
          val_2[ 'liabilities_assets' ] AS std_liabilities_assets_ci, 
          val_1[ 'return_on_asset' ] AS return_on_asset_ci, 
          val_2[ 'return_on_asset' ] AS std_return_on_asset_ci, 
          val_1[ 'sales_assets' ] AS sales_assets_ci, 
          val_2[ 'sales_assets' ] AS std_sales_assets_ci, 
          val_1[ 'sales_assets_andersen' ] AS sales_assets_andersen_ci, 
          val_2[ 'sales_assets_andersen' ] AS std_sales_assets_andersen_ci, 
          val_1[ 'account_paybable_to_asset' ] AS account_paybable_to_asset_ci, 
          val_2[ 'account_paybable_to_asset' ] AS std_account_paybable_to_asset_ci,
          val_1[ 'asset_tangibility' ] AS asset_tangibility_ci, 
          val_2[ 'asset_tangibility' ] AS std_asset_tangibility_ci, 
          
          val_1[ 'rd_intensity' ] AS rd_intensity_ci, 
          val_2[ 'rd_intensity' ] AS std_rd_intensity_ci, 
          val_1[ 'inventory_to_sales' ] AS inventory_to_sales_ci, 
          val_2[ 'inventory_to_sales' ] AS std_inventory_to_sales_ci
         
        FROM 
          (
            SELECT 
              field0, 
              field1,
              map_agg(w, field2) AS val_1, 
              map_agg(w, field3) AS val_2 
            FROM 
              (
                SELECT 
                  w, 
                  names.field0, 
                  names.field1, 
                  names.field2,
                  names.field3
                  
                FROM 
                  (
                    SELECT 
                      w, 
                      zip(
                        array_indu_2, 
                        array_geocode4_corr,
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
                          array_geocode4_corr,
                          std_w 
                        FROM 
                        (
                            SELECT 
                              'receivable_curasset' as w, 
                              AVG(receivable_curasset_i) as avg, 
                              ARRAY_AGG(receivable_curasset_i) as array_w, 
                              ARRAY_AGG(indu_2) as array_indu_2, 
                              ARRAY_AGG(geocode4_corr) as array_geocode4_corr, 
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
                              ARRAY_AGG(geocode4_corr) as array_geocode4_corr, 
                              stddev(cash_over_curasset_i) as std_w 
                            FROM 
                              agg 
                            GROUP BY 
                              fake
                          ) 
                        UNION 
                        (
                            SELECT 
                              'cash_over_totasset' as w, 
                              AVG(cash_over_totasset_i) as avg, 
                              ARRAY_AGG(cash_over_totasset_i) as array_w, 
                              ARRAY_AGG(indu_2) as array_indu_2, 
                              ARRAY_AGG(geocode4_corr) as array_geocode4_corr, 
                              stddev(cash_over_totasset_i) as std_w 
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
                              ARRAY_AGG(geocode4_corr) as array_geocode4_corr, 
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
                              ARRAY_AGG(geocode4_corr) as array_geocode4_corr, 
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
                              ARRAY_AGG(geocode4_corr) as array_geocode4_corr, 
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
                              ARRAY_AGG(geocode4_corr) as array_geocode4_corr, 
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
                              ARRAY_AGG(geocode4_corr) as array_geocode4_corr, 
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
                              ARRAY_AGG(geocode4_corr) as array_geocode4_corr, 
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
                              ARRAY_AGG(geocode4_corr) as array_geocode4_corr, 
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
                              ARRAY_AGG(geocode4_corr) as array_geocode4_corr, 
                              stddev(sales_assets_i) as std_w 
                            FROM 
                              agg 
                            GROUP BY 
                              fake
                          ) 
                        UNION 
                          (
                            SELECT 
                              'sales_assets_andersen' as w, 
                              AVG(sales_assets_andersen_i) as avg, 
                              ARRAY_AGG(sales_assets_andersen_i) as array_w, 
                              ARRAY_AGG(indu_2) as array_indu_2, 
                              ARRAY_AGG(geocode4_corr) as array_geocode4_corr, 
                              stddev(sales_assets_andersen_i) as std_w 
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
                              ARRAY_AGG(geocode4_corr) as array_geocode4_corr, 
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
                              ARRAY_AGG(geocode4_corr) as array_geocode4_corr, 
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
                              ARRAY_AGG(geocode4_corr) as array_geocode4_corr, 
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
                              ARRAY_AGG(geocode4_corr) as array_geocode4_corr, 
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
              field0, field1
          )
      )
      ORDER BY indu_2, geocode4_corr
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
partition_keys = ["geocode4_corr", "indu_2"]
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
schema = [{'Name': 'indu_2', 'Type': 'string', 'Comment': 'Two digits industry. If length cic equals to 3, then add 0 to indu_2'},
          {'Name': 'geocode4_corr', 'Type': 'string', 'Comment': ''},
          {'Name': 'receivable_curasset_ci', 'Type': 'double',
              'Comment': '应收帐款 (c80) / cuasset'},
          {'Name': 'std_receivable_curasset_ci', 'Type': 'double',
           'Comment': 'standaridzed values (x - x mean) / std)'},
          {'Name': 'cash_over_curasset_ci', 'Type': 'double',
           'Comment': '1 - (其中：短期投资 (c79) - 应收帐款 (c80) - 存货 (c81) - 其中：产成品 (c82)) /current asset'},
          {'Name': 'std_cash_over_curasset_ci', 'Type': 'double',
           'Comment': 'standaridzed values (x - x mean) / std)'},
          {'Name': '1 - cash_over_totasset_ci', 'Type': 'double', 'Comment': '(其中：短期投资 (c79) - 应收帐款 (c80) - 存货 (c81) - 其中：产成品 (c82)) /toasset'},
          {'Name': 'std_cash_over_totasset_ci', 'Type': 'double', 'Comment': 'standaridzed values (x - x mean) / std)'},
          {'Name': 'working_capital_ci', 'Type': 'double',
           'Comment': 'cuasset- 流动负债合计 (c95)'},
          {'Name': 'std_working_capital_ci', 'Type': 'double',
           'Comment': 'standaridzed values (x - x mean) / std)'},
          {'Name': 'working_capital_requirement_ci', 'Type': 'double',
           'Comment': '存货 (c81) + 应收帐款 (c80) - 应付帐款  (c96)'},
          {'Name': 'std_working_capital_requirement_ci',
           'Type': 'double',
           'Comment': 'standaridzed values (x - x mean) / std)'},
          {'Name': 'current_ratio_ci', 'Type': 'double',
           'Comment': 'cuasset/流动负债合计 (c95)'},
          {'Name': 'std_current_ratio_ci', 'Type': 'double',
           'Comment': 'standaridzed values (x - x mean) / std)'},
          {'Name': 'quick_ratio_ci', 'Type': 'double',
           'Comment': '(cuasset -  其中：短期投资 (c79) - 应收帐款 (c80) - 存货 (c81)) / 流动负债合计 (c95)'},
          {'Name': 'std_quick_ratio_ci', 'Type': 'double',
           'Comment': 'standaridzed values (x - x mean) / std)'},
          {'Name': 'cash_ratio_ci', 'Type': 'double',
           'Comment': '(cuasset - 其中：短期投资 (c79) - 应收帐款 (c80) - 存货 (c81) - 其中：产成品 (c82))/ 流动负债合计 (c95)'},
          {'Name': 'std_cash_ratio_ci', 'Type': 'double',
           'Comment': 'standaridzed values (x - x mean) / std)'},
          {'Name': 'liabilities_assets_ci', 'Type': 'double',
           'Comment': '(流动负债合计 (c95) + 长期负债合计 (c97)) / toasset'},
          {'Name': 'std_liabilities_assets_ci', 'Type': 'double',
           'Comment': 'standaridzed values (x - x mean) / std)'},
          {'Name': 'return_on_asset_ci', 'Type': 'double',
           'Comment': 'sales - (主营业务成本 (c108) + 营业费用 (c113) + 管理费用 (c114) + 财产保险费 (c116) + 劳动、失业保险费 (c118)+ 财务费用 (c124) + 本年应付工资总额 (wage)) /toasset'},
          {'Name': 'std_return_on_asset_ci', 'Type': 'double',
           'Comment': 'standaridzed values (x - x mean) / std)'},
          {'Name': 'sales_assets_ci', 'Type': 'double',
           'Comment': '全年营业收入合计 (c64) /(\Delta toasset/2)'},
          {'Name': 'std_sales_assets_ci', 'Type': 'double',
           'Comment': 'standaridzed values (x - x mean) / std)'},
          {'Name': 'sales_assets_andersen_ci', 'Type': 'double', 'Comment': 'Sales over asset'},
          {'Name': 'std_sales_assets_andersen_ci', 'Type': 'double', 'Comment': 'standaridzed values (x - x mean) / std)'},
          {'Name': 'account_paybable_to_asset_ci', 'Type': 'double',
           'Comment': '(\Delta 应付帐款  (c96))/ (\Delta (toasset))'},
          {'Name': 'std_account_paybable_to_asset_ci', 'Type': 'double',
           'Comment': 'standaridzed values (x - x mean) / std)'},
          {'Name': 'asset_tangibility_ci', 'Type': 'double',
           'Comment': 'Total fixed assets - Intangible assets'},
          {'Name': 'std_asset_tangibility_ci', 'Type': 'double',
           'Comment': 'standaridzed values (x - x mean) / std)'},
          {'Name': 'rd_intensity_ci', 'Type': 'double',
           'Comment': 'rdfee/全年营业收入合计 (c64)'},
          {'Name': 'std_rd_intensity_ci', 'Type': 'double',
           'Comment': 'standaridzed values (x - x mean) / std)'},
          {'Name': 'inventory_to_sales_ci', 'Type': 'double',
           'Comment': '存货 (c81) / sales'},
          {'Name': 'std_inventory_to_sales_ci', 'Type': 'double', 'Comment': 'standaridzed values (x - x mean) / std)'}]
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
partition_keys = ["geocode4_corr", "indu_2"]

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
