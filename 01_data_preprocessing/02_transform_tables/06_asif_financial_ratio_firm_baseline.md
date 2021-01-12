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

1. construct the following financial ratio
  1. asset tangibility
  2. current ratio
  3. cash over total asset
    1. don’t use variable c79, missing year before 2004
  4. liabilities asset
  5. sales over total asset
2. Construct firms ownership
3. merge city characteristic (tcz, cpz), policy mandate and normalize city code


## Example step by step

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
    c98 + c99 as total_asset, 
    CASE WHEN c79 IS NULL THEN 0 ELSE c79 END AS short_term_investment 
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
    WITH ratio AS (
      SELECT 
        firm, 
        year, 
        cic, 
        indu_2, 
        geocode4_corr, 
        ownership,
        CASE WHEN ownership = 'SOE' THEN 'SOE' ELSE 'PRIVATE' END AS soe_vs_pri,
        CASE WHEN ownership in (
        'HTM', 'FOREIGN'
      ) THEN 'FOREIGN' ELSE 'DOMESTIC' END AS for_vs_dom,
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
        CAST(
          toasset AS DECIMAL(16, 5)
        ) AS total_asset, 
        CAST(
          cuasset AS DECIMAL(16, 5)
        ) / NULLIF(
          CAST(
            c95 AS DECIMAL(16, 5)
          ), 
          0
        ) AS current_ratio_fcit,
      
        CAST(
          cuasset - short_term_investment - c80 - c81 AS DECIMAL(16, 5)
        ) / NULLIF(
          CAST(
            c95 AS DECIMAL(16, 5)
          ), 
          0
        ) AS quick_ratio_fcit, 
        -- Need to add asset or debt when bs requirement not meet
        CASE WHEN toasset - (c98 + c99) < 0 THEN CAST(
          c95 + c97 AS DECIMAL(16, 5)
        )/ NULLIF(
          CAST(
            toasset + ABS(
              toasset - (c98 + c99)
            ) AS DECIMAL(16, 5)
          ), 
          0
        ) WHEN toasset - (c98 + c99) > 0 THEN CAST(
          c95 + c97 + toasset - (c98 + c99) AS DECIMAL(16, 5)
        )/ NULLIF(
          CAST(
            toasset AS DECIMAL(16, 5)
          ), 
          0
        ) ELSE CAST(
          c95 + c97 AS DECIMAL(16, 5)
        )/ NULLIF(
          CAST(
            toasset AS DECIMAL(16, 5)
          ), 
          0
        ) END AS liabilities_assets_fcit, 
        CASE WHEN toasset - (c98 + c99) < 0 THEN CAST(
          sales - (
            c108 + c113 + c114 + c116 + c118 + c124 + wage
          ) AS DECIMAL(16, 5)
        )/ NULLIF(
          CAST(
            toasset + ABS(
              toasset - (c98 + c99)
            ) AS DECIMAL(16, 5)
          ), 
          0
        ) ELSE CAST(
          sales - (
            c108 + c113 + c114 + c116 + c118 + c124 + wage
          ) AS DECIMAL(16, 5)
        )/ NULLIF(
          CAST(
            toasset AS DECIMAL(16, 5)
          ), 
          0
        ) END AS return_on_asset_fcit, 
      
        CASE WHEN toasset - (c98 + c99) < 0 THEN CAST(
          sales AS DECIMAL(16, 5)
        )/ NULLIF(
          CAST(
            toasset + ABS(
              toasset - (c98 + c99)
            ) AS DECIMAL(16, 5)
          ), 
          0
        ) ELSE CAST(
          sales AS DECIMAL(16, 5)
        )/ NULLIF(
          CAST(
            toasset AS DECIMAL(16, 5)
          ), 
          0
        ) END AS sales_assets_andersen_fcit,
      
        CASE WHEN toasset - (c98 + c99) < 0 THEN CAST(
          cuasset - short_term_investment - c80 - c81 - c82 AS DECIMAL(16, 5)
        )/ NULLIF(
          CAST(
            toasset + ABS(
              toasset - (c98 + c99)
            ) AS DECIMAL(16, 5)
          ), 
          0
        ) ELSE CAST(
          cuasset - short_term_investment - c80 - c81 - c82 AS DECIMAL(16, 5)
        )/ NULLIF(
          CAST(
            toasset AS DECIMAL(16, 5)
          ), 
          0
        ) END AS cash_over_totasset_fcit, 
      
        CAST(
          tofixed - c92 AS DECIMAL(16, 5)
        ) AS asset_tangibility_fcit 
      FROM 
        test 
      WHERE 
        year in (
          '2001', '2002', '2003', '2004', '2005', 
          '2006', '2007'
        )
    ) 
    SELECT 
      ratio.firm, 
      ratio.year, 
      CASE WHEN ratio.year in (
        '2001', '2002', '2003', '2004', '2005'
      ) THEN 'FALSE' WHEN ratio.year in ('2006', '2007') THEN 'TRUE' END AS period, 
      ratio.cic, 
      indu_2, 
      CASE WHEN short IS NULL THEN 'Unknown' ELSE short END AS short, 
      ratio.geocode4_corr, 
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
      output, 
      employment, 
      capital, 
      sales, 
      total_asset, 
      credit_constraint, 
      asset_tangibility_fcit, 
      cash_over_totasset_fcit, 
      sales_assets_andersen_fcit, 
      return_on_asset_fcit, 
      liabilities_assets_fcit, 
      quick_ratio_fcit, 
      current_ratio_fcit, 
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
          firm, 
          COUNT(
            DISTINCT(geocode4_corr)
          ) AS count_city 
        FROM 
          ratio 
        GROUP BY 
          firm
      ) as multi_cities ON ratio.firm = multi_cities.firm 
      INNER JOIN (
        SELECT 
          firm, 
          COUNT(
            DISTINCT(ownership)
          ) AS count_ownership 
        FROM 
          ratio 
        GROUP BY 
          firm
      ) as multi_ownership ON ratio.firm = multi_ownership.firm 
      INNER JOIN (
        SELECT 
          firm, 
          COUNT(
            DISTINCT(cic)
          ) AS count_industry 
        FROM 
          ratio 
        GROUP BY 
          firm
      ) as multi_industry ON ratio.firm = multi_industry.firm 
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
      LEFT JOIN (
        SELECT 
          cic, 
          financial_dep_china AS credit_constraint 
        FROM 
          industry.china_credit_constraint
      ) as cred_constraint ON ratio.indu_2 = cred_constraint.cic 
    WHERE 
      count_ownership = 1 
      AND count_city = 1 
      AND count_industry = 1 
      AND output > 0 
      and capital > 0 
      and employment > 0 
      AND ratio.indu_2 != '43' 
      AND total_asset IS NOT NULL
      AND asset_tangibility_fcit IS NOT NULL
      AND cash_over_totasset_fcit IS NOT NULL 
      AND sales_assets_andersen_fcit IS NOT NULL 
      AND return_on_asset_fcit IS NOT NULL
      AND liabilities_assets_fcit IS NOT NULL 
      AND quick_ratio_fcit IS NOT NULL
      AND current_ratio_fcit IS NOT NULL
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

We also need to compute the sectors constraint vs not constraint. We use `fin dep` indicator to classify the sectors. If the value is above the median, then the sector is constraint else it is not.  

We know that this value will never change, so we can compute it manually and pass the value in the query

The median is -.47 and the average -.57

```python
query ="""
SELECT
         approx_percentile(financial_dep_china, .5) as median,
         AVG(financial_dep_china)
         FROM industry.china_credit_constraint
"""
output = s3.run_query(
                    query=query,
                    database=DatabaseName,
                    s3_output=s3_output_example,
    filename = 'example_4'
                )
output
```

# Table `asif_financial_ratio_baseline_firm`

Since the table to create has missing value, please use the following at the top of the query

```
CREATE TABLE database.table_name WITH (format = 'PARQUET') AS
```


Choose a location in S3 to save the CSV. It is recommended to save in it the `datalake-datascience` bucket. Locate an appropriate folder in the bucket, and make sure all output have the same format

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
    c98 + c99 as total_asset, 
    CASE WHEN c79 IS NULL THEN 0 ELSE c79 END AS short_term_investment 
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
    WITH ratio AS (
      SELECT 
        firm, 
        year, 
        cic, 
        indu_2, 
        geocode4_corr, 
        ownership,
        CASE WHEN ownership = 'SOE' THEN 'SOE' ELSE 'PRIVATE' END AS soe_vs_pri,
        CASE WHEN ownership in (
        'HTM', 'FOREIGN'
      ) THEN 'FOREIGN' ELSE 'DOMESTIC' END AS for_vs_dom,
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
        CAST(
          toasset AS DECIMAL(16, 5)
        ) AS total_asset, 
        CAST(
          cuasset AS DECIMAL(16, 5)
        ) / NULLIF(
          CAST(
            c95 AS DECIMAL(16, 5)
          ), 
          0
        ) AS current_ratio_fcit,
      
        CAST(
          cuasset - short_term_investment - c80 - c81 AS DECIMAL(16, 5)
        ) / NULLIF(
          CAST(
            c95 AS DECIMAL(16, 5)
          ), 
          0
        ) AS quick_ratio_fcit, 
        -- Need to add asset or debt when bs requirement not meet
        CASE WHEN toasset - (c98 + c99) < 0 THEN CAST(
          c95 + c97 AS DECIMAL(16, 5)
        )/ NULLIF(
          CAST(
            toasset + ABS(
              toasset - (c98 + c99)
            ) AS DECIMAL(16, 5)
          ), 
          0
        ) WHEN toasset - (c98 + c99) > 0 THEN CAST(
          c95 + c97 + toasset - (c98 + c99) AS DECIMAL(16, 5)
        )/ NULLIF(
          CAST(
            toasset AS DECIMAL(16, 5)
          ), 
          0
        ) ELSE CAST(
          c95 + c97 AS DECIMAL(16, 5)
        )/ NULLIF(
          CAST(
            toasset AS DECIMAL(16, 5)
          ), 
          0
        ) END AS liabilities_assets_fcit, 
        CASE WHEN toasset - (c98 + c99) < 0 THEN CAST(
          sales - (
            c108 + c113 + c114 + c116 + c118 + c124 + wage
          ) AS DECIMAL(16, 5)
        )/ NULLIF(
          CAST(
            toasset + ABS(
              toasset - (c98 + c99)
            ) AS DECIMAL(16, 5)
          ), 
          0
        ) ELSE CAST(
          sales - (
            c108 + c113 + c114 + c116 + c118 + c124 + wage
          ) AS DECIMAL(16, 5)
        )/ NULLIF(
          CAST(
            toasset AS DECIMAL(16, 5)
          ), 
          0
        ) END AS return_on_asset_fcit, 
      
        CASE WHEN toasset - (c98 + c99) < 0 THEN CAST(
          sales AS DECIMAL(16, 5)
        )/ NULLIF(
          CAST(
            toasset + ABS(
              toasset - (c98 + c99)
            ) AS DECIMAL(16, 5)
          ), 
          0
        ) ELSE CAST(
          sales AS DECIMAL(16, 5)
        )/ NULLIF(
          CAST(
            toasset AS DECIMAL(16, 5)
          ), 
          0
        ) END AS sales_assets_andersen_fcit,
      
        CASE WHEN toasset - (c98 + c99) < 0 THEN CAST(
          cuasset - short_term_investment - c80 - c81 - c82 AS DECIMAL(16, 5)
        )/ NULLIF(
          CAST(
            toasset + ABS(
              toasset - (c98 + c99)
            ) AS DECIMAL(16, 5)
          ), 
          0
        ) ELSE CAST(
          cuasset - short_term_investment - c80 - c81 - c82 AS DECIMAL(16, 5)
        )/ NULLIF(
          CAST(
            toasset AS DECIMAL(16, 5)
          ), 
          0
        ) END AS cash_over_totasset_fcit, 
      
        CAST(
          tofixed - c92 AS DECIMAL(16, 5)
        ) AS asset_tangibility_fcit 
      FROM 
        test 
      WHERE 
        year in (
          '2001', '2002', '2003', '2004', '2005', 
          '2006', '2007'
        )
    ) 
    SELECT 
      ratio.firm, 
      ratio.year, 
      CASE WHEN ratio.year in (
        '2001', '2002', '2003', '2004', '2005'
      ) THEN 'FALSE' WHEN ratio.year in ('2006', '2007') THEN 'TRUE' END AS period, 
      ratio.cic, 
      indu_2, 
      CASE WHEN short IS NULL THEN 'Unknown' ELSE short END AS short, 
      ratio.geocode4_corr, 
      CASE WHEN tcz IS NULL THEN '0' ELSE tcz END AS tcz, 
      CASE WHEN spz IS NULL 
      OR spz = '#N/A' THEN '0' ELSE spz END AS spz, 
      ratio.ownership, 
      soe_vs_pri, 
      for_vs_dom, 
      tso2_mandate_c, 
      in_10_000_tonnes, 
      output, 
      employment, 
      capital, 
      sales, 
      total_asset, 
      credit_constraint, 
      d_credit_constraint,
      asset_tangibility_fcit, 
      cash_over_totasset_fcit, 
      sales_assets_andersen_fcit, 
      return_on_asset_fcit, 
      liabilities_assets_fcit, 
      quick_ratio_fcit, 
      current_ratio_fcit, 
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
          firm, 
          COUNT(
            DISTINCT(geocode4_corr)
          ) AS count_city 
        FROM 
          ratio 
        GROUP BY 
          firm
      ) as multi_cities ON ratio.firm = multi_cities.firm 
      INNER JOIN (
        SELECT 
          firm, 
          COUNT(
            DISTINCT(ownership)
          ) AS count_ownership 
        FROM 
          ratio 
        GROUP BY 
          firm
      ) as multi_ownership ON ratio.firm = multi_ownership.firm 
      INNER JOIN (
        SELECT 
          firm, 
          COUNT(
            DISTINCT(cic)
          ) AS count_industry 
        FROM 
          ratio 
        GROUP BY 
          firm
      ) as multi_industry ON ratio.firm = multi_industry.firm -- Pollution
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
      LEFT JOIN (
        SELECT 
          cic, 
          financial_dep_china AS credit_constraint,
          CASE WHEN financial_dep_china > -.47 THEN 'ABOVE' ELSE 'BELOW' END AS d_credit_constraint
        FROM 
          industry.china_credit_constraint
      ) as cred_constraint ON ratio.indu_2 = cred_constraint.cic 
    WHERE 
      count_ownership = 1 
      AND count_city = 1 
      AND count_industry = 1 
      AND output > 0 
      and capital > 0 
      and employment > 0 
      AND ratio.indu_2 != '43' 
      AND total_asset IS NOT NULL
      AND asset_tangibility_fcit > 0
      AND cash_over_totasset_fcit > 0
      AND sales_assets_andersen_fcit IS NOT NULL 
      AND return_on_asset_fcit IS NOT NULL
      AND liabilities_assets_fcit > 0
      AND quick_ratio_fcit > 0
      AND current_ratio_fcit > 0
      
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

2. Add the steps number

```python
step = 5
```

3. Change the schema

Bear in mind that CSV SerDe (OpenCSVSerDe) does not support empty fields in columns defined as a numeric data type. All columns with missing values should be saved as string. 

```python
glue.get_table_information(
    database = DatabaseName,
    table = table_name)['Table']['StorageDescriptor']['Columns']
```

```python
schema = [{'Name': 'firm', 'Type': 'string', 'Comment': 'Firms ID'},
          {'Name': 'year', 'Type': 'string', 'Comment': ''},
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
           'Type': 'varchar(8)', 'Comment': ' FOREIGN vs DOMESTICT if ownership is HTM then FOREIGN'},
          {'Name': 'tso2_mandate_c', 'Type': 'float',
           'Comment': 'city reduction mandate in tonnes'},
          {'Name': 'in_10_000_tonnes', 'Type': 'float',
           'Comment': 'city reduction mandate in 10k tonnes'},
          {'Name': 'output', 'Type': 'decimal(16,5)', 'Comment': 'Output'},
          {'Name': 'employment',
              'Type': 'decimal(16,5)', 'Comment': 'employment'},
          {'Name': 'capital', 'Type': 'decimal(16,5)', 'Comment': 'capital'},
          {'Name': 'sales', 'Type': 'decimal(16,5)', 'Comment': 'sales'},
          {'Name': 'total_asset',
              'Type': 'decimal(16,5)', 'Comment': 'Total asset'},
          {'Name': 'credit_constraint', 'Type': 'float',
           'Comment': 'Financial dependency. From paper https://www.sciencedirect.com/science/article/pii/S0147596715000311'},
          {'Name': 'd_credit_constraint',
           'Type': 'varchar(5)', 'Comment': 'Sectors financially dependant when above median'},
          {'Name': 'asset_tangibility_fcit',
           'Type': 'decimal(16,5)', 'Comment': 'Total fixed assets - Intangible assets'},
          {'Name': 'cash_over_totasset_fcit',
           'Type': 'decimal(21,5)', 'Comment': 'cuasset - short_term_investment - c80 - c81 - c82 divided by toasset'},
          {'Name': 'sales_assets_andersen_fcit',
           'Type': 'decimal(21,5)',
           'Comment': 'Sales divided by total asset'},
          {'Name': 'return_on_asset_fcit',
           'Type': 'decimal(21,5)', 'Comment': 'sales - (主营业务成本 (c108) + 营业费用 (c113) + 管理费用 (c114) + 财产保险费 (c116) + 劳动、失业保险费 (c118)+ 财务费用 (c124) + 本年应付工资总额 (wage)) /toasset'},
          {'Name': 'liabilities_assets_fcit',
           'Type': 'decimal(21,5)', 'Comment': '(流动负债合计 (c95) + 长期负债合计 (c97)) / toasset'},
          {'Name': 'quick_ratio_fcit',
           'Type': 'decimal(21,5)', 'Comment': '(cuasset-存货 (c81) ) / 流动负债合计 (c95)'},
          {'Name': 'current_ratio_fcit',
           'Type': 'decimal(21,5)', 'Comment': 'cuasset/流动负债合计 (c95)'},
          {'Name': 'fe_c_i', 'Type': 'bigint',
              'Comment': 'City industry fixed effect'},
          {'Name': 'fe_t_i', 'Type': 'bigint',
              'Comment': 'year industry fixed effect'},
          {'Name': 'fe_c_t', 'Type': 'bigint', 'Comment': 'city industry fixed effect'}]
```

4. Provide a description

```python
description = """
Transform asif firms prepared data by merging china tcz spz, china city reduction mandate, china city code normalised, china credit constraint, ind cic 2 name data by constructing financial ratio, city-industry FE, city-year FE, industry-year FE, soe_vs_private, foreign_vs_domestic (add firms ownership soe and private and domestic and foreign, keep capital, output, sales and employment) to asif financial ratio baseline firm
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
