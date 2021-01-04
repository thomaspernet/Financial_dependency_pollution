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

Transform china city reduction mandate and others data by merging china city reduction mandate and others data by constructing city-industry FE and others (add city-level) to fin dep pollution baseline city 

# Business needs 

Transform china city reduction mandate and others data by merging china city reduction mandate, china tcz spz, china city code normalised, ind cic 2 name, china credit constraint, asif industry financial ratio city data by constructing city-industry FE, industry-year FE, city-year FE, period (Add city-industry financial ratio level) to fin dep pollution baseline city 

## Description
### Objective 

Use existing tables china city reduction mandate, china tcz spz, china city code normalised, ind cic 2 name, china credit constraint, asif industry financial ratio city, asif firms prepared to constructing a bunch of variables listed below

# Construction variables 

* city-industry FE
* industry-year FE
* city-year FE
* period

### Steps 




**Cautious**
Make sure there is no duplicates

# Target

* The file is saved in S3:
  * bucket: datalake-datascience
  * path: DATA/ENVIRONMENT/CHINA/FYP/FINANCIAL_CONTRAINT/PAPER_FYP_FINANCE_POL/BASELINE/CITY
* Glue data catalog should be updated
  * database:environment
  * Table prefix:fin_dep_pollution_baseline_
  * table name:fin_dep_pollution_baseline_city
* Analytics
  * HTML: ANALYTICS/HTML_OUTPUT/fin_dep_pollution_baseline_city
  * Notebook: ANALYTICS/OUTPUT/fin_dep_pollution_baseline_city

# Metadata
* Key: rkb30odtj11654r
* Parent key (for update parent):  
* Epic: Dataset transformation
* US: Baseline
* Task tag: #baseline-table,#city-level
* Notebook US Parent (i.e the one to update): 
https://github.com/thomaspernet/Financial_dependency_pollution/blob/master/01_data_preprocessing/02_transform_tables/04_fin_dep_pol_baseline_city.md
* Reports: https://htmlpreview.github.io/?https://github.com/thomaspernet/Financial_dependency_pollution/blob/master/01_data_preprocessing/02_transform_tables/Reports/04_fin_dep_pol_baseline_city.html
* Analytics reports:
https://htmlpreview.github.io/?https://github.com/thomaspernet/Financial_dependency_pollution/blob/master/00_data_catalogue/HTML_ANALYSIS/FIN_DEP_POLLUTION_BASELINE_CITY.html

# Input Cloud Storage [AWS/GCP]

## Table/file
* Name: 
* china_city_reduction_mandate
* china_tcz_spz
* china_city_code_normalised
* ind_cic_2_name
* china_credit_constraint
* asif_industry_financial_ratio_city
* asif_firms_prepared
* Github: 
  * https://github.com/thomaspernet/Financial_dependency_pollution/blob/master/01_data_preprocessing/00_download_data_from/CITY_REDUCTION_MANDATE/city_reduction_mandate.py
  * https://github.com/thomaspernet/Financial_dependency_pollution/blob/master/01_data_preprocessing/00_download_data_from/TCZ_SPZ/tcz_spz_policy.py
  * https://github.com/thomaspernet/Financial_dependency_pollution/blob/master/01_data_preprocessing/00_download_data_from/CITY_CODE_CORRESPONDANCE/city_code_correspondance.py
  * https://github.com/thomaspernet/Financial_dependency_pollution/blob/master/01_data_preprocessing/00_download_data_from/CIC_NAME/cic_industry_name.py
  * https://github.com/thomaspernet/Financial_dependency_pollution/blob/master/01_data_preprocessing/00_download_data_from/CIC_CREDIT_CONSTRAINT/financial_dependency.py
  * https://github.com/thomaspernet/Financial_dependency_pollution/blob/master/01_data_preprocessing/02_transform_tables/03_asif_financial_ratio_city.md
  * https://github.com/thomaspernet/Financial_dependency_pollution/blob/master/01_data_preprocessing/01_prepare_tables/00_prepare_asif.md

# Destination Output/Delivery
## Table/file
* Name:
* fin_dep_pollution_baseline_city
* GitHub:
* https://github.com/thomaspernet/Financial_dependency_pollution/blob/master/01_data_preprocessing/02_transform_tables/04_fin_dep_pol_baseline_city.md
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

```python
DatabaseName = 'environment'
s3_output_example = 'SQL_OUTPUT_ATHENA'
```

```python
query = """
WITH aggregate_pol AS (
  SELECT 
    year, 
    geocode4_corr, 
    cityen, 
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
        year, 
        aggregate_pol.geocode4_corr, 
        cityen, 
        ind2, 
        tso2, 
        tso2_mandate_c,
        in_10_000_tonnes
        lower_location, 
        larger_location, 
        coastal 
      FROM 
        aggregate_pol 
        INNER JOIN firms_survey.asif_industry_financial_ratio_city ON 
        aggregate_pol.ind2 = asif_industry_financial_ratio_city.indu_2 AND 
        aggregate_pol.geocode4_corr = asif_industry_financial_ratio_city.geocode4_corr 
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
          ind2, 
          COUNT(*) AS CNT 
        FROM 
          merge_asif 
        GROUP BY 
          geocode4_corr, 
          year, 
          ind2
      ) AS count_dup 
    GROUP BY 
      CNT
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

# Table `fin_dep_pollution_baseline_city`

Since the table to create has missing value, please use the following at the top of the query

```
CREATE TABLE database.table_name WITH (format = 'PARQUET') AS
```


Choose a location in S3 to save the CSV. It is recommended to save in it the `datalake-datascience` bucket. Locate an appropriate folder in the bucket, and make sure all output have the same format

```python
s3_output = 'DATA/ENVIRONMENT/CHINA/FYP/FINANCIAL_CONTRAINT/PAPER_FYP_FINANCE_POL/BASELINE/CITY'
table_name = 'fin_dep_pollution_baseline_city'
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
CREATE TABLE {0}.{1} WITH (format = 'PARQUET') AS WITH aggregate_pol AS (
SELECT 
    year, 
    geocode4_corr, 
    provinces, 
    cityen, 
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
    ind2, 
    lower_location, 
    larger_location, 
    coastal
) 
SELECT 
  aggregate_pol.year, 
  CASE WHEN aggregate_pol.year in (
    '2001', '2002', '2003', '2004', '2005'
  ) THEN 'FALSE' WHEN aggregate_pol.year in ('2006', '2007') THEN 'TRUE' END AS period, 
  provinces, 
  cityen, 
  aggregate_pol.geocode4_corr, 
  CASE WHEN tcz IS NULL THEN '0' ELSE tcz END AS tcz, 
  CASE WHEN spz IS NULL OR spz = '#N/A' THEN '0' ELSE spz END AS spz, 
  aggregate_pol.ind2, 
  CASE WHEN short IS NULL THEN 'Unknown' ELSE short END AS short, 
  polluted_di, 
  polluted_mi, 
  polluted_mei, 
  tso2, 
  CAST(
    tso2 AS DECIMAL(16, 5)
  ) / CAST(
    output AS DECIMAL(16, 5)
  ) AS so2_intensity, 
  tso2_mandate_c, 
  in_10_000_tonnes, 
  CAST(
    output AS DECIMAL(16, 5)
  ) AS output, 
  CAST(
    employment AS DECIMAL(16, 5)
  ) AS employment, 
  CAST(
    sales AS DECIMAL(16, 5)
  ) AS sales, 
  CAST(
    capital AS DECIMAL(16, 5)
  ) AS capital, 
  credit_constraint, 
  receivable_curasset_ci,
  std_receivable_curasset_ci,
  cash_over_curasset_ci,
  std_cash_over_curasset_ci,
  working_capital_ci,
  std_working_capital_ci,
  working_capital_requirement_ci,
  std_working_capital_requirement_ci,
  current_ratio_ci,
  std_current_ratio_ci,
  quick_ratio_ci,
  std_quick_ratio_ci,
  cash_ratio_ci,
  std_cash_ratio_ci,
  liabilities_assets_ci,
  std_liabilities_assets_ci,
  return_on_asset_ci,
  std_return_on_asset_ci,
  sales_assets_ci,
  std_sales_assets_ci,
  account_paybable_to_asset_ci,
  std_account_paybable_to_asset_ci,
  asset_tangibility_ci,
  std_asset_tangibility_ci,
  rd_intensity_ci,
  std_rd_intensity_ci,
  inventory_to_sales_ci,
  std_inventory_to_sales_ci,
  lower_location, 
  larger_location, 
  coastal, 
  DENSE_RANK() OVER (
    ORDER BY 
      city_mandate.geocode4_corr, 
      aggregate_pol.ind2
  ) AS fe_c_i, 
  DENSE_RANK() OVER (
    ORDER BY 
      aggregate_pol.year, 
      aggregate_pol.ind2
  ) AS fe_t_i, 
  DENSE_RANK() OVER (
    ORDER BY 
      city_mandate.geocode4_corr, 
      aggregate_pol.year
  ) AS fe_c_t 
FROM 
  aggregate_pol 
  INNER JOIN firms_survey.asif_industry_financial_ratio_city 
  ON aggregate_pol.ind2 = asif_industry_financial_ratio_city.indu_2 
  AND aggregate_pol.geocode4_corr = asif_industry_financial_ratio_city.geocode4_corr 
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
  ) as city_mandate ON aggregate_pol.geocode4_corr = city_mandate.geocode4_corr 
  LEFT JOIN policy.china_city_tcz_spz ON aggregate_pol.geocode4_corr = china_city_tcz_spz.geocode4_corr 
  LEFT JOIN chinese_lookup.ind_cic_2_name ON aggregate_pol.ind2 = ind_cic_2_name.cic 
  LEFT JOIN (
    SELECT 
      ind2, 
      polluted_di, 
      polluted_mi, 
      polluted_mei
    FROM 
      "environment"."china_sector_pollution_threshold" 
    WHERE 
      year = '2001'
  ) as polluted_sector ON aggregate_pol.ind2 = polluted_sector.ind2 
  LEFT JOIN (
    SELECT 
      cic, 
      financial_dep_china AS credit_constraint 
    FROM 
      industry.china_credit_constraint
  ) as cred_constraint ON aggregate_pol.ind2 = cred_constraint.cic 
  LEFT JOIN (
    SELECT 
      year, 
      geocode4_corr, 
      indu_2, 
      SUM(output) AS output, 
      SUM(employ) AS employment, 
      SUM(captal) AS capital,
      SUM(sales) as sales
    FROM 
      (
        SELECT 
          year, 
          geocode4_corr, 
          CASE WHEN LENGTH(cic) = 4 THEN substr(cic, 1, 2) ELSE concat(
            '0', 
            substr(cic, 1, 1)
          ) END AS indu_2, 
          output, 
          employ, 
          captal,
          sales
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
    WHERE 
      year in (
        '2001', '2002', '2003', '2004', '2005', 
        '2006', '2007'
      ) 
    GROUP BY 
      geocode4_corr, 
      indu_2, 
      year
  ) as agg_output ON aggregate_pol.geocode4_corr = agg_output.geocode4_corr 
  AND aggregate_pol.ind2 = agg_output.indu_2 
  AND aggregate_pol.year = agg_output.year 
WHERE 
  tso2 > 4863 
  AND output > 0 
  and capital > 0 
  and employment > 0
  AND aggregate_pol.ind2 != '43'

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

## Check industry vs city

We need to make sure both tables, industry and city have the same number of observations. We need to compare:

- year
- geocode4_corr
- indu_2

If there are differences, the reasons comes from missing pair city-industry during year 2004-2006

```python
query_count = """
SELECT year, geocode4_corr, ind2
FROM fin_dep_pollution_baseline_industry
""".format(DatabaseName, table_name)
df_industry = s3.run_query(
                    query=query_count,
                    database=DatabaseName,
                    s3_output=s3_output_example,
    filename = 'count_{}'.format(table_name)
                )
df_industry.head()
```

```python
df_industry.shape
```

```python
query_count = """
SELECT year, geocode4_corr, ind2
FROM fin_dep_pollution_baseline_city
""".format(DatabaseName, table_name)
df_city = s3.run_query(
                    query=query_count,
                    database=DatabaseName,
                    s3_output=s3_output_example,
    filename = 'count_{}'.format(table_name)
                )
df_city.head()
```

```python
df_city.shape
```

There are 150 missing pairs city-industry

```python
(
    (
        df_industry
        .set_index(['year', 'geocode4_corr', 'ind2'])
    ).merge(
        df_city.set_index(['year', 'geocode4_corr', 'ind2']),
        left_index=True, right_index=True, indicator=True, how='left'
    )
    .loc[lambda x: ~x['_merge'].isin(['both'])]
)
```

Let's check the example for city "2109" in industry "42" -> No value displays because missing 

```python
query_example = """
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
    FROM test
    WHERE year in ('2004', '2005', '2006') and geocode4_corr = '2109' and indu_2 = '42'
"""
output = s3.run_query(
                    query=query_example,
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
partition_keys = ["geocode4_corr", "year", "ind2"]
```

2. Add the steps number

```python
step = 3
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
 {'Name': 'period', 'Type': 'varchar(5)', 'Comment': 'False if year before 2005 included, True if year 2006 and 2007'},
 {'Name': 'provinces', 'Type': 'string', 'Comment': ''},
 {'Name': 'cityen', 'Type': 'string', 'Comment': ''},
 {'Name': 'geocode4_corr', 'Type': 'string', 'Comment': ''},
 {'Name': 'tcz', 'Type': 'string', 'Comment': 'Two control zone policy city'},
 {'Name': 'spz', 'Type': 'string', 'Comment': 'Special policy zone policy city'},
 {'Name': 'ind2', 'Type': 'string', 'Comment': '2 digits industry'},
 {'Name': 'short', 'Type': 'string', 'Comment': ''},
 {'Name': 'polluted_di', 'Type': 'varchar(5)', 'Comment': 'Sectors with values above Yearly 75th percentile of SO2 label as ABOVE else BELOW'},
 {'Name': 'polluted_mi', 'Type': 'varchar(5)', 'Comment': 'Sectors with values above Yearly average of SO2 label as ABOVE else BELOW'},
 {'Name': 'polluted_mei', 'Type': 'varchar(5)', 'Comment': 'Sectors with values above Yearly median of SO2 label as ABOVE else BELOW'},
 {'Name': 'tso2', 'Type': 'bigint', 'Comment': 'Total so2 city sector. Filtered values above  4863 (5% of the distribution)'},
 {'Name': 'so2_intensity', 'Type': 'decimal(21,5)', 'Comment': 'SO2 divided by output'},
 {'Name': 'tso2_mandate_c', 'Type': 'float', 'Comment': 'city reduction mandate in tonnes'},
 {'Name': 'in_10_000_tonnes', 'Type': 'float', 'Comment': 'city reduction mandate in 10k tonnes'},
 {'Name': 'output', 'Type': 'decimal(16,5)', 'Comment': 'Output'},
 {'Name': 'employment', 'Type': 'decimal(16,5)', 'Comment': 'Employemnt'},
 {'Name': 'sales', 'Type': 'decimal(16,5)', 'Comment': 'Sales'},
 {'Name': 'capital', 'Type': 'decimal(16,5)', 'Comment': 'Capital'},
 {'Name': 'credit_constraint', 'Type': 'float', 'Comment': 'Financial dependency. From paper https://www.sciencedirect.com/science/article/pii/S0147596715000311'},
 {'Name': 'receivable_curasset_ci', 'Type': 'double', 'Comment': '应收帐款 (c80) / cuasset'},
 {'Name': 'std_receivable_curasset_ci', 'Type': 'double', 'Comment': 'standaridzed values (x - x mean) / std)'},
 {'Name': 'cash_over_curasset_ci', 'Type': 'double', 'Comment': '(其中：短期投资 (c79) - 应收帐款 (c80) - 存货 (c81) - 其中：产成品 (c82)) /current asset'},
 {'Name': 'std_cash_over_curasset_ci', 'Type': 'double', 'Comment': 'standaridzed values (x - x mean) / std)'},
 {'Name': 'working_capital_ci', 'Type': 'double', 'Comment': 'cuasset- 流动负债合计 (c95)'},
 {'Name': 'std_working_capital_ci', 'Type': 'double', 'Comment': 'standaridzed values (x - x mean) / std)'},
 {'Name': 'working_capital_requirement_ci', 'Type': 'double', 'Comment': '存货 (c81) + 应收帐款 (c80) - 应付帐款  (c96)'},
 {'Name': 'std_working_capital_requirement_ci',
  'Type': 'double',
  'Comment': 'standaridzed values (x - x mean) / std)'},
 {'Name': 'current_ratio_ci', 'Type': 'double', 'Comment': 'cuasset/流动负债合计 (c95)'},
 {'Name': 'std_current_ratio_ci', 'Type': 'double', 'Comment': 'standaridzed values (x - x mean) / std)'},
 {'Name': 'quick_ratio_ci', 'Type': 'double', 'Comment': '(cuasset-存货 (c81) ) / 流动负债合计 (c95)'},
 {'Name': 'std_quick_ratio_ci', 'Type': 'double', 'Comment': 'standaridzed values (x - x mean) / std)'},
 {'Name': 'cash_ratio_ci', 'Type': 'double', 'Comment': '1 - (cuasset -  其中：短期投资 (c79) - 应收帐款 (c80) - 存货 (c81)/ 流动负债合计 (c95)'},
 {'Name': 'std_cash_ratio_ci', 'Type': 'double', 'Comment': 'standaridzed values (x - x mean) / std)'},
 {'Name': 'liabilities_assets_ci', 'Type': 'double', 'Comment': '1- (流动负债合计 (c95) + 长期负债合计 (c97)) / toasset'},
 {'Name': 'std_liabilities_assets_ci', 'Type': 'double', 'Comment': 'standaridzed values (x - x mean) / std)'},
 {'Name': 'return_on_asset_ci', 'Type': 'double', 'Comment': 'sales - (主营业务成本 (c108) + 营业费用 (c113) + 管理费用 (c114) + 财产保险费 (c116) + 劳动、失业保险费 (c118)+ 财务费用 (c124) + 本年应付工资总额 (wage)) /toasset'},
 {'Name': 'std_return_on_asset_ci', 'Type': 'double', 'Comment': 'standaridzed values (x - x mean) / std)'},
 {'Name': 'sales_assets_ci', 'Type': 'double', 'Comment': '全年营业收入合计 (c64) /(\Delta toasset/2)'},
 {'Name': 'std_sales_assets_ci', 'Type': 'double', 'Comment': 'standaridzed values (x - x mean) / std)'},
 {'Name': 'account_paybable_to_asset_ci', 'Type': 'double', 'Comment': '(\Delta 应付帐款  (c96))/ (\Delta (toasset))'},
 {'Name': 'std_account_paybable_to_asset_ci', 'Type': 'double', 'Comment': 'standaridzed values (x - x mean) / std)'},
 {'Name': 'asset_tangibility_ci', 'Type': 'double', 'Comment': 'Total fixed assets - Intangible assets'},
 {'Name': 'std_asset_tangibility_ci', 'Type': 'double', 'Comment': 'standaridzed values (x - x mean) / std)'},
 {'Name': 'rd_intensity_ci', 'Type': 'double', 'Comment': 'rdfee/全年营业收入合计 (c64)'},
 {'Name': 'std_rd_intensity_ci', 'Type': 'double', 'Comment': 'standaridzed values (x - x mean) / std)'},
 {'Name': 'inventory_to_sales_ci', 'Type': 'double', 'Comment': '存货 (c81) / sales'},
 {'Name': 'std_inventory_to_sales_ci', 'Type': 'double', 'Comment': 'standaridzed values (x - x mean) / std)'},
 {'Name': 'lower_location', 'Type': 'string', 'Comment': 'Location city. one of Coastal, Central, Northwest, Northeast, Southwest'},
 {'Name': 'larger_location', 'Type': 'string', 'Comment': 'Location city. one of Eastern, Central, Western'},
 {'Name': 'coastal', 'Type': 'string', 'Comment': 'City is bordered by sea or not'},
 {'Name': 'fe_c_i', 'Type': 'bigint', 'Comment': 'City industry fixed effect'},
 {'Name': 'fe_t_i', 'Type': 'bigint', 'Comment': 'year industry fixed effect'},
 {'Name': 'fe_c_t', 'Type': 'bigint', 'Comment': 'city industry fixed effect'}]
```

4. Provide a description

```python
description = """
Transform (creating time-break variables and fixed effect) asif_financial_ratio data and merging pollution, industry and city mandate tables
with financial ratio at the city level
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
partition_keys = ["geocode4_corr", "year", "ind2"]

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
            table_name_git = schema['metadata']['TableName']
        
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
create_report(extension = "html", keep_code = True, notebookname = "04_fin_dep_pol_baseline_city.ipynb")
```
