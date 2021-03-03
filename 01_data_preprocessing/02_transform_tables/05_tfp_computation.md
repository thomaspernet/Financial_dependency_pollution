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

<!-- #region kernel="SoS" -->
# Transform asif firms prepared data by constructing tfp_OP_f (estimate op TFP) to asif tfp firm level

# US Name

Transform asif firms prepared data by constructing tfp_OP_f (estimate op TFP) to asif tfp firm level 

# Business needs 

Transform asif firms prepared data by constructing tfp_OP_f (Estimate TFP using Olley and Pakes approach) to asif tfp firm level 

## Description
### Objective 

Use existing table asif firms prepared to construct ZZ

# Construction variables 

* tfp_OP_f

### Steps 

1. Remove outliers
2. Remove firm with different:
  1. ownership, cities and industries over time
3. Compute TFP using OP methodology


**Cautious**
Make sure there is no duplicates

# Target

* The file is saved in S3:
  * bucket: datalake-datascience
  * path: DATA/ECON/FIRM_SURVEY/ASIF_CHINA/TRANSFORMED/TFP/FIRM_LEVEL
* Glue data catalog should be updated
  * database:firms_survey
  * Table prefix:asif_tfp_
  * table name:asif_tfp_firm_level
* Analytics
  * HTML: ANALYTICS/HTML_OUTPUT/asif_tfp_firm_level
  * Notebook: ANALYTICS/OUTPUT/asif_tfp_firm_level

# Metadata

* Key: qdy59wtof20713d
* Parent key (for update parent):  
* Epic: Dataset transformation
* US: tfp_computation
* Task tag: #tfp,#productivity,#r_instance,#firm
* Notebook US Parent (i.e the one to update): 
https://github.com/thomaspernet/Financial_dependency_pollution/blob/master/01_data_preprocessing/02_transform_tables/05_tfp_computation.md
* Reports: https://htmlpreview.github.io/?https://github.com/thomaspernet/Financial_dependency_pollution/blob/master/01_data_preprocessing/02_transform_tables/Reports/05_tfp_computation.html
* Analytics reports:
https://htmlpreview.github.io/?https://github.com/thomaspernet/Financial_dependency_pollution/blob/master/00_data_catalogue/HTML_ANALYSIS/ASIF_TFP_FIRM_LEVEL.html

# Input Cloud Storage [AWS/GCP]

## Table/file
* Name: 
* asif_firms_prepared
* Github: 
  * https://github.com/thomaspernet/Financial_dependency_pollution/blob/master/01_data_preprocessing/01_prepare_tables/00_prepare_asif.md

# Destination Output/Delivery
## Table/file
* Name:
* asif_tfp_firm_level
* GitHub:
* https://github.com/thomaspernet/Financial_dependency_pollution/blob/master/01_data_preprocessing/02_transform_tables/05_tfp_computation.md
<!-- #endregion -->
```python inputHidden=false jupyter={"outputs_hidden": false} outputHidden=false kernel="python3"
from awsPy.aws_authorization import aws_connector
from awsPy.aws_s3 import service_s3
from awsPy.aws_glue import service_glue
from pathlib import Path
import pandas as pd
#import numpy as np
import seaborn as sns
import os, shutil, json

path = os.getcwd()
parent_path = str(Path(path).parent.parent)


name_credential = 'financial_dep_SO2_accessKeys.csv'
region = 'eu-west-3'
bucket = 'datalake-datascience'
path_cred = "{0}/creds/{1}".format(parent_path, name_credential)
```

```python inputHidden=false jupyter={"outputs_hidden": false} outputHidden=false kernel="python3"
con = aws_connector.aws_instantiate(credential = path_cred,
                                       region = region)
client= con.client_boto()
s3 = service_s3.connect_S3(client = client,
                      bucket = bucket, verbose = True) 
glue = service_glue.connect_glue(client = client) 
```

```python kernel="python3"
pandas_setting = True
if pandas_setting:
    cm = sns.light_palette("green", as_cmap=True)
    pd.set_option('display.max_columns', None)
    pd.set_option('display.max_colwidth', None)
```

<!-- #region kernel="SoS" -->
# Prepare query 

Write query and save the CSV back in the S3 bucket `datalake-datascience` 
<!-- #endregion -->

<!-- #region kernel="SoS" -->
# Steps

1. Remove outliers: Remove 5 and 95% of the firms's output, employ and captal by year
2. Remove when Input >= Output
3. Remove firm with different:
    - ownership, cities and industries over time

Variables needed:

- ID: `firm`
- year: `year`
- Output: `output`
- Employement: `employ`
- Capital: `captal`
- Input: `midput` 



<!-- #endregion -->

<!-- #region kernel="SoS" -->
## Example step by step
<!-- #endregion -->

```python kernel="python3"
DatabaseName = 'firms_survey'
s3_output_example = 'SQL_OUTPUT_ATHENA'
```

<!-- #region kernel="python3" -->
Check all firm ID are digits. 
<!-- #endregion -->

```python kernel="python3"
query= """
SELECT test, COUNT(test) as count
FROM (
SELECT regexp_like(firm, '[a-zA-Z]') as test
FROM "firms_survey"."asif_firms_prepared"
  )
  GROUP BY test
"""
output = s3.run_query(
                    query=query,
                    database=DatabaseName,
                    s3_output=s3_output_example,
    filename = 'example_1'
                )
output
```

<!-- #region kernel="python3" -->
Example firm with multiple cities, or ownerships or industries
<!-- #endregion -->

```python kernel="python3"
query = """
WITH test as (
  SELECT 
    firm, 
    asif_firms_prepared.year, 
    output, 
    employ, 
    captal, 
    midput, 
    ownership, 
    geocode4_corr, 
    cic 
  FROM 
    asif_firms_prepared 
    INNER JOIN (
      SELECT 
        year, 
        approx_percentile(output,.05) AS output_lower_bound, 
        approx_percentile(output,.98) AS output_upper_bound, 
        approx_percentile(employ,.05) AS employ_lower_bound, 
        approx_percentile(employ,.98) AS employ_upper_bound, 
        approx_percentile(captal,.05) AS captal_lower_bound, 
        approx_percentile(captal,.98) AS captal_upper_bound 
      FROM 
        "firms_survey"."asif_firms_prepared" 
      GROUP BY 
        year
    ) as outliers ON asif_firms_prepared.year = outliers.year 
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
  WHERE 
    (
      output > output_lower_bound 
      -- AND output < output_upper_bound 
      AND employ > employ_lower_bound 
      -- AND employ < employ_upper_bound 
      AND captal > captal_lower_bound 
      -- AND output < captal_upper_bound 
      AND asif_firms_prepared.year >= '2001' 
      AND asif_firms_prepared.year <= '2007'
      AND output > midput 
      AND midput > 0
    )
) 
SELECT 
  test.firm, 
  year, 
  output, 
  employ, 
  captal, 
  midput, 
  ownership, 
  geocode4_corr,
  count_city,
  count_ownership,
  count_industry
FROM 
  test 
  INNER JOIN (
    SELECT 
      firm, 
      COUNT(
        DISTINCT(geocode4_corr)
      ) AS count_city 
    FROM 
      test 
    GROUP BY 
      firm
  ) as multi_cities ON test.firm = multi_cities.firm 
  INNER JOIN (
    SELECT 
      firm, 
      COUNT(
        DISTINCT(ownership)
      ) AS count_ownership 
    FROM 
      test 
    GROUP BY 
      firm
  ) as multi_ownership ON test.firm = multi_ownership.firm 
  INNER JOIN (
    SELECT 
      firm, 
      COUNT(
        DISTINCT(cic)
      ) AS count_industry 
    FROM 
      test 
    GROUP BY 
      firm
  ) as multi_industry ON test.firm = multi_industry.firm 
WHERE 
  test.firm  = '255463' 
  ORDER BY year
"""
output = s3.run_query(
                    query=query,
                    database=DatabaseName,
                    s3_output=s3_output_example,
    filename = 'example_2'
                )
output
```

<!-- #region kernel="python3" -->
Make sure the number of observations before filtering is higher than after
<!-- #endregion -->

```python kernel="python3"
query = """
WITH test as (
  SELECT 
    firm, 
    asif_firms_prepared.year, 
    output, 
    employ, 
    captal, 
    midput, 
    ownership, 
    geocode4_corr, 
    cic 
  FROM 
    asif_firms_prepared 
    INNER JOIN (
      SELECT 
        year, 
        approx_percentile(output,.05) AS output_lower_bound, 
        approx_percentile(output,.98) AS output_upper_bound, 
        approx_percentile(employ,.05) AS employ_lower_bound, 
        approx_percentile(employ,.98) AS employ_upper_bound, 
        approx_percentile(captal,.05) AS captal_lower_bound, 
        approx_percentile(captal,.98) AS captal_upper_bound 
      FROM 
        "firms_survey"."asif_firms_prepared" 
      GROUP BY 
        year
    ) as outliers ON asif_firms_prepared.year = outliers.year 
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
  WHERE 
    (
      output > output_lower_bound 
      -- AND output < output_upper_bound 
      AND employ > employ_lower_bound 
      -- AND employ < employ_upper_bound 
      AND captal > captal_lower_bound 
      -- AND output < captal_upper_bound 
      AND asif_firms_prepared.year >= '2001' 
      AND asif_firms_prepared.year <= '2007'
      AND output > midput 
      AND midput > 0
    )
) 
SELECT 
  COUNT(*) as count
FROM 
  test 

"""
output = s3.run_query(
                    query=query,
                    database=DatabaseName,
                    s3_output=s3_output_example,
    filename = 'example_3'
                )
output
```

```python kernel="python3"
query = """
WITH test as (
  SELECT 
    firm, 
    asif_firms_prepared.year, 
    output, 
    employ, 
    captal, 
    midput, 
    ownership, 
    geocode4_corr, 
    cic 
  FROM 
    asif_firms_prepared 
    INNER JOIN (
      SELECT 
        year, 
        approx_percentile(output,.05) AS output_lower_bound, 
        approx_percentile(output,.98) AS output_upper_bound, 
        approx_percentile(employ,.05) AS employ_lower_bound, 
        approx_percentile(employ,.98) AS employ_upper_bound, 
        approx_percentile(captal,.05) AS captal_lower_bound, 
        approx_percentile(captal,.98) AS captal_upper_bound 
      FROM 
        "firms_survey"."asif_firms_prepared" 
      GROUP BY 
        year
    ) as outliers ON asif_firms_prepared.year = outliers.year 
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
  WHERE 
    (
      output > output_lower_bound 
      -- AND output < output_upper_bound 
      AND employ > employ_lower_bound 
      -- AND employ < employ_upper_bound 
      AND captal > captal_lower_bound 
      -- AND output < captal_upper_bound 
      AND asif_firms_prepared.year >= '2001' 
      AND asif_firms_prepared.year <= '2007'
      AND output > midput 
      AND midput > 0
    )
) 
SELECT 
  COUNT(*) AS count 
FROM 
  test 
  INNER JOIN (
    SELECT 
      firm, 
      COUNT(
        DISTINCT(geocode4_corr)
      ) AS count_city 
    FROM 
      test 
    GROUP BY 
      firm
  ) as multi_cities ON test.firm = multi_cities.firm 
  INNER JOIN (
    SELECT 
      firm, 
      COUNT(
        DISTINCT(ownership)
      ) AS count_ownership 
    FROM 
      test 
    GROUP BY 
      firm
  ) as multi_ownership ON test.firm = multi_ownership.firm 
  INNER JOIN (
    SELECT 
      firm, 
      COUNT(
        DISTINCT(cic)
      ) AS count_industry 
    FROM 
      test 
    GROUP BY 
      firm
  ) as multi_industry ON test.firm = multi_industry.firm 
WHERE 
  count_ownership = 1 
  AND count_city = 1 
  AND count_industry = 1 
"""
output = s3.run_query(
                    query=query,
                    database=DatabaseName,
                    s3_output=s3_output_example,
    filename = 'example_4'
                )
output
```

<!-- #region kernel="python3" -->
Count by year
<!-- #endregion -->

```python kernel="python3"
query ="""
WITH test as ( 
  SELECT 
firm,
asif_firms_prepared.year, 
output,
employ,
captal,
midput,
ownership,
geocode4_corr,cic
FROM asif_firms_prepared 
INNER JOIN (
SELECT year,
approx_percentile(output, .05) AS output_lower_bound,
approx_percentile(output, .98) AS output_upper_bound,
approx_percentile(employ, .05) AS employ_lower_bound,
approx_percentile(employ, .98) AS employ_upper_bound,
approx_percentile(captal, .05) AS captal_lower_bound,
approx_percentile(captal, .98) AS captal_upper_bound
FROM "firms_survey"."asif_firms_prepared"
GROUP BY year
  ) as outliers 
  ON asif_firms_prepared.year = outliers.year
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
WHERE (
  output > output_lower_bound 
      -- AND output < output_upper_bound 
      AND employ > employ_lower_bound 
      -- AND employ < employ_upper_bound 
      AND captal > captal_lower_bound 
      -- AND output < captal_upper_bound 
      AND asif_firms_prepared.year >= '2001' 
      AND asif_firms_prepared.year <= '2007'
      AND output > midput 
      AND midput > 0
  )
  )
  SELECT 
  year, COUNT(*) as count
  FROM test
  INNER JOIN (
  SELECT firm,COUNT(DISTINCT(geocode4_corr)) AS count_city
  FROM test
  GROUP BY firm 
    ) as multi_cities
    ON test.firm = multi_cities.firm 
  INNER JOIN (
  SELECT firm, COUNT(DISTINCT(ownership)) AS count_ownership
  FROM test
  GROUP BY firm  
    ) as multi_ownership
    ON test.firm = multi_ownership.firm
  INNER JOIN (
  SELECT firm, COUNT(DISTINCT(cic)) AS count_industry
  FROM test
  GROUP BY firm  
    ) as multi_industry
  ON test.firm = multi_industry.firm
WHERE 
  count_ownership = 1 
  AND count_city = 1 
  AND count_industry = 1
 GROUP BY year
 ORDER BY year

"""
output = s3.run_query(
                    query=query,
                    database=DatabaseName,
                    s3_output=s3_output_example,
    filename = 'example_5'
                )
output
```

<!-- #region kernel="SoS" -->
# Table `asif_tfp_firm_level`

<!-- #endregion -->

<!-- #region kernel="SoS" -->
By default, the query saves the data in `SQL_OUTPUT_ATHENA/CSV`, however, please paste the S3 key where the table transformed by R should be saved. 

Update:

Relax the constraint on:

- Big/small firms
- switching city
- switching industry
- switching ownership

We will predict the model using the constraint **but** we will predict on all firms
<!-- #endregion -->

```python kernel="python3"
s3_output = 'DATA/ECON/FIRM_SURVEY/ASIF_CHINA/TRANSFORMED/TFP/FIRM_LEVEL'
table_name = 'asif_tfp_firm_level'

LOCAL_PATH_CONFIG_FILE = os.path.join(str(Path(path).parent.parent),
                                      '00_data_catalogue',
                                      'temporary_local_data'
                                      )

path_temporary_file_out = os.path.join(str(Path(path).parent.parent),
                                      '00_data_catalogue',
                                      'temporary_local_data',
                                   table_name + ".csv"
                                      )
```

<!-- #region kernel="SoS" -->
Clean up the folder with the previous csv file. Be careful, it will erase all files inside the folder
<!-- #endregion -->

```python kernel="python3"
s3.remove_all_bucket(path_remove = s3_output)
```

```python kernel="python3"
query = """
WITH test as (
  SELECT 
    firm, 
    asif_firms_prepared.year, 
    output, 
    employ, 
    captal, 
    midput, 
    ownership, 
    geocode4_corr, 
    cic,
    CASE WHEN LENGTH(cic) = 4 THEN substr(cic, 1, 2) ELSE concat(
            '0', 
            substr(cic, 1, 1)
          ) END AS indu_2,
          output_upper_bound,
  employ_upper_bound,
  captal_upper_bound
  FROM 
    asif_firms_prepared 
    INNER JOIN (
      SELECT 
        year, 
        approx_percentile(output,.05) AS output_lower_bound, 
        approx_percentile(output,.98) AS output_upper_bound, 
        approx_percentile(employ,.05) AS employ_lower_bound, 
        approx_percentile(employ,.98) AS employ_upper_bound, 
        approx_percentile(captal,.05) AS captal_lower_bound, 
        approx_percentile(captal,.98) AS captal_upper_bound 
      FROM 
        "firms_survey"."asif_firms_prepared" 
      GROUP BY 
        year
    ) as outliers ON asif_firms_prepared.year = outliers.year 
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
  WHERE 
    (
      output > output_lower_bound 
      -- AND output < output_upper_bound 
      AND employ > employ_lower_bound 
      -- AND employ < employ_upper_bound 
      AND captal > captal_lower_bound 
      -- AND output < captal_upper_bound 
      AND asif_firms_prepared.year >= '2001' 
      AND asif_firms_prepared.year <= '2007'
      AND output > midput 
      AND midput > 0
    )
) 
SELECT 
  test.firm, 
  year, 
  output, 
  employ, 
  captal, 
  midput, 
  ownership, 
  geocode4_corr,
  indu_2,
  output_upper_bound,
  employ_upper_bound,
  captal_upper_bound,
  count_ownership,
  count_city,
  count_industry
FROM 
  test 
  INNER JOIN (
    SELECT 
      firm, 
      COUNT(
        DISTINCT(geocode4_corr)
      ) AS count_city 
    FROM 
      test 
    GROUP BY 
      firm
  ) as multi_cities ON test.firm = multi_cities.firm 
  INNER JOIN (
    SELECT 
      firm, 
      COUNT(
        DISTINCT(ownership)
      ) AS count_ownership 
    FROM 
      test 
    GROUP BY 
      firm
  ) as multi_ownership ON test.firm = multi_ownership.firm 
  INNER JOIN (
    SELECT 
      firm, 
      COUNT(
        DISTINCT(cic)
      ) AS count_industry 
    FROM 
      test 
    GROUP BY 
      firm
  ) as multi_industry ON test.firm = multi_industry.firm 
-- WHERE 
--  count_ownership = 1 
--  AND count_city = 1 
--  AND count_industry = 1 
"""

output = s3.run_query(
                    query=query,
                    database=DatabaseName,
                    s3_output='SQL_OUTPUT_ATHENA/CSV'
                )
output
```

<!-- #region kernel="SoS" -->
Need to load the data to the instance
<!-- #endregion -->

```python kernel="python3"
s3.download_file(
    key = os.path.join('SQL_OUTPUT_ATHENA/CSV', output['QueryID'] + ".csv"),
    path_local = LOCAL_PATH_CONFIG_FILE
)
os.rename(
    os.path.join(LOCAL_PATH_CONFIG_FILE,output['QueryID']+'.csv'),
    os.path.join(LOCAL_PATH_CONFIG_FILE,'temporary_file.csv'))
```

<!-- #region kernel="SoS" -->
Load the data in the instance, and open it using R. **DONT FORGET TO WRITE AGAIN THE TABLE NAME**
<!-- #endregion -->

```python kernel="SoS"
from pathlib import Path
import os
table_name = "asif_tfp_firm_level"

path = os.getcwd()
path_temporary_file = os.path.join(str(Path(path).parent.parent),
                                      '00_data_catalogue',
                                      'temporary_local_data',
                                   'temporary_file.csv'
                                      )
path_temporary_file_out = os.path.join(str(Path(path).parent.parent),
                                      '00_data_catalogue',
                                      'temporary_local_data',
                                   table_name + ".csv"
                                      )
```

```python kernel="R"
options(warn=-1)
library(tidyverse)
```

```python kernel="R"
%get path_temporary_file
df_input <- read_csv(path_temporary_file) 
df_input %>% head()
```

```python kernel="R"
dim(df_input)
```

### Compute TFP

<!-- #region kernel="R" -->
Prepare R code for transformation, rename the final table `df_output`. Make sure there is no missing values, the crawler cannot handle missing values, neither any econometrics or machine learning model
<!-- #endregion -->

<!-- #region kernel="R" -->
Note that, we change the program to make sure we can use it within our environment. The original file can be found here https://github.com/GabrieleRovigatti/prodest/tree/master/prodest/R

We bring together the file https://github.com/GabrieleRovigatti/prodest/blob/master/prodest/R/auxFun.R and https://github.com/GabrieleRovigatti/prodest/blob/master/prodest/R/prodestOPLP.R. We change a few lines of codes to avoid issue with the data preparation. 

The modified program is available in Github, https://github.com/thomaspernet/Financial_dependency_pollution/blob/master/01_data_preprocessing/02_transform_tables/TFP_R_PROGRAM/program_OP_TFP.R
<!-- #endregion -->

```python kernel="R"
path = "TFP_R_PROGRAM/program_OP_TFP.R"
source(path)
```

<!-- #region kernel="R" -->
Estimate TFP excluding largest firms and firms switching industry, city and ownership
<!-- #endregion -->

```python kernel="R"
df_train <- df_input %>% filter(
    output < output_upper_bound 
    & 
employ < employ_upper_bound
& 
captal < captal_upper_bound
&count_ownership == 1 
&count_city == 1 
&count_industry == 1 
)
df_train$id_1 <- df_train %>% group_indices(firm) 
dim(df_train)
```

```python kernel="python3"
import time
start_time = time.time()
```

```python kernel="R"
OP.fit <- prodestOP(Y = log(df_train$output),
                    fX = log(df_train$employ),
                    sX= log(df_train$captal),
                    pX = log(df_train$midput),
                    idvar = df_train$id_1,
                    timevar = df_train$year)
```

```python kernel="python3"
(time.time() - start_time)/60
```

```python kernel="R"
OP.fit
```

<!-- #region kernel="R" -->
Compute the TFP using the coefficients of employment and capital.

TFP is predicted on all firms
<!-- #endregion -->

```python kernel="R"
df_input$tfp_OP <- log(df_input$output) - (log(df_input$employ) * OP.fit$pars[1] +
                                      log(df_input$captal) * OP.fit$pars[2])
```

```python kernel="R"
df_output <- df_input #%>% select (-id_1)
```

```python kernel="R"
glimpse(df_output)
```

```python kernel="R"
df_output %>% filter(firm == '246379')
```

<!-- #region kernel="R" -->
Save the data with R in the temporary folder `00_data_catalogue/temporary_local_data`.

If need to save the model, use `saveRDS(XX.fit, "XX.rds")` and choose another path in S3
<!-- #endregion -->

```python kernel="R"
%get path_temporary_file_out
write.csv(df_output, path_temporary_file_out, row.names=FALSE)
```

<!-- #region kernel="SoS" -->
Save the data back in the S3 folder using Python
<!-- #endregion -->

```python kernel="python3"
s3.upload_file(file_to_upload = path_temporary_file_out, destination_in_s3 = s3_output)
```

<!-- #region kernel="SoS" -->
# Validate query

This step is mandatory to validate the query in the ETL. If you are not sure about the quality of the query, go to the next step.
<!-- #endregion -->

<!-- #region kernel="SoS" -->
To validate the query, please fillin the json below. Don't forget to change the schema so that the crawler can use it.

1. Add a partition key:
    - Inform if there is group in the table so that, the parser can compute duplicate
2. Add the steps number -> Not automtic yet. Start at 0
3. Change the schema if needed. It is highly recommanded to add comment to the fields
4. Provide a description -> detail the steps 
<!-- #endregion -->

<!-- #region kernel="SoS" -->
1. Add a partition key
<!-- #endregion -->

```python kernel="python3"
partition_keys = ['year', 'ownership']
```

<!-- #region kernel="SoS" -->
3. Change the schema

Bear in mind that CSV SerDe (OpenCSVSerDe) does not support empty fields in columns defined as a numeric data type. All columns with missing values should be saved as string. 
<!-- #endregion -->

```python kernel="python3"
#glue.get_table_information(
#    database = DatabaseName,
#    table = table_name)['Table']['StorageDescriptor']['Columns']
```

```python kernel="python3"
schema = [
    {
        "Name": "firm",
        "Type": "string",
        "Comment": "Firm ID"
    },
    {
        "Name": "year",
        "Type": "string",
        "Comment": ""
    },
    {
        "Name": "output",
        "Type": "double",
        "Comment": "output"
    },
    {
        "Name": "employ",
        "Type": "double",
        "Comment": "employement"
    },
    {
        "Name": "captal",
        "Type": "double",
        "Comment": "Capital"
    },
    {
        "Name": "midput",
        "Type": "double",
        "Comment": "Intermediate input"
    },
    {
        "Name": "ownership",
        "Type": "string",
        "Comment": "firm s ownership"
    },
    {
        "Name": "geocode4_corr",
        "Type": "string",
        "Comment": ""
    },
    {
        "Name": "indu_2",
        "Type": "string",
        "Comment": ""
    },
    {
        "Name": "output_upper_bound",
        "Type": "string",
        "Comment": ""
    },
    {
        "Name": "employ_upper_bound",
        "Type": "string",
        "Comment": ""
    },
    {
        "Name": "captal_upper_bound",
        "Type": "string",
        "Comment": ""
    },
    {
        "Name": "count_ownership",
        "Type": "string",
        "Comment": "Number of ownerships per firm"
    },
    {
        "Name": "count_city",
        "Type": "string",
        "Comment": "Number of cities per firm"
    },
    {
        "Name": "count_industry",
        "Type": "string",
        "Comment": "Number of industries per firm"
    },
    {
        "Name": "tfp_OP",
        "Type": "double",
        "Comment": "Estimate TFP using Olley and Pakes approach"
    }
]
```

<!-- #region kernel="SoS" -->
4. Provide a description
<!-- #endregion -->

```python kernel="python3"
description = """
Compute TFP using Olley and Pakes approach at the firm level
"""
```

<!-- #region kernel="SoS" -->
3. provide metadata

- DatabaseName:
- TablePrefix:
- input: 
- notebook name: to indicate
- Task ID: from Coda
- index_final_table: a list to indicate if the current table is used to prepare the final table(s). If more than one, pass the index. Start at 0
- if_final: A boolean. Indicates if the current table is the final table -> the one the model will be used to be trained
<!-- #endregion -->

```python kernel="python3"
with open(os.path.join(str(Path(path).parent.parent), 'utils','parameters_ETL_Financial_dependency_pollution.json')) as json_file:
    parameters = json.load(json_file)
```

```python kernel="python3"
TablePrefix = 'asif_tfp_'
filename =  "05_tfp_computation.ipynb"
index_final_table = [0,1]
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

```python kernel="python3"
json_etl = {
    'description': description,
    'query': query,
    'schema': schema,
    'partition_keys': partition_keys,
    'metadata': {
        'DatabaseName': DatabaseName,
        'TablePrefix' : TablePrefix,
        'TableName': table_name,
        'input': list_input,
        'target_S3URI': os.path.join('s3://', bucket, s3_output),
        'from_athena': 'True',
        'filename': notebookname,
        'index_final_table' : index_final_table,
        'if_final': if_final,
        'github_url':github_url
    }
}
json_etl['metadata']
```

<!-- #region kernel="SoS" -->
Remove the step number from the current file (if exist)
<!-- #endregion -->

```python kernel="python3"
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

```python kernel="python3"
parameters['TABLES']['TRANSFORMATION']['STEPS'].append(json_etl)
```

```python
print("Currently, the ETL has {} tables".format(len(parameters['TABLES']['TRANSFORMATION']['STEPS'])))
```

<!-- #region kernel="SoS" -->
Save JSON
<!-- #endregion -->

```python kernel="python3"
with open(os.path.join(str(Path(path).parent.parent), 'utils','parameters_ETL_Financial_dependency_pollution.json'), "w") as json_file:
    json.dump(parameters, json_file)
```

<!-- #region kernel="SoS" -->
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
<!-- #endregion -->

```python kernel="python3"
name_crawler = 'table-test-parser'
Role = 'arn:aws:iam::468786073381:role/AWSGlueServiceRole-crawler-datalake'
#DatabaseName = 'firms_survey'
#TablePrefix = 'asif_tfp_'
```

```python kernel="python3"
target_S3URI = os.path.join('s3://',bucket, s3_output)
table_name = '{}{}'.format(TablePrefix, os.path.basename(target_S3URI).lower())
```

```python kernel="python3"
glue.create_table_glue(
    target_S3URI,
    name_crawler,
    Role,
    DatabaseName,
    TablePrefix,
    from_athena=True,
    update_schema=schema,
)
```

```python nteract={"transient": {"deleting": false}} kernel="python3"
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

<!-- #region kernel="SoS" -->
## Check Duplicates

One of the most important step when creating a table is to check if the table contains duplicates. The cell below checks if the table generated before is empty of duplicates. The code uses the JSON file to create the query parsed in Athena. 

You are required to define the group(s) that Athena will use to compute the duplicate. For instance, your table can be grouped by COL1 and COL2 (need to be string or varchar), then pass the list ['COL1', 'COL2'] 
<!-- #endregion -->

```python kernel="python3"
partition_keys = ['firm', 'year']

with open(os.path.join(str(Path(path).parent), 'parameters_ETL_Financial_dependency_pollution.json')) as json_file:
    parameters = json.load(json_file)
```

```python kernel="python3"
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

<!-- #region kernel="SoS" -->
## Count missing values
<!-- #endregion -->

```python kernel="python3"
#table = 'XX'
schema = glue.get_table_information(
    database = DatabaseName,
    table = table_name
)['Table']
schema
```

```python kernel="python3"
from datetime import date
today = date.today().strftime('%Y%M%d')
```

```python kernel="python3"
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

<!-- #region kernel="SoS" -->
# Update Github Data catalog

The data catalog is available in Glue. Although, we might want to get a quick access to the tables in Github. In this part, we are generating a `README.md` in the folder `00_data_catalogue`. All tables used in the project will be added to the catalog. We use the ETL parameter file and the schema in Glue to create the README. 

Bear in mind the code will erase the previous README. 
<!-- #endregion -->

```python kernel="python3"
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

<!-- #region kernel="SoS" -->
# Analytics

In this part, we are providing basic summary statistic. Since we have created the tables, we can parse the schema in Glue and use our json file to automatically generates the analysis.

The cells below execute the job in the key `ANALYSIS`. You need to change the `primary_key` and `secondary_key` 
<!-- #endregion -->

<!-- #region kernel="SoS" -->
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
<!-- #endregion -->

```python kernel="python3"
import boto3

key, secret_ = con.load_credential()
client_lambda = boto3.client(
    'lambda',
    aws_access_key_id=key,
    aws_secret_access_key=secret_,
    region_name = region)
```

```python kernel="python3"
primary_key = 'year'
secondary_key = 'ownership'
y_var = 'tfp_OP'
```

```python kernel="python3"
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

```python kernel="python3"
response = client_lambda.invoke(
    FunctionName='RunNotebook',
    InvocationType='RequestResponse',
    LogType='Tail',
    Payload=json.dumps(payload),
)
response
```

<!-- #region kernel="SoS" -->
# Generation report
<!-- #endregion -->

```python kernel="python3"
import os, time, shutil, urllib, ipykernel, json
from pathlib import Path
from notebook import notebookapp
import sys
sys.path.append(os.path.join(parent_path, 'utils'))
import make_toc
import create_schema
```

```python kernel="python3"
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

```python kernel="python3"
create_report(extension = "html", keep_code = True, notebookname = '05_tfp_computation.ipynb')
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
