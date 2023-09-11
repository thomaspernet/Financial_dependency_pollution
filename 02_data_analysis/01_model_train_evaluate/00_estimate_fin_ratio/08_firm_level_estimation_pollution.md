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
    display_name: SoS
    language: sos
    name: sos
---

<!-- #region kernel="SoS" -->
# US Name
Model estimate Estimate internal finance and pollution emission firm level

# Description
None
# Metadata
- Key: 488_Financial_dependency_pollution
- Epic: Models
- US: Evaluate econometrics model
- Task tag: #internal-finance, #training-Financial-dependency-pollution
- Analytics reports: 
# Input
## Table/file
**Name**
- asif_financial_ratio_baseline_firm
- china_firm_pollution_data
**Github**
- https://github.com/thomaspernet/Financial_dependency_pollution/blob/master/02_data_analysis/01_model_train_evaluate/00_estimate_fin_ratio/08_firm_level_estimation_pollution.md


<!-- #endregion -->

<!-- #region kernel="SoS" -->
# Connexion server
<!-- #endregion -->

```sos kernel="SoS"
from awsPy.aws_authorization import aws_connector
from awsPy.aws_s3 import service_s3
from awsPy.aws_glue import service_glue
from pathlib import Path
import pandas as pd
import numpy as np
#import seaborn as sns
import os, shutil, json
import sys

from sklearn import preprocessing

le = preprocessing.LabelEncoder()

path = os.getcwd()
parent_path = str(Path(path).parent.parent.parent)


name_credential = 'financial_dep_SO2_accessKeys.csv'
region = 'eu-west-2'
bucket = 'datalake-london'
path_cred = "{0}/creds/{1}".format(parent_path, name_credential)
```

```sos kernel="SoS"
con = aws_connector.aws_instantiate(credential = path_cred,
                                       region = region)
client= con.client_boto()
s3 = service_s3.connect_S3(client = client,
                      bucket = bucket, verbose = False)
glue = service_glue.connect_glue(client = client) 
```

```sos kernel="SoS"
pandas_setting = True
if pandas_setting:
    #cm = sns.light_palette("green", as_cmap=True)
    pd.set_option('display.max_columns', None)
    pd.set_option('display.max_colwidth', None)
```

```sos kernel="SoS" nteract={"transient": {"deleting": false}}
os.environ['KMP_DUPLICATE_LIB_OK']='True'

```

<!-- #region kernel="SoS" -->
# Load tables

Since we load the data as a Pandas DataFrame, we want to pass the `dtypes`. We load the schema from Glue to guess the types

- 1=state -> 110 141 143 151
- 2=collective -> 120 130 142 149
- 3=private -171 172 173 174 190
- 4=foreign- 210 220 230 240
- 5=Hong Kong, Macau and Taiwan (4 and 5 can be combined into a single "foreign" category - 310 320 330 340
<!-- #endregion -->

```sos kernel="SoS"
db = 'environment'
table = 'china_financial_dependency_pollution'
```

```sos kernel="SoS"

```

```sos kernel="SoS"
dtypes = {}
schema = (glue.get_table_information(database = db,
                           table = table)
          ['Table']['StorageDescriptor']['Columns']
         )
for key, value in enumerate(schema):
    if value['Type'] in ['varchar(12)',
                         'varchar(3)',
                        'varchar(14)', 'varchar(11)']:
        format_ = 'string'
    elif value['Type'] in ['decimal(21,5)', 'double', 'bigint', 'int', 'float']:
        format_ = 'float'
    else:
        format_ = value['Type'] 
    dtypes.update(
        {value['Name']:format_}
    )
```

```sos kernel="SoS"
download_data = True
filename = 'df_{}'.format(table)
full_path_filename = 'SQL_OUTPUT_ATHENA/CSV/{}.csv'.format(filename)
path_local = os.path.join(str(Path(path).parent.parent.parent), 
                              "00_data_catalog/temporary_local_data")
df_path = 'df_asif.csv'#os.path.join(path_local, filename + '.csv')
if download_data:
    
    s3 = service_s3.connect_S3(client = client,
                          bucket = bucket, verbose = False)
    query = """
    SELECT *,CASE WHEN rd_tot_asset IS NULL THEN -1000 
    WHEN rd_tot_asset < 0 THEN 0
    ELSE rd_tot_asset END AS rd_tot_asset_trick,
     CASE 
WHEN ownership_new in (1) THEN 'SOE' 
ELSE 'NOT_SOE' END AS SOE,
 CASE 
WHEN ownership_new in (4,5) THEN 'FOREIGN' 
ELSE 'NOT_FOREIGN' END AS FOREIGN,
concat(indu_2,'-',year) as fe_indu2_year,
concat(china_financial_dependency_pollution.geocode4_corr,'-',year) as fe_city_year
from environment.china_financial_dependency_pollution
LEFT JOIN policy.china_city_tcz_spz
        ON china_financial_dependency_pollution.geocode4_corr = china_city_tcz_spz.geocode4_corr
WHERE asset_tangibility_tot_asset IS NOT NULL and
    sales IS NOT NULL and
    total_asset IS NOT NULL and 
    cashflow_to_tangible >0
ORDER BY firm, year
    """.format(db, table)
    df = (s3.run_query(
            query=query,
            database=db,
            s3_output='SQL_OUTPUT_ATHENA',
            filename=filename,  # Add filename to print dataframe
            destination_key='SQL_OUTPUT_ATHENA/CSV',  #Use it temporarily
            dtype = dtypes
        )
          .assign(
              tcz = lambda x: x['tcz'].fillna(0).astype('int').astype('str'),
              spz = lambda x: x['spz'].fillna(0).astype('int').astype('str'),
              fe_fo=lambda x: le.fit_transform(x["firm"].astype('str')),
              fe_indu2_year=lambda x: le.fit_transform(x["fe_indu2_year"].astype('str')),
              fe_city_year=lambda x: le.fit_transform(x["fe_city_year"].astype('str')),
            )
                )
    #s3.download_file(
    #    key = full_path_filename
    #)
    #shutil.move(
    #    filename + '.csv',
    #    os.path.join(path_local, filename + '.csv')
    #
    #s3.remove_file(full_path_filename)
    #df.head()
    #df.to_csv(df_path)
```

```sos kernel="SoS"
query= """
SELECT * FROM "industry"."china_credit_constraint"
"""
df_credit = (s3.run_query(
            query=query,
            database=db,
            s3_output='SQL_OUTPUT_ATHENA',
            filename=filename,  # Add filename to print dataframe
            destination_key='SQL_OUTPUT_ATHENA/CSV',  #Use it temporarily
            dtype = dtypes
        )
     )
df_credit.dtypes
```

```sos kernel="SoS"
df_credit.head(1)
```

```sos kernel="SoS"
df.shape
```

```sos kernel="SoS"
df = (
    df
    .merge(df_credit.rename(columns = {'cic':'indu_2'}), how= 'inner')
    .assign(
        constraint = lambda x: x['financial_dep_china'] > -0.44,
        constraint_1 = lambda x: x['financial_dep_china'] > -0.26
    )
)
```

```sos kernel="SoS"
df.shape
```

```sos kernel="SoS"
df[['quick_ratio']].isna().sum()
```

```sos kernel="SoS"
df.head(1)
```

<!-- #region kernel="SoS" -->
## Create lags

log(asset_tangibility_tot_asset) +
            log(lag_cashflow_to_tangible) +
            #log(lag_current_ratio) +
            log(sales) +
            log(total_asset) +
            log(lag_liabilities_tot_asset) +
            log(lag_sales_tot_asset)
<!-- #endregion -->

```sos kernel="SoS"
df_final = (
    df
    .assign(
    **{
        f'lag_{c}': df.groupby(['firm'])[c].transform('shift') for c in
        ['cashflow_to_tangible',
                     'current_ratio',
         "quick_ratio",
                     'sales_tot_asset',
                     'liabilities_tot_asset',
                     "asset_tangibility_tot_asset",
                     "sales",
                     "total_asset",
                     "tfp_op","tfp_lp"]
    }
    )
    .dropna(subset = [
        'lag_cashflow_to_tangible',
                    #'lag_current_ratio',
                     'lag_sales_tot_asset',
                     'lag_liabilities_tot_asset',
                     #"lag_asset_tangibility_tot_asset",
                     #"lag_sales",
                     #"lag_total_asset",
                     #"lag_tfp_op",
                     #"lag_tfp_lp"
                     ])
    #.to_csv(df_path)
)
```

```sos kernel="SoS"
df_final.reindex(columns = [
    'lag_asset_tangibility_tot_asset',
    'lag_cashflow_to_tangible',
    'lag_current_ratio',
    "lag_quick_ratio",
    'lag_sales',
    'lag_total_asset',
    'lag_liabilities_tot_asset',
    'lag_sales_tot_asset',
    'lag_tfp_op',
    'lag_tfp_lp'
]).describe()
```

```sos kernel="SoS"
df_final.shape
```

```sos kernel="SoS"
df_final[["SOE"]].isna().sum()
```

```sos kernel="SoS"
(
    df_final
    .to_csv(df_path)
)
```

```sos kernel="SoS" nteract={"transient": {"deleting": false}}
pd.DataFrame(schema)
```

<!-- #region kernel="SoS" nteract={"transient": {"deleting": false}} -->
## Schema Latex table

To rename a variable, please use the following template:

```
{
    'old':'XX',
    'new':'XX_1'
    }
```

if you need to pass a latex format with `\`, you need to duplicate it for instance, `\text` becomes `\\text:

```
{
    'old':'working\_capital\_i',
    'new':'\\text{working capital}_i'
    }
```

Then add it to the key `to_rename`
<!-- #endregion -->

```sos kernel="SoS" nteract={"transient": {"deleting": false}}
add_to_dic = False
if add_to_dic:
    if os.path.exists("schema_table.json"):
        os.remove("schema_table.json")
    data = {'to_rename':[], 'to_remove':[]}
    dic_rename = [
        {
        'old':'working\_capital\_i',
        'new':'\\text{working capital}_i'
        },
        {
        'old':'periodTRUE',
        'new':'\\text{period}'
        },
        {
        'old':'tso2\_mandate\_c',
        'new':'\\text{policy mandate}_'
        },
    ]

    data['to_rename'].extend(dic_rename)
    with open('schema_table.json', 'w') as outfile:
        json.dump(data, outfile)
```

```sos kernel="SoS"
sys.path.append(os.path.join(parent_path, 'utils'))
import latex.latex_beautify as lb
#%load_ext autoreload
#%autoreload 2
```

```sos kernel="R"
options(warn=-1)
library(tidyverse)
library(lfe)
#library(lazyeval)
library('progress')
path = "../../../utils/latex/table_golatex.R"
source(path)
```

```sos kernel="R"
%get df_path
df_final <- read_csv(df_path) %>%
mutate_if(is.character, as.factor) %>%
    mutate_at(vars(starts_with("fe")), as.factor)
```

```sos kernel="R"
#df_final %>%select(so2)
```

<!-- #region kernel="SoS" -->
## Table 1:XXX

$$
\begin{aligned}
\text{Write your equation}
\end{aligned}
$$


* Column 1: XXX
    * FE: 
        - fe 1: `XX`
        - fe 2: `XX`
        - fe 3: `XX`
* Column 2: XXX
    * FE: 
        - fe 1: `XX`
        - fe 2: `XX`
        - fe 3: `XX`
* Column 3: XXX
    * FE: 
        - fe 1: `XX`
        - fe 2: `XX`
        - fe 3: `XX`
* Column 4: XXX
    * FE: 
        - fe 1: `XX`
        - fe 2: `XX`
        - fe 3: `XX`
<!-- #endregion -->

```sos kernel="SoS" nteract={"transient": {"deleting": false}}
folder = 'Tables_0'
table_nb = 1
table = 'table_{}'.format(table_nb)
path = os.path.join(folder, table + '.txt')
if os.path.exists(folder) == False:
        os.mkdir(folder)
for ext in ['.txt', '.tex', '.pdf']:
    x = [a for a in os.listdir(folder) if a.endswith(ext)]
    [os.remove(os.path.join(folder, i)) for i in x]
```

```sos kernel="SoS"
(df_final
.assign(
share_o = lambda x: x['so2']/x['output']
)
.reindex(columns = [
    "share_o",
    'lag_cashflow_to_tangible',
                     'lag_current_ratio',
                     'lag_sales_tot_asset',
                     'lag_liabilities_tot_asset',
                     "lag_asset_tangibility_tot_asset",
                     "lag_sales",
                     "lag_total_asset",
                     "lag_tfp_op",
                     "lag_tfp_lp"
]).describe(percentiles=np.arange(0,1,.05))
)
```

<!-- #region kernel="SoS" -->
### Replicate tables

- current_ratio
- cashflow
- quick_ratio
- return_to_sale
- coverage_ratio
- liquidity
<!-- #endregion -->

```sos kernel="SoS"
df_final.head(1)
```

```sos kernel="SoS"
df_final.assign(eoutput = lambda x: x['output']/x['dso2_equip'])[[
    'so2','dso2_equip','eoutput'
]].corr()
```

```sos kernel="SoS"
df_final['year'].unique()
```

```sos kernel="SoS"
df_final.loc[lambda x: x['so2'] == 0].shape[0]/df_final.shape[0]
```

```sos kernel="SoS"
df_final.loc[lambda x: x['cod'] == 0].shape[0]/df_final.shape[0]
```

```sos kernel="SoS"
df_final.loc[lambda x: x['waste_water'] == 0].shape[0]/df_final.shape[0]
```

```sos kernel="SoS"
df_final.filter(like='tfp')
```

<!-- #region kernel="SoS" -->
Table 1
<!-- #endregion -->

```sos kernel="SoS"
folder = 'Tables_0'
table_nb = 1
table = 'table_{}'.format(table_nb)
path = os.path.join(folder, table + '.txt')
if os.path.exists(folder) == False:
        os.mkdir(folder)
for ext in ['.txt', '.pdf']:
    x = [a for a in os.listdir(folder) if a.endswith(ext)]
    [os.remove(os.path.join(folder, i)) for i in x]
```

```sos kernel="SoS"
df_final.head(1)
# total_coal_used, waste_gas, nox, smoke_dust, soot
```

```sos kernel="R"
%get path table
t_0 <- felm(log(so2 +1) ~ 
            log(asset_tangibility_tot_asset) +
            log(sales) +
            log(total_asset) +
            log(lag_liabilities_tot_asset) +
            log(lag_sales_tot_asset)
            | firm+year+geocode4_corr|0 | firm, df_final%>% 
            filter(lag_cashflow_to_tangible <0.837524),
            exactDOF = TRUE)
t_1 <- felm(log(cod +1) ~ 
            log(asset_tangibility_tot_asset) +
            log(sales) +
            log(total_asset) +
            log(lag_liabilities_tot_asset) +
            log(lag_sales_tot_asset)
            | firm+year+geocode4_corr|0 | firm, df_final%>% 
            filter(lag_cashflow_to_tangible <0.837524),
            exactDOF = TRUE)
t_2 <- felm(log(waste_water +1) ~ 
            log(asset_tangibility_tot_asset) +
            log(sales) +
            log(total_asset) +
            log(lag_liabilities_tot_asset) +
            log(lag_sales_tot_asset)
            | firm+year+geocode4_corr|0 | firm, df_final%>% 
            filter(lag_cashflow_to_tangible <0.837524),
            exactDOF = TRUE)
dep <- "Dependent variable: Pollution emissions"
fe1 <- list(
    c("firm", "Yes", "Yes", "Yes"),
    c("year", "Yes", "Yes", "Yes"),
    c("city", "Yes", "Yes", "Yes")
             )

table_1 <- go_latex(list(
    t_0,t_1, t_2
),
    title="Determinant of pollution emissions",
    dep_var = dep,
    addFE=fe1,
    save=TRUE,
    note = FALSE,
    name=path
) 
```

```sos kernel="SoS"
tbe1  = "This table estimates eq(3). " \
"Heteroskedasticity-robust standard errors" \
"clustered at the product level appear inparentheses."\
"\sym{*} Significance at the 10\%, \sym{**} Significance at the 5\%, \sym{***} Significance at the 1\%."

#multicolumn ={
#    'Eligible': 2,
#    'Non-Eligible': 1,
#    'All': 1,
#    'All benchmark': 1,
#}

#multi_lines_dep = '(city/product/trade regime/year)'
new_r = ['& SO2', 'COD', "Waste water"]
lb.beautify(table_number = table_nb,
            #reorder_var = reorder,
            #multi_lines_dep = multi_lines_dep,
            new_row= new_r,
            #multicolumn = multicolumn,
            table_nte = tbe1,
            jupyter_preview = True,
            resolution = 150,
            folder = folder)
```

```sos kernel="SoS"
folder = 'Tables_0'
table_nb = 1
table = 'table_{}'.format(table_nb)
path = os.path.join(folder, table + '.txt')
if os.path.exists(folder) == False:
        os.mkdir(folder)
for ext in ['.txt', '.pdf']:
    x = [a for a in os.listdir(folder) if a.endswith(ext)]
    [os.remove(os.path.join(folder, i)) for i in x]
```

```sos kernel="R"
%get path table
t_0 <- felm(log(so2 +1) ~ 
            log(tfp_op) +
            log(sales) +
            log(total_asset) +
            log(lag_liabilities_tot_asset) +
            log(lag_sales_tot_asset)
            | firm+year+geocode4_corr|0 | firm, df_final%>% 
            filter(lag_cashflow_to_tangible <0.837524),
            exactDOF = TRUE)
t_1 <- felm(log(cod +1) ~ 
            log(tfp_op) +
            log(sales) +
            log(total_asset) +
            log(lag_liabilities_tot_asset) +
            log(lag_sales_tot_asset)
            | firm+year+geocode4_corr|0 | firm, df_final%>% 
            filter(lag_cashflow_to_tangible <0.837524),
            exactDOF = TRUE)
t_2 <- felm(log(waste_water +1) ~ 
            log(tfp_op) +
            log(sales) +
            log(total_asset) +
            log(lag_liabilities_tot_asset) +
            log(lag_sales_tot_asset)
            | firm+year+geocode4_corr|0 | firm, df_final%>% 
            filter(lag_cashflow_to_tangible <0.837524),
            exactDOF = TRUE)
dep <- "Dependent variable: Pollution emissions"
fe1 <- list(
    c("firm", "Yes", "Yes", "Yes"),
    c("year", "Yes", "Yes", "Yes"),
    c("city", "Yes", "Yes", "Yes")
             )

table_1 <- go_latex(list(
    t_0,t_1, t_2
),
    title="Determinant of pollution emissions",
    dep_var = dep,
    addFE=fe1,
    save=TRUE,
    note = FALSE,
    name=path
) 
```

```sos kernel="SoS"
tbe1  = "This table estimates eq(3). " \
"Heteroskedasticity-robust standard errors" \
"clustered at the product level appear inparentheses."\
"\sym{*} Significance at the 10\%, \sym{**} Significance at the 5\%, \sym{***} Significance at the 1\%."

#multicolumn ={
#    'Eligible': 2,
#    'Non-Eligible': 1,
#    'All': 1,
#    'All benchmark': 1,
#}

#multi_lines_dep = '(city/product/trade regime/year)'
new_r = ['& SO2', 'COD', "Waste water"]
lb.beautify(table_number = table_nb,
            #reorder_var = reorder,
            #multi_lines_dep = multi_lines_dep,
            new_row= new_r,
            #multicolumn = multicolumn,
            table_nte = tbe1,
            jupyter_preview = True,
            resolution = 150,
            folder = folder)
```

```sos kernel="SoS"
folder = 'Tables_0'
table_nb = 1
table = 'table_{}'.format(table_nb)
path = os.path.join(folder, table + '.txt')
if os.path.exists(folder) == False:
        os.mkdir(folder)
for ext in ['.txt', '.pdf']:
    x = [a for a in os.listdir(folder) if a.endswith(ext)]
    [os.remove(os.path.join(folder, i)) for i in x]
```

```sos kernel="R"
%get path table
t_0 <- felm(log(so2 +1) ~ 
            rd_tot_asset_trick +
            log(sales) +
            log(total_asset) +
            log(lag_liabilities_tot_asset) +
            log(lag_sales_tot_asset)
            | firm+year+geocode4_corr|0 | firm, df_final%>% 
            filter(lag_cashflow_to_tangible <0.837524)%>% filter(year %in% list("2005","2006", "2007")),
            exactDOF = TRUE)
t_1 <- felm(log(cod +1) ~ 
            rd_tot_asset_trick +
            log(sales) +
            log(total_asset) +
            log(lag_liabilities_tot_asset) +
            log(lag_sales_tot_asset)
            | firm+year+geocode4_corr|0 | firm, df_final%>% 
            filter(lag_cashflow_to_tangible <0.837524)%>% filter(year %in% list("2005","2006", "2007")),
            exactDOF = TRUE)
t_2 <- felm(log(waste_water +1) ~ 
            rd_tot_asset_trick +
            log(sales) +
            log(total_asset) +
            log(lag_liabilities_tot_asset) +
            log(lag_sales_tot_asset)
            | firm+year+geocode4_corr|0 | firm, df_final%>% 
            filter(lag_cashflow_to_tangible <0.837524)%>% filter(year %in% list("2005","2006", "2007")),
            exactDOF = TRUE)
dep <- "Dependent variable: Pollution emissions"
fe1 <- list(
    c("firm", "Yes", "Yes", "Yes"),
    c("year", "Yes", "Yes", "Yes"),
    c("city", "Yes", "Yes", "Yes")
             )

table_1 <- go_latex(list(
    t_0,t_1, t_2
),
    title="Determinant of pollution emissions",
    dep_var = dep,
    addFE=fe1,
    save=TRUE,
    note = FALSE,
    name=path
) 
```

```sos kernel="SoS"
tbe1  = "This table estimates eq(3). " \
"Heteroskedasticity-robust standard errors" \
"clustered at the product level appear inparentheses."\
"\sym{*} Significance at the 10\%, \sym{**} Significance at the 5\%, \sym{***} Significance at the 1\%."

#multicolumn ={
#    'Eligible': 2,
#    'Non-Eligible': 1,
#    'All': 1,
#    'All benchmark': 1,
#}

#multi_lines_dep = '(city/product/trade regime/year)'
new_r = ['& SO2', 'COD', "Waste water"]
lb.beautify(table_number = table_nb,
            #reorder_var = reorder,
            #multi_lines_dep = multi_lines_dep,
            new_row= new_r,
            #multicolumn = multicolumn,
            table_nte = tbe1,
            jupyter_preview = True,
            resolution = 150,
            folder = folder)
```

```sos kernel="SoS"
df_final
```

<!-- #region kernel="R" -->
### Ownership
<!-- #endregion -->

```sos kernel="SoS"
folder = 'Tables_0'
table_nb = 1
table = 'table_{}'.format(table_nb)
path = os.path.join(folder, table + '.txt')
if os.path.exists(folder) == False:
        os.mkdir(folder)
for ext in ['.txt', '.pdf']:
    x = [a for a in os.listdir(folder) if a.endswith(ext)]
    [os.remove(os.path.join(folder, i)) for i in x]
```

```sos kernel="R"
%get path table
#### SO2
t_0 <- felm(log(so2 +1) ~ 
            log(asset_tangibility_tot_asset) +
            log(sales) +
            log(total_asset) +
            log(lag_liabilities_tot_asset) +
            log(lag_sales_tot_asset)
            | firm+year+geocode4_corr|0 | firm, df_final%>%filter(SOE == "SOE")%>% 
            filter(lag_cashflow_to_tangible <0.837524),
            exactDOF = TRUE)
t_1 <-felm(log(so2 +1) ~ 
            log(asset_tangibility_tot_asset) +
            log(sales) +
            log(total_asset) +
            log(lag_liabilities_tot_asset) +
            log(lag_sales_tot_asset)
            | firm+year+geocode4_corr|0 | firm, df_final%>%filter(SOE != "SOE")%>% 
            filter(lag_cashflow_to_tangible <0.837524),
            exactDOF = TRUE)

### COD
t_2 <- felm(log(cod +1) ~ 
            log(asset_tangibility_tot_asset) +
            log(sales) +
            log(total_asset) +
            log(lag_liabilities_tot_asset) +
            log(lag_sales_tot_asset)
            | firm+year+geocode4_corr|0 | firm, df_final%>%filter(SOE == "SOE")%>% 
            filter(lag_cashflow_to_tangible <0.837524),
            exactDOF = TRUE)
t_3 <-felm(log(cod +1) ~ 
            log(asset_tangibility_tot_asset) +
            log(sales) +
            log(total_asset) +
            log(lag_liabilities_tot_asset) +
            log(lag_sales_tot_asset)
            | firm+year+geocode4_corr|0 | firm, df_final%>%filter(SOE != "SOE")%>% 
            filter(lag_cashflow_to_tangible <0.837524),
            exactDOF = TRUE)
### Waste water
t_4 <- felm(log(waste_water +1) ~ 
            log(asset_tangibility_tot_asset) +
            log(sales) +
            log(total_asset) +
            log(lag_liabilities_tot_asset) +
            log(lag_sales_tot_asset)
            | firm+year+geocode4_corr|0 | firm, df_final%>%filter(SOE == "SOE")%>% 
            filter(lag_cashflow_to_tangible <0.837524),
            exactDOF = TRUE)
t_5 <-felm(log(waste_water +1) ~ 
            log(asset_tangibility_tot_asset) +
            log(sales) +
            log(total_asset) +
            log(lag_liabilities_tot_asset) +
            log(lag_sales_tot_asset)
            | firm+year+geocode4_corr|0 | firm, df_final%>%filter(SOE != "SOE")%>% 
            filter(lag_cashflow_to_tangible <0.837524),
            exactDOF = TRUE)

dep <- "Dependent variable: pollution emissions"
fe1 <- list(
    c("firm", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes"),
    c("year", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes"),
    c("city", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes")
             )

table_1 <- go_latex(list(
    t_0,t_1, t_2, t_3, t_4, t_5
),
    title="Pollution emissions and firm's ownership",
    dep_var = dep,
    addFE=fe1,
    save=TRUE,
    note = FALSE,
    name=path
) 
```

```sos kernel="SoS"
tbe1  = "This table estimates eq(3). " \
"Heteroskedasticity-robust standard errors" \
"clustered at the product level appear inparentheses."\
"\sym{*} Significance at the 10\%, \sym{**} Significance at the 5\%, \sym{***} Significance at the 1\%."

multicolumn ={
    'SO2': 2,
    'COD': 2,
    'Waste water': 2
}

#multi_lines_dep = '(city/product/trade regime/year)'
new_r = ['& SOE', 'No SOE', "SOE","No SOE", "SOE","No SOE"]
lb.beautify(table_number = table_nb,
            #reorder_var = reorder,
            #multi_lines_dep = multi_lines_dep,
            new_row= new_r,
            multicolumn = multicolumn,
            table_nte = tbe1,
            jupyter_preview = True,
            resolution = 150,
            folder = folder)
```

```sos kernel="SoS"
folder = 'Tables_0'
table_nb = 1
table = 'table_{}'.format(table_nb)
path = os.path.join(folder, table + '.txt')
if os.path.exists(folder) == False:
        os.mkdir(folder)
for ext in ['.txt', '.pdf']:
    x = [a for a in os.listdir(folder) if a.endswith(ext)]
    [os.remove(os.path.join(folder, i)) for i in x]
```

```sos kernel="R"
%get path table
#### SO2
t_0 <- felm(log(so2 +1) ~ 
            log(asset_tangibility_tot_asset) +
            log(sales) +
            log(total_asset) +
            log(lag_liabilities_tot_asset) +
            log(lag_sales_tot_asset)
            | firm+year+geocode4_corr|0 | firm, df_final%>%filter(FOREIGN == "FOREIGN")%>% 
            filter(lag_cashflow_to_tangible <0.837524),
            exactDOF = TRUE)
t_1 <-felm(log(so2 +1) ~ 
            log(asset_tangibility_tot_asset) +
            log(sales) +
            log(total_asset) +
            log(lag_liabilities_tot_asset) +
            log(lag_sales_tot_asset)
            | firm+year+geocode4_corr|0 | firm, df_final%>%filter(FOREIGN != "FOREIGN")%>% 
            filter(lag_cashflow_to_tangible <0.837524),
            exactDOF = TRUE)

### COD
t_2 <- felm(log(cod +1) ~ 
            log(asset_tangibility_tot_asset) +
            log(sales) +
            log(total_asset) +
            log(lag_liabilities_tot_asset) +
            log(lag_sales_tot_asset)
            | firm+year+geocode4_corr|0 | firm, df_final%>%filter(FOREIGN == "FOREIGN")%>% 
            filter(lag_cashflow_to_tangible <0.837524),
            exactDOF = TRUE)
t_3 <-felm(log(cod +1) ~ 
            log(asset_tangibility_tot_asset) +
            log(sales) +
            log(total_asset) +
            log(lag_liabilities_tot_asset) +
            log(lag_sales_tot_asset)
            | firm+year+geocode4_corr|0 | firm, df_final%>%filter(FOREIGN != "FOREIGN")%>% 
            filter(lag_cashflow_to_tangible <0.837524),
            exactDOF = TRUE)
### Waste water
t_4 <- felm(log(waste_water +1) ~ 
            log(asset_tangibility_tot_asset) +
            log(sales) +
            log(total_asset) +
            log(lag_liabilities_tot_asset) +
            log(lag_sales_tot_asset)
            | firm+year+geocode4_corr|0 | firm, df_final%>%filter(FOREIGN == "FOREIGN")%>% 
            filter(lag_cashflow_to_tangible <0.837524),
            exactDOF = TRUE)
t_5 <-felm(log(waste_water +1) ~ 
            log(asset_tangibility_tot_asset) +
            log(sales) +
            log(total_asset) +
            log(lag_liabilities_tot_asset) +
            log(lag_sales_tot_asset)
            | firm+year+geocode4_corr|0 | firm, df_final%>%filter(FOREIGN != "FOREIGN")%>% 
            filter(lag_cashflow_to_tangible <0.837524),
            exactDOF = TRUE)

dep <- "Dependent variable: pollution emissions"
fe1 <- list(
    c("firm", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes"),
    c("year", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes"),
    c("city", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes")
             )

table_1 <- go_latex(list(
    t_0,t_1, t_2, t_3, t_4, t_5
),
    title="Pollution emissions and firm's ownership",
    dep_var = dep,
    addFE=fe1,
    save=TRUE,
    note = FALSE,
    name=path
) 
```

```sos kernel="SoS"
tbe1  = "This table estimates eq(3). " \
"Heteroskedasticity-robust standard errors" \
"clustered at the product level appear inparentheses."\
"\sym{*} Significance at the 10\%, \sym{**} Significance at the 5\%, \sym{***} Significance at the 1\%."

multicolumn ={
    'SO2': 2,
    'COD': 2,
    'Waste water': 2
}

#multi_lines_dep = '(city/product/trade regime/year)'
new_r = ['& Foreign', 'No Foreign', "Foreign","No Foreign", "Foreign","No Foreign"]
lb.beautify(table_number = table_nb,
            #reorder_var = reorder,
            #multi_lines_dep = multi_lines_dep,
            new_row= new_r,
            multicolumn = multicolumn,
            table_nte = tbe1,
            jupyter_preview = True,
            resolution = 150,
            folder = folder)
```

<!-- #region kernel="SoS" -->
## Firm's size
<!-- #endregion -->

```sos kernel="R"
df_final_size <- df_final %>%
  left_join(df_final %>%
              group_by(year, cic_adj) %>%
              summarise(output = quantile(output, 0.75),
                        sales = quantile(sales, 0.75),
                        employment = quantile(employment, 0.75),
                        total_asset = quantile(total_asset, 0.75)) %>%
              ungroup() %>%
              rename(
                s_output = output,
                s_sales = sales,
                s_employment = employment,
                s_total_asset = total_asset
              )
  ) %>%
  mutate(d_output = if_else(output > s_output, "Yes", "No"),
         d_sales = if_else(sales > s_sales, "Yes", "No"),
         d_employment = if_else(employment > s_employment, "Yes", "No"),
         d_total_asset = if_else(total_asset > s_total_asset, "Yes", "No"))
```

```sos kernel="R"
dim(df_final_size %>% filter(d_output == "Yes"))
```

```sos kernel="SoS"
folder = 'Tables_0'
table_nb = 1
table = 'table_{}'.format(table_nb)
path = os.path.join(folder, table + '.txt')
if os.path.exists(folder) == False:
        os.mkdir(folder)
for ext in ['.txt', '.pdf']:
    x = [a for a in os.listdir(folder) if a.endswith(ext)]
    [os.remove(os.path.join(folder, i)) for i in x]
```

```sos kernel="R"
%get path table
#### SO2
t_0 <- felm(log(so2 +1) ~ 
            log(asset_tangibility_tot_asset) +
            log(sales) +
            log(total_asset) +
            log(lag_liabilities_tot_asset) +
            log(lag_sales_tot_asset)
            | firm+year+geocode4_corr|0 | firm, df_final_size%>% filter(d_output == "Yes")%>% 
            filter(lag_cashflow_to_tangible <0.837524),
            exactDOF = TRUE)
t_1 <-felm(log(so2 +1) ~ 
            log(asset_tangibility_tot_asset) +
            log(sales) +
            log(total_asset) +
            log(lag_liabilities_tot_asset) +
            log(lag_sales_tot_asset)
            | firm+year+geocode4_corr|0 | firm, df_final_size%>% filter(d_output != "Yes")%>% 
            filter(lag_cashflow_to_tangible <0.837524),
            exactDOF = TRUE)

### COD
t_2 <- felm(log(cod +1) ~ 
            log(asset_tangibility_tot_asset) +
            log(sales) +
            log(total_asset) +
            log(lag_liabilities_tot_asset) +
            log(lag_sales_tot_asset)
            | firm+year+geocode4_corr|0 | firm, df_final_size%>% filter(d_output == "Yes")%>% 
            filter(lag_cashflow_to_tangible <0.837524),
            exactDOF = TRUE)
t_3 <-felm(log(cod +1) ~ 
            log(asset_tangibility_tot_asset) +
            log(sales) +
            log(total_asset) +
            log(lag_liabilities_tot_asset) +
            log(lag_sales_tot_asset)
            | firm+year+geocode4_corr|0 | firm, df_final_size%>% filter(d_output != "Yes")%>% 
            filter(lag_cashflow_to_tangible <0.837524),
            exactDOF = TRUE)
### Waste water
t_4 <- felm(log(waste_water +1) ~ 
            log(asset_tangibility_tot_asset) +
            log(sales) +
            log(total_asset) +
            log(lag_liabilities_tot_asset) +
            log(lag_sales_tot_asset)
            | firm+year+geocode4_corr|0 | firm, df_final_size%>% filter(d_output == "Yes")%>% 
            filter(lag_cashflow_to_tangible <0.837524),
            exactDOF = TRUE)
t_5 <-felm(log(waste_water +1) ~ 
            log(asset_tangibility_tot_asset) +
            log(sales) +
            log(total_asset) +
            log(lag_liabilities_tot_asset) +
            log(lag_sales_tot_asset)
            | firm+year+geocode4_corr|0 | firm, df_final_size%>% filter(d_output != "Yes")%>% 
            filter(lag_cashflow_to_tangible <0.837524),
            exactDOF = TRUE)

dep <- "Dependent variable: pollution emissions"
fe1 <- list(
    c("firm", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes"),
    c("year", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes"),
    c("city", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes")
             )

table_1 <- go_latex(list(
    t_0,t_1, t_2, t_3, t_4, t_5
),
    title="Pollution emissions and firm's size",
    dep_var = dep,
    addFE=fe1,
    save=TRUE,
    note = FALSE,
    name=path
) 
```

```sos kernel="SoS"
tbe1  = "This table estimates eq(3). " \
"Heteroskedasticity-robust standard errors" \
"clustered at the product level appear inparentheses."\
"\sym{*} Significance at the 10\%, \sym{**} Significance at the 5\%, \sym{***} Significance at the 1\%."

multicolumn ={
    'SO2': 2,
    'COD': 2,
    'Waste water': 2
}

#multi_lines_dep = '(city/product/trade regime/year)'
new_r = ['& Large', 'No Large', "Large","No Large", "Large","No Large"]
lb.beautify(table_number = table_nb,
            #reorder_var = reorder,
            #multi_lines_dep = multi_lines_dep,
            new_row= new_r,
            multicolumn = multicolumn,
            table_nte = tbe1,
            jupyter_preview = True,
            resolution = 150,
            folder = folder)
```

```sos kernel="SoS"
folder = 'Tables_0'
table_nb = 1
table = 'table_{}'.format(table_nb)
path = os.path.join(folder, table + '.txt')
if os.path.exists(folder) == False:
        os.mkdir(folder)
for ext in ['.txt', '.pdf']:
    x = [a for a in os.listdir(folder) if a.endswith(ext)]
    [os.remove(os.path.join(folder, i)) for i in x]
```

```sos kernel="R"
%get path table
#### SO2
t_0 <- felm(log(so2 +1) ~ 
            log(tfp_op) +
            log(sales) +
            log(total_asset) +
            log(lag_liabilities_tot_asset) +
            log(lag_sales_tot_asset)
            | firm+year+geocode4_corr|0 | firm, df_final_size%>% filter(d_employment == "Yes")%>% 
            filter(lag_cashflow_to_tangible <0.837524),
            exactDOF = TRUE)
t_1 <-felm(log(so2 +1) ~ 
            log(tfp_op) +
            log(sales) +
            log(total_asset) +
            log(lag_liabilities_tot_asset) +
            log(lag_sales_tot_asset)
            | firm+year+geocode4_corr|0 | firm, df_final_size%>% filter(d_employment != "Yes")%>% 
            filter(lag_cashflow_to_tangible <0.837524),
            exactDOF = TRUE)

### COD
t_2 <- felm(log(cod +1) ~ 
            log(tfp_op) +
            log(sales) +
            log(total_asset) +
            log(lag_liabilities_tot_asset) +
            log(lag_sales_tot_asset)
            | firm+year+geocode4_corr|0 | firm, df_final_size%>% filter(d_employment == "Yes")%>% 
            filter(lag_cashflow_to_tangible <0.837524),
            exactDOF = TRUE)
t_3 <-felm(log(cod +1) ~ 
            log(tfp_op) +
            log(sales) +
            log(total_asset) +
            log(lag_liabilities_tot_asset) +
            log(lag_sales_tot_asset)
            | firm+year+geocode4_corr|0 | firm, df_final_size%>% filter(d_employment != "Yes")%>% 
            filter(lag_cashflow_to_tangible <0.837524),
            exactDOF = TRUE)
### Waste water
t_4 <- felm(log(waste_water +1) ~ 
            log(tfp_op) +
            log(sales) +
            log(total_asset) +
            log(lag_liabilities_tot_asset) +
            log(lag_sales_tot_asset)
            | firm+year+geocode4_corr|0 | firm, df_final_size%>% filter(d_employment == "Yes")%>% 
            filter(lag_cashflow_to_tangible <0.837524),
            exactDOF = TRUE)
t_5 <-felm(log(waste_water +1) ~ 
            log(tfp_op) +
            log(sales) +
            log(total_asset) +
            log(lag_liabilities_tot_asset) +
            log(lag_sales_tot_asset)
            | firm+year+geocode4_corr|0 | firm, df_final_size%>% filter(d_employment != "Yes")%>% 
            filter(lag_cashflow_to_tangible <0.837524),
            exactDOF = TRUE)

dep <- "Dependent variable: pollution emissions"
fe1 <- list(
    c("firm", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes"),
    c("year", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes"),
    c("city", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes")
             )

table_1 <- go_latex(list(
    t_0,t_1, t_2, t_3, t_4, t_5
),
    title="Pollution emissions and firm's size",
    dep_var = dep,
    addFE=fe1,
    save=TRUE,
    note = FALSE,
    name=path
) 
```

```sos kernel="SoS"
tbe1  = "This table estimates eq(3). " \
"Heteroskedasticity-robust standard errors" \
"clustered at the product level appear inparentheses."\
"\sym{*} Significance at the 10\%, \sym{**} Significance at the 5\%, \sym{***} Significance at the 1\%."

multicolumn ={
    'SO2': 2,
    'COD': 2,
    'Waste water': 2
}

#multi_lines_dep = '(city/product/trade regime/year)'
new_r = ['& Large', 'No Large', "Large","No Large", "Large","No Large"]
lb.beautify(table_number = table_nb,
            #reorder_var = reorder,
            #multi_lines_dep = multi_lines_dep,
            new_row= new_r,
            multicolumn = multicolumn,
            table_nte = tbe1,
            jupyter_preview = True,
            resolution = 150,
            folder = folder)
```

```sos kernel="SoS"

```

```sos kernel="SoS"

```

```sos kernel="SoS"
folder = 'Tables_0'
table_nb = 1
table = 'table_{}'.format(table_nb)
path = os.path.join(folder, table + '.txt')
if os.path.exists(folder) == False:
        os.mkdir(folder)
for ext in ['.txt', '.pdf']:
    x = [a for a in os.listdir(folder) if a.endswith(ext)]
    [os.remove(os.path.join(folder, i)) for i in x]
```

```sos kernel="R"
%get path table
#### SO2
t_0 <- felm(log(so2 +1) ~ 
            log(asset_tangibility_tot_asset) +
            log(sales) +
            log(total_asset) +
            log(lag_liabilities_tot_asset) +
            log(lag_sales_tot_asset)
            | firm+year+geocode4_corr|0 | firm, df_final_size%>% filter(d_sales == "Yes")%>% 
            filter(lag_cashflow_to_tangible <0.837524),
            exactDOF = TRUE)
t_1 <-felm(log(so2 +1) ~ 
            log(asset_tangibility_tot_asset) +
            log(sales) +
            log(total_asset) +
            log(lag_liabilities_tot_asset) +
            log(lag_sales_tot_asset)
            | firm+year+geocode4_corr|0 | firm, df_final_size%>% filter(d_sales != "Yes")%>% 
            filter(lag_cashflow_to_tangible <0.837524),
            exactDOF = TRUE)

### COD
t_2 <- felm(log(cod +1) ~ 
            log(asset_tangibility_tot_asset) +
            log(sales) +
            log(total_asset) +
            log(lag_liabilities_tot_asset) +
            log(lag_sales_tot_asset)
            | firm+year+geocode4_corr|0 | firm, df_final_size%>% filter(d_sales == "Yes")%>% 
            filter(lag_cashflow_to_tangible <0.837524),
            exactDOF = TRUE)
t_3 <-felm(log(cod +1) ~ 
            log(asset_tangibility_tot_asset) +
            log(sales) +
            log(total_asset) +
            log(lag_liabilities_tot_asset) +
            log(lag_sales_tot_asset)
            | firm+year+geocode4_corr|0 | firm, df_final_size%>% filter(d_sales != "Yes")%>% 
            filter(lag_cashflow_to_tangible <0.837524),
            exactDOF = TRUE)
### Waste water
t_4 <- felm(log(waste_water +1) ~ 
            log(asset_tangibility_tot_asset) +
            log(sales) +
            log(total_asset) +
            log(lag_liabilities_tot_asset) +
            log(lag_sales_tot_asset)
            | firm+year+geocode4_corr|0 | firm, df_final_size%>% filter(d_sales == "Yes")%>% 
            filter(lag_cashflow_to_tangible <0.837524),
            exactDOF = TRUE)
t_5 <-felm(log(waste_water +1) ~ 
            log(asset_tangibility_tot_asset) +
            log(sales) +
            log(total_asset) +
            log(lag_liabilities_tot_asset) +
            log(lag_sales_tot_asset)
            | firm+year+geocode4_corr|0 | firm, df_final_size%>% filter(d_sales != "Yes")%>% 
            filter(lag_cashflow_to_tangible <0.837524),
            exactDOF = TRUE)

dep <- "Dependent variable: pollution emissions"
fe1 <- list(
    c("firm", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes"),
    c("year", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes"),
    c("city", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes")
             )

table_1 <- go_latex(list(
    t_0,t_1, t_2, t_3, t_4, t_5
),
    title="Pollution emissions and firm's size",
    dep_var = dep,
    addFE=fe1,
    save=TRUE,
    note = FALSE,
    name=path
) 
```

```sos kernel="SoS"
tbe1  = "This table estimates eq(3). " \
"Heteroskedasticity-robust standard errors" \
"clustered at the product level appear inparentheses."\
"\sym{*} Significance at the 10\%, \sym{**} Significance at the 5\%, \sym{***} Significance at the 1\%."

multicolumn ={
    'SO2': 2,
    'COD': 2,
    'Waste water': 2
}

#multi_lines_dep = '(city/product/trade regime/year)'
new_r = ['& Large', 'No Large', "Large","No Large", "Large","No Large"]
lb.beautify(table_number = table_nb,
            #reorder_var = reorder,
            #multi_lines_dep = multi_lines_dep,
            new_row= new_r,
            multicolumn = multicolumn,
            table_nte = tbe1,
            jupyter_preview = True,
            resolution = 150,
            folder = folder)
```

```sos kernel="SoS"
folder = 'Tables_0'
table_nb = 1
table = 'table_{}'.format(table_nb)
path = os.path.join(folder, table + '.txt')
if os.path.exists(folder) == False:
        os.mkdir(folder)
for ext in ['.txt', '.pdf']:
    x = [a for a in os.listdir(folder) if a.endswith(ext)]
    [os.remove(os.path.join(folder, i)) for i in x]
```

```sos kernel="R"
%get path table
#### SO2
t_0 <- felm(log(so2 +1) ~ 
            log(asset_tangibility_tot_asset) +
            log(sales) +
            log(total_asset) +
            log(lag_liabilities_tot_asset) +
            log(lag_sales_tot_asset)
            | firm+year+geocode4_corr|0 | firm, df_final_size%>% filter(d_employment == "Yes")%>% 
            filter(lag_cashflow_to_tangible <0.837524),
            exactDOF = TRUE)
t_1 <-felm(log(so2 +1) ~ 
            log(asset_tangibility_tot_asset) +
            log(sales) +
            log(total_asset) +
            log(lag_liabilities_tot_asset) +
            log(lag_sales_tot_asset)
            | firm+year+geocode4_corr|0 | firm, df_final_size%>% filter(d_employment != "Yes")%>% 
            filter(lag_cashflow_to_tangible <0.837524),
            exactDOF = TRUE)

### COD
t_2 <- felm(log(cod +1) ~ 
            log(asset_tangibility_tot_asset) +
            log(sales) +
            log(total_asset) +
            log(lag_liabilities_tot_asset) +
            log(lag_sales_tot_asset)
            | firm+year+geocode4_corr|0 | firm, df_final_size%>% filter(d_employment == "Yes")%>% 
            filter(lag_cashflow_to_tangible <0.837524),
            exactDOF = TRUE)
t_3 <-felm(log(cod +1) ~ 
            log(asset_tangibility_tot_asset) +
            log(sales) +
            log(total_asset) +
            log(lag_liabilities_tot_asset) +
            log(lag_sales_tot_asset)
            | firm+year+geocode4_corr|0 | firm, df_final_size%>% filter(d_employment != "Yes")%>% 
            filter(lag_cashflow_to_tangible <0.837524),
            exactDOF = TRUE)
### Waste water
t_4 <- felm(log(waste_water +1) ~ 
            log(asset_tangibility_tot_asset) +
            log(sales) +
            log(total_asset) +
            log(lag_liabilities_tot_asset) +
            log(lag_sales_tot_asset)
            | firm+year+geocode4_corr|0 | firm, df_final_size%>% filter(d_employment == "Yes")%>% 
            filter(lag_cashflow_to_tangible <0.837524),
            exactDOF = TRUE)
t_5 <-felm(log(waste_water +1) ~ 
            log(asset_tangibility_tot_asset) +
            log(sales) +
            log(total_asset) +
            log(lag_liabilities_tot_asset) +
            log(lag_sales_tot_asset)
            | firm+year+geocode4_corr|0 | firm, df_final_size%>% filter(d_employment != "Yes")%>% 
            filter(lag_cashflow_to_tangible <0.837524),
            exactDOF = TRUE)

dep <- "Dependent variable: pollution emissions"
fe1 <- list(
    c("firm", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes"),
    c("year", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes"),
    c("city", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes")
             )

table_1 <- go_latex(list(
    t_0,t_1, t_2, t_3, t_4, t_5
),
    title="Pollution emissions and firm's size",
    dep_var = dep,
    addFE=fe1,
    save=TRUE,
    note = FALSE,
    name=path
) 
```

```sos kernel="SoS"
tbe1  = "This table estimates eq(3). " \
"Heteroskedasticity-robust standard errors" \
"clustered at the product level appear inparentheses."\
"\sym{*} Significance at the 10\%, \sym{**} Significance at the 5\%, \sym{***} Significance at the 1\%."

multicolumn ={
    'SO2': 2,
    'COD': 2,
    'Waste water': 2
}

#multi_lines_dep = '(city/product/trade regime/year)'
new_r = ['& Large', 'No Large', "Large","No Large", "Large","No Large"]
lb.beautify(table_number = table_nb,
            #reorder_var = reorder,
            #multi_lines_dep = multi_lines_dep,
            new_row= new_r,
            multicolumn = multicolumn,
            table_nte = tbe1,
            jupyter_preview = True,
            resolution = 150,
            folder = folder)
```

```sos kernel="SoS"
folder = 'Tables_0'
table_nb = 1
table = 'table_{}'.format(table_nb)
path = os.path.join(folder, table + '.txt')
if os.path.exists(folder) == False:
        os.mkdir(folder)
for ext in ['.txt', '.pdf']:
    x = [a for a in os.listdir(folder) if a.endswith(ext)]
    [os.remove(os.path.join(folder, i)) for i in x]
```

```sos kernel="R"
%get path table
#### SO2
t_0 <- felm(log(so2 +1) ~ 
            log(asset_tangibility_tot_asset) +
            log(sales) +
            log(total_asset) +
            log(lag_liabilities_tot_asset) +
            log(lag_sales_tot_asset)
            | firm+year+geocode4_corr|0 | firm, df_final_size%>% filter(d_total_asset == "Yes")%>% 
            filter(lag_cashflow_to_tangible <0.837524),
            exactDOF = TRUE)
t_1 <-felm(log(so2 +1) ~ 
            log(asset_tangibility_tot_asset) +
            log(sales) +
            log(total_asset) +
            log(lag_liabilities_tot_asset) +
            log(lag_sales_tot_asset)
            | firm+year+geocode4_corr|0 | firm, df_final_size%>% filter(d_total_asset != "Yes")%>% 
            filter(lag_cashflow_to_tangible <0.837524),
            exactDOF = TRUE)

### COD
t_2 <- felm(log(cod +1) ~ 
            log(asset_tangibility_tot_asset) +
            log(sales) +
            log(total_asset) +
            log(lag_liabilities_tot_asset) +
            log(lag_sales_tot_asset)
            | firm+year+geocode4_corr|0 | firm, df_final_size%>% filter(d_total_asset == "Yes")%>% 
            filter(lag_cashflow_to_tangible <0.837524),
            exactDOF = TRUE)
t_3 <-felm(log(cod +1) ~ 
            log(asset_tangibility_tot_asset) +
            log(sales) +
            log(total_asset) +
            log(lag_liabilities_tot_asset) +
            log(lag_sales_tot_asset)
            | firm+year+geocode4_corr|0 | firm, df_final_size%>% filter(d_total_asset != "Yes")%>% 
            filter(lag_cashflow_to_tangible <0.837524),
            exactDOF = TRUE)
### Waste water
t_4 <- felm(log(waste_water +1) ~ 
            log(asset_tangibility_tot_asset) +
            log(sales) +
            log(total_asset) +
            log(lag_liabilities_tot_asset) +
            log(lag_sales_tot_asset)
            | firm+year+geocode4_corr|0 | firm, df_final_size%>% filter(d_total_asset == "Yes")%>% 
            filter(lag_cashflow_to_tangible <0.837524),
            exactDOF = TRUE)
t_5 <-felm(log(waste_water +1) ~ 
            log(asset_tangibility_tot_asset) +
            log(sales) +
            log(total_asset) +
            log(lag_liabilities_tot_asset) +
            log(lag_sales_tot_asset)
            | firm+year+geocode4_corr|0 | firm, df_final_size%>% filter(d_total_asset != "Yes")%>% 
            filter(lag_cashflow_to_tangible <0.837524),
            exactDOF = TRUE)

dep <- "Dependent variable: pollution emissions"
fe1 <- list(
    c("firm", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes"),
    c("year", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes"),
    c("city", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes")
             )

table_1 <- go_latex(list(
    t_0,t_1, t_2, t_3, t_4, t_5
),
    title="Pollution emissions and firm's size",
    dep_var = dep,
    addFE=fe1,
    save=TRUE,
    note = FALSE,
    name=path
) 
```

```sos kernel="SoS"
tbe1  = "This table estimates eq(3). " \
"Heteroskedasticity-robust standard errors" \
"clustered at the product level appear inparentheses."\
"\sym{*} Significance at the 10\%, \sym{**} Significance at the 5\%, \sym{***} Significance at the 1\%."

multicolumn ={
    'SO2': 2,
    'COD': 2,
    'Waste water': 2
}

#multi_lines_dep = '(city/product/trade regime/year)'
new_r = ['& Large', 'No Large', "Large","No Large", "Large","No Large"]
lb.beautify(table_number = table_nb,
            #reorder_var = reorder,
            #multi_lines_dep = multi_lines_dep,
            new_row= new_r,
            multicolumn = multicolumn,
            table_nte = tbe1,
            jupyter_preview = True,
            resolution = 150,
            folder = folder)
```

<!-- #region kernel="R" -->
### TZC/SPZ
<!-- #endregion -->

```sos kernel="SoS"
folder = 'Tables_0'
table_nb = 1
table = 'table_{}'.format(table_nb)
path = os.path.join(folder, table + '.txt')
if os.path.exists(folder) == False:
        os.mkdir(folder)
for ext in ['.txt', '.pdf']:
    x = [a for a in os.listdir(folder) if a.endswith(ext)]
    [os.remove(os.path.join(folder, i)) for i in x]
```

```sos kernel="R"
%get path table
#### SO2
t_0 <- felm(log(so2 +1) ~ 
            log(asset_tangibility_tot_asset) +
            log(sales) +
            log(total_asset) +
            log(lag_liabilities_tot_asset) +
            log(lag_sales_tot_asset)
            | firm+year+geocode4_corr|0 | firm, df_final%>%filter(tcz == 1)%>% 
            filter(lag_cashflow_to_tangible <0.837524),
            exactDOF = TRUE)
t_1 <-felm(log(so2 +1) ~ 
            log(asset_tangibility_tot_asset) +
            log(sales) +
            log(total_asset) +
            log(lag_liabilities_tot_asset) +
            log(lag_sales_tot_asset)
            | firm+year+geocode4_corr|0 | firm, df_final%>%filter(tcz != 1)%>% 
            filter(lag_cashflow_to_tangible <0.837524),
            exactDOF = TRUE)

### COD
t_2 <- felm(log(cod +1) ~ 
            log(asset_tangibility_tot_asset) +
            log(sales) +
            log(total_asset) +
            log(lag_liabilities_tot_asset) +
            log(lag_sales_tot_asset)
            | firm+year+geocode4_corr|0 | firm, df_final%>%filter(tcz == 1)%>% 
            filter(lag_cashflow_to_tangible <0.837524),
            exactDOF = TRUE)
t_3 <-felm(log(cod +1) ~ 
            log(asset_tangibility_tot_asset) +
            log(sales) +
            log(total_asset) +
            log(lag_liabilities_tot_asset) +
            log(lag_sales_tot_asset)
            | firm+year+geocode4_corr|0 | firm, df_final%>%filter(tcz !=1)%>% 
            filter(lag_cashflow_to_tangible <0.837524),
            exactDOF = TRUE)
### Waste water
t_4 <- felm(log(waste_water +1) ~ 
            log(asset_tangibility_tot_asset) +
            log(sales) +
            log(total_asset) +
            log(lag_liabilities_tot_asset) +
            log(lag_sales_tot_asset)
            | firm+year+geocode4_corr|0 | firm, df_final%>%filter(tcz == 1)%>% 
            filter(lag_cashflow_to_tangible <0.837524),
            exactDOF = TRUE)
t_5 <-felm(log(waste_water +1) ~ 
            log(asset_tangibility_tot_asset) +
            log(sales) +
            log(total_asset) +
            log(lag_liabilities_tot_asset) +
            log(lag_sales_tot_asset)
            | firm+year+geocode4_corr|0 | firm, df_final%>%filter(tcz != 1)%>% 
            filter(lag_cashflow_to_tangible <0.837524),
            exactDOF = TRUE)

dep <- "Dependent variable: pollution emissions"
fe1 <- list(
    c("firm", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes"),
    c("year", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes"),
    c("city", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes")
             )

table_1 <- go_latex(list(
    t_0,t_1, t_2, t_3, t_4, t_5
),
    title="Pollution emissions and city type",
    dep_var = dep,
    addFE=fe1,
    save=TRUE,
    note = FALSE,
    name=path
) 
```

```sos kernel="SoS"
tbe1  = "This table estimates eq(3). " \
"Heteroskedasticity-robust standard errors" \
"clustered at the product level appear inparentheses."\
"\sym{*} Significance at the 10\%, \sym{**} Significance at the 5\%, \sym{***} Significance at the 1\%."

multicolumn ={
    'SO2': 2,
    'COD': 2,
    'Waste water': 2
}

#multi_lines_dep = '(city/product/trade regime/year)'
new_r = ['& TCZ', 'No TCZ', "TCZ","No TCZ", "TCZ","No TCZ"]
lb.beautify(table_number = table_nb,
            #reorder_var = reorder,
            #multi_lines_dep = multi_lines_dep,
            new_row= new_r,
            multicolumn = multicolumn,
            table_nte = tbe1,
            jupyter_preview = True,
            resolution = 150,
            folder = folder)
```

```sos kernel="SoS"
folder = 'Tables_0'
table_nb = 1
table = 'table_{}'.format(table_nb)
path = os.path.join(folder, table + '.txt')
if os.path.exists(folder) == False:
        os.mkdir(folder)
for ext in ['.txt', '.pdf']:
    x = [a for a in os.listdir(folder) if a.endswith(ext)]
    [os.remove(os.path.join(folder, i)) for i in x]
```

```sos kernel="R"
%get path table
#### SO2
t_0 <- felm(log(so2 +1) ~ 
            log(asset_tangibility_tot_asset) +
            log(sales) +
            log(total_asset) +
            log(lag_liabilities_tot_asset) +
            log(lag_sales_tot_asset)
            | firm+year+geocode4_corr|0 | firm, df_final%>%filter(spz == 1)%>% 
            filter(lag_cashflow_to_tangible <0.837524),
            exactDOF = TRUE)
t_1 <-felm(log(so2 +1) ~ 
            log(asset_tangibility_tot_asset) +
            log(sales) +
            log(total_asset) +
            log(lag_liabilities_tot_asset) +
            log(lag_sales_tot_asset)
            | firm+year+geocode4_corr|0 | firm, df_final%>%filter(spz != 1)%>% 
            filter(lag_cashflow_to_tangible <0.837524),
            exactDOF = TRUE)

### COD
t_2 <- felm(log(cod +1) ~ 
            log(asset_tangibility_tot_asset) +
            log(sales) +
            log(total_asset) +
            log(lag_liabilities_tot_asset) +
            log(lag_sales_tot_asset)
            | firm+year+geocode4_corr|0 | firm, df_final%>%filter(spz == 1)%>% 
            filter(lag_cashflow_to_tangible <0.837524),
            exactDOF = TRUE)
t_3 <-felm(log(cod +1) ~ 
            log(asset_tangibility_tot_asset) +
            log(sales) +
            log(total_asset) +
            log(lag_liabilities_tot_asset) +
            log(lag_sales_tot_asset)
            | firm+year+geocode4_corr|0 | firm, df_final%>%filter(spz !=1)%>% 
            filter(lag_cashflow_to_tangible <0.837524),
            exactDOF = TRUE)
### Waste water
t_4 <- felm(log(waste_water +1) ~ 
            log(asset_tangibility_tot_asset) +
            log(sales) +
            log(total_asset) +
            log(lag_liabilities_tot_asset) +
            log(lag_sales_tot_asset)
            | firm+year+geocode4_corr|0 | firm, df_final%>%filter(spz == 1)%>% 
            filter(lag_cashflow_to_tangible <0.837524),
            exactDOF = TRUE)
t_5 <-felm(log(waste_water +1) ~ 
            log(asset_tangibility_tot_asset) +
            log(sales) +
            log(total_asset) +
            log(lag_liabilities_tot_asset) +
            log(lag_sales_tot_asset)
            | firm+year+geocode4_corr|0 | firm, df_final%>%filter(spz != 1)%>% 
            filter(lag_cashflow_to_tangible <0.837524),
            exactDOF = TRUE)

dep <- "Dependent variable: pollution emissions"
fe1 <- list(
    c("firm", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes"),
    c("year", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes"),
    c("city", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes")
             )

table_1 <- go_latex(list(
    t_0,t_1, t_2, t_3, t_4, t_5
),
    title="Pollution emissions and city type",
    dep_var = dep,
    addFE=fe1,
    save=TRUE,
    note = FALSE,
    name=path
) 
```

```sos kernel="SoS"
tbe1  = "This table estimates eq(3). " \
"Heteroskedasticity-robust standard errors" \
"clustered at the product level appear inparentheses."\
"\sym{*} Significance at the 10\%, \sym{**} Significance at the 5\%, \sym{***} Significance at the 1\%."

multicolumn ={
    'SO2': 2,
    'COD': 2,
    'Waste water': 2
}

#multi_lines_dep = '(city/product/trade regime/year)'
new_r = ['& SPZ', 'No SPZ', "SPZ","No SPZ", "SPZ","No SPZ"]
lb.beautify(table_number = table_nb,
            #reorder_var = reorder,
            #multi_lines_dep = multi_lines_dep,
            new_row= new_r,
            multicolumn = multicolumn,
            table_nte = tbe1,
            jupyter_preview = True,
            resolution = 150,
            folder = folder)
```

<!-- #region kernel="SoS" -->
### Constraint
<!-- #endregion -->

```sos kernel="SoS"
folder = 'Tables_0'
table_nb = 1
table = 'table_{}'.format(table_nb)
path = os.path.join(folder, table + '.txt')
if os.path.exists(folder) == False:
        os.mkdir(folder)
for ext in ['.txt', '.pdf']:
    x = [a for a in os.listdir(folder) if a.endswith(ext)]
    [os.remove(os.path.join(folder, i)) for i in x]
```

```sos kernel="SoS"
df_final.head(1)
```

```sos kernel="R"
%get path table
#### SO2
t_0 <- felm(log(so2 +1) ~ 
            log(asset_tangibility_tot_asset) +
            log(sales) +
            log(total_asset) +
            log(lag_liabilities_tot_asset) +
            log(lag_sales_tot_asset)
            | firm+year+geocode4_corr|0 | firm, df_final%>%filter(constraint == TRUE)%>% 
            filter(lag_cashflow_to_tangible <0.837524),
            exactDOF = TRUE)
t_1 <-felm(log(so2 +1) ~ 
            log(asset_tangibility_tot_asset) +
            log(sales) +
            log(total_asset) +
            log(lag_liabilities_tot_asset) +
            log(lag_sales_tot_asset)
            | firm+year+geocode4_corr|0 | firm, df_final%>%filter(constraint != TRUE)%>% 
            filter(lag_cashflow_to_tangible <0.837524),
            exactDOF = TRUE)

### COD
t_2 <- felm(log(cod +1) ~ 
            log(asset_tangibility_tot_asset) +
            log(sales) +
            log(total_asset) +
            log(lag_liabilities_tot_asset) +
            log(lag_sales_tot_asset)
            | firm+year+geocode4_corr|0 | firm, df_final%>%filter(constraint == TRUE)%>% 
            filter(lag_cashflow_to_tangible <0.837524),
            exactDOF = TRUE)
t_3 <-felm(log(cod +1) ~ 
            log(asset_tangibility_tot_asset) +
            log(sales) +
            log(total_asset) +
            log(lag_liabilities_tot_asset) +
            log(lag_sales_tot_asset)
            | firm+year+geocode4_corr|0 | firm, df_final%>%filter(constraint !=TRUE)%>% 
            filter(lag_cashflow_to_tangible <0.837524),
            exactDOF = TRUE)
### Waste water
t_4 <- felm(log(waste_water +1) ~ 
            log(asset_tangibility_tot_asset) +
            log(sales) +
            log(total_asset) +
            log(lag_liabilities_tot_asset) +
            log(lag_sales_tot_asset)
            | firm+year+geocode4_corr|0 | firm, df_final%>%filter(constraint == TRUE)%>% 
            filter(lag_cashflow_to_tangible <0.837524),
            exactDOF = TRUE)
t_5 <-felm(log(waste_water +1) ~ 
            log(asset_tangibility_tot_asset) +
            log(sales) +
            log(total_asset) +
            log(lag_liabilities_tot_asset) +
            log(lag_sales_tot_asset)
            | firm+year+geocode4_corr|0 | firm, df_final%>%filter(constraint != TRUE)%>% 
            filter(lag_cashflow_to_tangible <0.837524),
            exactDOF = TRUE)

dep <- "Dependent variable: pollution emissions"
fe1 <- list(
    c("firm", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes"),
    c("year", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes"),
    c("city", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes")
             )

table_1 <- go_latex(list(
    t_0,t_1, t_2, t_3, t_4, t_5
),
    title="Pollution emissions and Financial constraint",
    dep_var = dep,
    addFE=fe1,
    save=TRUE,
    note = FALSE,
    name=path
) 
```

```sos kernel="SoS"
tbe1  = "This table estimates eq(3). " \
"Heteroskedasticity-robust standard errors" \
"clustered at the product level appear inparentheses."\
"\sym{*} Significance at the 10\%, \sym{**} Significance at the 5\%, \sym{***} Significance at the 1\%."

multicolumn ={
    'SO2': 2,
    'COD': 2,
    'Waste water': 2
}

#multi_lines_dep = '(city/product/trade regime/year)'
new_r = ['& Contraint', 'No Contraint', "Contraint","No Contraint", "Contraint","No Contraint"]
lb.beautify(table_number = table_nb,
            #reorder_var = reorder,
            #multi_lines_dep = multi_lines_dep,
            new_row= new_r,
            multicolumn = multicolumn,
            table_nte = tbe1,
            jupyter_preview = True,
            resolution = 150,
            folder = folder)
```

```sos kernel="SoS"
folder = 'Tables_0'
table_nb = 1
table = 'table_{}'.format(table_nb)
path = os.path.join(folder, table + '.txt')
if os.path.exists(folder) == False:
        os.mkdir(folder)
for ext in ['.txt', '.pdf']:
    x = [a for a in os.listdir(folder) if a.endswith(ext)]
    [os.remove(os.path.join(folder, i)) for i in x]
```

```sos kernel="R"
%get path table
#### SO2
t_0 <- felm(log(so2 +1) ~ 
            log(tfp_op) +
            log(sales) +
            log(total_asset) +
            log(lag_liabilities_tot_asset) +
            log(lag_sales_tot_asset)
            | firm+year+geocode4_corr|0 | firm, df_final%>%filter(constraint == TRUE)%>% 
            filter(lag_cashflow_to_tangible <0.837524),
            exactDOF = TRUE)
t_1 <-felm(log(so2 +1) ~ 
            log(tfp_op) +
            log(sales) +
            log(total_asset) +
            log(lag_liabilities_tot_asset) +
            log(lag_sales_tot_asset)
            | firm+year+geocode4_corr|0 | firm, df_final%>%filter(constraint != TRUE)%>% 
            filter(lag_cashflow_to_tangible <0.837524),
            exactDOF = TRUE)

### COD
t_2 <- felm(log(cod +1) ~ 
            log(tfp_op) +
            log(sales) +
            log(total_asset) +
            log(lag_liabilities_tot_asset) +
            log(lag_sales_tot_asset)
            | firm+year+geocode4_corr|0 | firm, df_final%>%filter(constraint == TRUE)%>% 
            filter(lag_cashflow_to_tangible <0.837524),
            exactDOF = TRUE)
t_3 <-felm(log(cod +1) ~ 
            log(tfp_op) +
            log(sales) +
            log(total_asset) +
            log(lag_liabilities_tot_asset) +
            log(lag_sales_tot_asset)
            | firm+year+geocode4_corr|0 | firm, df_final%>%filter(constraint !=TRUE)%>% 
            filter(lag_cashflow_to_tangible <0.837524),
            exactDOF = TRUE)
### Waste water
t_4 <- felm(log(waste_water +1) ~ 
            log(tfp_op) +
            log(sales) +
            log(total_asset) +
            log(lag_liabilities_tot_asset) +
            log(lag_sales_tot_asset)
            | firm+year+geocode4_corr|0 | firm, df_final%>%filter(constraint == TRUE)%>% 
            filter(lag_cashflow_to_tangible <0.837524),
            exactDOF = TRUE)
t_5 <-felm(log(waste_water +1) ~ 
            log(tfp_op) +
            log(sales) +
            log(total_asset) +
            log(lag_liabilities_tot_asset) +
            log(lag_sales_tot_asset)
            | firm+year+geocode4_corr|0 | firm, df_final%>%filter(constraint != TRUE)%>% 
            filter(lag_cashflow_to_tangible <0.837524),
            exactDOF = TRUE)

dep <- "Dependent variable: pollution emissions"
fe1 <- list(
    c("firm", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes"),
    c("year", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes"),
    c("city", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes")
             )

table_1 <- go_latex(list(
    t_0,t_1, t_2, t_3, t_4, t_5
),
    title="Pollution emissions and Financial constraint",
    dep_var = dep,
    addFE=fe1,
    save=TRUE,
    note = FALSE,
    name=path
) 
```

```sos kernel="SoS"
tbe1  = "This table estimates eq(3). " \
"Heteroskedasticity-robust standard errors" \
"clustered at the product level appear inparentheses."\
"\sym{*} Significance at the 10\%, \sym{**} Significance at the 5\%, \sym{***} Significance at the 1\%."

multicolumn ={
    'SO2': 2,
    'COD': 2,
    'Waste water': 2
}

#multi_lines_dep = '(city/product/trade regime/year)'
new_r = ['& Contraint', 'No Contraint', "Contraint","No Contraint", "Contraint","No Contraint"]
lb.beautify(table_number = table_nb,
            #reorder_var = reorder,
            #multi_lines_dep = multi_lines_dep,
            new_row= new_r,
            multicolumn = multicolumn,
            table_nte = tbe1,
            jupyter_preview = True,
            resolution = 150,
            folder = folder)
```

```sos kernel="SoS"

```

<!-- #region kernel="R" -->
### Transmission channel
<!-- #endregion -->

<!-- #region kernel="R" -->
Faire avec taille
<!-- #endregion -->

<!-- #region kernel="R" -->
Regarder l'hétérogénéity, SOE/foreign/taille

- Changer de polluants

Est ce que l'accès a la finance interne affecte les emissions. 

Une entreprise publique/large est moins sensible a la contrainte de crédit.
<!-- #endregion -->

```sos kernel="SoS"
folder = 'Tables_0'
table_nb = 1
table = 'table_{}'.format(table_nb)
path = os.path.join(folder, table + '.txt')
if os.path.exists(folder) == False:
        os.mkdir(folder)
for ext in ['.txt', '.pdf']:
    x = [a for a in os.listdir(folder) if a.endswith(ext)]
    [os.remove(os.path.join(folder, i)) for i in x]
```

```sos kernel="R"
t_0 <-felm(log(asset_tangibility_tot_asset) ~ 
            log(cashflow_to_tangible) + 
            log(liabilities_tot_asset) + 
            log(total_asset) + 
            log(age)
            | firm+year+geocode4_corr|0 | firm, df_final%>% 
            filter(lag_cashflow_to_tangible <0.837524),
            exactDOF = TRUE)
t_1<-felm(rd_tot_asset_trick ~ 
            log(cashflow_to_tangible) + 
            log(liabilities_tot_asset) + 
            log(total_asset) + 
            log(age)
            | firm+year+geocode4_corr|0 | firm, df_final%>% 
            filter(lag_cashflow_to_tangible <0.837524) %>% filter(year %in% list("2005","2006", "2007")),
            exactDOF = TRUE)
t_2<-felm(log(tfp_op) ~ 
            log(cashflow_to_tangible) + 
            log(liabilities_tot_asset) + 
            log(total_asset) + 
            log(age)
            | firm+year+geocode4_corr|0 | firm, df_final%>% 
            filter(lag_cashflow_to_tangible <0.837524) ,
            exactDOF = TRUE)
dep <- "Dependent variable: pollution emissions"
fe1 <- list(
    c("firm", "Yes", "Yes", "Yes"),
    c("year", "Yes", "Yes", "Yes"),
    c("city", "Yes", "Yes", "Yes")
             )

table_1 <- go_latex(list(
    t_0,t_1, t_2
),
    title="Transmission channel",
    dep_var = dep,
    addFE=fe1,
    save=TRUE,
    note = FALSE,
    name=path
)
```

```sos kernel="SoS"
tbe1  = "This table estimates eq(3). " \
"Heteroskedasticity-robust standard errors" \
"clustered at the product level appear inparentheses."\
"\sym{*} Significance at the 10\%, \sym{**} Significance at the 5\%, \sym{***} Significance at the 1\%."

multicolumn ={
    'SO2': 2,
    'COD': 2,
    'Waste water': 2
}

#multi_lines_dep = '(city/product/trade regime/year)'
new_r = ['& Asset tangilbility', 'R\&D', "TFP"]
lb.beautify(table_number = table_nb,
            #reorder_var = reorder,
            #multi_lines_dep = multi_lines_dep,
            new_row= new_r,
            #multicolumn = multicolumn,
            table_nte = tbe1,
            jupyter_preview = True,
            resolution = 150,
            folder = folder)
```

```sos kernel="SoS"
#- current_ratio
#- cashflow
#- quick_ratio
#- return_to_sale
#- coverage_ratio
#- liquidity
```

```sos kernel="SoS"
folder = 'Tables_0'
table_nb = 1
table = 'table_{}'.format(table_nb)
path = os.path.join(folder, table + '.txt')
if os.path.exists(folder) == False:
        os.mkdir(folder)
for ext in ['.txt', '.pdf']:
    x = [a for a in os.listdir(folder) if a.endswith(ext)]
    [os.remove(os.path.join(folder, i)) for i in x]
```

```sos kernel="SoS"
df_final['liquidity'].describe()
```

```sos kernel="R"
t_0 <-felm(log(asset_tangibility_tot_asset) ~ 
            log(current_ratio) + 
            log(liabilities_tot_asset) + 
            log(total_asset) + 
            log(age)
            | firm+year+geocode4_corr|0 | firm, df_final%>% 
            filter(lag_cashflow_to_tangible <0.837524),
            exactDOF = TRUE)
t_1<-felm(rd_tot_asset_trick ~ 
            log(current_ratio) + 
            log(liabilities_tot_asset) + 
            log(total_asset) + 
            log(age)
            | firm+year+geocode4_corr|0 | firm, df_final%>% 
            filter(lag_cashflow_to_tangible <0.837524) %>% filter(year %in% list("2005","2006", "2007")),
            exactDOF = TRUE)
t_2<-felm(log(tfp_op) ~ 
            log(current_ratio) + 
            log(liabilities_tot_asset) + 
            log(total_asset) + 
            log(age)
            | firm+year+geocode4_corr|0 | firm, df_final%>% 
            filter(lag_cashflow_to_tangible <0.837524) ,
            exactDOF = TRUE)
dep <- "Dependent variable: pollution emissions"
fe1 <- list(
    c("firm", "Yes", "Yes", "Yes"),
    c("year", "Yes", "Yes", "Yes"),
    c("city", "Yes", "Yes", "Yes")
             )

table_1 <- go_latex(list(
    t_0,t_1, t_2
),
    title="Transmission channel",
    dep_var = dep,
    addFE=fe1,
    save=TRUE,
    note = FALSE,
    name=path
)
```

```sos kernel="SoS"
tbe1  = "This table estimates eq(3). " \
"Heteroskedasticity-robust standard errors" \
"clustered at the product level appear inparentheses."\
"\sym{*} Significance at the 10\%, \sym{**} Significance at the 5\%, \sym{***} Significance at the 1\%."

multicolumn ={
    'SO2': 2,
    'COD': 2,
    'Waste water': 2
}

#multi_lines_dep = '(city/product/trade regime/year)'
new_r = ['& Asset tangilbility', 'R\&D', "TFP"]
lb.beautify(table_number = table_nb,
            #reorder_var = reorder,
            #multi_lines_dep = multi_lines_dep,
            new_row= new_r,
            #multicolumn = multicolumn,
            table_nte = tbe1,
            jupyter_preview = True,
            resolution = 150,
            folder = folder)
```

```sos kernel="SoS"

```

```sos kernel="SoS"

```

```sos kernel="SoS"
folder = 'Tables_0'
table_nb = 1
table = 'table_{}'.format(table_nb)
path = os.path.join(folder, table + '.txt')
if os.path.exists(folder) == False:
        os.mkdir(folder)
for ext in ['.txt', '.pdf']:
    x = [a for a in os.listdir(folder) if a.endswith(ext)]
    [os.remove(os.path.join(folder, i)) for i in x]
```

```sos kernel="R"
t_0 <-felm(log(asset_tangibility_tot_asset) ~ 
            log(cashflow_to_tangible) + 
            log(liabilities_tot_asset) + 
            log(total_asset) + 
            log(age)
            | firm+year+geocode4_corr|0 | firm, df_final%>% filter(SOE == "SOE")%>% 
            filter(lag_cashflow_to_tangible <0.837524),
            exactDOF = TRUE)

t_1 <-felm(log(asset_tangibility_tot_asset) ~ 
            log(cashflow_to_tangible) + 
            log(liabilities_tot_asset) + 
            log(total_asset) + 
            log(age)
            | firm+year+geocode4_corr|0 | firm, df_final%>% filter(SOE != "SOE")%>% 
            filter(lag_cashflow_to_tangible <0.837524),
            exactDOF = TRUE)

t_2<-felm(rd_tot_asset_trick ~ 
            log(cashflow_to_tangible) + 
            log(liabilities_tot_asset) + 
            log(total_asset) + 
            log(age)
            | firm+year+geocode4_corr|0 | firm, df_final%>% 
            filter(lag_cashflow_to_tangible <0.837524) %>% filter(SOE == "SOE")%>% filter(year %in% list("2005","2006", "2007")),
            exactDOF = TRUE)

t_3<-felm(rd_tot_asset_trick ~ 
            log(cashflow_to_tangible) + 
            log(liabilities_tot_asset) + 
            log(total_asset) + 
            log(age)
            | firm+year+geocode4_corr|0 | firm, df_final%>% 
            filter(lag_cashflow_to_tangible <0.837524) %>% filter(SOE != "SOE")%>% filter(year %in% list("2005","2006", "2007")),
            exactDOF = TRUE)

t_4<-felm(log(tfp_op) ~ 
            log(cashflow_to_tangible) + 
            log(liabilities_tot_asset) + 
            log(total_asset) + 
            log(age)
            | firm+year+geocode4_corr|0 | firm, df_final%>% filter(SOE == "SOE")%>% 
            filter(lag_cashflow_to_tangible <0.837524) ,
            exactDOF = TRUE)

t_5<-felm(log(tfp_op) ~ 
            log(cashflow_to_tangible) + 
            log(liabilities_tot_asset) + 
            log(total_asset) + 
            log(age)
            | firm+year+geocode4_corr|0 | firm, df_final%>% filter(SOE != "SOE")%>% 
            filter(lag_cashflow_to_tangible <0.837524) ,
            exactDOF = TRUE)

dep <- "Dependent variable: pollution emissions"
fe1 <- list(
    c("firm", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes"),
    c("year", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes"),
    c("city", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes")
             )

table_1 <- go_latex(list(
    t_0,t_1, t_2, t_3, t_4, t_5
),
    title="Transmission channel",
    dep_var = dep,
    addFE=fe1,
    save=TRUE,
    note = FALSE,
    name=path
)
```

```sos kernel="SoS"
tbe1  = "This table estimates eq(3). " \
"Heteroskedasticity-robust standard errors" \
"clustered at the product level appear inparentheses."\
"\sym{*} Significance at the 10\%, \sym{**} Significance at the 5\%, \sym{***} Significance at the 1\%."

multicolumn ={
    'Asset tangilbility': 2,
    'R\&D': 2,
    'TFP': 2
}

#multi_lines_dep = '(city/product/trade regime/year)'
new_r = ['& SOE', 'No SOE', "SOE","No SOE", "SOE","No SOE"]
lb.beautify(table_number = table_nb,
            #reorder_var = reorder,
            #multi_lines_dep = multi_lines_dep,
            new_row= new_r,
            multicolumn = multicolumn,
            table_nte = tbe1,
            jupyter_preview = True,
            resolution = 150,
            folder = folder)
```

```sos kernel="SoS"
folder = 'Tables_0'
table_nb = 1
table = 'table_{}'.format(table_nb)
path = os.path.join(folder, table + '.txt')
if os.path.exists(folder) == False:
        os.mkdir(folder)
for ext in ['.txt', '.pdf']:
    x = [a for a in os.listdir(folder) if a.endswith(ext)]
    [os.remove(os.path.join(folder, i)) for i in x]
```

```sos kernel="R"
t_0 <-felm(log(asset_tangibility_tot_asset) ~ 
            log(cashflow_to_tangible) + 
            log(liabilities_tot_asset) + 
            log(total_asset) + 
            log(age)
            | firm+year+geocode4_corr|0 | firm, df_final%>% filter(constraint == TRUE)%>% 
            filter(lag_cashflow_to_tangible <0.837524),
            exactDOF = TRUE)

t_1 <-felm(log(asset_tangibility_tot_asset) ~ 
            log(cashflow_to_tangible) + 
            log(liabilities_tot_asset) + 
            log(total_asset) + 
            log(age)
            | firm+year+geocode4_corr|0 | firm, df_final%>% filter(constraint != TRUE)%>% 
            filter(lag_cashflow_to_tangible <0.837524),
            exactDOF = TRUE)

t_2<-felm(rd_tot_asset_trick ~ 
            log(cashflow_to_tangible) + 
            log(liabilities_tot_asset) + 
            log(total_asset) + 
            log(age)
            | firm+year+geocode4_corr|0 | firm, df_final%>% 
            filter(lag_cashflow_to_tangible <0.837524) %>% filter(constraint == TRUE)%>% filter(year %in% list("2005","2006", "2007")),
            exactDOF = TRUE)

t_3<-felm(rd_tot_asset_trick ~ 
            log(cashflow_to_tangible) + 
            log(liabilities_tot_asset) + 
            log(total_asset) + 
            log(age)
            | firm+year+geocode4_corr|0 | firm, df_final%>% 
            filter(lag_cashflow_to_tangible <0.837524) %>% filter(constraint != TRUE)%>% filter(year %in% list("2005","2006", "2007")),
            exactDOF = TRUE)

t_4<-felm(log(tfp_op) ~ 
            log(cashflow_to_tangible) + 
            log(liabilities_tot_asset) + 
            log(total_asset) + 
            log(age)
            | firm+year+geocode4_corr|0 | firm, df_final%>% filter(constraint == TRUE)%>% 
            filter(lag_cashflow_to_tangible <0.837524) ,
            exactDOF = TRUE)

t_5<-felm(log(tfp_op) ~ 
            log(cashflow_to_tangible) + 
            log(liabilities_tot_asset) + 
            log(total_asset) + 
            log(age)
            | firm+year+geocode4_corr|0 | firm, df_final%>% filter(constraint != TRUE)%>% 
            filter(lag_cashflow_to_tangible <0.837524) ,
            exactDOF = TRUE)

dep <- "Dependent variable: pollution emissions"
fe1 <- list(
    c("firm", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes"),
    c("year", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes"),
    c("city", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes")
             )

table_1 <- go_latex(list(
    t_0,t_1, t_2, t_3, t_4, t_5
),
    title="Transmission channel",
    dep_var = dep,
    addFE=fe1,
    save=TRUE,
    note = FALSE,
    name=path
)
```

```sos kernel="SoS"
tbe1  = "This table estimates eq(3). " \
"Heteroskedasticity-robust standard errors" \
"clustered at the product level appear inparentheses."\
"\sym{*} Significance at the 10\%, \sym{**} Significance at the 5\%, \sym{***} Significance at the 1\%."

multicolumn ={
    'Asset tangilbility': 2,
    'R\&D': 2,
    'TFP': 2
}

#multi_lines_dep = '(city/product/trade regime/year)'
new_r = ['& Contraint', 'No Contraint', "Contraint","No Contraint", "Contraint","No Contraint"]
lb.beautify(table_number = table_nb,
            #reorder_var = reorder,
            #multi_lines_dep = multi_lines_dep,
            new_row= new_r,
            multicolumn = multicolumn,
            table_nte = tbe1,
            jupyter_preview = True,
            resolution = 150,
            folder = folder)
```

```sos kernel="SoS"
folder = 'Tables_0'
table_nb = 1
table = 'table_{}'.format(table_nb)
path = os.path.join(folder, table + '.txt')
if os.path.exists(folder) == False:
        os.mkdir(folder)
for ext in ['.txt', '.pdf']:
    x = [a for a in os.listdir(folder) if a.endswith(ext)]
    [os.remove(os.path.join(folder, i)) for i in x]
```

```sos kernel="R"
t_0 <-felm(log(asset_tangibility_tot_asset) ~ 
            log(cashflow_to_tangible) + 
            log(liabilities_tot_asset) + 
            log(total_asset) + 
            log(age)
            | firm+year+geocode4_corr|0 | firm, df_final_size%>% filter(d_employment == "Yes")%>% 
            filter(lag_cashflow_to_tangible <0.837524),
            exactDOF = TRUE)

t_1 <-felm(log(asset_tangibility_tot_asset) ~ 
            log(cashflow_to_tangible) + 
            log(liabilities_tot_asset) + 
            log(total_asset) + 
            log(age)
            | firm+year+geocode4_corr|0 | firm, df_final_size%>% filter(d_employment != "Yes")%>% 
            filter(lag_cashflow_to_tangible <0.837524),
            exactDOF = TRUE)

t_2<-felm(rd_tot_asset_trick ~ 
            log(cashflow_to_tangible) + 
            log(liabilities_tot_asset) + 
            log(total_asset) + 
            log(age)
            | firm+year+geocode4_corr|0 | firm, df_final_size%>% 
            filter(lag_cashflow_to_tangible <0.837524) %>% filter(d_employment == "Yes")%>% filter(year %in% list("2005","2006", "2007")),
            exactDOF = TRUE)

t_3<-felm(rd_tot_asset_trick ~ 
            log(cashflow_to_tangible) + 
            log(liabilities_tot_asset) + 
            log(total_asset) + 
            log(age)
            | firm+year+geocode4_corr|0 | firm, df_final_size%>% 
            filter(lag_cashflow_to_tangible <0.837524) %>% filter(d_employment != "Yes")%>% filter(year %in% list("2005","2006", "2007")),
            exactDOF = TRUE)

t_4<-felm(log(tfp_op) ~ 
            log(cashflow_to_tangible) + 
            log(liabilities_tot_asset) + 
            log(total_asset) + 
            log(age)
            | firm+year+geocode4_corr|0 | firm, df_final_size%>% filter(d_employment == "Yes")%>% 
            filter(lag_cashflow_to_tangible <0.837524) ,
            exactDOF = TRUE)

t_5<-felm(log(tfp_op) ~ 
            log(cashflow_to_tangible) + 
            log(liabilities_tot_asset) + 
            log(total_asset) + 
            log(age)
            | firm+year+geocode4_corr|0 | firm, df_final_size%>% filter(d_employment != "Yes")%>% 
            filter(lag_cashflow_to_tangible <0.837524) ,
            exactDOF = TRUE)

dep <- "Dependent variable: pollution emissions"
fe1 <- list(
    c("firm", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes"),
    c("year", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes"),
    c("city", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes")
             )

table_1 <- go_latex(list(
    t_0,t_1, t_2, t_3, t_4, t_5
),
    title="Transmission channel",
    dep_var = dep,
    addFE=fe1,
    save=TRUE,
    note = FALSE,
    name=path
)
```

```sos kernel="R"
summary(
felm(log(asset_tangibility_tot_asset) ~ 
            log(cashflow_to_tangible) * log(total_asset)+ 
            log(liabilities_tot_asset) + 
            log(total_asset) + 
            log(age)
            | firm+year+geocode4_corr|0 | firm, df_final_size%>% 
            filter(lag_cashflow_to_tangible <0.837524),
            exactDOF = TRUE))
```

```sos kernel="R"
summary(
felm(log(asset_tangibility_tot_asset) ~ 
            log(cashflow_to_tangible) * log(employment)+ 
            log(liabilities_tot_asset) + 
            log(total_asset) + 
            log(age)
            | firm+year+geocode4_corr|0 | firm, df_final_size%>% 
            filter(lag_cashflow_to_tangible <0.837524),
            exactDOF = TRUE))
```

```sos kernel="SoS"
tbe1  = "This table estimates eq(3). " \
"Heteroskedasticity-robust standard errors" \
"clustered at the product level appear inparentheses."\
"\sym{*} Significance at the 10\%, \sym{**} Significance at the 5\%, \sym{***} Significance at the 1\%."

multicolumn ={
    'Asset tangilbility': 2,
    'R\&D': 2,
    'TFP': 2
}

#multi_lines_dep = '(city/product/trade regime/year)'
new_r = ['& Large', 'No Large', "Large","No Large", "Large","No Large"]
lb.beautify(table_number = table_nb,
            #reorder_var = reorder,
            #multi_lines_dep = multi_lines_dep,
            new_row= new_r,
            multicolumn = multicolumn,
            table_nte = tbe1,
            jupyter_preview = True,
            resolution = 150,
            folder = folder)
```

<!-- #region kernel="R" -->
# Replicate one internal finance


-> prendre que asset tangilbilty et mettre les polluants, on explique que asset tangiblity

Mecanisme:

Cashflow
<!-- #endregion -->

<!-- #region kernel="R" -->
- Journal:
    - Environment and development economics: https://www.cambridge.org/core/journals/environment-and-development-economics
    - Review of environmental econ and policy: https://www.journals.uchicago.edu/toc/reep/current
<!-- #endregion -->

```sos kernel="R"
%get path table
t_0 <- felm(YYY ~XXX
            | FE|0 | CLUSTER, df_final %>% filter(XXX == 'YYY'),
            exactDOF = TRUE)

t_0 <- felm(YYY ~XXX
            | FE|0 | CLUSTER, df_final %>% filter(XXX != 'YYY'),
            exactDOF = TRUE)

t_2 <- felm(kYYY ~XXX
            | FE|0 | CLUSTER, df_final,
            exactDOF = TRUE)

t_3 <- felm(kYYY ~XXX
            | FE|0 | CLUSTER, df_final,
            exactDOF = TRUE)
            
dep <- "Dependent variable: YYYY"
fe1 <- list(
    c("XXXXX", "Yes", "Yes", "No", "No"),
    
    c("XXXXX", "Yes", "Yes", "No", "No"),
    
    c("XXXXX","Yes", "Yes", "Yes", "No"),
    
    c("XXXXX","No", "No", "Yes", "Yes"),
    
    c("XXXXX","No", "No", "Yes", "Yes"),
    
    c("XXXXX", "No", "No", "No", "Yes")
             )

table_1 <- go_latex(list(
    t_0,t_1, t_2, t_3
),
    title="TITLE",
    dep_var = dep,
    addFE=fe1,
    save=TRUE,
    note = FALSE,
    name=path
) 
```

```sos kernel="SoS"
tbe1  = "This table estimates eq(3). " \
"Heteroskedasticity-robust standard errors" \
"clustered at the product level appear inparentheses."\
"\sym{*} Significance at the 10\%, \sym{**} Significance at the 5\%, \sym{***} Significance at the 1\%."

#multicolumn ={
#    'Eligible': 2,
#    'Non-Eligible': 1,
#    'All': 1,
#    'All benchmark': 1,
#}

#multi_lines_dep = '(city/product/trade regime/year)'
#new_r = ['& test1', 'test2']
lb.beautify(table_number = table_nb,
            #reorder_var = reorder,
            #multi_lines_dep = multi_lines_dep,
            #new_row= new_r,
            #multicolumn = multicolumn,
            table_nte = tbe1,
            jupyter_preview = True,
            resolution = 150,
            folder = folder)
```

<!-- #region kernel="SoS" nteract={"transient": {"deleting": false}} -->
# Generate reports
<!-- #endregion -->

```sos kernel="python3" nteract={"transient": {"deleting": false}} outputExpanded=false
import os, time, shutil, urllib, ipykernel, json
from pathlib import Path
from notebook import notebookapp
import sys
path = os.getcwd()
parent_path = str(Path(path).parent.parent.parent)
sys.path.append(os.path.join(parent_path, 'utils'))
import make_toc
import create_report
```

```sos kernel="SoS"
name_json = 'parameters_ETL_pollution_credit_constraint.json'
path_json = os.path.join(str(Path(path).parent.parent), 'utils',name_json)
```

```sos kernel="python3" nteract={"transient": {"deleting": false}} outputExpanded=false
create_report.create_report(extension = "html", keep_code = False, notebookname = None)
```

```sos kernel="python3"
### Update TOC in Github
for p in [parent_path,
          str(Path(path).parent),
          #os.path.join(str(Path(path).parent), "00_download_data_from"),
          #os.path.join(str(Path(path).parent.parent), "02_data_analysis"),
          #os.path.join(str(Path(path).parent.parent), "02_data_analysis", "00_statistical_exploration"),
          #os.path.join(str(Path(path).parent.parent), "02_data_analysis", "01_model_estimation"),
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

```sos kernel="python3"
!jupyter nbconvert --no-input --to html 08_firm_level_estimation_pollution.ipynb
```
