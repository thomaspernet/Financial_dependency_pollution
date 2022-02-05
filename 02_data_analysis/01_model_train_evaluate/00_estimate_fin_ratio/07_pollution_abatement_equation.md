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
Model estimate Estimate pollution abatement equipment and internal finance


# Description

None

# Metadata

- Key: 317_Financial_dependency_pollution
- Epic: Models
- US: Evaluate econometrics model
- Task tag: #econometrics-strategy, #pollution-abatement-equipment, #training-Financial-dependency-pollution
- Analytics reports: 

# Input

## Table/file

**Name**

None

**Github**

- https://github.com/thomaspernet/Financial_dependency_pollution/blob/master/02_data_analysis/01_model_train_evaluate/00_estimate_fin_ratio/07_pollution_abatement_equation.md


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
<!-- #endregion -->

```sos kernel="SoS"
db = 'environment'
table = 'fin_dep_pollution_baseline_city'
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
from sklearn.preprocessing import StandardScaler
```

```sos kernel="SoS"
download_data = True
filename = 'df_{}'.format(table)
full_path_filename = 'SQL_OUTPUT_ATHENA/CSV/{}.csv'.format(filename)
path_local = os.path.join(str(Path(path).parent.parent.parent), 
                              "00_data_catalogue/temporary_local_data")
df_path = os.path.join(path_local, filename + '.csv')
if download_data:
    
    s3 = service_s3.connect_S3(client = client,
                          bucket = bucket, verbose = False)
    query = """
    SELECT * 
    FROM {}.{}
    WHERE 
      year in (
        '2001', '2002', '2003', '2004', '2005', 
        '2006', '2007'
      ) 
      AND 
      lag_current_ratio > 0 
      AND
      lag_cashflow_to_tangible > 0 
      AND 
      tfp_cit > 0
    """.format(db, table)
    df = (s3.run_query(
        query=query,
        database=db,
        s3_output='SQL_OUTPUT_ATHENA',
        filename=filename,  # Add filename to print dataframe
        destination_key='SQL_OUTPUT_ATHENA/CSV',  #Use it temporarily
        dtype = dtypes
    )
    .sort_values(by = ['geocode4_corr','ind2', 'year'])
    .assign(
        tso2_eq_output = lambda x: (x['tdso2_equip']+1)/(x['ttoutput']/1000),
         tso2_eq_output_1 = lambda x: (x['tdso2_equip']+1)/(x['output']/1000),
        lag_equipment = lambda x: x.groupby(['geocode4_corr','ind2'])['tdso2_equip'].transform("shift"),
        gross_change = lambda x: x['tdso2_equip'] - x['lag_equipment'],
        gross_change_output = lambda x: (x['tdso2_equip'] - x['lag_equipment'])/(x['output']/1000),
    )
          .assign(
          constraint = lambda x: x['credit_constraint'] > -0.47,
          std_eq_ind =lambda x: 
              x.groupby(['year', 'ind2'])['tso2_eq_output_1'].transform(lambda x: (x-x.min())/(x.max()-x.min()),
                                                                       ),
          std_eq_c =lambda x: 
              x.groupby(['year', 'geocode4_corr'])['tso2_eq_output_1'].transform(lambda x: (x-x.min())/(x.max()-x.min()),
                                                                       ),
          std_eq_year =lambda x: 
              x.groupby(['year'])['tso2_eq_output_1'].transform(lambda x: (x-x.min())/(x.max()-x.min()),
                                                                       ),
          std_eq =lambda x: (x['tso2_eq_output_1']-x['tso2_eq_output_1'].min())/(
              x['tso2_eq_output_1'].max()-x['tso2_eq_output_1'].min()),
        
         std_eq_ind_1 =lambda x: 
              x.groupby(['year', 'ind2'])['tdso2_equip'].transform(lambda x: (x-x.min())/(x.max()-x.min()),
                                                                       ),
          std_eq_c_1 =lambda x: 
              x.groupby(['year', 'geocode4_corr'])['tdso2_equip'].transform(lambda x: (x-x.min())/(x.max()-x.min()),
                                                                       ),
          std_eq_year_1 =lambda x: 
              x.groupby(['year'])['tdso2_equip'].transform(lambda x: (x-x.min())/(x.max()-x.min()),
                                                                       ),
          std_eq_1 =lambda x: (x['tdso2_equip']-x['tdso2_equip'].min())/(
              x['tdso2_equip'].max()-x['tdso2_equip'].min()),
          pct_change_eq = lambda x: x.groupby(['geocode4_corr','ind2'])['tso2_eq_output_1'].transform('pct_change'),
          pct_change_cash = lambda x: x.groupby(['geocode4_corr','ind2'])['lag_cashflow_to_tangible'].transform('pct_change'),
          pct_change_curr = lambda x: x.groupby(['geocode4_corr','ind2'])['lag_current_ratio'].transform('pct_change'),
          pct_change_cash_1 = lambda x: x.groupby(['geocode4_corr','ind2'])['cashflow_to_tangible'].transform('pct_change'),
          pct_change_curr_1 = lambda x: x.groupby(['geocode4_corr','ind2'])['current_ratio'].transform('pct_change'),
          pct_change_sales = lambda x: x.groupby(['geocode4_corr','ind2'])['sales'].transform('pct_change'),
          pct_change_liabilities_tot_asset = lambda x: x.groupby(['geocode4_corr','ind2'])['liabilities_tot_asset'].transform('pct_change'),
          pct_change_sales_tot_asset = lambda x: x.groupby(['geocode4_corr','ind2'])['sales_tot_asset'].transform('pct_change'),
          )
            )
    s3.download_file(
        key = full_path_filename
    )
    shutil.move(
        filename + '.csv',
        os.path.join(path_local, filename + '.csv')
    )
    s3.remove_file(full_path_filename)
    df.head()
```

```sos kernel="SoS"
df.reindex(columns = ['geocode4_corr','ind2', 'year',
                      'tso2_eq_output_1', 
                      'pct_change_eq', 
                      "std_eq_ind",
                      "std_eq_c",
                      "std_eq_year",
                      'std_eq',
                      "lag_cashflow_to_tangible",
                      "lag_current_ratio",
                     "pct_change_cash", 
                     'pct_change_curr'])
```

```sos kernel="SoS"
df.to_csv(os.path.join(path_local, filename + '.csv'))
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
add_to_dic = True
if add_to_dic:
    if os.path.exists("schema_table.json"):
        os.remove("schema_table.json")
        data = {'to_rename':[], 'to_remove':[]}
    dic_rename =  [
        {
        'old':'periodTRUE',
        'new':'\\text{period}'
        },
        ### depd
        {
        'old':'total\_asset',
        'new':'\\text{total asset}'
        },
        {
        'old':'tangible',
        'new':'\\text{tangible asset}'
        },
        {
        'old':'investment\_tot\_asset',
        'new':'\\text{investment to asset}'
        },
        {
        'old':'rd\_tot\_asset',
        'new':'\\text{rd to asset}'
        },
        {
        'old':'asset\_tangibility\_tot\_asset',
        'new':'\\text{asset tangibility}'
        },
        
        ### ind
        {
        'old':'current\_ratio',
        'new':'\\text{current ratio}'
        },
        {
        'old':'lag\_current\_ratio',
        'new':'\\text{current ratio}'
        },
        {
        'old':'quick\_ratio',
        'new':'\\text{quick ratio}'
        },
        {
        'old':'lag\_liabilities\_tot\_asset',
        'new':'\\text{liabilities to asset}'
        },
        {
        'old':'liabilities\_tot\_asset',
        'new':'\\text{liabilities to asset}'
        },
        {
        'old':'sales\_tot\_asset',
        'new':'\\text{sales to asset}'
        },
        {
        'old':'lag\_sales\_tot\_asset',
        'new':'\\text{sales to asset}'
        },
        {
        'old':'cash\_tot\_asset',
        'new':'\\text{cash to asset}'
        },
        {
        'old':'cashflow\_tot\_asset',
        'new':'\\text{cashflow to asset}'
        },
        {
        'old':'cashflow\_to\_tangible',
        'new':'\\text{cashflow}'
        },
        {
        'old':'lag\_cashflow\_to\_tangible',
        'new':'\\text{cashflow}'
        },
        {
        'old':'d\_credit\_constraintBELOW',
        'new':'\\text{Fin dep}_{i}'
        },
        ## control
        {
        'old':'age + 1',
        'new':'\\text{age}'
        },
        {
        'old':'export\_to\_sale',
        'new':'\\text{export to sale}'
        },
        {
        'old':'labor\_capital',
        'new':'\\text{labor to capital}'
        },
        ### Supply demand external finance
        {
        'old':'supply\_all\_credit',
        'new':'\\text{all credit}'
        },
        {
        'old':'supply\_long\_term\_credit',
        'new':'\\text{long term credit}'
        },
        {
        'old':'credit\_constraint',
        'new':'\\text{credit demand}'
        },
        {
        'old':'soe\_vs\_priPRIVATE',
        'new':'\\text{private}'
        },
        ## TFP
        {
        'old':'tfp\_cit',
        'new':'\\text{TFP}'
        }
        
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
    mutate_at(vars(starts_with("fe")), as.factor) %>%
mutate(
    period = relevel(as.factor(period), ref='FALSE'),
    constraint = relevel(as.factor(constraint), ref='FALSE'),
    polluted_d50i = relevel(as.factor(polluted_d50i), ref='BELOW'),
    polluted_d75i = relevel(as.factor(polluted_d75i), ref='BELOW'),
    polluted_d80i = relevel(as.factor(polluted_d80i), ref='BELOW'),
    polluted_d85i = relevel(as.factor(polluted_d85i), ref='BELOW'),
    polluted_d90i = relevel(as.factor(polluted_d90i), ref='BELOW'),
    polluted_d95i = relevel(as.factor(polluted_d95i), ref='BELOW'),
    polluted_d75_cit = relevel(as.factor(polluted_d75_cit), ref='BELOW'),
    polluted_d80_cit = relevel(as.factor(polluted_d80_cit), ref='BELOW'),
    polluted_d90_cit = relevel(as.factor(polluted_d90_cit), ref='BELOW'),
    polluted_mi = relevel(as.factor(polluted_mi), ref='BELOW'),
)
```

<!-- #region kernel="SoS" -->
## Table 1: Pollution abatement channel

$$\begin{aligned} \text{Equipment}_{cit} &=  \alpha_2 \text{Internal finance}_{cit-1}+\beta \text{X}_{cit} + \gamma_{it} +\gamma_{ct} + \epsilon_{cit} \end{aligned}$$

The following variables are lagged:

- cashflow
- current ratio
- sale over asset
<!-- #endregion -->

<!-- #region kernel="R" -->
Is the data correct?
<!-- #endregion -->

```sos kernel="SoS"
df.groupby(['constraint'])['ind2'].unique()
```

```sos kernel="SoS"
(
    df[[
        "lag_cashflow_to_tangible",
        "lag_current_ratio",
        "tfqzlssnl",
        "ttlssnl",
        "tdwastegas_equip",
        "tdso2_equip",
        'tso2_eq_output',
        'tso2_eq_output_1',
        "std_eq_ind",
        "std_eq_c",
        "std_eq_year",
        'std_eq']]
    .describe(percentiles=[.5, .7, .8, .9, .95, .99])
    .style
    .format("{0:,.2f}")
)
```

```sos kernel="SoS"
(
    df
    .assign(
            log_cash = lambda x: np.log(x['lag_cashflow_to_tangible']),
            log_current = lambda x: np.log(x['lag_current_ratio']),
            log_eq = lambda x: np.log(x['tdso2_equip']),
            log_tso2_eq_output = lambda x: np.log(x['tso2_eq_output_1']),
            log_ttlssnl = lambda x: np.log(x['ttlssnl']),
            log_tso2 = lambda x: np.log(x['tso2'])
               )
    [[
        'tso2_eq_output_1',
        "log_cash",
        "log_current",
    ]]
    #.loc[lambda x: x["tso2_eq_output_1"] > 0]
    #.loc[lambda x: x["tso2_eq_output_1"] < 20]
    #.loc[lambda x: x["lag_cashflow_to_tangible"] < 0.31]
    .corr()
)
```

<!-- #region kernel="SoS" -->
Test on `tdso2_equip` and `tdso2_equip/output`
<!-- #endregion -->

```sos kernel="SoS"
import seaborn as sns; sns.set_theme(color_codes=True)
```

```sos kernel="SoS"
test = (
    df
    #.loc[lambda x: x['ind2'].isin([13])]
        .assign(
            log_cash = lambda x: np.log(x['lag_cashflow_to_tangible']),
            log_current = lambda x: np.log(x['lag_current_ratio']),
            log_eq = lambda x: np.log(x['tdso2_equip'] + 1),
            log_tso2_eq_output = lambda x: np.log(x['tso2_eq_output_1']),
            log_ttlssnl = lambda x: np.log(x['ttlssnl']),
            log_tso2 = lambda x: np.log(x['tso2'])
               )
        .reindex(columns = ['year',
                            'log_cash',
                            'log_current',
                            "log_ttlssnl",
                            'log_eq',
                            'tso2_eq_output_1',
                            'log_tso2_eq_output',
                            "constraint",
                            'polluted_d75i',
                            "log_tso2"
                           ])
    .loc[lambda x: x["tso2_eq_output_1"] > 0]
    .loc[lambda x: x["tso2_eq_output_1"] < 20]
    .sort_values(by = ['log_ttlssnl'])
)
sns.regplot(x="log_cash", y="log_tso2_eq_output", data=test)
```

```sos kernel="SoS"
g = sns.FacetGrid(test, col="constraint", aspect=2, height =4)
g.map(sns.regplot, "log_cash", "log_tso2_eq_output")
```

```sos kernel="SoS"
g = sns.FacetGrid(test, col="polluted_d75i", aspect=2, height =4)
g.map(sns.regplot, "log_cash", "log_tso2_eq_output")
```

```sos kernel="SoS"

sns.regplot(x="log_current", y="log_tso2_eq_output", data=test)
```

```sos kernel="SoS"
g = sns.FacetGrid(test, col="constraint", aspect=2, height =4)
g.map(sns.regplot, "log_current", "log_tso2_eq_output")
```

```sos kernel="SoS"
g = sns.FacetGrid(test, col="polluted_d75i", aspect=2, height =4)
g.map(sns.regplot, "log_current", "log_tso2_eq_output")
```

```sos kernel="SoS"
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

```sos kernel="R"
%get path table
t_0 <- felm(log(tso2_eq_output_1) ~ 
            log(lag_cashflow_to_tangible) 
            | fe_t_i + fe_c_t|0 | geocode4_corr,df_final %>% filter(tso2_eq_output_1 > 0)
         ,
            exactDOF = TRUE)

t_1 <- felm(log(tso2_eq_output_1) ~ 
            log(lag_cashflow_to_tangible) +
            log(lag_liabilities_tot_asset) +
            log(lag_sales_tot_asset)
            | fe_t_i + fe_c_t|0 | geocode4_corr,df_final%>% filter(tso2_eq_output_1 > 0)
         ,
            exactDOF = TRUE)

t_2 <- felm(log(tso2_eq_output_1) ~ 
            log(lag_cashflow_to_tangible) * constraint
            | fe_t_i + fe_c_t|0 | geocode4_corr,df_final%>% filter(tso2_eq_output_1 > 0)
         ,
            exactDOF = TRUE)

t_3 <- felm(log(tso2_eq_output_1) ~ 
            log(lag_cashflow_to_tangible) * constraint+
            log(lag_liabilities_tot_asset) +
            log(lag_sales_tot_asset)
            | fe_t_i + fe_c_t|0 | geocode4_corr,df_final%>% filter(tso2_eq_output_1 > 0)
         ,
            exactDOF = TRUE)

dep <- "Dependent variable: pollution abattement equipment SO2"
fe1 <- list(
    c("industry-year", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes"),
    c("city-year", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes")
             )
table_1 <- go_latex(list(
    t_0,t_1, t_2, t_3
),
    title="Baseline Estimate, pollution abattement equipment SO2",
    dep_var = dep,
    addFE=fe1,
    save=TRUE,
    note = FALSE,
    name=path
)
```

```sos kernel="SoS"
tbe1  = "This table estimates eq(3). " \
"Heteroskedasticity-robust standard errors " \
"clustered at the city level appear inp arentheses. "\
" Independent variable cashflow is measured as net income + depreciation over asset;"\
" current ratio is measured as current asset over current liabilities. " \
"The following variables are lagged one year: Current Ratio, Cashflow, Liabilities/Assets, and Sales/Assets"
"\sym{*} Significance at the 10\%, \sym{**} Significance at the 5\%, \sym{***} Significance at the 1\%."

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

```sos kernel="SoS"
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

```sos kernel="R"
%get path table
t_0 <- felm(log(tso2_eq_output_1) ~ 
            log(lag_current_ratio) 
            | fe_t_i + fe_c_t|0 | geocode4_corr,df_final %>% filter(tso2_eq_output_1 >0)
         ,
            exactDOF = TRUE)

t_1 <- felm(log(tso2_eq_output_1) ~ 
            log(lag_current_ratio) +
            log(lag_liabilities_tot_asset) +
            log(lag_sales_tot_asset)
            | fe_t_i + fe_c_t|0 | geocode4_corr,df_final%>% filter(tso2_eq_output_1 >0)
         ,
            exactDOF = TRUE)

t_2 <- felm(log(tso2_eq_output_1) ~ 
            log(lag_current_ratio) * constraint
            | fe_t_i + fe_c_t|0 | geocode4_corr,df_final%>% filter(tso2_eq_output_1 >0)
         ,
            exactDOF = TRUE)

t_3 <- felm(log(tso2_eq_output_1) ~ 
            log(lag_current_ratio) * constraint+
            log(lag_liabilities_tot_asset) +
            log(lag_sales_tot_asset)
            | fe_t_i + fe_c_t|0 | geocode4_corr,df_final%>% filter(tso2_eq_output_1 >0),
            exactDOF = TRUE)

dep <- "Dependent variable: pollution abattement equipment SO2"
fe1 <- list(
    c("industry-year", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes"),
    c("city-year", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes")
             )
table_1 <- go_latex(list(
    t_0,t_1, t_2, t_3
),
    title="Baseline Estimate, pollution abattement equipment SO2",
    dep_var = dep,
    addFE=fe1,
    save=TRUE,
    note = FALSE,
    name=path
)
```

```sos kernel="SoS"
tbe1  = "This table estimates eq(3). " \
"Heteroskedasticity-robust standard errors " \
"clustered at the city level appear inp arentheses. "\
" Independent variable cashflow is measured as net income + depreciation over asset;"\
" current ratio is measured as current asset over current liabilities. " \
"The following variables are lagged one year: Current Ratio, Cashflow, Liabilities/Assets, and Sales/Assets"
"\sym{*} Significance at the 10\%, \sym{**} Significance at the 5\%, \sym{***} Significance at the 1\%."

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

```sos kernel="SoS"
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

```sos kernel="R"
change_target <- function(table){
    ## SOE
    check_target_current_ratio <- grep("constraintTRUE:log\\(lag_current_ratio\\)", rownames(table$coef))
    check_target_cashflow <- grep("log\\(lag_cashflow_to_tangible\\):constraintTRUE", rownames(table$coef))
    ## foreign
    rownames(table$coefficients)[check_target_current_ratio] <- 'log(current_ratio):constraint'
    rownames(table$beta)[check_target_current_ratio] <- 'log(current_ratio):constraint'
    
    rownames(table$coefficients)[check_target_cashflow] <- 'log(lag_cashflow_to_tangible):constraint'
    rownames(table$beta)[check_target_cashflow] <- 'log(lag_cashflow_to_tangible):constraint'
    return (table)
    }
```

```sos kernel="R"
%get path table
t_0 <- felm(log(tso2_eq_output_1) ~ 
            log(lag_cashflow_to_tangible) +
            log(lag_current_ratio) 
            | fe_t_i + fe_c_t|0 | geocode4_corr,df_final %>% filter(tso2_eq_output_1 >0)
         ,
            exactDOF = TRUE)

t_1 <- felm(log(tso2_eq_output_1) ~ 
            log(lag_cashflow_to_tangible) +
            log(lag_current_ratio) +
            log(lag_liabilities_tot_asset) +
            log(lag_sales_tot_asset)
            | fe_t_i + fe_c_t|0 | geocode4_corr,df_final%>% filter(tso2_eq_output_1 >0)
         ,
            exactDOF = TRUE)

t_2 <- felm(log(tso2_eq_output_1) ~ 
            log(lag_cashflow_to_tangible)*constraint +
            log(lag_current_ratio) *constraint 
            | fe_t_i + fe_c_t|0 | geocode4_corr,df_final%>% filter(tso2_eq_output_1 >0)
         ,
            exactDOF = TRUE)
t_2 <- change_target(t_2)
t_3 <- felm(log(tso2_eq_output_1) ~ 
           log(lag_cashflow_to_tangible) *constraint +
            log(lag_current_ratio) *constraint +
            log(lag_liabilities_tot_asset) +
            log(lag_sales_tot_asset)
            | fe_t_i + fe_c_t|0 | geocode4_corr,df_final%>% filter(tso2_eq_output_1 >0),
            exactDOF = TRUE)
t_3 <- change_target(t_3)
dep <- "Dependent variable: pollution abattement equipment SO2"
fe1 <- list(
    c("industry-year", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes"),
    c("city-year", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes")
             )
table_1 <- go_latex(list(
    t_0,t_1, t_2, t_3
),
    title="Baseline Estimate, pollution abattement equipment SO2",
    dep_var = dep,
    addFE=fe1,
    save=TRUE,
    note = FALSE,
    name=path
)
```

```sos kernel="SoS"
tbe1  = "This table estimates eq(3). " \
"Heteroskedasticity-robust standard errors " \
"clustered at the city level appear inp arentheses. "\
" Independent variable cashflow is measured as net income + depreciation over asset;"\
" current ratio is measured as current asset over current liabilities. " \
"The following variables are lagged one year: Current Ratio, Cashflow, Liabilities/Assets, and Sales/Assets"
"\sym{*} Significance at the 10\%, \sym{**} Significance at the 5\%, \sym{***} Significance at the 1\%."

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

<!-- #region kernel="R" -->
### Percentage change
<!-- #endregion -->

```sos kernel="SoS"
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

```sos kernel="R"
%get path table
t_0 <- (
    felm(pct_change_eq ~ 
            pct_change_cash +
         lag_equipment 
            | fe_t_i + fe_c_t|0 | geocode4_corr,df_final%>% 
  filter_at(vars(pct_change_eq, pct_change_cash), all_vars(!is.infinite(.)))
         ,
            exactDOF = TRUE)
)

t_1 <- (
    felm(pct_change_eq ~ 
            pct_change_sales +
            pct_change_cash +
            lag_equipment +
            pct_change_liabilities_tot_asset +
            pct_change_sales_tot_asset 
            | fe_t_i + fe_c_t|0 | geocode4_corr,df_final%>% 
  filter_at(vars(pct_change_eq, pct_change_cash), all_vars(!is.infinite(.)))
         ,
            exactDOF = TRUE)
)

t_2 <- (
    felm(pct_change_eq ~ 
            pct_change_cash * constraint+
         lag_equipment 
            | fe_t_i + fe_c_t|0 | geocode4_corr,df_final%>% 
  filter_at(vars(pct_change_eq, pct_change_cash), all_vars(!is.infinite(.)))
         ,
            exactDOF = TRUE)
)

t_3 <- (
    felm(pct_change_eq ~ 
            pct_change_sales +
            pct_change_cash* constraint +
            lag_equipment +
            pct_change_liabilities_tot_asset +
            pct_change_sales_tot_asset  
            | fe_t_i + fe_c_t|0 | geocode4_corr,df_final%>% 
  filter_at(vars(pct_change_eq, pct_change_cash), all_vars(!is.infinite(.)))
         ,
            exactDOF = TRUE)
)

dep <- "Dependent variable: pollution abattement equipment SO2"
fe1 <- list(
    c("industry-year", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes"),
    c("city-year", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes")
             )
table_1 <- go_latex(list(
    t_0,t_1, t_2, t_3
),
    title="Baseline Estimate, pollution abattement equipment SO2",
    dep_var = dep,
    addFE=fe1,
    save=TRUE,
    note = FALSE,
    name=path
)
```

```sos kernel="SoS"
tbe1  = "This table estimates eq(3). " \
"Heteroskedasticity-robust standard errors " \
"clustered at the city level appear inp arentheses. "\
" Independent variable cashflow is measured as net income + depreciation over asset;"\
" current ratio is measured as current asset over current liabilities. " \
"The following variables are lagged one year: Current Ratio, Cashflow, Liabilities/Assets, and Sales/Assets"
"\sym{*} Significance at the 10\%, \sym{**} Significance at the 5\%, \sym{***} Significance at the 1\%."

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

```sos kernel="SoS"
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

```sos kernel="R"
%get path table
t_0 <- (
    felm(pct_change_eq ~ 
            pct_change_curr +
         lag_equipment 
            | fe_t_i + fe_c_t|0 | geocode4_corr,df_final%>% 
  filter_at(vars(pct_change_eq, pct_change_cash), all_vars(!is.infinite(.)))
         ,
            exactDOF = TRUE)
)

t_1 <- (
    felm(pct_change_eq ~ 
            pct_change_sales +
            pct_change_curr +
            lag_equipment +
            pct_change_liabilities_tot_asset +
            pct_change_sales_tot_asset 
            | fe_t_i + fe_c_t|0 | geocode4_corr,df_final%>% 
  filter_at(vars(pct_change_eq, pct_change_cash), all_vars(!is.infinite(.)))
         ,
            exactDOF = TRUE)
)

t_2 <- (
    felm(pct_change_eq ~ 
            pct_change_curr * constraint+
         lag_equipment 
            | fe_t_i + fe_c_t|0 | geocode4_corr,df_final%>% 
  filter_at(vars(pct_change_eq, pct_change_cash), all_vars(!is.infinite(.)))
         ,
            exactDOF = TRUE)
)

t_3 <- (
    felm(pct_change_eq ~ 
            pct_change_sales +
            pct_change_curr* constraint +
            lag_equipment +
            pct_change_liabilities_tot_asset +
            pct_change_sales_tot_asset  
            | fe_t_i + fe_c_t|0 | geocode4_corr,df_final%>% 
  filter_at(vars(pct_change_eq, pct_change_cash), all_vars(!is.infinite(.)))
         ,
            exactDOF = TRUE)
)

dep <- "Dependent variable: pollution abattement equipment SO2"
fe1 <- list(
    c("industry-year", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes"),
    c("city-year", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes")
             )
table_1 <- go_latex(list(
    t_0,t_1, t_2, t_3
),
    title="Baseline Estimate, pollution abattement equipment SO2",
    dep_var = dep,
    addFE=fe1,
    save=TRUE,
    note = FALSE,
    name=path
)
```

```sos kernel="SoS"
tbe1  = "This table estimates eq(3). " \
"Heteroskedasticity-robust standard errors " \
"clustered at the city level appear inp arentheses. "\
" Independent variable cashflow is measured as net income + depreciation over asset;"\
" current ratio is measured as current asset over current liabilities. " \
"The following variables are lagged one year: Current Ratio, Cashflow, Liabilities/Assets, and Sales/Assets"
"\sym{*} Significance at the 10\%, \sym{**} Significance at the 5\%, \sym{***} Significance at the 1\%."

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

<!-- #region kernel="SoS" -->
## Gross change
<!-- #endregion -->

```sos kernel="SoS"
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

```sos kernel="R"
%get path table
t_0 <- (
    felm(gross_change ~ 
            lag_cashflow_to_tangible +
         lag_equipment 
            | fe_t_i + fe_c_t|0 | geocode4_corr,df_final%>% 
  filter_at(vars(pct_change_eq, pct_change_cash), all_vars(!is.infinite(.)))
         ,
            exactDOF = TRUE)
)

t_1 <- (
    felm(gross_change ~ 
            log(sales) +
            lag_cashflow_to_tangible +
         lag_equipment +
            log(lag_current_ratio) +
            log(lag_liabilities_tot_asset) +
            log(lag_sales_tot_asset) 
            | fe_t_i + fe_c_t|0 | geocode4_corr,df_final%>% 
  filter_at(vars(pct_change_eq, pct_change_cash), all_vars(!is.infinite(.)))
         ,
            exactDOF = TRUE)
)

t_2 <- (
    felm(gross_change ~ 
            lag_cashflow_to_tangible * constraint+
         lag_equipment 
            | fe_t_i + fe_c_t|0 | geocode4_corr,df_final%>% 
  filter_at(vars(pct_change_eq, pct_change_cash), all_vars(!is.infinite(.)))
         ,
            exactDOF = TRUE)
)

t_3 <- (
    felm(gross_change ~ 
            log(sales) +
            lag_cashflow_to_tangible * constraint+
         lag_equipment +
            log(lag_current_ratio) +
            log(lag_liabilities_tot_asset) +
            log(lag_sales_tot_asset) 
            | fe_t_i + fe_c_t|0 | geocode4_corr,df_final%>% 
  filter_at(vars(pct_change_eq, pct_change_cash), all_vars(!is.infinite(.)))
         ,
            exactDOF = TRUE)
)

dep <- "Dependent variable: pollution abattement equipment SO2"
fe1 <- list(
    c("industry-year", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes"),
    c("city-year", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes")
             )
table_1 <- go_latex(list(
    t_0,t_1, t_2, t_3
),
    title="Baseline Estimate, pollution abattement equipment SO2",
    dep_var = dep,
    addFE=fe1,
    save=TRUE,
    note = FALSE,
    name=path
)
```

```sos kernel="SoS"
tbe1  = "This table estimates eq(3). " \
"Heteroskedasticity-robust standard errors " \
"clustered at the city level appear inp arentheses. "\
" Independent variable cashflow is measured as net income + depreciation over asset;"\
" current ratio is measured as current asset over current liabilities. " \
"The following variables are lagged one year: Current Ratio, Cashflow, Liabilities/Assets, and Sales/Assets"
"\sym{*} Significance at the 10\%, \sym{**} Significance at the 5\%, \sym{***} Significance at the 1\%."

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

```sos kernel="SoS"
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

```sos kernel="R"
%get path table
t_0 <- (
    felm(gross_change ~ 
            lag_current_ratio +
         lag_equipment
            | fe_t_i + fe_c_t|0 | geocode4_corr,df_final%>% 
  filter_at(vars(pct_change_eq, pct_change_cash), all_vars(!is.infinite(.)))
         ,
            exactDOF = TRUE)
)

t_1 <- (
    felm(gross_change ~ 
            log(sales) +
            lag_current_ratio +
         lag_equipment +
            log(lag_current_ratio) +
            log(lag_liabilities_tot_asset) +
            log(lag_sales_tot_asset) 
            | fe_t_i + fe_c_t|0 | geocode4_corr,df_final%>% 
  filter_at(vars(pct_change_eq, pct_change_cash), all_vars(!is.infinite(.)))
         ,
            exactDOF = TRUE)
)

t_2 <- (
    felm(gross_change ~ 
            lag_current_ratio * constraint +
         lag_equipment 
            | fe_t_i + fe_c_t|0 | geocode4_corr,df_final%>% 
  filter_at(vars(pct_change_eq, pct_change_cash), all_vars(!is.infinite(.)))
         ,
            exactDOF = TRUE)
)

t_3 <- (
    felm(gross_change ~ 
            log(sales) +
            lag_current_ratio * constraint+
         lag_equipment +
            log(lag_current_ratio) +
            log(lag_liabilities_tot_asset) +
            log(lag_sales_tot_asset) 
            | fe_t_i + fe_c_t|0 | geocode4_corr,df_final%>% 
  filter_at(vars(pct_change_eq, pct_change_cash), all_vars(!is.infinite(.)))
         ,
            exactDOF = TRUE)
)

dep <- "Dependent variable: pollution abattement equipment SO2"
fe1 <- list(
    c("industry-year", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes"),
    c("city-year", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes")
             )
table_1 <- go_latex(list(
    t_0,t_1, t_2, t_3
),
    title="Baseline Estimate, pollution abattement equipment SO2",
    dep_var = dep,
    addFE=fe1,
    save=TRUE,
    note = FALSE,
    name=path
)
```

```sos kernel="SoS"
tbe1  = "This table estimates eq(3). " \
"Heteroskedasticity-robust standard errors " \
"clustered at the city level appear inp arentheses. "\
" Independent variable cashflow is measured as net income + depreciation over asset;"\
" current ratio is measured as current asset over current liabilities. " \
"The following variables are lagged one year: Current Ratio, Cashflow, Liabilities/Assets, and Sales/Assets"
"\sym{*} Significance at the 10\%, \sym{**} Significance at the 5\%, \sym{***} Significance at the 1\%."

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

<!-- #region kernel="R" -->
- Augmentation du cashflow finance les investissements qui réduisent les SO2, mais pas le meme résulat pour le current ratio, pourquoi? 
- Que mesure finalement la variable equipment -> est ce que les investissments dans les technos green ne sont mesurés que par la variable equipment.
- Est ce que les descriptives stats sont correctes?
- interaction: industry & industry-city
- Que mesure précisement SO2 equipment?
<!-- #endregion -->

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

```sos kernel="python3"
name_json = 'parameters_ETL_pollution_credit_constraint.json'
path_json = os.path.join(str(Path(path).parent.parent), 'utils',name_json)
```

```sos kernel="python3" nteract={"transient": {"deleting": false}} outputExpanded=false
create_report.create_report(extension = "html", keep_code = False, notebookname = "07_pollution_abatement_equation.ipynb")
```
