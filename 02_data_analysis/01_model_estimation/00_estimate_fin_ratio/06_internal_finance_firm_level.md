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

Estimate tangible asset as a function of  current ratio and others (Evaluate role of internal finance) 

# Business needs 

Estimate tangible asset as a function of  current ratio, cash_over_total_asset, liabilities over asset, Cash flow to total asset, cash flow to total tangible asset, credit constraint (Evaluate the impact of internal finance on asset) 

## Description
### Objective 

Test the coefficient sign and significant of the main variable

### Tables

Table 1: Baseline Estimate, tangible asset and current ratio, cash_over_total_asset, liabilities over asset, Cash flow to total asset, cash flow to total tangible asset, credit constraint

**Cautious**
Make sure no empty rows, otherwise it will be filtered out in the estimate


# Metadata

- Key: gac25vjvp54377n
- Epic: Models
- US: Internal finance estimate
- Task tag: #internal-finance, #credit-constraint, #firm-level
- Analytics reports: 

# Input Cloud Storage

## Table/file

**Name** 

- https://github.com/thomaspernet/Financial_dependency_pollution/blob/master/01_data_preprocessing/02_transform_tables/09_asif_tfp_firm_baseline.md

**Github**

- DATA/ECON/FIRM_SURVEY/ASIF_CHINA/TRANSFORMED/TFP/CREDIT_CONSTRAINT


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

path = os.getcwd()
parent_path = str(Path(path).parent.parent.parent)


name_credential = 'financial_dep_SO2_accessKeys.csv'
region = 'eu-west-3'
bucket = 'datalake-datascience'
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

R has a hard time importing correctly values with many missing values, so to force R to import `rd_tot_asset` as a float, we use the following trick

- `rd_tot_asset` lowest value is -.013. Hence, when `rd_tot_asset` is missing replace by -1000
<!-- #endregion -->

```sos kernel="SoS"
db = 'firms_survey'
table = 'asif_tfp_credit_constraint'
```

```sos kernel="SoS"
dtypes = {}
schema = (glue.get_table_information(database = db,
                           table = table)
          ['Table']['StorageDescriptor']['Columns']
         )
for key, value in enumerate(schema):
    if value['Type'] in ['varchar(12)']:
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
                              "00_data_catalogue/temporary_local_data")
df_path = os.path.join(path_local, filename + '.csv')
if download_data:
    
    s3 = service_s3.connect_S3(client = client,
                          bucket = bucket, verbose = False)
    query = """
    SELECT *,
    CASE WHEN rd_tot_asset IS NULL THEN -1000 ELSE rd_tot_asset END AS rd_tot_asset_trick,
    CASE WHEN rd_tot_asset IS NULL and year in ('2005', 
        '2006', '2007'
      ) THEN 0
    WHEN rd_tot_asset < 0 THEN 0
    ELSE rd_tot_asset END AS rd_tot_asset_trick_1
    FROM {}.{}
    WHERE
    cashflow_to_tangible > 0
    AND current_ratio > 0
    AND liabilities_tot_asset > 0
    AND year in (
        '2001', '2002', '2003', '2004', '2005', 
        '2006', '2007'
      )
    """.format(db, table)
    df = (s3.run_query(
        query=query,
        database=db,
        s3_output='SQL_OUTPUT_ATHENA',
        filename=filename,  # Add filename to print dataframe
        destination_key='SQL_OUTPUT_ATHENA/CSV',  #Use it temporarily
        dtype = dtypes
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
        'new':'\\text{size}'
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
        'new':'\\text{collateral}'
        },
        
        ### ind
        {
        'old':'current\_ratio',
        'new':'\\text{current ratio}'
        },
        {
        'old':'quick\_ratio',
        'new':'\\text{quick ratio}'
        },
        {
        'old':'liabilities\_tot\_asset',
        'new':'\\text{leverage}'
        },
        {
        'old':'sales\_tot\_asset',
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
    ]
    

    data['to_rename'].extend(dic_rename)
    with open('schema_table.json', 'w') as outfile:
        json.dump(data, outfile)
```

```sos kernel="SoS"
import function.latex_beautify as lb

#%load_ext autoreload
#%autoreload 2
```

```sos kernel="R"
options(warn=-1)
library(tidyverse)
library(lfe)
#library(lazyeval)
library('progress')
path = "function/table_golatex.R"
source(path)
```

```sos kernel="R"
%get df_path
df_final <- read_csv(df_path) %>%
mutate_if(is.character, as.factor) %>%
    mutate_at(vars(starts_with("fe")), as.factor) %>%
mutate(
    period = relevel(as.factor(period), ref='FALSE'),
    soe_vs_pri = relevel(as.factor(soe_vs_pri), ref='SOE'))
```

```sos kernel="R"
glimpse(df_final)
```

<!-- #region kernel="SoS" -->
## Table 1: Effect of internal credit on asset accumulation

$$
\begin{aligned}
\text { (Asset) }_{i t}= a_{1}\left(\right.\text { Cash flow) }_{i t} + \text { error term, }
\end{aligned}
$$



* Column 1: total asset
    * FE: 
        - fe 1: `firm`
        - fe 2: `industry-year`
* Column 2: tangible
    * FE: 
        - fe 1: `firm`
        - fe 2: `industry-year`
* Column 3: investment to total asset
    * FE: 
        - fe 1: `firm`
        - fe 2: `industry-year`
* Column 4: tangible to total asset
    * FE: 
        - fe 1: `firm`
        - fe 2: `industry-year`
* Column 4: RD to total asset
    * FE: 
        - fe 1: `firm`
        - fe 2: `industry-year`

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

```sos kernel="R"
%get path table
t_0 <- felm(log(total_asset) ~
            log(current_ratio) +
            log(liabilities_tot_asset) +
            log(cashflow_to_tangible) +
            log(age) +
            export_to_sale 
            | firm + year + indu_2|0 | firm,df_final,
            exactDOF = TRUE)

t_1 <- felm(log(tangible) ~
            log(current_ratio) +
            log(liabilities_tot_asset) +
            log(cashflow_to_tangible) +
            log(age) +
            export_to_sale 
            | firm + year + indu_2|0 | firm,df_final,
            exactDOF = TRUE)

t_2 <- felm(log(asset_tangibility_tot_asset) ~
            log(current_ratio) +
            log(liabilities_tot_asset) +
            log(cashflow_to_tangible) +
            log(age) +
            export_to_sale 
            | firm + year + indu_2|0 | firm,df_final,
            exactDOF = TRUE)

t_3 <- felm(investment_tot_asset ~
            log(current_ratio) +
            log(liabilities_tot_asset) +
            log(cashflow_to_tangible) +
            log(age) +
            export_to_sale 
            | firm + year + indu_2|0 | firm,df_final ,
            exactDOF = TRUE)

t_4 <- felm(rd_tot_asset_trick ~
            log(current_ratio) +
            log(liabilities_tot_asset) +
            log(cashflow_to_tangible) +
            log(age) +
            export_to_sale 
            | firm + year + indu_2|0 | firm,df_final %>% filter(year %in% list("2005","2006", "2007")),
            exactDOF = TRUE)
            
dep <- "Dependent variable Asset"
fe1 <- list(
    c("firm", "Yes", "Yes", "Yes", "Yes", "Yes"),
    
    c("industry-year", "Yes", "Yes", "Yes", "Yes", "Yes")
             )

table_1 <- go_latex(list(
    t_0,t_1, t_2, t_3, t_4
),
    title="Effect of internal credit on asset accumulation",
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
new_r = ['& Asset', 'Tangible', 'Tangible to asset', 'Investment to asset', 'RD']
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

<!-- #region kernel="SoS" -->
## Table 2: Effect of external credit supply on asset accumulation, all credits

1. Effect of external credit supply
    - all credits
    - long term credits
2. Effect of external credit demand
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
t_0 <- felm(log(total_asset) ~
            supply_all_credit +
            log(current_ratio) +
            log(liabilities_tot_asset) +
            log(cashflow_to_tangible) +
            log(age) +
            export_to_sale 
            | firm + year + indu_2|0 | firm,df_final,
            exactDOF = TRUE)

t_1 <- felm(log(tangible) ~
            supply_all_credit +
            log(current_ratio) +
            log(liabilities_tot_asset) +
            log(cashflow_to_tangible) +
            log(age) +
            export_to_sale 
            | firm + year + indu_2|0 | firm,df_final,
            exactDOF = TRUE)

t_2 <- felm(log(asset_tangibility_tot_asset) ~
            supply_all_credit +
            log(current_ratio) +
            log(liabilities_tot_asset) +
            log(cashflow_to_tangible) +
            log(age) +
            export_to_sale 
            | firm + year + indu_2|0 | firm,df_final,
            exactDOF = TRUE)

t_3 <- felm(investment_tot_asset ~
            supply_all_credit +
            log(current_ratio) +
            log(liabilities_tot_asset) +
            log(cashflow_to_tangible) +
            log(age) +
            export_to_sale 
            | firm + year + indu_2|0 | firm,df_final ,
            exactDOF = TRUE)

t_4 <- felm(rd_tot_asset_trick ~
            supply_all_credit +
            log(current_ratio) +
            log(liabilities_tot_asset) +
            log(cashflow_to_tangible) +
            log(age) +
            export_to_sale 
            | firm + year + indu_2|0 | firm,df_final %>% filter(year %in% list("2005","2006", "2007")),
            exactDOF = TRUE)
            
dep <- "Dependent variable Asset"
fe1 <- list(
    c("firm", "Yes", "Yes", "Yes", "Yes", "Yes"),
    
    c("industry-year", "Yes", "Yes", "Yes", "Yes", "Yes")
             )

table_1 <- go_latex(list(
    t_0,t_1, t_2, t_3, t_4
),
    title="Effect of external credit supply on asset accumulation, all credits",
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
new_r = ['& Asset', 'Tangible', 'Tangible to asset','Investment to asset', 'RD']
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

<!-- #region kernel="SoS" -->
## Table 3: Effect of external credit supply on asset accumulation,  long term credits

1. Effect of external credit supply
    - all credits
    - long term credits
2. Effect of external credit demand
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
t_0 <- felm(log(total_asset) ~
            supply_long_term_credit +
            log(current_ratio) +
            log(liabilities_tot_asset) +
            log(cashflow_to_tangible) +
            log(age) +
            export_to_sale 
            | firm + year + indu_2|0 | firm,df_final,
            exactDOF = TRUE)

t_1 <- felm(log(tangible) ~
            supply_long_term_credit +
            log(current_ratio) +
            log(liabilities_tot_asset) +
            log(cashflow_to_tangible) +
            log(age) +
            export_to_sale 
            | firm + year + indu_2|0 | firm,df_final,
            exactDOF = TRUE)

t_2 <- felm(log(asset_tangibility_tot_asset) ~
            supply_long_term_credit +
            log(current_ratio) +
            log(liabilities_tot_asset) +
            log(cashflow_to_tangible) +
            log(age) +
            export_to_sale 
            | firm + year + indu_2|0 | firm,df_final,
            exactDOF = TRUE)

t_3 <- felm(investment_tot_asset ~
            supply_long_term_credit +
            log(current_ratio) +
            log(liabilities_tot_asset) +
            log(cashflow_to_tangible) +
            log(age) +
            export_to_sale 
            | firm + year + indu_2|0 | firm,df_final ,
            exactDOF = TRUE)

t_4 <- felm(rd_tot_asset_trick ~
            supply_long_term_credit +
            log(current_ratio) +
            log(liabilities_tot_asset) +
            log(cashflow_to_tangible) +
            log(age) +
            export_to_sale 
            | firm + year + indu_2|0 | firm,df_final %>% filter(year %in% list("2005","2006", "2007")),
            exactDOF = TRUE)
            
dep <- "Dependent variable Asset"
fe1 <- list(
    c("firm", "Yes", "Yes", "Yes", "Yes", "Yes"),
    
    c("industry-year", "Yes", "Yes", "Yes", "Yes", "Yes")
             )

table_1 <- go_latex(list(
    t_0,t_1, t_2, t_3, t_4
),
    title="Effect of external credit supply on asset accumulation, long term credits",
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
new_r = ['& Asset', 'Tangible', 'Tangible to asset','Investment to asset', 'RD']
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

<!-- #region kernel="SoS" -->
## Table 4: Effect of external credit demand on asset accumulation

1. Effect of external credit supply
    - all credits
    - long term credits
2. Effect of external credit demand
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
t_0 <- felm(log(total_asset) ~
            credit_constraint +
            log(current_ratio) +
            log(liabilities_tot_asset) +
            log(cashflow_to_tangible) +
            log(age) +
            export_to_sale 
            | firm + year |0 | firm,df_final,
            exactDOF = TRUE)

t_1 <- felm(log(tangible) ~
            credit_constraint +
            log(current_ratio) +
            log(liabilities_tot_asset) +
            log(cashflow_to_tangible) +
            log(age) +
            export_to_sale 
            | firm + year |0 | firm,df_final,
            exactDOF = TRUE)

t_2 <- felm(log(asset_tangibility_tot_asset) ~
            credit_constraint +
            log(current_ratio) +
            log(liabilities_tot_asset) +
            log(cashflow_to_tangible) +
            log(age) +
            export_to_sale 
            | firm + year |0 | firm,df_final,
            exactDOF = TRUE)

t_3 <- felm(investment_tot_asset ~
            credit_constraint +
            log(current_ratio) +
            log(liabilities_tot_asset) +
            log(cashflow_to_tangible) +
            log(age) +
            export_to_sale 
            | firm + year |0 | firm,df_final ,
            exactDOF = TRUE)

t_4 <- felm(rd_tot_asset_trick ~
            credit_constraint +
            log(current_ratio) +
            log(liabilities_tot_asset) +
            log(cashflow_to_tangible) +
            log(age) +
            export_to_sale 
            | firm + year |0 | firm,df_final %>% filter(year %in% list("2005","2006", "2007")),
            exactDOF = TRUE)
            
dep <- "Dependent variable Asset"
fe1 <- list(
    c("firm", "Yes", "Yes", "Yes", "Yes", "Yes"),
    
    c("year", "Yes", "Yes", "Yes", "Yes", "Yes")
             )

table_1 <- go_latex(list(
    t_0,t_1, t_2, t_3, t_4
),
    title="Effect of external credit demand on asset accumulation",
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
new_r = ['& Asset', 'Tangible', 'Tangible to asset','Investment to asset', 'RD']
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

<!-- #region kernel="SoS" -->
## Table 5: interaction effect of external credit supply on asset accumulation, all credits
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
t_0 <- felm(log(total_asset) ~
            log(current_ratio) +
            log(liabilities_tot_asset) +
            log(cashflow_to_tangible) * supply_all_credit+
            log(age) +
            export_to_sale 
            | firm + year + indu_2|0 | firm,df_final,
            exactDOF = TRUE)

t_1 <- felm(log(tangible) ~
            log(current_ratio) +
            log(liabilities_tot_asset) +
            log(cashflow_to_tangible) * supply_all_credit+
            log(age) +
            export_to_sale 
            | firm + year + indu_2|0 | firm,df_final,
            exactDOF = TRUE)

t_2 <- felm(log(asset_tangibility_tot_asset) ~
            log(current_ratio) +
            log(liabilities_tot_asset) +
            log(cashflow_to_tangible) * supply_all_credit+
            log(age) +
            export_to_sale 
            | firm + year + indu_2|0 | firm,df_final,
            exactDOF = TRUE)

t_3 <- felm(investment_tot_asset ~
            log(current_ratio) +
            log(liabilities_tot_asset) +
            log(cashflow_to_tangible) * supply_all_credit+
            log(age) +
            export_to_sale 
            | firm + year + indu_2|0 | firm,df_final ,
            exactDOF = TRUE)

t_4 <- felm(rd_tot_asset_trick ~
            log(current_ratio) +
            log(liabilities_tot_asset) +
            log(cashflow_to_tangible) * supply_all_credit+
            log(age) +
            export_to_sale 
            | firm + year + indu_2|0 | firm,df_final %>% filter(year %in% list("2005","2006", "2007")),
            exactDOF = TRUE)
            
dep <- "Dependent variable Asset"
fe1 <- list(
    c("firm", "Yes", "Yes", "Yes", "Yes", "Yes"),
    
    c("industry-year", "Yes", "Yes", "Yes", "Yes", "Yes")
             )

table_1 <- go_latex(list(
    t_0,t_1, t_2, t_3, t_4
),
    title="interaction effect of external credit supply on asset accumulation, all credits",
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
new_r = ['& Asset', 'Tangible', 'Tangible to asset','Investment to asset', 'RD']
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

<!-- #region kernel="SoS" -->
## Table 6: interaction effect of external credit supply on asset accumulation,  long term credits
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
t_0 <- felm(log(total_asset) ~
            log(current_ratio) +
            log(liabilities_tot_asset) +
            log(cashflow_to_tangible) * supply_long_term_credit+
            log(age) +
            export_to_sale 
            | firm + year + indu_2|0 | firm,df_final,
            exactDOF = TRUE)

t_1 <- felm(log(tangible) ~
            log(current_ratio) +
            log(liabilities_tot_asset) +
            log(cashflow_to_tangible) * supply_long_term_credit+
            log(age) +
            export_to_sale 
            | firm + year + indu_2|0 | firm,df_final,
            exactDOF = TRUE)

t_2 <- felm(log(asset_tangibility_tot_asset) ~
            log(current_ratio) +
            log(liabilities_tot_asset) +
            log(cashflow_to_tangible) * supply_long_term_credit+
            log(age) +
            export_to_sale 
            | firm + year + indu_2|0 | firm,df_final,
            exactDOF = TRUE)

t_3 <- felm(investment_tot_asset ~
            log(current_ratio) +
            log(liabilities_tot_asset) +
            log(cashflow_to_tangible) * supply_long_term_credit+
            log(age) +
            export_to_sale 
            | firm + year + indu_2|0 | firm,df_final ,
            exactDOF = TRUE)

t_4 <- felm(rd_tot_asset_trick ~
            log(current_ratio) +
            log(liabilities_tot_asset) +
            log(cashflow_to_tangible) * supply_long_term_credit+
            log(age) +
            export_to_sale 
            | firm + year + indu_2|0 | firm,df_final %>% filter(year %in% list("2005","2006", "2007")),
            exactDOF = TRUE)
            
dep <- "Dependent variable Asset"
fe1 <- list(
    c("firm", "Yes", "Yes", "Yes", "Yes", "Yes"),
    
    c("industry-year", "Yes", "Yes", "Yes", "Yes", "Yes")
             )

table_1 <- go_latex(list(
    t_0,t_1, t_2, t_3, t_4
),
    title="interaction effect of external credit supply on asset accumulation, long term credits",
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
new_r = ['& Asset', 'Tangible', 'Tangible to asset','Investment to asset', 'RD']
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

<!-- #region kernel="SoS" -->
## Table 7: interaction effect of external credit demand on asset accumulation
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
t_0 <- felm(log(total_asset) ~
            log(current_ratio) +
            log(liabilities_tot_asset) +
            log(cashflow_to_tangible) * credit_constraint+
            log(age) +
            export_to_sale 
            | firm + year + indu_2 |0 | firm,df_final,
            exactDOF = TRUE)

t_1 <- felm(log(tangible) ~
            log(current_ratio) +
            log(liabilities_tot_asset) +
            log(cashflow_to_tangible) * credit_constraint+
            log(age) +
            export_to_sale 
            | firm + year + indu_2|0 | firm,df_final,
            exactDOF = TRUE)

t_2 <- felm(log(asset_tangibility_tot_asset) ~
            log(current_ratio) +
            log(liabilities_tot_asset) +
            log(cashflow_to_tangible) * credit_constraint+
            log(age) +
            export_to_sale 
            | firm + year + indu_2|0 | firm,df_final,
            exactDOF = TRUE)

t_3 <- felm(investment_tot_asset ~
            log(current_ratio) +
            log(liabilities_tot_asset) +
            log(cashflow_to_tangible) * credit_constraint+
            log(age) +
            export_to_sale 
            | firm + year + indu_2 |0 | firm,df_final ,
            exactDOF = TRUE)
            
t_4 <- felm(rd_tot_asset_trick ~
            log(current_ratio) +
            log(liabilities_tot_asset) +
            log(cashflow_to_tangible) * credit_constraint+
            log(age) +
            export_to_sale 
            | firm + year + indu_2 |0 | firm,df_final %>% filter(year %in% list("2005","2006", "2007")),
            exactDOF = TRUE)

dep <- "Dependent variable Asset"
fe1 <- list(
    c("firm", "Yes", "Yes", "Yes", "Yes", "Yes"),
    
    c("indsutry-year", "Yes", "Yes", "Yes", "Yes", "Yes")
             )

table_1 <- go_latex(list(
    t_0,t_1, t_2, t_3, t_4
),
    title="Interaction effect of external credit demand on asset accumulation",
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
new_r = ['& Asset', 'Tangible', 'Tangible to asset','Investment to asset', 'RD']
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
for ext in ['.txt', '.tex', '.pdf']:
    x = [a for a in os.listdir(folder) if a.endswith(ext)]
    [os.remove(os.path.join(folder, i)) for i in x]
```

<!-- #region kernel="SoS" nteract={"transient": {"deleting": false}} -->
# Generate reports
<!-- #endregion -->

```sos kernel="python3" nteract={"transient": {"deleting": false}} outputExpanded=false
import os, time, shutil, urllib, ipykernel, json
from pathlib import Path
from notebook import notebookapp
```

```sos kernel="python3" nteract={"transient": {"deleting": false}} outputExpanded=false
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

```sos kernel="python3" nteract={"transient": {"deleting": false}} outputExpanded=false
create_report(extension = "html", keep_code = False, notebookname = "06_internal_finance_firm_level.ipynb")
```
