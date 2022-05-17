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

Estimate tfp_OP_f as a function of  asset_tangibility and others (Estimate table 6 with TFP) 

# Business needs 

Estimate tfp_OP_f as a function of  asset_tangibility, cash_over_total_asset, credit constraint, current ratio, sales over asset Andersen method, liabilities over asset (Estimate table 6 by using TFP instead of asset tangibility to explain a new channel of impact) 

## Description
### Objective 

Test the coefficient sign and significant of the main variable

### Tables

Table 1: Baseline Estimate, tfp_OP_f and asset_tangibility, cash_over_total_asset, credit constraint, current ratio, sales over asset Andersen method, liabilities over asset

**Cautious**
Make sure no empty rows, otherwise it will be filtered out in the estimate


# Metadata

- Key: rae81txps49928f
- Epic: Models
- US: Estimate TFP
- Task tag: #analytics, #andersen, #tfp, #econometrics
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

```sos kernel="R"
change_target <- function(table){
    ## SOE
    check_target_current_ratio_soe <- grep("ownershipPRIVATE:log\\(current_ratio\\)", rownames(table$coef))
    check_target_liabilities_soe <- grep("ownershipPRIVATE:log\\(liabilities_tot_asset\\)", rownames(table$coef))
    ## foreign
    check_target_current_ratio_for <- grep("ownership_adjustedDOMESTIC:log\\(current_ratio\\)", rownames(table$coef))
    check_target_liabilities_for <- grep("ownership_adjustedDOMESTIC:log\\(liabilities_tot_asset\\)", rownames(table$coef))
    
    ## TCZ
    check_target_current_ratio_tcz <- grep("tcz:log\\(current_ratio\\)", rownames(table$coef))
    check_target_liabilities_tcz <- grep("tcz:log\\(liabilities_tot_asset\\)", rownames(table$coef))
    #check_target_cashflow_tcz <- grep("tcz:log\\(cashflow_to_tangible\\)", rownames(table$coef))
    
    ## SPZ
    check_target_current_ratio_spz <- grep("spz:log\\(current_ratio\\)", rownames(table$coef))
    check_target_liabilities_spz <- grep("spz:log\\(liabilities_tot_asset\\)", rownames(table$coef))
    #check_target_cashflow_spz <- grep("tcz:log\\(cashflow_to_tangible\\)", rownames(table$coef))
    
    if (length(check_target_current_ratio_soe) !=0) {
    ## SOE
    rownames(table$coefficients)[check_target_current_ratio_soe] <- 'log(current_ratio):ownershipPRIVATE'
    rownames(table$beta)[check_target_current_ratio_soe] <- 'log(current_ratio):ownershipPRIVATE'
    rownames(table$coefficients)[check_target_liabilities_soe] <- 'log(liabilities_tot_asset):ownershipPRIVATE'
    rownames(table$beta)[check_target_liabilities_soe] <- 'log(liabilities_tot_asset):ownershipPRIVATE'
    } else if (length(check_target_current_ratio_tcz) !=0){
    
    ## TCZ
    rownames(table$coefficients)[check_target_current_ratio_tcz] <- 'log(current_ratio):tcz'
    rownames(table$beta)[check_target_current_ratio_tcz] <- 'log(current_ratio):tcz'
    rownames(table$coefficients)[check_target_liabilities_tcz] <- 'log(liabilities_tot_asset):tcz'
    rownames(table$beta)[check_target_liabilities_tcz] <- 'log(liabilities_tot_asset):tcz'
    #rownames(table$coefficients)[check_target_cashflow_tcz] <- 'log(cashflow_to_tangible):tcz'
    #rownames(table$beta)[check_target_cashflow_tcz] <- 'log(cashflow_to_tangible):tcz'
        } else if (length(check_target_current_ratio_spz) !=0){
        
    ## SPZ
    rownames(table$coefficients)[check_target_current_ratio_spz] <- 'log(current_ratio):spz'
    rownames(table$beta)[check_target_current_ratio_spz] <- 'log(current_ratio):spz'
    rownames(table$coefficients)[check_target_liabilities_spz] <- 'log(liabilities_tot_asset):spz'
    rownames(table$beta)[check_target_liabilities_spz] <- 'log(liabilities_tot_asset):spz'
    #rownames(table$coefficients)[check_target_cashflow_spz] <- 'log(cashflow_to_tangible):tcz'
    #rownames(table$beta)[check_target_cashflow_spz] <- 'log(cashflow_to_tangible):tcz'
        }
    else if (length(check_target_current_ratio_for) !=0){
        
    ## for
    rownames(table$coefficients)[check_target_current_ratio_for] <- 'log(current_ratio):ownership_adjustedDOMESTIC'
    rownames(table$beta)[check_target_current_ratio_for] <- 'log(current_ratio):ownership_adjustedDOMESTIC'
    rownames(table$coefficients)[check_target_liabilities_for] <- 'log(liabilities_tot_asset):ownership_adjustedDOMESTIC'
    rownames(table$beta)[check_target_liabilities_for] <- 'log(liabilities_tot_asset):ownership_adjustedDOMESTIC'
    #rownames(table$coefficients)[check_target_cashflow_spz] <- 'log(cashflow_to_tangible):tcz'
    #rownames(table$beta)[check_target_cashflow_spz] <- 'log(cashflow_to_tangible):tcz'
        }
    return (table)
}
```

<!-- #region kernel="SoS" -->
# Load tables

Since we load the data as a Pandas DataFrame, we want to pass the `dtypes`. We load the schema from Glue to guess the types
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
    SELECT * 
    FROM {}.{}
    WHERE 
    cashflow_to_tangible > 0
    AND current_ratio > 0
    AND liabilities_tot_asset > 0
    AND age > 0
    AND year in (
        '2001', '2002', '2003', '2004', '2005', 
        '2006', '2007'
      )
    -- AND ownership not in ('COLLECTIVE')  
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
              ownership = lambda x: np.where(x['ownership'].isin(['COLLECTIVE']), "SOE", x['ownership']),
          )
          .assign(
              ownership = lambda x: np.where(x['ownership'].isin(['HTM']), "FOREIGN", x['ownership']),
              ownership_adjusted = lambda x: np.where(x['ownership'].isin(['PRIVATE', 'SOE', 'COLLECTIVE']), 'DOMESTIC', 'FOREIGN'),
              fe_fo=lambda x: le.fit_transform(x["firm"].astype('str') + x['ownership'])
            )
        )
    #s3.download_file(
    #    key = full_path_filename
    #)
    #shutil.move(
    #    filename + '.csv',
    #    os.path.join(path_local, filename + '.csv')
    #)
    #s3.remove_file(full_path_filename)
```

```sos kernel="SoS"
df.to_csv(df_path, index = False)
```

```sos kernel="SoS"
df['ownership'].value_counts()
```

```sos kernel="SoS"
df['ownership_adjusted'].value_counts()
```

```sos kernel="SoS"
df.isna().sum().loc[lambda x: x>0]
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
        'new':'\\text{liabilities to asset}'
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
        {
        'old':'ownershipPRIVATE',
        'new':'\\text{private}'
        },
        {
        'old':'ownership\_adjustedDOMESTIC',
        'new':'\\text{domestic}'
        },
        {
        'old':'all_loan',
        'new':'\\text{all credit}'
        },
        {
        'old':'short_term_loan',
        'new':'\\text{short term credit}'
        },
        {
        'old':'long_term_loan',
        'new':'\\text{long term credit}'
        }
    ]
    

    data['to_rename'].extend(dic_rename)
    with open('schema_table.json', 'w') as outfile:
        json.dump(data, outfile)
```

```sos kernel="SoS"
import sys
sys.path.append(os.path.join(parent_path, 'utils'))
import latex.latex_beautify as lb
```

```sos kernel="SoS"
query = """
SELECT year,
province_en, 
total_short_term/total_gdp as short_term_loan,
total_bank_loan/total_gdp as all_loan,
total_long_term_loan/total_gdp as long_term_loan
FROM "almanac_bank_china"."province_loan_and_credit" 
"""
df_loan = (
    s3.run_query(
        query=query,
        database=db,
        s3_output='SQL_OUTPUT_ATHENA',
        filename='loan',  # Add filename to print dataframe
        destination_key='SQL_OUTPUT_ATHENA/CSV',  #Use it temporarily
    )
    .to_csv('temp_loan.csv', index = False)
)
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
#    period = relevel(as.factor(period), ref='FALSE'),
    #soe_vs_pri = relevel(as.factor(soe_vs_pri), ref='SOE')
    ownership = relevel(as.factor(ownership), ref='SOE'),
    ownership_adjusted = relevel(as.factor(ownership_adjusted), ref='FOREIGN')
) %>% left_join(read_csv('temp_loan.csv'))
#group_by(firm) %>% 
#mutate(test  = n_distinct(province_en)) 
dim(df_final)
```

<!-- #region kernel="R" -->
## Recall

* Variables of interest capturing internal finance:
  * current ratio
  * cash over tangible asset
* Variables of interest capturing external finance
  * external finance dependance → demand of credit → industry level
  * external supply of credit → supply of credit → province year
    * all credits to gdp
    * long term loan to gdp
* Control
  * return on sale
  * leverage
  * coverage ratio
  * liquidity
  * labor productivity
  * age
  * export to sale
<!-- #endregion -->

<!-- #region kernel="R" -->
## Table 1: Baseline effect of internal finance on firm TFP

$$ \begin{aligned}
\begin{equation}
\mathrm{TFP}_{\mathrm{it}}=\beta_{1} \text { Internal finance }_{\mathrm{it}}+\beta 2 \text { X }_{\mathrm{it}}+v_{t}+v_{k}+\varepsilon_{\mathrm{it}}
\end{equation}
\end{aligned} $$
<!-- #endregion -->

```sos kernel="SoS"
folder = 'Tables_1'
table_nb = 15
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
#t_0 <- felm(log(tfp_op) ~ 
#            log(cashflow_to_tangible) + 
#            log(asset_tangibility_tot_asset) +
#            log(liabilities_tot_asset) + 
#            log(labor_capital) +
#            log(total_asset) + 
#            log(age) +
#            export_to_sale 
#            | firm + year + indu_2|0 | firm, df_final,
#            exactDOF = TRUE)
#t_1 <- felm(log(tfp_op) ~ 
#            log(current_ratio) + 
#            log(asset_tangibility_tot_asset) +
#            log(liabilities_tot_asset) + 
#            log(labor_capital) +
#            log(total_asset) + 
#            log(age) +
#            export_to_sale 
#            | firm + year + indu_2|0 | firm, df_final,
#            exactDOF = TRUE)
#t_2 <- felm(log(tfp_op) ~ 
#            log(liabilities_tot_asset) + 
#            log(asset_tangibility_tot_asset) +
#            log(labor_capital) +
#            log(total_asset) + 
#            log(age) +
#            export_to_sale 
#            | firm + year + indu_2|0 | firm, df_final,
#            exactDOF = TRUE)
###
t_2 <- felm(log(tfp_op) ~ 
            log(cashflow_to_tangible) + 
            log(current_ratio) + 
            log(liabilities_tot_asset) + 
            log(asset_tangibility_tot_asset) +
            log(labor_capital) +
            log(total_asset) + 
            log(age) +
            export_to_sale 
            | firm + year + indu_2|0 | firm, df_final,
            exactDOF = TRUE)

### More control
t_3 <- felm(log(tfp_op) ~ 
            log(cashflow_to_tangible) + 
            log(current_ratio) + 
            log(liabilities_tot_asset) + 
            log(asset_tangibility_tot_asset) +
            log(labor_capital) +
            log(total_asset) + 
            log(age) +
            export_to_sale +
            all_loan
            | firm + year + indu_2|0 | firm, df_final,
            exactDOF = TRUE)
t_4 <- felm(log(tfp_op) ~ 
            log(cashflow_to_tangible) + 
            log(current_ratio) + 
            log(liabilities_tot_asset) + 
            log(asset_tangibility_tot_asset) +
            log(labor_capital) +
            log(total_asset) + 
            log(age) +
            export_to_sale +
            long_term_loan 
            | firm + year + indu_2|0 | firm, df_final,
            exactDOF = TRUE)
dep <- "Dependent variable: Total factor productivity"
fe1 <- list(
    c("firm", "Yes", "Yes", "Yes", "Yes", "Yes"),
    c("industry", "Yes", "Yes", "Yes", "Yes", "Yes"),
    c("year", "Yes", "Yes", "Yes", "Yes", "Yes")
             )

table_1 <- go_latex(list(
    #t_0, t_1, 
    t_2, t_3, t_4
),
    title="Baseline effect of internal finance on firm TFP",
    dep_var = dep,
    addFE=fe1,
    save=TRUE,
    note = FALSE,
    name=path
) 
```

```sos kernel="SoS"
tbe1  = "This table estimates eq(X). " \
"Heteroskedasticity-robust standard errors" \
"clustered at the firm level appear inparentheses."\
"Dependent variables include firm's TFP level " \
" Independent variable cashflow is measured as net income + depreciation over asset;"\
" current ratio is measured as current asset over current liabilities. " \
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

```sos kernel="SoS"
os.rename('Tables_1/{}.tex'.format(table), 'Tables_1/{}_TFP.tex'.format(table))
```

<!-- #region kernel="SoS" -->
## Table 2: Ownership structure effect
<!-- #endregion -->

<!-- #region kernel="SoS" -->
## Private vs SOE
<!-- #endregion -->

```sos kernel="SoS"
folder = 'Tables_1'
table_nb = 16
table = 'table_{}'.format(table_nb)
path = os.path.join(folder, table + '.txt')
if os.path.exists(folder) == False:
        os.mkdir(folder)

try:
    for ext in ['table_{}.txt'.format(table_nb), 'table_{}.tex'.format(table_nb), 'table_{}.pdf'.format(table_nb)]:
        os.remove(os.path.join(folder, ext))
except:
    pass
#    x = [a for a in os.listdir(folder) if a.endswith(ext)]
#    [os.remove(os.path.join(folder, i)) for i in x]
```

```sos kernel="R"
%get path table 
#t_0 <- felm(log(tfp_op) ~ 
#            log(cashflow_to_tangible) * ownership+ 
#            log(liabilities_tot_asset)+ 
#            log(asset_tangibility_tot_asset) +
#            log(labor_capital) +
#            log(total_asset) + 
#            log(age) +
#            export_to_sale 
#            | fe_fo + year + indu_2|0 | firm, df_final %>% 
#mutate(ownership = replace(ownership, ownership %in% c('FOREIGN', 'HTM'), 'PRIVATE'))
#            %>%filter(ownership %in% c('SOE','PRIVATE')),
#            exactDOF = TRUE)
#t_1 <- felm(log(tfp_op) ~ 
#            log(current_ratio) * ownership+ 
#            log(liabilities_tot_asset)+ 
#            log(asset_tangibility_tot_asset) +
#            log(labor_capital) +
#            log(total_asset) + 
#            log(age) +
#            export_to_sale 
#            | fe_fo + year + indu_2|0 | firm, df_final %>% 
#mutate(ownership = replace(ownership, ownership %in% c('FOREIGN', 'HTM'), 'PRIVATE'))
#            %>%filter(ownership %in% c('SOE','PRIVATE')),
#            exactDOF = TRUE)
###
t_2 <- felm(log(tfp_op) ~ 
            log(cashflow_to_tangible) * ownership+ 
            log(current_ratio) * ownership+ 
            log(liabilities_tot_asset)+ 
            log(asset_tangibility_tot_asset) +
            log(labor_capital) +
            log(total_asset) + 
            log(age) +
            export_to_sale 
            | fe_fo + year + indu_2|0 | firm, df_final %>% 
mutate(ownership = replace(ownership, ownership %in% c('FOREIGN', 'HTM'), 'PRIVATE'))
            %>%filter(ownership %in% c('SOE','PRIVATE')),
            exactDOF = TRUE)
t_2 <- change_target(t_2)
### More control
t_3 <- felm(log(tfp_op) ~ 
            log(cashflow_to_tangible) * ownership+ 
            log(current_ratio) * ownership+ 
            log(liabilities_tot_asset)+ 
            log(asset_tangibility_tot_asset) +
            log(labor_capital) +
            log(total_asset) + 
            log(age) +
            export_to_sale +
            all_loan
            | fe_fo + year + indu_2|0 | firm, df_final %>% 
mutate(ownership = replace(ownership, ownership %in% c('FOREIGN', 'HTM'), 'PRIVATE'))
            %>%filter(ownership %in% c('SOE','PRIVATE')),
            exactDOF = TRUE)
t_3 <- change_target(t_3)
t_4 <- felm(log(tfp_op) ~ 
            log(cashflow_to_tangible)* ownership + 
            log(current_ratio) * ownership+ 
            log(liabilities_tot_asset) + 
            log(asset_tangibility_tot_asset) +
            log(labor_capital) +
            log(total_asset) + 
            log(age) +
            export_to_sale +
            long_term_loan 
            | fe_fo + year + indu_2|0 | firm, df_final %>% 
mutate(ownership = replace(ownership, ownership %in% c('FOREIGN', 'HTM'), 'PRIVATE'))
            %>%filter(ownership %in% c('SOE','PRIVATE')),
            exactDOF = TRUE)
t_4 <- change_target(t_4)
dep <- "Dependent variable: Total factor productivity"
fe1 <- list(
    c("firm-ownership", "Yes", "Yes", "Yes", "Yes", "Yes"),
    c("industry", "Yes", "Yes", "Yes", "Yes", "Yes"),
    c("year", "Yes", "Yes", "Yes", "Yes", "Yes")
             )

table_1 <- go_latex(list(
    #t_0, t_1, 
    t_2, t_3, t_4
),
    title="Ownership structure effect",
    dep_var = dep,
    addFE=fe1,
    save=TRUE,
    note = FALSE,
    name=path
) 
```

```sos kernel="SoS"
tbe1  = "This table estimates eq(X). " \
"Heteroskedasticity-robust standard errors" \
"clustered at the firm level appear inparentheses."\
"Dependent variables include firm's TFP level " \
" Independent variable cashflow is measured as net income + depreciation over asset;"\
" current ratio is measured as current asset over current liabilities. " \
"\sym{*} Significance at the 10\%, \sym{**} Significance at the 5\%, \sym{***} Significance at the 1\%."

reorder = {
    10:1,
    11:3,
    #13:5
}

#multi_lines_dep = '(city/product/trade regime/year)'
#new_r = ['& test1', 'test2']
lb.beautify(table_number = table_nb,
            reorder_var = reorder,
            #multi_lines_dep = multi_lines_dep,
            #new_row= new_r,
            #multicolumn = multicolumn,
            table_nte = tbe1,
            jupyter_preview = True,
            resolution = 200,
            folder = folder)
```

```sos kernel="SoS"
os.rename('Tables_1/{}.tex'.format(table), 'Tables_1/{}_SOE_PRI.tex'.format(table))
```

<!-- #region kernel="SoS" -->
## Foreign vs domestic
<!-- #endregion -->

```sos kernel="SoS"
folder = 'Tables_1'
table_nb = 17
table = 'table_{}'.format(table_nb)
path = os.path.join(folder, table + '.txt')

if os.path.exists(folder) == False:
        os.mkdir(folder)

try:
    for ext in ['.txt', '.pdf']:
        x = [a for a in os.listdir(folder) if a.endswith(ext)]
        [os.remove(os.path.join(folder, i)) for i in x]
except:
    pass
```

```sos kernel="R"
%get path table 
#t_0 <- felm(log(tfp_op) ~ 
#            log(cashflow_to_tangible) * ownership_adjusted+ 
#            log(liabilities_tot_asset)+ 
#            log(asset_tangibility_tot_asset) +
#            log(labor_capital) +
#            log(total_asset) + 
#            log(age) +
#            export_to_sale 
#            | fe_fo + year + indu_2|0 | firm, df_final %>%filter(ownership_adjusted %in% c('DOMESTIC','FOREIGN')),
#            exactDOF = TRUE)
#t_1 <- felm(log(tfp_op) ~ 
#            log(current_ratio) * ownership_adjusted+ 
#            log(liabilities_tot_asset)+ 
#            log(asset_tangibility_tot_asset) +
#            log(labor_capital) +
#            log(total_asset) + 
#            log(age) +
#            export_to_sale 
#            | fe_fo + year + indu_2|0 | firm, df_final %>%filter(ownership_adjusted %in% c('DOMESTIC','FOREIGN')),
#            exactDOF = TRUE)
###
t_2 <- felm(log(tfp_op) ~ 
            log(cashflow_to_tangible) * ownership_adjusted+ 
            log(current_ratio) * ownership_adjusted+ 
            log(liabilities_tot_asset)+ 
            log(asset_tangibility_tot_asset) +
            log(labor_capital) +
            log(total_asset) + 
            log(age) +
            export_to_sale 
            | fe_fo + year + indu_2|0 | firm, df_final %>%filter(ownership_adjusted %in% c('DOMESTIC','FOREIGN')),
            exactDOF = TRUE)
t_2 <- change_target(t_2)
### More control
t_3 <- felm(log(tfp_op) ~ 
            log(cashflow_to_tangible) * ownership_adjusted+ 
            log(current_ratio) * ownership_adjusted+ 
            log(liabilities_tot_asset)+ 
            log(asset_tangibility_tot_asset) +
            log(labor_capital) +
            log(total_asset) + 
            log(age) +
            export_to_sale +
            all_loan
            | fe_fo + year + indu_2|0 | firm, df_final %>%filter(ownership_adjusted %in% c('DOMESTIC','FOREIGN')),
            exactDOF = TRUE)
t_3 <- change_target(t_3)
t_4 <- felm(log(tfp_op) ~ 
            log(cashflow_to_tangible)* ownership_adjusted + 
            log(current_ratio) * ownership_adjusted+ 
            log(liabilities_tot_asset) + 
            log(asset_tangibility_tot_asset) +
            log(labor_capital) +
            log(total_asset) + 
            log(age) +
            export_to_sale +
            long_term_loan 
            | fe_fo + year + indu_2|0 | firm, df_final %>%filter(ownership_adjusted %in% c('DOMESTIC','FOREIGN')),
            exactDOF = TRUE)
t_4 <- change_target(t_4)

dep <- "Dependent variable: Total factor productivity"
fe1 <- list(
    c("firm-ownership", "Yes", "Yes", "Yes", "Yes", "Yes"),
    c("industry", "Yes", "Yes", "Yes", "Yes", "Yes"),
    c("year", "Yes", "Yes", "Yes", "Yes", "Yes")
             )

table_1 <- go_latex(list(
    #t_0, t_1,
    t_2, t_3, t_4
),
    title="Ownership structure effect",
    dep_var = dep,
    addFE=fe1,
    save=TRUE,
    note = FALSE,
    name=path
) 
```

```sos kernel="SoS"
tbe1  = "This table estimates eq(X). " \
"Heteroskedasticity-robust standard errors" \
"clustered at the firm level appear inparentheses."\
"Dependent variables include firm's TFP level " \
" Independent variable cashflow is measured as net income + depreciation over asset;"\
" current ratio is measured as current asset over current liabilities. " \
"\sym{*} Significance at the 10\%, \sym{**} Significance at the 5\%, \sym{***} Significance at the 1\%."

reorder = {
    10:1,
    11:3,
    #13:5
}

#multi_lines_dep = '(city/product/trade regime/year)'
#new_r = ['& test1', 'test2']
lb.beautify(table_number = table_nb,
            reorder_var = reorder,
            #multi_lines_dep = multi_lines_dep,
            #new_row= new_r,
            #multicolumn = multicolumn,
            table_nte = tbe1,
            jupyter_preview = True,
            resolution = 200,
            folder = folder)
```

```sos kernel="SoS"
os.rename('Tables_1/{}.tex'.format(table), 'Tables_1/{}_FOR_DOM.tex'.format(table))
```

```sos kernel="SoS"
import glob
for ext in ['*.txt', '*.pdf']:
    for zippath in glob.iglob(os.path.join(folder, ext)):
        os.remove(zippath)
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
create_report(extension = "html", keep_code = True, notebookname = "05_tfp_firm_level.ipynb")
```
