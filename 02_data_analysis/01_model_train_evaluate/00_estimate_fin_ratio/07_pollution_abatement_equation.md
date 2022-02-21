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
        tso2_eq_output = lambda x: (x['tdso2_equip'])/(x['output']/1000),
        tso2_eq_output_1 = lambda x: (x['tdso2_equip']+1)/(x['output']/1000),
        constraint = lambda x: x['credit_constraint'] > -0.47
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
                      "pct_change_eq_raw",
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
#library('emmeans')
path = "../../../utils/latex/table_golatex.R"
source(path)
```

```sos kernel="R"
%get df_path
df_final <- read_csv(df_path) %>%
mutate_if(is.character, as.factor) %>%
    mutate_at(vars(starts_with("fe")), as.factor) %>%
mutate(
    constraint = relevel(as.factor(constraint), ref='FALSE'),
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
(
    df[[
        "lag_cashflow_to_tangible",
        "lag_current_ratio",
        "tfqzlssnl",
        "ttlssnl",
        "tdwastegas_equip",
        "tdso2_equip",
        #'tso2_eq_output',
        'tso2_eq_output_1',
        #"std_eq_ind",
        #"std_eq_c",
        #"std_eq_year",
        #'std_eq'
    ]]
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

<!-- #region kernel="SoS" -->
# Small vs large

- Internal finance is a driver of pollution abatement systems
- The acquisition of pollution abatement systems is realized in a dissimilar way: 
    - Small firms use their cashflow to invest while large firms can use a credit because they have a collateral and are less constraint than small firms

Follow methodology: [Greening Through Finance?](https://drive.google.com/file/d/1E6hPTzv6CPgR-uJydgzhcfN2G-CjFUWu/view?usp=sharing)
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
change_target <- function(table){
    ## SOE
    check_target_current_ratio <- grep("log\\(total_asset\\):log\\(lag_current_ratio\\)",
                                       rownames(table$coef))
    #check_target_cashflow <- grep("log\\(lag_cashflow_to_tangible\\):constraintTRUE", rownames(table$coef))
    ## foreign
    rownames(table$coefficients)[check_target_current_ratio] <- 'log(lag_current_ratio):log(total_asset) '
    rownames(table$beta)[check_target_current_ratio] <- 'log(lag_current_ratio):log(total_asset) '
    
    #rownames(table$coefficients)[check_target_cashflow] <- 'log(lag_cashflow_to_tangible):constraint'
    #rownames(table$beta)[check_target_cashflow] <- 'log(lag_cashflow_to_tangible):constraint'
    return (table)
    }
```

```sos kernel="R"
%get path table
t_0 <- felm(log(tso2_eq_output_1) ~ 
            log(lag_cashflow_to_tangible) * log(total_asset)
            | fe_t_i + fe_c_t|0 | geocode4_corr,df_final 
         ,
            exactDOF = TRUE)

t_1 <- felm(log(tso2_eq_output_1) ~ 
            log(lag_current_ratio) * log(total_asset)
            | fe_t_i + fe_c_t|0 | geocode4_corr,df_final 
         ,
            exactDOF = TRUE)
t_2 <- felm(log(tso2_eq_output_1) ~ 
            log(lag_cashflow_to_tangible) * log(total_asset) +
            log(lag_liabilities_tot_asset) +
            log(lag_sales_tot_asset)  
            | fe_t_i + fe_c_t|0 | geocode4_corr,df_final
            )
t_2 <- change_target(t_2)
t_3 <- felm(log(tso2_eq_output_1) ~ 
            log(lag_current_ratio) * log(total_asset) +
            log(lag_liabilities_tot_asset) +
            log(lag_sales_tot_asset)  
            | fe_t_i + fe_c_t|0 | geocode4_corr,df_final 
            )
t_3 <- change_target(t_3)

t_4 <- felm(log(tso2_eq_output_1) ~ 
            log(lag_cashflow_to_tangible) * log(total_asset)+
            log(lag_current_ratio) * log(total_asset)+
            log(lag_liabilities_tot_asset) +
            log(lag_sales_tot_asset)
            | fe_t_i + fe_c_t|0 | geocode4_corr,df_final
         ,
            exactDOF = TRUE)
t_4 <- change_target(t_4)

dep <- "Dependent variable: pollution abattement equipment SO2"
fe1 <- list(
    c("industry-year", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes"),
    c("city-year", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes")
             )
table_1 <- go_latex(list(
    t_0,t_1, t_2, t_3, t_4
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
For a continuous by continuous interaction model we can re-arrange the equation such that

$$
\hat{Y}=b_{0}+b_{2} W+\left(b_{1}+b_{3} W\right) X
$$

Rerranging the terms allows you to obtain a new simple slope of $X$ defined as $(b1 + b3W) X$, which means that the slope of $X$ depends on values of $W$.

initially, but incorect

We get the derivative of cash respect to Y as .238 cash - 0.019 cash * asset = 0 <- tells at what values of asset the equipment is equal to 0, which does not make sense. 

Instead, we can compare the differential equipment acquisition based on large industries and small industries
<!-- #endregion -->

```sos kernel="R"
t_2 <- felm(log(tso2_eq_output_1) ~ 
            log(lag_cashflow_to_tangible) * log(total_asset) +
            log(lag_liabilities_tot_asset) +
            log(lag_sales_tot_asset)  
            | fe_t_i + fe_c_t|0 | geocode4_corr,df_final
            )
```

```sos kernel="R"
m_lag_liabilities_tot_asset <- mean(log(df_final$lag_liabilities_tot_asset))
m_lag_sales_tot_asset <- mean(log(df_final$lag_sales_tot_asset))
W_low <- mean(log(df_final$total_asset)) - sd(log(df_final$total_asset))
W_medium <-mean(log(df_final$total_asset))
W_high <- mean(log(df_final$total_asset)) + sd(log(df_final$total_asset))
X <- seq(min(log(df_final$lag_cashflow_to_tangible)),
        max(log(df_final$lag_cashflow_to_tangible)),by=0.1)
```

```sos kernel="R"
df_preds <- bind_rows(
tibble(
  x = X, 
  y = #t_2$coef[3] * m_lag_liabilities_tot_asset + t_2$coef[4] *m_lag_sales_tot_asset + 
t_2$coef[2] * W_low + (t_2$coef[1] + t_2$coef[5] * W_low) * X,
  group = 'low'  
),
tibble(
  x = X, 
  y = #t_2$coef[3] * m_lag_liabilities_tot_asset + t_2$coef[4] * m_lag_sales_tot_asset + 
t_2$coef[2] * W_medium + (t_2$coef[1] + t_2$coef[5] * W_medium) * X,
  group = 'medium'  
),
tibble(
  x = X, 
  y = #t_2$coef[3] * m_lag_liabilities_tot_asset + t_2$coef[4] * m_lag_sales_tot_asset + 
t_2$coef[2] * W_high + (t_2$coef[1] + t_2$coef[5] * W_high) * X,
  group = 'high'  
))
write.csv(df_preds, 'preds.csv',row.names = FALSE)
```

```sos kernel="SoS"
import matplotlib.pyplot as plt
import seaborn as sns
sns.set(rc={'figure.figsize':(11.7,8.27)})
```

```sos kernel="SoS"
sns.lineplot(data=pd.read_csv( 'preds.csv'), x="x", y="y", hue="group")
plt.legend(bbox_to_anchor=(1.05, 1), loc=2, borderaxespad=0.)
```

```sos kernel="SoS"
df['total_asset'].mean()
```

```sos kernel="SoS"
(
    df
    .assign(
        rank = lambda x: x['total_asset'].rank(pct = True)
    )
    .reindex(columns = ['rank', 'total_asset'])
    .sort_values(by = ['total_asset'])
    .loc[lambda x: x['total_asset'] <= int(np.exp(.238/.019))]
    .tail()
)
```

```sos kernel="SoS"
df['total_asset'].describe().reset_index().style.format("{0:,.0f}", subset = ['total_asset'])
```

<!-- #region kernel="SoS" -->
Check without Beijing, Shanghai, Chongqing and Tianjin
<!-- #endregion -->

```sos kernel="SoS"
folder = 'Tables_0'
table_nb = 2
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
t_0 <- felm(log(tso2_eq_output_1) ~ 
            log(lag_cashflow_to_tangible) * log(total_asset)
            | fe_t_i + fe_c_t|0 | geocode4_corr,df_final %>% filter(tso2_eq_output_1 >0 &
                   !geocode4_corr %in% c('1101', '1201', '3101', '5001'))
         ,
            exactDOF = TRUE)

t_1 <- felm(log(tso2_eq_output_1) ~ 
            log(lag_current_ratio) * log(total_asset)
            | fe_t_i + fe_c_t|0 | geocode4_corr,df_final %>% filter(tso2_eq_output_1 >0 &
                   !geocode4_corr %in% c('1101', '1201', '3101', '5001'))
         ,
            exactDOF = TRUE)
t_2 <- felm(log(tso2_eq_output_1) ~ 
            log(lag_cashflow_to_tangible) * log(total_asset) +
            log(lag_liabilities_tot_asset) +
            log(lag_sales_tot_asset)  
            | fe_t_i + fe_c_t|0 | geocode4_corr,df_final %>% filter(tso2_eq_output_1 >0 &
                   !geocode4_corr %in% c('1101', '1201', '3101', '5001'))
            )
t_2 <- change_target(t_2)
t_3 <- felm(log(tso2_eq_output_1) ~ 
            log(lag_current_ratio) * log(total_asset) +
            log(lag_liabilities_tot_asset) +
            log(lag_sales_tot_asset)  
            | fe_t_i + fe_c_t|0 | geocode4_corr,df_final %>% filter(tso2_eq_output_1 >0 &
                   !geocode4_corr %in% c('1101', '1201', '3101', '5001'))
            )
t_3 <- change_target(t_3)

t_4 <- felm(log(tso2_eq_output_1) ~ 
            log(lag_cashflow_to_tangible) * log(total_asset)+
            log(lag_current_ratio) * log(total_asset)+
            log(lag_liabilities_tot_asset) +
            log(lag_sales_tot_asset)
            | fe_t_i + fe_c_t|0 | geocode4_corr,df_final %>% filter(tso2_eq_output_1 >0 &
                   !geocode4_corr %in% c('1101', '1201', '3101', '5001'))
         ,
            exactDOF = TRUE)
t_4 <- change_target(t_4)

dep <- "Dependent variable: pollution abattement equipment SO2"
fe1 <- list(
    c("industry-year", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes"),
    c("city-year", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes")
             )
table_1 <- go_latex(list(
    t_0,t_1, t_2, t_3, t_4
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
# SOE vs Private

City ownership are available for the following variables:

- output
- capital
- employment
- sales

**How is it constructed** 

* city ownership public vs private in 2002
  * Aggregate output by ownership and city
    * A given city will have SOE asset tangibility and PRIVATE asset tangibility [output, employment, capital and sales]
  * If asset tangibility SOE above Private then city is dominated by SOE
  
Notebook reference: https://github.com/thomaspernet/Financial_dependency_pollution/blob/master/01_data_preprocessing/02_transform_tables/07_dominated_city_ownership.md
<!-- #endregion -->

```sos kernel="SoS"
query = """
WITH test AS (
  SELECT 
    *,
    CASE WHEN LENGTH(cic) = 4 THEN substr(cic, 1, 2) ELSE concat(
      '0', 
      substr(cic, 1, 1)
    ) END AS indu_2,
    CASE WHEN ownership = 'SOE' THEN 'SOE' ELSE 'PRIVATE' END AS soe_vs_pri,
    CASE WHEN ownership in ('HTM', 'FOREIGN') THEN 'FOREIGN' ELSE 'DOMESTIC' END AS for_vs_dom 
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
SELECT year, soe, geocode4_corr, indu_2,SUM(output) as output, SUM(employ) as employ, SUM(captal) as capital
FROM (
SELECT *,
CASE WHEN ownership in ('SOE') THEN 'SOE' ELSE 'PRIVATE' END AS soe
FROM test 
  )
  GROUP BY soe, geocode4_corr, year, indu_2
"""
df = (s3.run_query(
        query=query,
        database=db,
        s3_output='SQL_OUTPUT_ATHENA',
        filename="test",  # Add filename to print dataframe
        destination_key='SQL_OUTPUT_ATHENA/CSV',  #Use it temporarily
        dtype = dtypes
    )
     )
```

<!-- #region kernel="SoS" -->
Dirty code
<!-- #endregion -->

```sos kernel="SoS"
import janitor
```

```sos kernel="SoS"
for v in ['output','employ', 'capital']:
    for t in [.5, .4, .3, .2, .1]:
        df_ = (
            df
            .set_index(['year','indu_2', 'soe', 'geocode4_corr'])
            .unstack(-2)
            .assign(
                soe_dominated = lambda x: x[(v, 'SOE')] > x[(v, 'PRIVATE')],
                share_soe = lambda x: x[(v, 'SOE')] / (x[(v, 'SOE')] + x[(v, 'PRIVATE')])
            )
            #.loc[lambda x: x['soe_dominated'].isin([True])]
            .collapse_levels("_")
            .reset_index()
            [['year','geocode4_corr', 'indu_2', "soe_dominated", 
             'share_soe'
             ]]
            .loc[lambda x: x['year'].isin(["2002"])]
            .drop(columns = ['year'])
            .rename(columns = {'indu_2':'ind2'})
            .loc[lambda x: x['share_soe']> t]
            #.groupby(['soe_dominated'])
            #.agg({'share_soe':'describe'})
            .to_csv('list_city_soe_{}_{}.csv'.format(v, t), index = False)
        )
```

```sos kernel="SoS"
query = """
WITH test AS (
  SELECT 
    *,
    CASE WHEN LENGTH(cic) = 4 THEN substr(cic, 1, 2) ELSE concat(
      '0', 
      substr(cic, 1, 1)
    ) END AS indu_2,
    CASE WHEN ownership = 'SOE' THEN 'SOE' ELSE 'PRIVATE' END AS soe_vs_pri,
    CASE WHEN ownership in ('HTM', 'FOREIGN') THEN 'FOREIGN' ELSE 'DOMESTIC' END AS for_vs_dom 
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
SELECT year, foreign, geocode4_corr, indu_2,SUM(output) as output, SUM(employ) as employ, SUM(captal) as capital
FROM (
SELECT *,
CASE WHEN ownership in ('HTM', 'FOREIGN') THEN 'FOREIGN' ELSE 'DOMESTIC' END AS foreign
FROM test 
  )
  GROUP BY foreign, geocode4_corr, year, indu_2

"""
df = (s3.run_query(
        query=query,
        database=db,
        s3_output='SQL_OUTPUT_ATHENA',
        filename="test",  # Add filename to print dataframe
        destination_key='SQL_OUTPUT_ATHENA/CSV',  #Use it temporarily
        dtype = dtypes
    )
     )
```

```sos kernel="SoS"
for v in ['output','employ', 'capital']:
    for t in [.5, .4, .3, .2, .1]:
        (
            df
            .set_index(['year','indu_2', 'foreign', 'geocode4_corr'])
            .unstack(-2)
            .assign(
                for_dominated = lambda x: x[(v, 'FOREIGN')] > x[(v, 'DOMESTIC')],
                share_for = lambda x: x[(v, 'FOREIGN')] / (x[(v, 'FOREIGN')] + x[(v, 'DOMESTIC')])
            )
            .collapse_levels("_")
            .reset_index()
            [['year','geocode4_corr', 'indu_2', "for_dominated", 
             'share_for'
             ]]
            .loc[lambda x: x['year'].isin(["2002"])]
            .drop(columns = ['year'])
            .rename(columns = {'indu_2':'ind2'})
            .loc[lambda x: x['share_for']> t]
            #.groupby(['soe_dominated'])
            #.agg({'share_soe':'describe'})
            .to_csv('list_city_for_{}_{}.csv'.format(v, t), index = False)
        )
```

```sos kernel="SoS"
folder = 'Tables_1'
table_nb = 3
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
df_soe <- df_final %>% inner_join(read_csv('list_city_soe_employ_0.2.csv'))
df_priv <- df_final %>% left_join(read_csv('list_city_soe_employ_0.2.csv')) %>% filter(is.na(share_soe))
df_for <- df_final %>% inner_join(read_csv('list_city_for_employ_0.2.csv'))
df_dom <- df_final %>% left_join(read_csv('list_city_for_employ_0.2.csv')) %>% filter(is.na(share_for))
t_0 <- felm(log(tso2_eq_output_1) ~ 
            log(lag_cashflow_to_tangible) 
            | fe_t_i + fe_c_t|0 | geocode4_corr,df_soe
         ,
            exactDOF = TRUE)

t_1 <- felm(log(tso2_eq_output_1) ~ 
            log(lag_cashflow_to_tangible) 
            | fe_t_i + fe_c_t|0 | geocode4_corr,df_priv
         ,
            exactDOF = TRUE)

t_2 <- felm(log(tso2_eq_output_1) ~ 
            log(lag_cashflow_to_tangible) +
            log(lag_liabilities_tot_asset) +
            log(lag_sales_tot_asset)
            | fe_t_i + fe_c_t|0 | geocode4_corr,df_soe
         ,
            exactDOF = TRUE)
t_3 <- felm(log(tso2_eq_output_1) ~ 
            log(lag_cashflow_to_tangible) +
            log(lag_liabilities_tot_asset) +
            log(lag_sales_tot_asset)
            | fe_t_i + fe_c_t|0 | geocode4_corr,df_priv
         ,
            exactDOF = TRUE)

t_4 <- felm(log(tso2_eq_output_1) ~ 
            log(lag_cashflow_to_tangible) * constraint
            | fe_t_i + fe_c_t|0 | geocode4_corr,df_soe
         ,
            exactDOF = TRUE)
t_5 <- felm(log(tso2_eq_output_1) ~ 
            log(lag_cashflow_to_tangible) * constraint
            | fe_t_i + fe_c_t|0 | geocode4_corr,df_priv
         ,
            exactDOF = TRUE)

t_6 <- felm(log(tso2_eq_output_1) ~ 
            log(lag_cashflow_to_tangible) * constraint+
            log(lag_liabilities_tot_asset) +
            log(lag_sales_tot_asset)
            | fe_t_i + fe_c_t|0 | geocode4_corr,df_soe
         ,
            exactDOF = TRUE)
t_7 <- felm(log(tso2_eq_output_1) ~ 
            log(lag_cashflow_to_tangible) * constraint+
            log(lag_liabilities_tot_asset) +
            log(lag_sales_tot_asset)
            | fe_t_i + fe_c_t|0 | geocode4_corr,df_priv
         ,
            exactDOF = TRUE)

dep <- "Dependent variable: pollution abattement equipment SO2"
fe1 <- list(
    c("industry-year", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes"),
    c("city-year", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes")
             )
table_1 <- go_latex(list(
    t_0,t_1, t_2, t_3,t_4, t_5, t_6, t_7
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
"A city is labeled with SOE (Private) if the total industrial output of SOE's (Foreign) companies exceeds the total output of Private's (Domestic)" \
"companies within city. " \
"The following variables are lagged one year: Current Ratio, Cashflow, Liabilities/Assets, and Sales/Assets"
"\sym{*} Significance at the 10\%, \sym{**} Significance at the 5\%, \sym{***} Significance at the 1\%." 
#multi_lines_dep = '(city/product/trade regime/year)'
new_r = ['& SOE', 'Private', 'SOE', 'Private', 'SOE', 'Private', 'SOE', 'Private']
lb.beautify(table_number = table_nb,
            #reorder_var = reorder,
            #multi_lines_dep = multi_lines_dep,
            new_row= new_r,
            #multicolumn = multicolumn,
            table_nte = tbe1,
            jupyter_preview = True,
            resolution = 200,
           folder = folder)
```

```sos kernel="SoS"
folder = 'Tables_1'
table_nb = 3
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
df_soe <- df_final %>% inner_join(read_csv('list_city_soe_employ_0.2.csv'))
df_priv <- df_final %>% left_join(read_csv('list_city_soe_employ_0.2.csv')) %>% filter(is.na(share_soe))
df_for <- df_final %>% inner_join(read_csv('list_city_for_employ_0.2.csv'))
df_dom <- df_final %>% left_join(read_csv('list_city_for_employ_0.2.csv')) %>% filter(is.na(share_for))
t_0 <- felm(log(tso2_eq_output_1) ~ 
            log(lag_current_ratio) 
            | fe_t_i + fe_c_t|0 | geocode4_corr,df_soe
         ,
            exactDOF = TRUE)

t_1 <- felm(log(tso2_eq_output_1) ~ 
            log(lag_current_ratio) 
            | fe_t_i + fe_c_t|0 | geocode4_corr,df_priv
         ,
            exactDOF = TRUE)

t_2 <- felm(log(tso2_eq_output_1) ~ 
            log(lag_current_ratio) +
            log(lag_liabilities_tot_asset) +
            log(lag_sales_tot_asset)
            | fe_t_i + fe_c_t|0 | geocode4_corr,df_soe
         ,
            exactDOF = TRUE)
t_3 <- felm(log(tso2_eq_output_1) ~ 
            log(lag_current_ratio) +
            log(lag_liabilities_tot_asset) +
            log(lag_sales_tot_asset)
            | fe_t_i + fe_c_t|0 | geocode4_corr,df_priv
         ,
            exactDOF = TRUE)

t_4 <- felm(log(tso2_eq_output_1) ~ 
            log(lag_current_ratio) * constraint
            | fe_t_i + fe_c_t|0 | geocode4_corr,df_soe
         ,
            exactDOF = TRUE)
t_5 <- felm(log(tso2_eq_output_1) ~ 
            log(lag_current_ratio) * constraint
            | fe_t_i + fe_c_t|0 | geocode4_corr,df_priv
         ,
            exactDOF = TRUE)

t_6 <- felm(log(tso2_eq_output_1) ~ 
            log(lag_current_ratio) * constraint+
            log(lag_liabilities_tot_asset) +
            log(lag_sales_tot_asset)
            | fe_t_i + fe_c_t|0 | geocode4_corr,df_soe
         ,
            exactDOF = TRUE)
t_7 <- felm(log(tso2_eq_output_1) ~ 
            log(lag_current_ratio) * constraint+
            log(lag_liabilities_tot_asset) +
            log(lag_sales_tot_asset)
            | fe_t_i + fe_c_t|0 | geocode4_corr,df_priv
         ,
            exactDOF = TRUE)

dep <- "Dependent variable: pollution abattement equipment SO2"
fe1 <- list(
    c("industry-year", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes"),
    c("city-year", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes")
             )
table_1 <- go_latex(list(
    t_0,t_1, t_2, t_3,t_4, t_5, t_6, t_7
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
"A city is labeled with SOE (Private) if the total industrial output of SOE's (Foreign) companies exceeds the total output of Private's (Domestic)" \
"companies within city. " \
"The following variables are lagged one year: Current Ratio, Cashflow, Liabilities/Assets, and Sales/Assets"
"\sym{*} Significance at the 10\%, \sym{**} Significance at the 5\%, \sym{***} Significance at the 1\%." 
#multi_lines_dep = '(city/product/trade regime/year)'
new_r = ['& SOE', 'Private', 'SOE', 'Private', 'SOE', 'Private', 'SOE', 'Private']
lb.beautify(table_number = table_nb,
            #reorder_var = reorder,
            #multi_lines_dep = multi_lines_dep,
            new_row= new_r,
            #multicolumn = multicolumn,
            table_nte = tbe1,
            jupyter_preview = True,
            resolution = 200,
           folder = folder)
```

```sos kernel="SoS"
folder = 'Tables_1'
table_nb = 3
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
df_soe <- df_final %>% inner_join(read_csv('list_city_soe_employ_0.2.csv'))
df_priv <- df_final %>% left_join(read_csv('list_city_soe_employ_0.2.csv')) %>% filter(is.na(share_soe))
df_for <- df_final %>% inner_join(read_csv('list_city_for_employ_0.2.csv'))
df_dom <- df_final %>% left_join(read_csv('list_city_for_employ_0.2.csv')) %>% filter(is.na(share_for))
t_0 <- felm(log(tso2_eq_output_1) ~ 
            log(lag_cashflow_to_tangible) +
            log(lag_current_ratio)
            | fe_t_i + fe_c_t|0 | geocode4_corr,df_soe
         ,
            exactDOF = TRUE)

t_1 <- felm(log(tso2_eq_output_1) ~ 
            log(lag_cashflow_to_tangible)  +
            log(lag_current_ratio)
            | fe_t_i + fe_c_t|0 | geocode4_corr,df_priv
         ,
            exactDOF = TRUE)

t_2 <- felm(log(tso2_eq_output_1) ~ 
            log(lag_cashflow_to_tangible) +
            log(lag_current_ratio) +
            log(lag_liabilities_tot_asset) +
            log(lag_sales_tot_asset)
            | fe_t_i + fe_c_t|0 | geocode4_corr,df_soe
         ,
            exactDOF = TRUE)
t_3 <- felm(log(tso2_eq_output_1) ~ 
            log(lag_cashflow_to_tangible) +
            log(lag_current_ratio)+
            log(lag_liabilities_tot_asset) +
            log(lag_sales_tot_asset)
            | fe_t_i + fe_c_t|0 | geocode4_corr,df_priv
         ,
            exactDOF = TRUE)

t_4 <- felm(log(tso2_eq_output_1) ~ 
            log(lag_cashflow_to_tangible) * constraint +
            log(lag_current_ratio) * constraint
            | fe_t_i + fe_c_t|0 | geocode4_corr,df_soe,
            exactDOF = TRUE)
t_4 <- change_target(t_4)
t_5 <- felm(log(tso2_eq_output_1) ~ 
            log(lag_cashflow_to_tangible) * constraint +
            log(lag_current_ratio) * constraint
            | fe_t_i + fe_c_t|0 | geocode4_corr,df_priv,
            exactDOF = TRUE)
t_5 <- change_target(t_5)
t_6 <- felm(log(tso2_eq_output_1) ~ 
            log(lag_cashflow_to_tangible) * constraint+
            log(lag_current_ratio) * constraint+
            log(lag_liabilities_tot_asset) +
            log(lag_sales_tot_asset)
            | fe_t_i + fe_c_t|0 | geocode4_corr,df_soe,
            exactDOF = TRUE)
t_6 <- change_target(t_6)
t_7 <- felm(log(tso2_eq_output_1) ~ 
            log(lag_cashflow_to_tangible) * constraint+
            log(lag_current_ratio) * constraint+
            log(lag_liabilities_tot_asset) +
            log(lag_sales_tot_asset)
            | fe_t_i + fe_c_t|0 | geocode4_corr,df_priv,
            exactDOF = TRUE)
t_7 <- change_target(t_7)
dep <- "Dependent variable: pollution abattement equipment SO2"
fe1 <- list(
    c("industry-year", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes"),
    c("city-year", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes")
             )
table_1 <- go_latex(list(
    t_0,t_1, t_2, t_3,t_4, t_5, t_6, t_7
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
"A city is labeled with SOE (Private) if the total industrial output of SOE's (Foreign) companies exceeds the total output of Private's (Domestic)" \
"companies within city. " \
"The following variables are lagged one year: Current Ratio, Cashflow, Liabilities/Assets, and Sales/Assets"
"\sym{*} Significance at the 10\%, \sym{**} Significance at the 5\%, \sym{***} Significance at the 1\%." 
#multi_lines_dep = '(city/product/trade regime/year)'
new_r = ['& SOE', 'Private', 'SOE', 'Private', 'SOE', 'Private', 'SOE', 'Private']
lb.beautify(table_number = table_nb,
            #reorder_var = reorder,
            #multi_lines_dep = multi_lines_dep,
            new_row= new_r,
            #multicolumn = multicolumn,
            table_nte = tbe1,
            jupyter_preview = True,
            resolution = 200,
           folder = folder)
```

<!-- #region kernel="SoS" -->
## Dummy
<!-- #endregion -->

```sos kernel="SoS"
folder = 'Tables_1'
table_nb = 3
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
#df_soe <- df_final %>% inner_join(read_csv('list_city_soe_employ_0.2.csv'))
#df_priv <- df_final %>% left_join(read_csv('list_city_soe_employ_0.2.csv')) %>% filter(is.na(share_soe))
#df_for <- df_final %>% inner_join(read_csv('list_city_for_employ_0.2.csv'))
#df_dom <- df_final %>% left_join(read_csv('list_city_for_employ_0.2.csv')) %>% filter(is.na(share_for))
t_0 <- felm(log(tso2_eq_output_1) ~ 
            log(lag_cashflow_to_tangible) * soe_dominated
            | fe_t_i + fe_c_t|0 | geocode4_corr,df_final %>% left_join(read_csv('list_city_soe_employ_0.2.csv')) %>%
mutate(soe_dominated = replace_na(soe_dominated, FALSE))
         ,
            exactDOF = TRUE)

t_1 <- felm(log(tso2_eq_output_1) ~ 
            log(lag_cashflow_to_tangible) * soe_dominated+
            log(lag_liabilities_tot_asset) +
            log(lag_sales_tot_asset)
            | fe_t_i + fe_c_t|0 | geocode4_corr,df_final %>% left_join(read_csv('list_city_soe_employ_0.2.csv')) %>%
mutate(soe_dominated = replace_na(soe_dominated, FALSE))
         ,
            exactDOF = TRUE)

t_2 <- felm(log(tso2_eq_output_1) ~ 
            log(lag_cashflow_to_tangible) * constraint * soe_dominated
            | fe_t_i + fe_c_t|0 | geocode4_corr,df_final %>% left_join(read_csv('list_city_soe_employ_0.2.csv')) %>%
mutate(soe_dominated = replace_na(soe_dominated, FALSE))
         ,
            exactDOF = TRUE)

t_3 <- felm(log(tso2_eq_output_1) ~ 
            log(lag_cashflow_to_tangible) * constraint * soe_dominated+
            log(lag_liabilities_tot_asset) +
            log(lag_sales_tot_asset)
            | fe_t_i + fe_c_t|0 | geocode4_corr,df_final %>% left_join(read_csv('list_city_soe_employ_0.2.csv')) %>%
mutate(soe_dominated = replace_na(soe_dominated, FALSE))
         ,
            exactDOF = TRUE)

dep <- "Dependent variable: pollution abattement equipment SO2"
fe1 <- list(
    c("industry-year", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes"),
    c("city-year", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes")
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
"A city is labeled with SOE (Private) if the total industrial output of SOE's (Foreign) companies exceeds the total output of Private's (Domestic)" \
"companies within city. " \
"The following variables are lagged one year: Current Ratio, Cashflow, Liabilities/Assets, and Sales/Assets"
"\sym{*} Significance at the 10\%, \sym{**} Significance at the 5\%, \sym{***} Significance at the 1\%." 
#multi_lines_dep = '(city/product/trade regime/year)'
#new_r = ['& SOE', 'Private', 'SOE', 'Private', 'SOE', 'Private', 'SOE', 'Private']
lb.beautify(table_number = table_nb,
            #reorder_var = reorder,
            #multi_lines_dep = multi_lines_dep,
            #new_row= new_r,
            #multicolumn = multicolumn,
            table_nte = tbe1,
            jupyter_preview = True,
            resolution = 200,
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

```sos kernel="python3"
name_json = 'parameters_ETL_pollution_credit_constraint.json'
path_json = os.path.join(str(Path(path).parent.parent), 'utils',name_json)
```

```sos kernel="python3" nteract={"transient": {"deleting": false}} outputExpanded=false
create_report.create_report(extension = "html", keep_code = False, notebookname = "07_pollution_abatement_equation.ipynb")
```
