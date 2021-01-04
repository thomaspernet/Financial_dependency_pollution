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
# Estimate model pollution and financial dependency with city mandate

# Objective(s)

**Business needs**

Estimate the SO2 emission as a function of financial ratio, policy mandate and a time break

**Description**

**Objective**

Test the coefficient sign and significant of the main variable, the triple interaction term: financial ratio, time break and policy mandate. 

Test only with financial ratio computed at the industry level → more observation 

**Tables**

1. Table 1: Baseline estimate, SO2 emission reduction and industry financial ratio

**Cautious**


# Metadata

* Key: nii81ldai42313s
* Parent key (for update parent):  
* Notebook US Parent (i.e the one to update): 
* Epic: Epic 2
* US: US 1
* Date Begin: 11/28/2020
* Duration Task: 1
* Description: Estimate the SO2 emission as a function of financial ratio, policy mandate and a time break
* Step type: Evaluate model
* Status: Active
* Source URL: US 01 Baseline financial ratio
* Task type: Jupyter Notebook
* Users: Thomas Pernet
* Watchers: Thomas Pernet
* User Account: https://468786073381.signin.aws.amazon.com/console
* Estimated Log points: 7
* Task tag: #econometrics,#financial-ratio,#policy,#so2
* Toggl Tag: #model-estimate
* current nb commits: 0
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
* asif_city_industry_financial_ratio
* Github: 
  * https://github.com/thomaspernet/Financial_dependency_pollution/blob/master/01_data_preprocessing/02_transform_tables/00_asif_financial_ratio.md


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

```sos kernel="SoS"
os.environ['KMP_DUPLICATE_LIB_OK']='True'
```

<!-- #region kernel="SoS" -->
# Load tables

Since we load the data as a Pandas DataFrame, we want to pass the `dtypes`. We load the schema from Glue to guess the types
<!-- #endregion -->

```sos kernel="SoS"
db = 'environment'
table = 'fin_dep_pollution_baseline_industry'
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

if download_data:
    filename = 'df_{}'.format(table)
    full_path_filename = 'SQL_OUTPUT_ATHENA/CSV/{}.csv'.format(filename)
    s3 = service_s3.connect_S3(client = client,
                          bucket = bucket, verbose = False)
    query = """
    SELECT * 
    FROM {}.{}
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
    path_local = os.path.join(str(Path(path).parent.parent.parent), 
                              "00_data_catalogue/temporary_local_data")
    shutil.move(
        filename + '.csv',
        os.path.join(path_local, filename + '.csv')
    )
    s3.remove_file(full_path_filename)
    df.head()
```

```sos kernel="SoS"
pd.DataFrame(schema)
```

<!-- #region kernel="SoS" -->
# Correlation matrix indicator

Summary of the ratio definition

| Metrics                        | comments                                           | variables                                                                                                                                                           | Roam_link                                       | Exepected sign |
|--------------------------------|----------------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------|-------------------------------------------------|----------------|
| External finance dependence    |                                                    |                                                                                                                                                                     | #external-finance-dependence                    | Negative       |
| R&D intensity                  | RD / Sales                                         | rdfee/sales                                                                                                                                                         | #rd-intensity                                   | Negative       |
| Inventory to sales             | Inventory / sales                                  | 存货 (c81) / sales                                                                                                                                                  | #inventory-to-sales                             | Negative       |
| % cash                         | Current asset - cash / current asset               | (cuasset- 其中：短期投资 (c79) - 应收帐款 (c80) - 存货 (c81) - 其中：产成品 (c82)) /current asset                                                                   | #current-asset #cash                            | Negative       |
| current ratio                  | Current asset /current liabilities                 | cuasset/流动负债合计 (c95)                                                                                                                                          | #current-ratio                                  | Negative       |
| Quick ratio                    | (Current asset - Inventory)/current liabilities    | (cuasset -  其中：短期投资 (c79) - 应收帐款 (c80) - 存货 (c81)) / 流动负债合计 (c95)                                                                                | #quick-ratio                                    | Negative       |
| cash ratio                     | (Cash + marketable securities)/current liabilities | (cuasset - 其中：短期投资 (c79) - 应收帐款 (c80) - 存货 (c81) - 其中：产成品 (c82))/ 流动负债合计 (c95)                                                             | #cash-asset #cash-ratio                         | Negative       |
| Working capital                | Current asset - current liabilities                | cuasset- 流动负债合计 (c95)                                                                                                                                         | #working-capital-requirement                    | Negative       |
| Total Debt to Total Assets     | (Short-Tern Debt + Long-Term Debt)/total asset     | (流动负债合计 (c95) + 长期负债合计 (c97)) / toasset                                                                                                                 | #total-debt-to-total-assets                     | Negative       |
| % receivable                   | receivable account / current asset                 | 应收帐款 (c80) / cuasset                                                                                                                                            | #account-receivable #current-asset              | Negative       |
| working capital requirement    | Inventory + Accounts receivable - Accounts payable | 存货 (c81) + 应收帐款 (c80) - 应付帐款  (c96)                                                                                                                       | #working-capital                                | Positive       |
| Return on Asset                | Net income / Total assets                          | sales - (主营业务成本 (c108) + 营业费用 (c113) + 管理费用 (c114) + 财产保险费 (c116) + 劳动、失业保险费 (c118)+ 财务费用 (c124) + 本年应付工资总额 (wage)) /toasset | #return-on-asset                                | Ambiguous      |
| Asset Turnover Ratio           | Total sales / ((delta total asset)/2)              | 全年营业收入合计 (c64) /($\Delta$ toasset/2)                                                                                                                        | #asset-turnover-ratio                           | Ambiguous      |
| Asset tangibility              | Total fixed assets - Intangible assets             | tofixed - 无形资产 (c92)                                                                                                                                            | #asset-tangibility                              | Ambiguous      |
| Account payable to total asset | (delta account payable)/ (delta total asset)       | ($\Delta$ 应付帐款  (c96))/ ($\Delta$$ (toasset))                                                                                                                   | #change-account-paybable-to-change-total-assets | Ambiguous      |
<!-- #endregion -->

```sos kernel="SoS"
query = """
SELECT 
china_credit_constraint.cic,
industry_name,
china_credit_constraint.financial_dep_china as credit_constraint,
rd_intensity_i,
  std_rd_intensity_i,
  inventory_to_sales_i,
  std_inventory_to_sales_i,
  cash_over_curasset_i,
  std_cash_over_curasset_i,
  
  receivable_curasset_i,
  std_receivable_curasset_i,
  current_ratio_i,
  std_current_ratio_i,
  quick_ratio_i,
  std_quick_ratio_i,
  cash_ratio_i,
  std_cash_ratio_i,
  working_capital_i,
  std_working_capital_i,
  working_capital_requirement_i,
  std_working_capital_requirement_i,
  return_on_asset_i,
  std_return_on_asset_i,
  sales_assets_i,
  std_sales_assets_i,
  liabilities_assets_i,
  std_liabilities_assets_i,
  asset_tangibility_i,
  std_asset_tangibility_i,
  account_paybable_to_asset_i,
  std_account_paybable_to_asset_i

FROM "industry"."china_credit_constraint"
LEFT JOIN (
  SELECT 
indu_2 as cic,
  receivable_curasset_i,
  std_receivable_curasset_i,
  cash_over_curasset_i,
  std_cash_over_curasset_i,
  working_capital_i,
  std_working_capital_i,
  working_capital_requirement_i,
  std_working_capital_requirement_i,
  current_ratio_i,
  std_current_ratio_i,
  quick_ratio_i,
  std_quick_ratio_i,
  cash_ratio_i,
  std_cash_ratio_i,
  liabilities_assets_i,
  std_liabilities_assets_i,
  return_on_asset_i,
  std_return_on_asset_i,
  sales_assets_i,
  std_sales_assets_i,
  account_paybable_to_asset_i,
  std_account_paybable_to_asset_i,
  asset_tangibility_i,
  std_asset_tangibility_i,
  rd_intensity_i,
  std_rd_intensity_i,
  inventory_to_sales_i,
  std_inventory_to_sales_i
FROM "firms_survey"."asif_industry_financial_ratio_industry"
  ) as fin
on china_credit_constraint.cic = fin.cic
"""
df = (
    s3.run_query(
        query=query,
        database=db,
        s3_output='SQL_OUTPUT_ATHENA',
        filename='correl'  # Add filename to print dataframe
    )
    .assign(
    loc=lambda x: x[['cic', 'industry_name']].apply(
            lambda x: '-'.join(x.astype('str')), axis=1)
    )
    .drop(columns = ['cic','industry_name'])
    .set_index('loc')
    .sort_values(by = ['credit_constraint'])
)
```

<!-- #region kernel="SoS" -->
Not standardized value
<!-- #endregion -->

```sos kernel="SoS"
(
    df
    .filter(regex='^(?!std)\w+$|credit')
    .style
    .format('{0:,.2f}')
    
)
```

<!-- #region kernel="SoS" -->
Standardized value
<!-- #endregion -->

```sos kernel="SoS"
(
    df
    .filter(regex='std|credit')
    .style
    .format('{0:,.2f}')
    
)
```

```sos kernel="SoS"
import plotly.express as px
import seaborn as sns
import matplotlib.pyplot as plt
```

```sos kernel="SoS"
cm = sns.light_palette("green", as_cmap=True)
(
    df
    .filter(regex='^(?!std)\w+$|credit')
    .rank()
    .astype('int64')
    .style
    .background_gradient(cmap=cm)
)
```

```sos kernel="SoS"
fig = px.parallel_coordinates(
    df.filter(regex="^(?!std)\w+$|credit").rank(),
    labels={
        "credit_constraint": "credit_constraint",
        "rd_intensity_i": "rd_intensity_i",
        "inventory_to_sales_i": "inventory_to_sales_i",
        "receivable_curasset_i": "receivable_curasset_i",
        "cash_over_curasset_i": "cash_over_curasset_i",
        "current_ratio_i": "current_ratio_i",
        "quick_ratio_i": "quick_ratio_i",
        "cash_ratio_i": "cash_ratio_i",
        "working_capital_i": "working_capital_i",
        "working_capital_requirement_i": "working_capital_requirement_i",
        "return_on_asset_i": "return_on_asset_i",
        "sales_assets_i": "sales_assets_i",
        "liabilities_assets_i": "liabilities_assets_i",
        "asset_tangibility_i": "asset_tangibility_i",
        "account_paybable_to_asset_i": "account_paybable_to_asset_i",
    },
    color_continuous_scale=px.colors.diverging.Tealrose,
    color_continuous_midpoint=2,
)
fig
```

```sos kernel="SoS"
sns.set_theme(style="white")

# Compute the correlation matrix
corr = df.filter(regex='^(?!std)\w+$|credit').rank().corr()

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

<!-- #region kernel="SoS" -->
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

```sos kernel="SoS"
add_to_dic = True
if add_to_dic:
    with open('schema_table.json') as json_file:
        data = json.load(json_file)
    data['to_rename'] = []
    dic_rename = [
        ### control variables
        {
        'old':'output',
        'new':'\\text{output}_{cit}'
        },
        {
        'old':'employment',
        'new':'\\text{employment}_{cit}'
        },
        {
        'old':'capital',
        'new':'\\text{capital}_{cit}'
        },
       # {
       # 'old':'sales',
       # 'new':'\\text{sales}_{cit}'
       # },
        
        ### Polluted sector
        {
        'old':'polluted_di',
        'new':'\\text{polluted sector, decile}_{i}'
        },
        {
        'old':'polluted\_diABOVE',
        'new':'\\text{polluted sector, decile}_{i}'
        },
        {
        'old':'polluted_mi',
        'new':'\\text{polluted sector, mean}_{i}'
        },
        {
        'old':'polluted\_miABOVE',
        'new':'\\text{polluted sector, mean}_{i}'
        },
        {
        'old':'polluted_mei',
        'new':'\\text{polluted sector, median}_{i}'
        },
        {
        'old':'polluted\_meiABOVE',
        'new':'\\text{polluted sector, median}_{i}'
        },

        ### financial ratio
        #### Industry
        {
        'old':'credit\_constraint',
        'new':'\\text{credit constraint}_i'
        },
        {
        'old':'std\_receivable\_curasset\_i',
        'new':'\\text{std receivable asset ratio}_i'
        },
        {
        'old':'receivable\_curasset\_i',
        'new':'\\text{receivable asset ratio}_i'
        },
        {
        'old':'std\_cash\_over\_curasset\_i',
        'new':'\\text{std cash over asset}_i'
        },
        {
        'old':'cash\_over\_curasset\_i',
        'new':'\\text{cash over asset}_i'
        },
        {
        'old':'std\_working\_capital\_i',
        'new':'\\text{std working capital}_i'
        },
        {
        'old':'working\_capital\_i',
        'new':'\\text{working capital}_i'
        },
        {
        'old':'std\_working\_capital\_requirement\_i',
        'new':'\\text{std working capital requirement}_i'
        },
        {
        'old':'working\_capital\_requirement\_i',
        'new':'\\text{working capital requirement}_i'
        },
        {
        'old':'std\_current\_ratio\_i',
        'new':'\\text{std current ratio}_i'
        },
        {
        'old':'current\_ratio\_i',
        'new':'\\text{current ratio}_i'
        },
        {
        'old':'std\_quick\_ratio\_i',
        'new':'\\text{std quick ratio}_i'
        },
        {
        'old':'quick\_ratio\_i',
        'new':'\\text{quick ratio}_i'
        },
        {
        'old':'std\_cash\_ratio\_i',
        'new':'\\text{std cash ratio}_i'
        },
        {
        'old':'cash\_ratio\_i',
        'new':'\\text{cash ratio}_i'
        },
        {
        'old':'std\_liabilities\_assets\_i',
        'new':'\\text{std liabilities assets}_i'
        },{
        'old':'liabilities\_assets\_i',
        'new':'\\text{liabilities assets}_i'
        },
        {
        'old':'std\_return\_on\_asset\_i',
        'new':'\\text{std return on asset}_i'
        },{
        'old':'return\_on\_asset\_i',
        'new':'\\text{return on asset}_i'
        },
        {
        'old':'std\_sales\_assets\_i',
        'new':'\\text{std sales assets}_i'
        },
        {
        'old':'sales\_assets\_i',
        'new':'\\text{sales assets}_i'
        },
        {
        'old':'std\_rd\_intensity\_i',
        'new':'\\text{std rd intensity}_i'
        },
        {
        'old':'rd\_intensity\_i',
        'new':'\\text{rd intensity}_i'
        },
        {
        'old':'std\_inventory\_to\_sales\_i',
        'new':'\\text{std inventory to sales}_i'
        },
        {
        'old':'inventory\_to\_sales\_i',
        'new':'\\text{inventory to sales}_i'
        },
        {
        'old':'std\_asset\_tangibility\_i',
        'new':'\\text{std asset tangibility}_i'
        },
        {
        'old':'asset\_tangibility\_i',
        'new':'\\text{asset tangibility}_i'
        },
        {
        'old':'std\_account\_paybable\_to\_asset\_i',
        'new':'\\text{std account paybable to asset}_i'
        },
        {
        'old':'account\_paybable\_to\_asset\_i',
        'new':'\\text{account paybable to asset}_i'
        },
        #### 
        {
        'old':'periodTRUE',
        'new':'\\text{period}'
        },
        {
        'old':'period',
        'new':'\\text{period}'
        },
        {
        'old':'tso2\_mandate\_c',
        'new':'\\text{policy mandate}_c'
        },
    ]

    data['to_rename'].extend(dic_rename)
    with open('schema_table.json', 'w') as outfile:
        json.dump(data, outfile)
    #print(data)
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
path = '../../../00_Data_catalogue/temporary_local_data/df_fin_dep_pollution_baseline.csv'
df_final <- read_csv(path) %>%
mutate_if(is.character, as.factor) %>%
    mutate_at(vars(starts_with("fe")), as.factor) %>%
mutate(
    period = relevel(as.factor(period), ref='FALSE'),
    polluted_di = relevel(as.factor(polluted_di), ref='BELOW'),
    polluted_mi = relevel(as.factor(polluted_mi), ref='BELOW'),
    polluted_mei = relevel(as.factor(polluted_mei), ref='BELOW'),
    #working_capital_i = working_capital_i /1000000,
    #working_capital_requirement_i = working_capital_requirement_i /1000000,
    #liabilities_assets_m2_i = liabilities_assets_m2_i /1000000,
    #asset_tangibility_i = asset_tangibility_i /1000000,
    #polluted_thre = relevel(as.factor(polluted_thre), ref='BELOW'),
    
)
```

```sos kernel="R"
head(df_final)
```

<!-- #region kernel="R" -->
# 1. Table 1 & 2: Determinant of SO2 emission, financial ratio

$$
\begin{aligned}
\text{SO2}_{cit}  &= \alpha \text{Financial ratio}_i  + \gamma_{c} + \gamma_{t}  + \epsilon_{cit}
\end{aligned}
$$

<!-- #endregion -->

<!-- #region kernel="R" -->
### Base value
<!-- #endregion -->

```sos kernel="SoS"
folder = 'Tables_0'
table_nb = 0
table = 'table_{}'.format(table_nb)
path = os.path.join(folder, table + '.txt')
if os.path.exists(folder) == False:
        os.mkdir(folder)
for ext in ['.txt', '.tex', '.pdf']:
    x = [a for a in os.listdir(folder) if a.endswith(ext)]
    [os.remove(os.path.join(folder, i)) for i in x]
path
```

```sos kernel="R"
%get path table
t_0 <- felm(log(tso2) ~ credit_constraint  +
            log(output) + log(employment) + log(capital)
            | geocode4_corr + year |0 | ind2, df_final,
            exactDOF = TRUE)

t_1 <- felm(log(tso2) ~ rd_intensity_i +
            log(output) + log(employment) + log(capital)
            | geocode4_corr + year|0 | ind2, df_final,
            exactDOF = TRUE)

t_2 <- felm(log(tso2) ~ inventory_to_sales_i +
            log(output) + log(employment) + log(capital)
            | geocode4_corr + year|0 | ind2, df_final,
            exactDOF = TRUE)

t_3 <- felm(log(tso2) ~ receivable_curasset_i +
            log(output) + log(employment) + log(capital)
            | geocode4_corr + year|0 | ind2, df_final,
            exactDOF = TRUE)

t_4 <- felm(log(tso2) ~ cash_over_curasset_i +
            log(output) + log(employment) + log(capital)
            | geocode4_corr + year|0 | ind2, df_final,
            exactDOF = TRUE)

t_5 <- felm(log(tso2) ~ current_ratio_i  +
            log(output) + log(employment) + log(capital)
            | geocode4_corr + year|0 | ind2, df_final,
            exactDOF = TRUE)

t_6 <- felm(log(tso2) ~ quick_ratio_i  +
            log(output) + log(employment) + log(capital)
            | geocode4_corr + year|0 | ind2, df_final,
            exactDOF = TRUE)

t_7 <- felm(log(tso2) ~ cash_ratio_i  +
            log(output) + log(employment) + log(capital)
            | geocode4_corr + year|0 | ind2, df_final,
            exactDOF = TRUE)

t_8 <- felm(log(tso2) ~ working_capital_i  +
            log(output) + log(employment) + log(capital)
            | geocode4_corr + year|0 | ind2, df_final,
            exactDOF = TRUE)

t_9 <- felm(log(tso2) ~ working_capital_requirement_i  +
            log(output) + log(employment) + log(capital)
            | geocode4_corr + year|0 | ind2, df_final,
            exactDOF = TRUE)

t_10 <- felm(log(tso2) ~ return_on_asset_i +
            log(output) + log(employment) + log(capital)
            | geocode4_corr + year|0 | ind2, df_final,
            exactDOF = TRUE)

t_11 <- felm(log(tso2) ~ sales_assets_i +
            log(output) + log(employment) + log(capital)
            | geocode4_corr + year|0 | ind2, df_final,
            exactDOF = TRUE)

t_12 <- felm(log(tso2) ~ liabilities_assets_i  +
            log(output) + log(employment) + log(capital)
            | geocode4_corr + year|0 | ind2, df_final,
            exactDOF = TRUE)

t_13 <- felm(log(tso2) ~ asset_tangibility_i +
            log(output) + log(employment) + log(capital)
            | geocode4_corr + year|0 | ind2, df_final,
            exactDOF = TRUE)

t_14 <- felm(log(tso2) ~ account_paybable_to_asset_i +
            log(output) + log(employment) + log(capital)
            | geocode4_corr + year|0 | ind2, df_final,
            exactDOF = TRUE)


dep <- "Dependent variable: SO2 emission"
fe1 <- list(
    c("City", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes"),
    
    c("Time", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes")
             )

table_1 <- go_latex(list(
    t_0,t_1, t_2, t_3, t_4, t_5, t_6, t_7, t_8, t_9, t_10, t_11, t_12, t_13, t_14
),
    title="Determinant of SO2 emission, financial ratio",
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
"\sym{*} Significance at the 10\%, \sym{**} Significance at the 5\%, \sym{***} Significance at the 1\%."

multicolumn ={
    'Tight': 1,
    'Loose': 12
}

#reorder = {
    
#    7:0,
#    8:1,
    #9:2
#}

#multi_lines_dep = '(city/product/trade regime/year)'
#new_r = ['& test1', 'test2']
lb.beautify(table_number = table_nb,
            #reorder_var = reorder,
            #multi_lines_dep = multi_lines_dep,
            #new_row= new_r,
            multicolumn = multicolumn,
            table_nte = tbe1,
            jupyter_preview = True,
            resolution = 280,
           folder = folder)
```

<!-- #region kernel="SoS" -->
### Standardized value
<!-- #endregion -->

```sos kernel="SoS"
table_nb = 1
table = 'table_{}'.format(table_nb)
path = os.path.join(folder, table + '.txt')
if os.path.exists(folder) == False:
        os.mkdir(folder)
for ext in ['.txt', '.tex', '.pdf']:
    x = [a for a in os.listdir(folder) if a.endswith(ext)]
    [os.remove(os.path.join(folder, i)) for i in x]
path
```

```sos kernel="R"
%get path table
t_0 <- felm(log(tso2) ~ credit_constraint  +
            log(output) + log(employment) + log(capital)
            | geocode4_corr + year |0 | ind2, df_final,
            exactDOF = TRUE)

t_1 <- felm(log(tso2) ~ std_rd_intensity_i +
            log(output) + log(employment) + log(capital)
            | geocode4_corr + year|0 | ind2, df_final,
            exactDOF = TRUE)

t_2 <- felm(log(tso2) ~ std_inventory_to_sales_i +
            log(output) + log(employment) + log(capital)
            | geocode4_corr + year|0 | ind2, df_final,
            exactDOF = TRUE)

t_3 <- felm(log(tso2) ~ std_receivable_curasset_i +
            log(output) + log(employment) + log(capital)
            | geocode4_corr + year|0 | ind2, df_final,
            exactDOF = TRUE)

t_4 <- felm(log(tso2) ~ std_cash_over_curasset_i +
            log(output) + log(employment) + log(capital)
            | geocode4_corr + year|0 | ind2, df_final,
            exactDOF = TRUE)

t_5 <- felm(log(tso2) ~ std_current_ratio_i  +
            log(output) + log(employment) + log(capital)
            | geocode4_corr + year|0 | ind2, df_final,
            exactDOF = TRUE)

t_6 <- felm(log(tso2) ~ std_quick_ratio_i  +
            log(output) + log(employment) + log(capital)
            | geocode4_corr + year|0 | ind2, df_final,
            exactDOF = TRUE)

t_7 <- felm(log(tso2) ~ std_cash_ratio_i  +
            log(output) + log(employment) + log(capital)
            | geocode4_corr + year|0 | ind2, df_final,
            exactDOF = TRUE)

t_8 <- felm(log(tso2) ~ std_working_capital_i  +
            log(output) + log(employment) + log(capital)
            | geocode4_corr + year|0 | ind2, df_final,
            exactDOF = TRUE)

t_9 <- felm(log(tso2) ~ std_working_capital_requirement_i  +
            log(output) + log(employment) + log(capital)
            | geocode4_corr + year|0 | ind2, df_final,
            exactDOF = TRUE)

t_10 <- felm(log(tso2) ~ std_return_on_asset_i +
            log(output) + log(employment) + log(capital)
            | geocode4_corr + year|0 | ind2, df_final,
            exactDOF = TRUE)

t_11 <- felm(log(tso2) ~ std_sales_assets_i +
            log(output) + log(employment) + log(capital)
            | geocode4_corr + year|0 | ind2, df_final,
            exactDOF = TRUE)

t_12 <- felm(log(tso2) ~ std_liabilities_assets_i  +
            log(output) + log(employment) + log(capital)
            | geocode4_corr + year|0 | ind2, df_final,
            exactDOF = TRUE)

t_13 <- felm(log(tso2) ~ std_asset_tangibility_i +
            log(output) + log(employment) + log(capital)
            | geocode4_corr + year|0 | ind2, df_final,
            exactDOF = TRUE)

t_14 <- felm(log(tso2) ~ std_account_paybable_to_asset_i +
            log(output) + log(employment) + log(capital)
            | geocode4_corr + year|0 | ind2, df_final,
            exactDOF = TRUE)

dep <- "Dependent variable: SO2 emission"
fe1 <- list(
    c("City", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes"),
    
    c("Time", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes")
             )
table_1 <- go_latex(list(
    t_0,t_1, t_2, t_3, t_4, t_5, t_6, t_7, t_8, t_9, t_10, t_11, t_12
),
    title="Determinant of SO2 emission, financial ratio (standardized values)",
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
"\sym{*} Significance at the 10\%, \sym{**} Significance at the 5\%, \sym{***} Significance at the 1\%."

multicolumn ={
    'Tight': 1,
    'Loose': 5
}

#reorder = {
    
#    7:0,
#    8:1,
    #9:2
#}

#multi_lines_dep = '(city/product/trade regime/year)'
#new_r = ['& test1', 'test2']
lb.beautify(table_number = table_nb,
            #reorder_var = reorder,
            #multi_lines_dep = multi_lines_dep,
            #new_row= new_r,
            multicolumn = multicolumn,
            table_nte = tbe1,
            jupyter_preview = True,
            resolution = 280,
           folder = folder)
```

<!-- #region kernel="SoS" -->
# Table 3 & 4: S02 emission reduction, financial ratio and period

$$
\begin{aligned}
\text{SO2}_{cit}  &= \alpha \text{Financial ratio}_i \times \text{Period}  + \gamma_{c} + \gamma_{t}  + \epsilon_{cit}
\end{aligned}
$$

<!-- #endregion -->

<!-- #region kernel="SoS" -->
### Base value
<!-- #endregion -->

```sos kernel="SoS"
table_nb = 2
table = 'table_{}'.format(table_nb)
path = os.path.join(folder, table + '.txt')
if os.path.exists(folder) == False:
        os.mkdir(folder)
for ext in ['.txt', '.tex', '.pdf']:
    x = [a for a in os.listdir(folder) if a.endswith(ext)]
    [os.remove(os.path.join(folder, i)) for i in x]
path
```

```sos kernel="R"
%get path table
t_0 <- felm(log(tso2) ~ credit_constraint  * period  +
            log(output) + log(employment) + log(capital)
            | fe_c_t + fe_c_i |0 | ind2, df_final,
            exactDOF = TRUE)

t_1 <- felm(log(tso2) ~ rd_intensity_i  * period  +
            log(output) + log(employment) + log(capital)
            | fe_c_t + fe_c_i|0 | ind2, df_final,
            exactDOF = TRUE)

t_2 <- felm(log(tso2) ~ inventory_to_sales_i  * period  +
            log(output) + log(employment) + log(capital)
            | fe_c_t + fe_c_i|0 | ind2, df_final,
            exactDOF = TRUE)

t_3 <- felm(log(tso2) ~ receivable_curasset_i  * period  +
            log(output) + log(employment) + log(capital)
            |fe_c_t + fe_c_i|0 | ind2, df_final,
            exactDOF = TRUE)

t_4 <- felm(log(tso2) ~ cash_over_curasset_i  * period  +
            log(output) + log(employment) + log(capital)
            | fe_c_t + fe_c_i|0 | ind2, df_final,
            exactDOF = TRUE)

t_5 <- felm(log(tso2) ~ current_ratio_i   * period  +
            log(output) + log(employment) + log(capital)
            | fe_c_t + fe_c_i|0 | ind2, df_final,
            exactDOF = TRUE)

t_6 <- felm(log(tso2) ~ quick_ratio_i   * period  +
            log(output) + log(employment) + log(capital)
            | fe_c_t + fe_c_i|0 | ind2, df_final,
            exactDOF = TRUE)

t_7 <- felm(log(tso2) ~ cash_ratio_i   * period  +
            log(output) + log(employment) + log(capital)
            | fe_c_t + fe_c_i|0 | ind2, df_final,
            exactDOF = TRUE)

t_8 <- felm(log(tso2) ~ working_capital_i   * period  +
            log(output) + log(employment) + log(capital)
            | fe_c_t + fe_c_i|0 | ind2, df_final,
            exactDOF = TRUE)

t_9 <- felm(log(tso2) ~ working_capital_requirement_i   * period  +
            log(output) + log(employment) + log(capital)
            | fe_c_t + fe_c_i|0 | ind2, df_final,
            exactDOF = TRUE)

t_10 <- felm(log(tso2) ~ return_on_asset_i  * period  +
            log(output) + log(employment) + log(capital)
            | fe_c_t + fe_c_i|0 | ind2, df_final,
            exactDOF = TRUE)

t_11 <- felm(log(tso2) ~ sales_assets_i  * period  +
            log(output) + log(employment) + log(capital)
            | fe_c_t + fe_c_i|0 | ind2, df_final,
            exactDOF = TRUE)

t_12 <- felm(log(tso2) ~ liabilities_assets_i   * period  +
            log(output) + log(employment) + log(capital)
            | fe_c_t + fe_c_i|0 | ind2, df_final,
            exactDOF = TRUE)

t_13 <- felm(log(tso2) ~ asset_tangibility_i  * period  +
            log(output) + log(employment) + log(capital)
            | fe_c_t + fe_c_i|0 | ind2, df_final,
            exactDOF = TRUE)

t_14 <- felm(log(tso2) ~ account_paybable_to_asset_i  * period  +
            log(output) + log(employment) + log(capital)
            |fe_c_t + fe_c_i|0 | ind2, df_final,
            exactDOF = TRUE)

dep <- "Dependent variable: SO2 emission"
fe1 <- list(
    c("City", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes"),
    
    c("Time", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes")
             )

table_1 <- go_latex(list(
    t_0,t_1, t_2, t_3, t_4, t_5, t_6, t_7, t_8, t_9, t_10, t_11, t_12, t_13, t_14
),
    title="S02 emission reduction, financial ratio and period",
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
"\sym{*} Significance at the 10\%, \sym{**} Significance at the 5\%, \sym{***} Significance at the 1\%."

multicolumn ={
    'Tight': 1,
    'Loose': 5
}

#multi_lines_dep = '(city/product/trade regime/year)'
#new_r = ['& test1', 'test2']
lb.beautify(table_number = table_nb,
            #reorder_var = reorder,
            #multi_lines_dep = multi_lines_dep,
            #new_row= new_r,
            multicolumn = multicolumn,
            table_nte = tbe1,
            jupyter_preview = True,
            resolution = 280,
           folder = folder)
```

<!-- #region kernel="SoS" -->
### Standardized value
<!-- #endregion -->

```sos kernel="SoS"
table_nb = 3
table = 'table_{}'.format(table_nb)
path = os.path.join(folder, table + '.txt')
if os.path.exists(folder) == False:
        os.mkdir(folder)
for ext in ['.txt', '.tex', '.pdf']:
    x = [a for a in os.listdir(folder) if a.endswith(ext)]
    [os.remove(os.path.join(folder, i)) for i in x]
path
```

```sos kernel="R"
%get path table
t_0 <- felm(log(tso2) ~ credit_constraint  * period  +
            log(output) + log(employment) + log(capital)
            | fe_c_t + fe_c_i |0 | ind2, df_final,
            exactDOF = TRUE)

t_1 <- felm(log(tso2) ~ std_rd_intensity_i  * period  +
            log(output) + log(employment) + log(capital)
            | fe_c_t + fe_c_i|0 | ind2, df_final,
            exactDOF = TRUE)

t_2 <- felm(log(tso2) ~ std_inventory_to_sales_i  * period  +
            log(output) + log(employment) + log(capital)
            | fe_c_t + fe_c_i|0 | ind2, df_final,
            exactDOF = TRUE)

t_3 <- felm(log(tso2) ~ std_receivable_curasset_i  * period  +
            log(output) + log(employment) + log(capital)
            |fe_c_t + fe_c_i|0 | ind2, df_final,
            exactDOF = TRUE)

t_4 <- felm(log(tso2) ~ std_cash_over_curasset_i  * period  +
            log(output) + log(employment) + log(capital)
            | fe_c_t + fe_c_i|0 | ind2, df_final,
            exactDOF = TRUE)

t_5 <- felm(log(tso2) ~ std_current_ratio_i   * period  +
            log(output) + log(employment) + log(capital)
            | fe_c_t + fe_c_i|0 | ind2, df_final,
            exactDOF = TRUE)

t_6 <- felm(log(tso2) ~ std_quick_ratio_i   * period  +
            log(output) + log(employment) + log(capital)
            | fe_c_t + fe_c_i|0 | ind2, df_final,
            exactDOF = TRUE)

t_7 <- felm(log(tso2) ~ std_cash_ratio_i   * period  +
            log(output) + log(employment) + log(capital)
            | fe_c_t + fe_c_i|0 | ind2, df_final,
            exactDOF = TRUE)

t_8 <- felm(log(tso2) ~ std_working_capital_i   * period  +
            log(output) + log(employment) + log(capital)
            | fe_c_t + fe_c_i|0 | ind2, df_final,
            exactDOF = TRUE)

t_9 <- felm(log(tso2) ~ std_working_capital_requirement_i   * period  +
            log(output) + log(employment) + log(capital)
            | fe_c_t + fe_c_i|0 | ind2, df_final,
            exactDOF = TRUE)

t_10 <- felm(log(tso2) ~ std_return_on_asset_i  * period  +
            log(output) + log(employment) + log(capital)
            | fe_c_t + fe_c_i|0 | ind2, df_final,
            exactDOF = TRUE)

t_11 <- felm(log(tso2) ~ std_sales_assets_i  * period  +
            log(output) + log(employment) + log(capital)
            | fe_c_t + fe_c_i|0 | ind2, df_final,
            exactDOF = TRUE)

t_12 <- felm(log(tso2) ~ std_liabilities_assets_i   * period  +
            log(output) + log(employment) + log(capital)
            | fe_c_t + fe_c_i|0 | ind2, df_final,
            exactDOF = TRUE)

t_13 <- felm(log(tso2) ~ std_asset_tangibility_i  * period  +
            log(output) + log(employment) + log(capital)
            | fe_c_t + fe_c_i|0 | ind2, df_final,
            exactDOF = TRUE)

t_14 <- felm(log(tso2) ~ std_account_paybable_to_asset_i  * period  +
            log(output) + log(employment) + log(capital)
            |fe_c_t + fe_c_i|0 | ind2, df_final,
            exactDOF = TRUE)

dep <- "Dependent variable: SO2 emission"
fe1 <- list(
    c("City", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes"),
    
    c("Time", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes")
             )

table_1 <- go_latex(list(
    t_0,t_1, t_2, t_3, t_4, t_5, t_6, t_7, t_8, t_9, t_10, t_11, t_12, t_13, t_14
),
    title="S02 emission reduction, financial ratio and period (standardized values)",
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
"\sym{*} Significance at the 10\%, \sym{**} Significance at the 5\%, \sym{***} Significance at the 1\%."

multicolumn ={
    'Tight': 1,
    'Loose': 5
}

#multi_lines_dep = '(city/product/trade regime/year)'
#new_r = ['& test1', 'test2']
lb.beautify(table_number = table_nb,
            #reorder_var = reorder,
            #multi_lines_dep = multi_lines_dep,
            #new_row= new_r,
            #multicolumn = multicolumn,
            table_nte = tbe1,
            jupyter_preview = True,
            resolution = 280,
           folder = folder)
```

<!-- #region kernel="SoS" -->
# Table 5 & 6: S02 emission reduction, financial ratio, Filter polluted sector

$$
\begin{aligned}
\text{SO2}_{cit}  &= \alpha \text{Financial ratio}_i \times \text{Period} + \gamma_{c} + \gamma_{t}  + \epsilon_{cit}
\end{aligned}
$$
<!-- #endregion -->

<!-- #region kernel="SoS" -->
### Base value
<!-- #endregion -->

```sos kernel="SoS"
table_nb = 4
table = 'table_{}'.format(table_nb)
path = os.path.join(folder, table + '.txt')
if os.path.exists(folder) == False:
        os.mkdir(folder)
for ext in ['.txt', '.tex', '.pdf']:
    x = [a for a in os.listdir(folder) if a.endswith(ext)]
    [os.remove(os.path.join(folder, i)) for i in x]
path
```

```sos kernel="R"
%get path table
t_0 <- felm(log(tso2) ~ credit_constraint  * period  +
            log(output) + log(employment) + log(capital)
            | fe_c_t + fe_c_i |0 | ind2, df_final %>% filter(polluted_di == 'ABOVE'),
            exactDOF = TRUE)

t_1 <- felm(log(tso2) ~ rd_intensity_i  * period  +
            log(output) + log(employment) + log(capital)
            | fe_c_t + fe_c_i|0 | ind2, df_final %>% filter(polluted_di == 'ABOVE'),
            exactDOF = TRUE)

t_2 <- felm(log(tso2) ~ inventory_to_sales_i  * period  +
            log(output) + log(employment) + log(capital)
            | fe_c_t + fe_c_i|0 | ind2, df_final %>% filter(polluted_di == 'ABOVE'),
            exactDOF = TRUE)

t_3 <- felm(log(tso2) ~ receivable_curasset_i  * period  +
            log(output) + log(employment) + log(capital)
            |fe_c_t + fe_c_i|0 | ind2, df_final %>% filter(polluted_di == 'ABOVE'),
            exactDOF = TRUE)

t_4 <- felm(log(tso2) ~ cash_over_curasset_i  * period  +
            log(output) + log(employment) + log(capital)
            | fe_c_t + fe_c_i|0 | ind2, df_final %>% filter(polluted_di == 'ABOVE'),
            exactDOF = TRUE)

t_5 <- felm(log(tso2) ~ current_ratio_i   * period  +
            log(output) + log(employment) + log(capital)
            | fe_c_t + fe_c_i|0 | ind2, df_final %>% filter(polluted_di == 'ABOVE'),
            exactDOF = TRUE)

t_6 <- felm(log(tso2) ~ quick_ratio_i   * period  +
            log(output) + log(employment) + log(capital)
            | fe_c_t + fe_c_i|0 | ind2, df_final %>% filter(polluted_di == 'ABOVE'),
            exactDOF = TRUE)

t_7 <- felm(log(tso2) ~ cash_ratio_i   * period  +
            log(output) + log(employment) + log(capital)
            | fe_c_t + fe_c_i|0 | ind2, df_final %>% filter(polluted_di == 'ABOVE'),
            exactDOF = TRUE)

t_8 <- felm(log(tso2) ~ working_capital_i   * period  +
            log(output) + log(employment) + log(capital)
            | fe_c_t + fe_c_i|0 | ind2, df_final %>% filter(polluted_di == 'ABOVE'),
            exactDOF = TRUE)

t_9 <- felm(log(tso2) ~ working_capital_requirement_i   * period  +
            log(output) + log(employment) + log(capital)
            | fe_c_t + fe_c_i|0 | ind2, df_final %>% filter(polluted_di == 'ABOVE'),
            exactDOF = TRUE)

t_10 <- felm(log(tso2) ~ return_on_asset_i  * period  +
            log(output) + log(employment) + log(capital)
            | fe_c_t + fe_c_i|0 | ind2,df_final %>% filter(polluted_di == 'ABOVE'),
            exactDOF = TRUE)

t_11 <- felm(log(tso2) ~ sales_assets_i  * period  +
            log(output) + log(employment) + log(capital)
            | fe_c_t + fe_c_i|0 | ind2, df_final %>% filter(polluted_di == 'ABOVE'),
            exactDOF = TRUE)

t_12 <- felm(log(tso2) ~ liabilities_assets_i   * period  +
            log(output) + log(employment) + log(capital)
            | fe_c_t + fe_c_i|0 | ind2, df_final %>% filter(polluted_di == 'ABOVE'),
            exactDOF = TRUE)

t_13 <- felm(log(tso2) ~ asset_tangibility_i  * period  +
            log(output) + log(employment) + log(capital)
            | fe_c_t + fe_c_i|0 | ind2, df_final %>% filter(polluted_di == 'ABOVE'),
            exactDOF = TRUE)

t_14 <- felm(log(tso2) ~ account_paybable_to_asset_i  * period  +
            log(output) + log(employment) + log(capital)
            |fe_c_t + fe_c_i|0 | ind2, df_final %>% filter(polluted_di == 'ABOVE'),
            exactDOF = TRUE)



dep <- "Dependent variable: SO2 emission"
fe1 <- list(
    c("City", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes"),
    
    c("Time", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes")
             )

table_1 <- go_latex(list(
    t_0,t_1, t_2, t_3, t_4, t_5, t_6, t_7, t_8, t_9, t_10, t_11, t_12, t_13, t_14
),
    title="Baseline estimate, S02 emission reduction, financial ratio, Filter polluted sector",
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
"\sym{*} Significance at the 10\%, \sym{**} Significance at the 5\%, \sym{***} Significance at the 1\%."

multicolumn ={
    'Tight': 1,
    'Loose': 5
}

#multi_lines_dep = '(city/product/trade regime/year)'
#new_r = ['& test1', 'test2']
lb.beautify(table_number = table_nb,
            #reorder_var = reorder,
            #multi_lines_dep = multi_lines_dep,
            #new_row= new_r,
            multicolumn = multicolumn,
            table_nte = tbe1,
            jupyter_preview = True,
            resolution = 280,
           folder = folder)
```

<!-- #region kernel="SoS" -->
### Standardized value
<!-- #endregion -->

```sos kernel="SoS"
table_nb = 5
table = 'table_{}'.format(table_nb)
path = os.path.join(folder, table + '.txt')
if os.path.exists(folder) == False:
        os.mkdir(folder)
for ext in ['.txt', '.tex', '.pdf']:
    x = [a for a in os.listdir(folder) if a.endswith(ext)]
    [os.remove(os.path.join(folder, i)) for i in x]
path
```

```sos kernel="R"
%get path table
t_0 <- felm(log(tso2) ~ credit_constraint  * period  +
            log(output) + log(employment) + log(capital)
            | fe_c_t + fe_c_i |0 | ind2, df_final %>% filter(polluted_di == 'ABOVE'),
            exactDOF = TRUE)

t_1 <- felm(log(tso2) ~ std_rd_intensity_i  * period  +
            log(output) + log(employment) + log(capital)
            | fe_c_t + fe_c_i|0 | ind2, df_final %>% filter(polluted_di == 'ABOVE'),
            exactDOF = TRUE)

t_2 <- felm(log(tso2) ~ std_inventory_to_sales_i  * period  +
            log(output) + log(employment) + log(capital)
            | fe_c_t + fe_c_i|0 | ind2, df_final %>% filter(polluted_di == 'ABOVE'),
            exactDOF = TRUE)

t_3 <- felm(log(tso2) ~ std_receivable_curasset_i  * period  +
            log(output) + log(employment) + log(capital)
            |fe_c_t + fe_c_i|0 | ind2, df_final %>% filter(polluted_di == 'ABOVE'),
            exactDOF = TRUE)

t_4 <- felm(log(tso2) ~ std_cash_over_curasset_i  * period  +
            log(output) + log(employment) + log(capital)
            | fe_c_t + fe_c_i|0 | ind2, df_final %>% filter(polluted_di == 'ABOVE'),
            exactDOF = TRUE)

t_5 <- felm(log(tso2) ~ std_current_ratio_i   * period  +
            log(output) + log(employment) + log(capital)
            | fe_c_t + fe_c_i|0 | ind2, df_final %>% filter(polluted_di == 'ABOVE'),
            exactDOF = TRUE)

t_6 <- felm(log(tso2) ~ std_quick_ratio_i   * period  +
            log(output) + log(employment) + log(capital)
            | fe_c_t + fe_c_i|0 | ind2, df_final %>% filter(polluted_di == 'ABOVE'),
            exactDOF = TRUE)

t_7 <- felm(log(tso2) ~ std_cash_ratio_i   * period  +
            log(output) + log(employment) + log(capital)
            | fe_c_t + fe_c_i|0 | ind2, df_final %>% filter(polluted_di == 'ABOVE'),
            exactDOF = TRUE)

t_8 <- felm(log(tso2) ~ std_working_capital_i   * period  +
            log(output) + log(employment) + log(capital)
            | fe_c_t + fe_c_i|0 | ind2, df_final %>% filter(polluted_di == 'ABOVE'),
            exactDOF = TRUE)

t_9 <- felm(log(tso2) ~ std_working_capital_requirement_i   * period  +
            log(output) + log(employment) + log(capital)
            | fe_c_t + fe_c_i|0 | ind2, df_final %>% filter(polluted_di == 'ABOVE'),
            exactDOF = TRUE)

t_10 <- felm(log(tso2) ~ std_return_on_asset_i  * period  +
            log(output) + log(employment) + log(capital)
            | fe_c_t + fe_c_i|0 | ind2,df_final %>% filter(polluted_di == 'ABOVE'),
            exactDOF = TRUE)

t_11 <- felm(log(tso2) ~ std_sales_assets_i  * period  +
            log(output) + log(employment) + log(capital)
            | fe_c_t + fe_c_i|0 | ind2, df_final %>% filter(polluted_di == 'ABOVE'),
            exactDOF = TRUE)

t_12 <- felm(log(tso2) ~ std_liabilities_assets_i   * period  +
            log(output) + log(employment) + log(capital)
            | fe_c_t + fe_c_i|0 | ind2, df_final %>% filter(polluted_di == 'ABOVE'),
            exactDOF = TRUE)

t_13 <- felm(log(tso2) ~ std_asset_tangibility_i  * period  +
            log(output) + log(employment) + log(capital)
            | fe_c_t + fe_c_i|0 | ind2, df_final %>% filter(polluted_di == 'ABOVE'),
            exactDOF = TRUE)

t_14 <- felm(log(tso2) ~ std_account_paybable_to_asset_i  * period  +
            log(output) + log(employment) + log(capital)
            |fe_c_t + fe_c_i|0 | ind2, df_final %>% filter(polluted_di == 'ABOVE'),
            exactDOF = TRUE)


dep <- "Dependent variable: SO2 emission"
fe1 <- list(
    c("City", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes"),
    
    c("Time", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes")
             )

table_1 <- go_latex(list(
    t_0,t_1, t_2, t_3, t_4, t_5, t_6, t_7, t_8, t_9, t_10, t_11, t_12, t_13, t_14
),
    title="Baseline estimate, S02 emission reduction, financial ratio, Filter polluted sector (standardized)",
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
"\sym{*} Significance at the 10\%, \sym{**} Significance at the 5\%, \sym{***} Significance at the 1\%."

multicolumn ={
    'Tight': 1,
    'Loose': 5
}

reorder = {
    # Old, New
    10:3, ## Working capital
    11:5, 
    12:7,
    13:9,
    14:11,
    15:13,
    16:15
}

#multi_lines_dep = '(city/product/trade regime/year)'
#new_r = ['& test1', 'test2']
lb.beautify(table_number = table_nb,
            #reorder_var = reorder,
            #multi_lines_dep = multi_lines_dep,
            #new_row= new_r,
            multicolumn = multicolumn,
            table_nte = tbe1,
            jupyter_preview = True,
            resolution = 280,
           folder = folder)
```

<!-- #region kernel="SoS" -->
# Table 7 & 8: S02 emission reduction, financial ratio, Filter no polluted sector

$$
\begin{aligned}
\text{SO2}_{cit}  &= \alpha \text{Financial ratio}_i \times \text{Period} + \gamma_{c} + \gamma_{t}  + \epsilon_{cit}
\end{aligned}
$$
<!-- #endregion -->

<!-- #region kernel="SoS" -->
### Base value
<!-- #endregion -->

```sos kernel="SoS"
table_nb = 6
table = 'table_{}'.format(table_nb)
path = os.path.join(folder, table + '.txt')
if os.path.exists(folder) == False:
        os.mkdir(folder)
for ext in ['.txt', '.tex', '.pdf']:
    x = [a for a in os.listdir(folder) if a.endswith(ext)]
    [os.remove(os.path.join(folder, i)) for i in x]
path
```

```sos kernel="R"
%get path table
t_0 <- felm(log(tso2) ~ credit_constraint  * period  +
            log(output) + log(employment) + log(capital)
            | fe_c_t + fe_c_i |0 | ind2, df_final %>% filter(polluted_di == 'BELOW'),
            exactDOF = TRUE)

t_1 <- felm(log(tso2) ~ rd_intensity_i  * period  +
            log(output) + log(employment) + log(capital)
            | fe_c_t + fe_c_i|0 | ind2, df_final %>% filter(polluted_di == 'BELOW'),
            exactDOF = TRUE)

t_2 <- felm(log(tso2) ~ inventory_to_sales_i  * period  +
            log(output) + log(employment) + log(capital)
            | fe_c_t + fe_c_i|0 | ind2, df_final %>% filter(polluted_di == 'BELOW'),
            exactDOF = TRUE)

t_3 <- felm(log(tso2) ~ receivable_curasset_i  * period  +
            log(output) + log(employment) + log(capital)
            |fe_c_t + fe_c_i|0 | ind2, df_final %>% filter(polluted_di == 'BELOW'),
            exactDOF = TRUE)

t_4 <- felm(log(tso2) ~ cash_over_curasset_i  * period  +
            log(output) + log(employment) + log(capital)
            | fe_c_t + fe_c_i|0 | ind2, df_final %>% filter(polluted_di == 'BELOW'),
            exactDOF = TRUE)

t_5 <- felm(log(tso2) ~ current_ratio_i   * period  +
            log(output) + log(employment) + log(capital)
            | fe_c_t + fe_c_i|0 | ind2, df_final %>% filter(polluted_di == 'BELOW'),
            exactDOF = TRUE)

t_6 <- felm(log(tso2) ~ quick_ratio_i   * period  +
            log(output) + log(employment) + log(capital)
            | fe_c_t + fe_c_i|0 | ind2, df_final %>% filter(polluted_di == 'BELOW'),
            exactDOF = TRUE)

t_7 <- felm(log(tso2) ~ cash_ratio_i   * period  +
            log(output) + log(employment) + log(capital)
            | fe_c_t + fe_c_i|0 | ind2, df_final %>% filter(polluted_di == 'BELOW'),
            exactDOF = TRUE)

t_8 <- felm(log(tso2) ~ working_capital_i   * period  +
            log(output) + log(employment) + log(capital)
            | fe_c_t + fe_c_i|0 | ind2, df_final %>% filter(polluted_di == 'BELOW'),
            exactDOF = TRUE)

t_9 <- felm(log(tso2) ~ working_capital_requirement_i   * period  +
            log(output) + log(employment) + log(capital)
            | fe_c_t + fe_c_i|0 | ind2, df_final %>% filter(polluted_di == 'BELOW'),
            exactDOF = TRUE)

t_10 <- felm(log(tso2) ~ return_on_asset_i  * period  +
            log(output) + log(employment) + log(capital)
            | fe_c_t + fe_c_i|0 | ind2,df_final %>% filter(polluted_di == 'BELOW'),
            exactDOF = TRUE)

t_11 <- felm(log(tso2) ~ sales_assets_i  * period  +
            log(output) + log(employment) + log(capital)
            | fe_c_t + fe_c_i|0 | ind2, df_final %>% filter(polluted_di == 'BELOW'),
            exactDOF = TRUE)

t_12 <- felm(log(tso2) ~ liabilities_assets_i   * period  +
            log(output) + log(employment) + log(capital)
            | fe_c_t + fe_c_i|0 | ind2, df_final %>% filter(polluted_di == 'BELOW'),
            exactDOF = TRUE)

t_13 <- felm(log(tso2) ~ asset_tangibility_i  * period  +
            log(output) + log(employment) + log(capital)
            | fe_c_t + fe_c_i|0 | ind2, df_final %>% filter(polluted_di == 'BELOW'),
            exactDOF = TRUE)

t_14 <- felm(log(tso2) ~ account_paybable_to_asset_i  * period  +
            log(output) + log(employment) + log(capital)
            |fe_c_t + fe_c_i|0 | ind2, df_final %>% filter(polluted_di == 'BELOW'),
            exactDOF = TRUE)

dep <- "Dependent variable: SO2 emission"
fe1 <- list(
    c("City", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes"),
    
    c("Time", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes")
             )

table_1 <- go_latex(list(
    t_0,t_1, t_2, t_3, t_4, t_5, t_6, t_7, t_8, t_9, t_10, t_11, t_12, t_13, t_14
),
    title="S02 emission reduction, financial ratio, Filter no polluted sector",
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
"\sym{*} Significance at the 10\%, \sym{**} Significance at the 5\%, \sym{***} Significance at the 1\%."

multicolumn ={
    'Tight': 1,
    'Loose': 5
}

#multi_lines_dep = '(city/product/trade regime/year)'
#new_r = ['& test1', 'test2']
lb.beautify(table_number = table_nb,
            #reorder_var = reorder,
            #multi_lines_dep = multi_lines_dep,
            #new_row= new_r,
            multicolumn = multicolumn,
            table_nte = tbe1,
            jupyter_preview = True,
            resolution = 280,
           folder = folder)
```

<!-- #region kernel="SoS" -->
### Standardized value
<!-- #endregion -->

```sos kernel="SoS"
table_nb = 7
table = 'table_{}'.format(table_nb)
path = os.path.join(folder, table + '.txt')
if os.path.exists(folder) == False:
        os.mkdir(folder)
for ext in ['.txt', '.tex', '.pdf']:
    x = [a for a in os.listdir(folder) if a.endswith(ext)]
    [os.remove(os.path.join(folder, i)) for i in x]
path
```

```sos kernel="R"
%get path table
t_0 <- felm(log(tso2) ~ credit_constraint  * period  +
            log(output) + log(employment) + log(capital)
            | fe_c_t + fe_c_i |0 | ind2, df_final %>% filter(polluted_di == 'BELOW'),
            exactDOF = TRUE)

t_1 <- felm(log(tso2) ~ std_rd_intensity_i  * period  +
            log(output) + log(employment) + log(capital)
            | fe_c_t + fe_c_i|0 | ind2, df_final %>% filter(polluted_di == 'BELOW'),
            exactDOF = TRUE)

t_2 <- felm(log(tso2) ~ std_inventory_to_sales_i  * period  +
            log(output) + log(employment) + log(capital)
            | fe_c_t + fe_c_i|0 | ind2, df_final %>% filter(polluted_di == 'BELOW'),
            exactDOF = TRUE)

t_3 <- felm(log(tso2) ~ std_receivable_curasset_i  * period  +
            log(output) + log(employment) + log(capital)
            |fe_c_t + fe_c_i|0 | ind2, df_final %>% filter(polluted_di == 'BELOW'),
            exactDOF = TRUE)

t_4 <- felm(log(tso2) ~ std_cash_over_curasset_i  * period  +
            log(output) + log(employment) + log(capital)
            | fe_c_t + fe_c_i|0 | ind2, df_final %>% filter(polluted_di == 'BELOW'),
            exactDOF = TRUE)

t_5 <- felm(log(tso2) ~ std_current_ratio_i   * period  +
            log(output) + log(employment) + log(capital)
            | fe_c_t + fe_c_i|0 | ind2, df_final %>% filter(polluted_di == 'BELOW'),
            exactDOF = TRUE)

t_6 <- felm(log(tso2) ~ std_quick_ratio_i   * period  +
            log(output) + log(employment) + log(capital)
            | fe_c_t + fe_c_i|0 | ind2, df_final %>% filter(polluted_di == 'BELOW'),
            exactDOF = TRUE)

t_7 <- felm(log(tso2) ~ std_cash_ratio_i   * period  +
            log(output) + log(employment) + log(capital)
            | fe_c_t + fe_c_i|0 | ind2, df_final %>% filter(polluted_di == 'BELOW'),
            exactDOF = TRUE)

t_8 <- felm(log(tso2) ~ std_working_capital_i   * period  +
            log(output) + log(employment) + log(capital)
            | fe_c_t + fe_c_i|0 | ind2, df_final %>% filter(polluted_di == 'BELOW'),
            exactDOF = TRUE)

t_9 <- felm(log(tso2) ~ std_working_capital_requirement_i   * period  +
            log(output) + log(employment) + log(capital)
            | fe_c_t + fe_c_i|0 | ind2, df_final %>% filter(polluted_di == 'BELOW'),
            exactDOF = TRUE)

t_10 <- felm(log(tso2) ~ std_return_on_asset_i  * period  +
            log(output) + log(employment) + log(capital)
            | fe_c_t + fe_c_i|0 | ind2,df_final %>% filter(polluted_di == 'BELOW'),
            exactDOF = TRUE)

t_11 <- felm(log(tso2) ~ std_sales_assets_i  * period  +
            log(output) + log(employment) + log(capital)
            | fe_c_t + fe_c_i|0 | ind2, df_final %>% filter(polluted_di == 'BELOW'),
            exactDOF = TRUE)

t_12 <- felm(log(tso2) ~ std_liabilities_assets_i   * period  +
            log(output) + log(employment) + log(capital)
            | fe_c_t + fe_c_i|0 | ind2, df_final %>% filter(polluted_di == 'BELOW'),
            exactDOF = TRUE)

t_13 <- felm(log(tso2) ~ std_asset_tangibility_i  * period  +
            log(output) + log(employment) + log(capital)
            | fe_c_t + fe_c_i|0 | ind2, df_final %>% filter(polluted_di == 'BELOW'),
            exactDOF = TRUE)

t_14 <- felm(log(tso2) ~ std_account_paybable_to_asset_i  * period  +
            log(output) + log(employment) + log(capital)
            |fe_c_t + fe_c_i|0 | ind2, df_final %>% filter(polluted_di == 'BELOW'),
            exactDOF = TRUE)


dep <- "Dependent variable: SO2 emission"
fe1 <- list(
    c("City", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes"),
    
    c("Time", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes")
             )

table_1 <- go_latex(list(
    t_0,t_1, t_2, t_3, t_4, t_5, t_6, t_7, t_8, t_9, t_10, t_11, t_12, t_13, t_14
),
    title="S02 emission reduction, financial ratio, Filter no polluted sector (standardized values)",
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
"\sym{*} Significance at the 10\%, \sym{**} Significance at the 5\%, \sym{***} Significance at the 1\%."

multicolumn ={
    'Tight': 1,
    'Loose': 5
}

#multi_lines_dep = '(city/product/trade regime/year)'
#new_r = ['& test1', 'test2']
lb.beautify(table_number = table_nb,
            #reorder_var = reorder,
            #multi_lines_dep = multi_lines_dep,
            #new_row= new_r,
            #multicolumn = multicolumn,
            table_nte = tbe1,
            jupyter_preview = True,
            resolution = 280,
           folder = folder)
```

<!-- #region kernel="SoS" -->
# Table 11 & 12: SO2 emission reduction, industry financial ratio and policy mandate

$$
\begin{aligned}
\text{SO2}_{cit}  &= \alpha \text{Financial ratio}_i \times \text{Period} \times \text{policy mandate}_c  + \gamma_{ci} + \gamma_{ti} +\gamma_{ct}  + \epsilon_{cit}
\end{aligned}
$$



**Andersen results**

![](https://drive.google.com/uc?export=view&id=1HrqaA5NLRPjWk2lqvHyrZAjO3wSP5r-9)
<!-- #endregion -->

<!-- #region kernel="R" -->
Please, change the folder and table name. If the folder is not created, one will be added.
<!-- #endregion -->

<!-- #region kernel="SoS" -->
### Base value
<!-- #endregion -->

```sos kernel="SoS"
table_nb = 8
table = 'table_{}'.format(table_nb)
path = os.path.join(folder, table + '.txt')
if os.path.exists(folder) == False:
        os.mkdir(folder)
for ext in ['.txt', '.tex', '.pdf']:
    x = [a for a in os.listdir(folder) if a.endswith(ext)]
    [os.remove(os.path.join(folder, i)) for i in x]
path
```

```sos kernel="R"
%get path table
t_0 <- felm(log(tso2) ~ credit_constraint * period * tso2_mandate_c +
            log(output) + log(employment) + log(capital)
            | fe_c_i + fe_t_i + fe_c_t |0 | ind2, df_final,
            exactDOF = TRUE)

t_1 <- felm(log(tso2) ~ rd_intensity_i * period * tso2_mandate_c +
            log(output) + log(employment) + log(capital)
            |fe_c_i + fe_t_i + fe_c_t |0 | ind2, df_final,
            exactDOF = TRUE)

t_2 <- felm(log(tso2) ~ inventory_to_sales_i  * period * tso2_mandate_c +
            log(output) + log(employment) + log(capital)
            | fe_c_i + fe_t_i + fe_c_t |0 | ind2, df_final,
            exactDOF = TRUE)

t_3 <- felm(log(tso2) ~ receivable_curasset_i  * period * tso2_mandate_c +
            log(output) + log(employment) + log(capital)
            |fe_c_i + fe_t_i + fe_c_t |0 | ind2, df_final,
            exactDOF = TRUE)

t_4 <- felm(log(tso2) ~ cash_over_curasset_i  * period * tso2_mandate_c +
            log(output) + log(employment) + log(capital)
            | fe_c_i + fe_t_i + fe_c_t |0 | ind2, df_final,
            exactDOF = TRUE)

t_5 <- felm(log(tso2) ~ current_ratio_i   * period * tso2_mandate_c +
            log(output) + log(employment) + log(capital)
            | fe_c_i + fe_t_i + fe_c_t |0 | ind2, df_final,
            exactDOF = TRUE)

t_6 <- felm(log(tso2) ~ quick_ratio_i   * period * tso2_mandate_c +
            log(output) + log(employment) + log(capital)
            | fe_c_i + fe_t_i + fe_c_t |0 | ind2, df_final,
            exactDOF = TRUE)

t_7 <- felm(log(tso2) ~ cash_ratio_i  * period * tso2_mandate_c +
            log(output) + log(employment) + log(capital)
            | fe_c_i + fe_t_i + fe_c_t |0 | ind2, df_final,
            exactDOF = TRUE)

t_8 <- felm(log(tso2) ~ working_capital_i  * period * tso2_mandate_c +
            log(output) + log(employment) + log(capital)
            | fe_c_i + fe_t_i + fe_c_t |0 | ind2, df_final,
            exactDOF = TRUE)

t_9 <- felm(log(tso2) ~ working_capital_requirement_i  * period * tso2_mandate_c +
            log(output) + log(employment) + log(capital)
            | fe_c_i + fe_t_i + fe_c_t |0 | ind2, df_final,
            exactDOF = TRUE)

t_10 <- felm(log(tso2) ~ return_on_asset_i  * period * tso2_mandate_c +
            log(output) + log(employment) + log(capital)
            | fe_c_i + fe_t_i + fe_c_t |0 | ind2, df_final,
            exactDOF = TRUE)

t_11 <- felm(log(tso2) ~ sales_assets_i * period * tso2_mandate_c +
            log(output) + log(employment) + log(capital)
            | fe_c_i + fe_t_i + fe_c_t |0 | ind2, df_final,
            exactDOF = TRUE)

t_12 <- felm(log(tso2) ~ liabilities_assets_i * period * tso2_mandate_c +
            log(output) + log(employment) + log(capital)
            | fe_c_i + fe_t_i + fe_c_t |0 | ind2, df_final,
            exactDOF = TRUE)

t_13 <- felm(log(tso2) ~ asset_tangibility_i  * period * tso2_mandate_c +
            log(output) + log(employment) + log(capital)
            | fe_c_i + fe_t_i + fe_c_t |0 | ind2, df_final,
            exactDOF = TRUE)

t_14 <- felm(log(tso2) ~ account_paybable_to_asset_i * period * tso2_mandate_c +
            log(output) + log(employment) + log(capital)
            |fe_c_i + fe_t_i + fe_c_t |0 | ind2, df_final,
            exactDOF = TRUE)


dep <- "Dependent variable: SO2 emission"
fe1 <- list(
    c("City-industry", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes"),
    c("Time-industry", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes"),
    c("City-Time", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes")
             )

table_1 <- go_latex(list(
    t_0,t_1, t_2, t_3, t_4, t_5, t_6, t_7, t_8, t_9, t_10, t_11, t_12, t_13, t_14
),
    title="SO2 emission reduction, industry financial ratio and policy mandate",
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
"clustered at the industry level appear inp arentheses. "\
"\sym{*} Significance at the 10\%, \sym{**} Significance at the 5\%, \sym{***} Significance at the 1\%."

multicolumn ={
    'Tight': 1,
    'Loose': 5
}

#multi_lines_dep = '(city/product/trade regime/year)'
#new_r = ['& test1', 'test2']
lb.beautify(table_number = table_nb,
            #reorder_var = reorder,
            #multi_lines_dep = multi_lines_dep,
            #new_row= new_r,
            #multicolumn = multicolumn,
            table_nte = tbe1,
            jupyter_preview = True,
            resolution = 280,
           folder = folder)
```

<!-- #region kernel="SoS" -->
### Standardized value
<!-- #endregion -->

```sos kernel="SoS"
table_nb = 9
table = 'table_{}'.format(table_nb)
path = os.path.join(folder, table + '.txt')
if os.path.exists(folder) == False:
        os.mkdir(folder)
for ext in ['.txt', '.tex', '.pdf']:
    x = [a for a in os.listdir(folder) if a.endswith(ext)]
    [os.remove(os.path.join(folder, i)) for i in x]
path
```

```sos kernel="R"
%get path table
t_0 <- felm(log(tso2) ~ credit_constraint * period * tso2_mandate_c +
            log(output) + log(employment) + log(capital)
            | fe_c_i + fe_t_i + fe_c_t |0 | ind2, df_final,
            exactDOF = TRUE)

t_1 <- felm(log(tso2) ~ std_rd_intensity_i * period * tso2_mandate_c +
            log(output) + log(employment) + log(capital)
            |fe_c_i + fe_t_i + fe_c_t |0 | ind2, df_final,
            exactDOF = TRUE)

t_2 <- felm(log(tso2) ~ std_inventory_to_sales_i  * period * tso2_mandate_c +
            log(output) + log(employment) + log(capital)
            | fe_c_i + fe_t_i + fe_c_t |0 | ind2, df_final,
            exactDOF = TRUE)

t_3 <- felm(log(tso2) ~ std_receivable_curasset_i  * period * tso2_mandate_c +
            log(output) + log(employment) + log(capital)
            |fe_c_i + fe_t_i + fe_c_t |0 | ind2, df_final,
            exactDOF = TRUE)

t_4 <- felm(log(tso2) ~ std_cash_over_curasset_i  * period * tso2_mandate_c +
            log(output) + log(employment) + log(capital)
            | fe_c_i + fe_t_i + fe_c_t |0 | ind2, df_final,
            exactDOF = TRUE)

t_5 <- felm(log(tso2) ~ std_current_ratio_i   * period * tso2_mandate_c +
            log(output) + log(employment) + log(capital)
            | fe_c_i + fe_t_i + fe_c_t |0 | ind2, df_final,
            exactDOF = TRUE)

t_6 <- felm(log(tso2) ~ std_quick_ratio_i   * period * tso2_mandate_c +
            log(output) + log(employment) + log(capital)
            | fe_c_i + fe_t_i + fe_c_t |0 | ind2, df_final,
            exactDOF = TRUE)

t_7 <- felm(log(tso2) ~ std_cash_ratio_i  * period * tso2_mandate_c +
            log(output) + log(employment) + log(capital)
            | fe_c_i + fe_t_i + fe_c_t |0 | ind2, df_final,
            exactDOF = TRUE)

t_8 <- felm(log(tso2) ~ std_working_capital_i  * period * tso2_mandate_c +
            log(output) + log(employment) + log(capital)
            | fe_c_i + fe_t_i + fe_c_t |0 | ind2, df_final,
            exactDOF = TRUE)

t_9 <- felm(log(tso2) ~ std_working_capital_requirement_i  * period * tso2_mandate_c +
            log(output) + log(employment) + log(capital)
            | fe_c_i + fe_t_i + fe_c_t |0 | ind2, df_final,
            exactDOF = TRUE)

t_10 <- felm(log(tso2) ~ std_return_on_asset_i  * period * tso2_mandate_c +
            log(output) + log(employment) + log(capital)
            | fe_c_i + fe_t_i + fe_c_t |0 | ind2, df_final,
            exactDOF = TRUE)

t_11 <- felm(log(tso2) ~ std_sales_assets_i * period * tso2_mandate_c +
            log(output) + log(employment) + log(capital)
            | fe_c_i + fe_t_i + fe_c_t |0 | ind2, df_final,
            exactDOF = TRUE)

t_12 <- felm(log(tso2) ~ std_liabilities_assets_i * period * tso2_mandate_c +
            log(output) + log(employment) + log(capital)
            | fe_c_i + fe_t_i + fe_c_t |0 | ind2, df_final,
            exactDOF = TRUE)

t_13 <- felm(log(tso2) ~ std_asset_tangibility_i  * period * tso2_mandate_c +
            log(output) + log(employment) + log(capital)
            | fe_c_i + fe_t_i + fe_c_t |0 | ind2, df_final,
            exactDOF = TRUE)

t_14 <- felm(log(tso2) ~ std_account_paybable_to_asset_i * period * tso2_mandate_c +
            log(output) + log(employment) + log(capital)
            |fe_c_i + fe_t_i + fe_c_t |0 | ind2, df_final,
            exactDOF = TRUE)


dep <- "Dependent variable: SO2 emission"
fe1 <- list(
    c("City-industry", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes"),
    c("Time-industry", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes"),
    c("City-Time", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes")
             )

table_1 <- go_latex(list(
    t_0,t_1, t_2, t_3, t_4, t_5, t_6, t_7, t_8, t_9, t_10, t_11, t_12, t_13, t_14
),
    title="SO2 emission reduction, industry financial ratio and policy mandate (standardized values)",
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
"clustered at the industry level appear inp arentheses. "\
"\sym{*} Significance at the 10\%, \sym{**} Significance at the 5\%, \sym{***} Significance at the 1\%."

multicolumn ={
    'Tight': 1,
    'Loose': 5
}

#multi_lines_dep = '(city/product/trade regime/year)'
#new_r = ['& test1', 'test2']
lb.beautify(table_number = table_nb,
            #reorder_var = reorder,
            #multi_lines_dep = multi_lines_dep,
            #new_row= new_r,
            #multicolumn = multicolumn,
            table_nte = tbe1,
            jupyter_preview = True,
            resolution = 280,
           folder = folder)
```

<!-- #region kernel="SoS" -->
### Standardized value & credit constraint
<!-- #endregion -->

```sos kernel="SoS"
table_nb = 10
table = 'table_{}'.format(table_nb)
path = os.path.join(folder, table + '.txt')
if os.path.exists(folder) == False:
        os.mkdir(folder)
for ext in ['.txt', '.tex', '.pdf']:
    x = [a for a in os.listdir(folder) if a.endswith(ext)]
    [os.remove(os.path.join(folder, i)) for i in x]
path
```

```sos kernel="R"
%get path table
t_0 <- felm(log(tso2) ~ credit_constraint * period * tso2_mandate_c +
            log(output) + log(employment) + log(capital)
            | fe_c_i + fe_t_i + fe_c_t |0 | ind2, df_final,
            exactDOF = TRUE)

t_1 <- felm(log(tso2) ~ 
            credit_constraint * period * tso2_mandate_c +
            std_rd_intensity_i * period * tso2_mandate_c +
            log(output) + log(employment) + log(capital)
            |fe_c_i + fe_t_i + fe_c_t |0 | ind2, df_final,
            exactDOF = TRUE)

t_2 <- felm(log(tso2) ~ 
            credit_constraint * period * tso2_mandate_c +
            std_inventory_to_sales_i  * period * tso2_mandate_c +
            log(output) + log(employment) + log(capital)
            | fe_c_i + fe_t_i + fe_c_t |0 | ind2, df_final,
            exactDOF = TRUE)

t_3 <- felm(log(tso2) ~ 
            credit_constraint * period * tso2_mandate_c +
            std_receivable_curasset_i  * period * tso2_mandate_c +
            log(output) + log(employment) + log(capital)
            |fe_c_i + fe_t_i + fe_c_t |0 | ind2, df_final,
            exactDOF = TRUE)

t_4 <- felm(log(tso2) ~ 
            credit_constraint * period * tso2_mandate_c +
            std_cash_over_curasset_i  * period * tso2_mandate_c +
            log(output) + log(employment) + log(capital)
            | fe_c_i + fe_t_i + fe_c_t |0 | ind2, df_final,
            exactDOF = TRUE)

t_5 <- felm(log(tso2) ~ 
            credit_constraint * period * tso2_mandate_c +
            std_current_ratio_i   * period * tso2_mandate_c +
            log(output) + log(employment) + log(capital)
            | fe_c_i + fe_t_i + fe_c_t |0 | ind2, df_final,
            exactDOF = TRUE)

t_6 <- felm(log(tso2) ~ 
            credit_constraint * period * tso2_mandate_c +
            std_quick_ratio_i   * period * tso2_mandate_c +
            log(output) + log(employment) + log(capital)
            | fe_c_i + fe_t_i + fe_c_t |0 | ind2, df_final,
            exactDOF = TRUE)

t_7 <- felm(log(tso2) ~ 
            credit_constraint * period * tso2_mandate_c +
            std_cash_ratio_i  * period * tso2_mandate_c +
            log(output) + log(employment) + log(capital)
            | fe_c_i + fe_t_i + fe_c_t |0 | ind2, df_final,
            exactDOF = TRUE)

t_8 <- felm(log(tso2) ~ 
            credit_constraint * period * tso2_mandate_c +
            std_working_capital_i  * period * tso2_mandate_c +
            log(output) + log(employment) + log(capital)
            | fe_c_i + fe_t_i + fe_c_t |0 | ind2, df_final,
            exactDOF = TRUE)

t_9 <- felm(log(tso2) ~ 
             credit_constraint * period * tso2_mandate_c +
             std_working_capital_requirement_i  * period * tso2_mandate_c +
            log(output) + log(employment) + log(capital)
            | fe_c_i + fe_t_i + fe_c_t |0 | ind2, df_final,
            exactDOF = TRUE)

t_10 <- felm(log(tso2) ~ 
            credit_constraint * period * tso2_mandate_c +
            std_return_on_asset_i  * period * tso2_mandate_c +
            log(output) + log(employment) + log(capital)
            | fe_c_i + fe_t_i + fe_c_t |0 | ind2, df_final,
            exactDOF = TRUE)

t_11 <- felm(log(tso2) ~ 
             credit_constraint * period * tso2_mandate_c +
             std_sales_assets_i * period * tso2_mandate_c +
            log(output) + log(employment) + log(capital)
            | fe_c_i + fe_t_i + fe_c_t |0 | ind2, df_final,
            exactDOF = TRUE)

t_12 <- felm(log(tso2) ~ 
             credit_constraint * period * tso2_mandate_c +
             std_liabilities_assets_i * period * tso2_mandate_c +
            log(output) + log(employment) + log(capital)
            | fe_c_i + fe_t_i + fe_c_t |0 | ind2, df_final,
            exactDOF = TRUE)

t_13 <- felm(log(tso2) ~ 
             credit_constraint * period * tso2_mandate_c +
             std_asset_tangibility_i  * period * tso2_mandate_c +
            log(output) + log(employment) + log(capital)
            | fe_c_i + fe_t_i + fe_c_t |0 | ind2, df_final,
            exactDOF = TRUE)

t_14 <- felm(log(tso2) ~ 
             credit_constraint * period * tso2_mandate_c +
             std_account_paybable_to_asset_i * period * tso2_mandate_c +
            log(output) + log(employment) + log(capital)
            |fe_c_i + fe_t_i + fe_c_t |0 | ind2, df_final,
            exactDOF = TRUE)


dep <- "Dependent variable: SO2 emission"
fe1 <- list(
    c("City-industry", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes"),
    c("Time-industry", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes"),
    c("City-Time", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes")
             )

table_1 <- go_latex(list(
    t_0,t_1, t_2, t_3, t_4, t_5, t_6, t_7, t_8, t_9, t_10, t_11, t_12, t_13, t_14
),
    title="SO2 emission reduction, industry financial ratio and policy mandate (standardized values)",
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
"clustered at the industry level appear inp arentheses. "\
"\sym{*} Significance at the 10\%, \sym{**} Significance at the 5\%, \sym{***} Significance at the 1\%."

multicolumn ={
    'Tight': 1,
    'Loose': 5
}

#multi_lines_dep = '(city/product/trade regime/year)'
#new_r = ['& test1', 'test2']
lb.beautify(table_number = table_nb,
            #reorder_var = reorder,
            #multi_lines_dep = multi_lines_dep,
            #new_row= new_r,
            #multicolumn = multicolumn,
            table_nte = tbe1,
            jupyter_preview = True,
            resolution = 280,
           folder = folder)
```

<!-- #region nteract={"transient": {"deleting": false}} kernel="SoS" -->
# Generate reports
<!-- #endregion -->

```sos kernel="SoS"
table_nb = 9
table = 'table_{}'.format(table_nb)
path = os.path.join(folder, table + '.txt')
if os.path.exists(folder) == False:
        os.mkdir(folder)
for ext in ['.txt', '.tex', '.pdf']:
    x = [a for a in os.listdir(folder) if a.endswith(ext)]
    [os.remove(os.path.join(folder, i)) for i in x]
path
```

```sos nteract={"transient": {"deleting": false}} outputExpanded=false kernel="Python 3"
import os, time, shutil, urllib, ipykernel, json
from pathlib import Path
from notebook import notebookapp
```

```sos nteract={"transient": {"deleting": false}} outputExpanded=false kernel="Python 3"
def create_report(extension = "html", keep_code = False):
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
            pass  
    
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

```sos nteract={"transient": {"deleting": false}} outputExpanded=false kernel="Python 3"
create_report(extension = "html", keep_code = False)
```
