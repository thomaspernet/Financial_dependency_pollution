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
# Estimate so2 as a function of  financial ratio (sector by sector)

# US Name

Estimate so2 as a function of  financial ratio (sector by sector) 

# Business needs 

Estimate so2 as a function of  financial ratio (Evalutate model for each sector separately) 

## Description
### Objective 

Test the coefficient sign and significant of the main variable

### Tables

1. Table 1: Baseline Estimate, so2 and financial ratio

**Cautious**
* Make sure no empty rows, otherwise it will be filtered out in the estimate


# Metadata
* Key: pkv56foxv40011s
* Parent key (for update parent):  
* Epic: Models
* US: Sector by sector
* Task tag: #analytics,#baseline-table,#sector-estimate
* Notebook US Parent (i.e the one to update): 
https://github.com/thomaspernet/Financial_dependency_pollution/blob/master/02_data_analysis/01_model_estimation/00_estimate_fin_ratio/02_so2_fin_ratio_city_sector_by_sector.md
* Reports: https://htmlpreview.github.io/?https://github.com/thomaspernet/Financial_dependency_pollution/blob/master/02_data_analysis/01_model_estimation/00_estimate_fin_ratio/Reports/02_so2_fin_ratio_city_sector_by_sector.html
* Analytics reports:
 

# Input Cloud Storage [AWS/GCP]

## Table/file
* Name: 
* asif_industry_financial_ratio_city
* Github: 
  * https://github.com/thomaspernet/Financial_dependency_pollution/blob/master/01_data_preprocessing/02_transform_tables/03_asif_financial_ratio_city.md

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
        'new':'\\text{polluted sector, decile}_{ci}'
        },
        {
        'old':'polluted\_diABOVE',
        'new':'\\text{polluted sector, decile}_{ci}'
        },
        {
        'old':'polluted_mi',
        'new':'\\text{polluted sector, mean}_{ci}'
        },
        {
        'old':'polluted\_miABOVE',
        'new':'\\text{polluted sector, mean}_{ci}'
        },
        {
        'old':'polluted_mei',
        'new':'\\text{polluted sector, median}_{ci}'
        },
        {
        'old':'polluted\_meiABOVE',
        'new':'\\text{polluted sector, median}_{ci}'
        },

        ### financial ratio
        #### Industry
        {
        'old':'credit\_constraint',
        'new':'\\text{credit constraint}_i'
        },
        {
        'old':'std\_receivable\_curasset\_ci',
        'new':'\\text{std receivable asset ratio}_{ci}'
        },
        {
        'old':'receivable\_curasset\_ci',
        'new':'\\text{receivable asset ratio}_{ci}'
        },
        {
        'old':'std\_cash\_over\_curasset\_ci',
        'new':'\\text{std cash over asset}_{ci}'
        },
        {
        'old':'cash\_over\_curasset\_ci',
        'new':'\\text{cash over asset}_{ci}'
        },
        {
        'old':'std\_working\_capital\_ci',
        'new':'\\text{std working capital}_{ci}'
        },
        {
        'old':'working\_capital\_ci',
        'new':'\\text{working capital}_{ci}'
        },
        {
        'old':'std\_working\_capital\_requirement\_ci',
        'new':'\\text{std working capital requirement}_{ci}'
        },
        {
        'old':'working\_capital\_requirement\_ci',
        'new':'\\text{working capital requirement}_{ci}'
        },
        {
        'old':'std\_current\_ratio\_ci',
        'new':'\\text{std current ratio}_{ci}'
        },
        {
        'old':'current\_ratio\_ci',
        'new':'\\text{current ratio}_{ci}'
        },
        {
        'old':'std\_quick\_ratio\_ci',
        'new':'\\text{std quick ratio}_{ci}'
        },
        {
        'old':'quick\_ratio\_ci',
        'new':'\\text{quick ratio}_{ci}'
        },
        {
        'old':'std\_cash\_ratio\_ci',
        'new':'\\text{std cash ratio}_{ci}'
        },
        {
        'old':'cash\_ratio\_ci',
        'new':'\\text{cash ratio}_{ci}'
        },
        {
        'old':'std\_liabilities\_assets\_ci',
        'new':'\\text{std liabilities assets}_{ci}'
        },{
        'old':'liabilities\_assets\_ci',
        'new':'\\text{liabilities assets}_{ci}'
        },
        {
        'old':'std\_return\_on\_asset\_ci',
        'new':'\\text{std return on asset}_{ci}'
        },{
        'old':'return\_on\_asset\_ci',
        'new':'\\text{return on asset}_{ci}'
        },
        {
        'old':'std\_sales\_assets\_ci',
        'new':'\\text{std sales assets}_{ci}'
        },
        {
        'old':'sales\_assets\_ci',
        'new':'\\text{sales assets}_{ci}'
        },
        {
        'old':'std\_rd\_intensity\_ci',
        'new':'\\text{std rd intensity}_{ci}'
        },
        {
        'old':'rd\_intensity\_ci',
        'new':'\\text{rd intensity}_{ci}'
        },
        {
        'old':'std\_inventory\_to\_sales\_ci',
        'new':'\\text{std inventory to sales}_{ci}'
        },
        {
        'old':'inventory\_to\_sales\_ci',
        'new':'\\text{inventory to sales}_{ci}'
        },
        {
        'old':'std\_asset\_tangibility\_ci',
        'new':'\\text{std asset tangibility}_{ci}'
        },
        {
        'old':'asset\_tangibility\_ci',
        'new':'\\text{asset tangibility}_{ci}'
        },
        {
        'old':'std\_account\_paybable\_to\_asset\_ci',
        'new':'\\text{std account paybable to asset}_{ci}'
        },
        {
        'old':'account\_paybable\_to\_asset\_ci',
        'new':'\\text{account paybable to asset}_{ci}'
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

<!-- #region kernel="R" -->
# Expected signs

| index | Metrics                        | comments                                                                                                    | variables                                                                                                                                                           | Roam_link                                       | Exepected sign              | Comment                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
|-------|--------------------------------|-------------------------------------------------------------------------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------|-------------------------------------------------|-----------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| 1     | External finance dependence    | From #[[Fan et al. 2015 - Credit constraints, quality, and export prices - Theory and evidence from China]] |                                                                                                                                                                     | #external-finance-dependence                    | Negative                    | An industry’s external finance dependence (ExtFini) is defined as the share of capital expenditure not financed with cash flows from operations. If external finance dependence is high, the industry is more financially vulnerable and have higher credit needs                                                                                                                                                                                                                                                                              |
| 2     | R&D intensity                  | RD / Sales                                                                                                  | rdfee/sales                                                                                                                                                         | #rd-intensity                                   | Negative                    | Share of RD expenditure over sales. larger values indicates larger use of sales to spend on RD. Say differently, lower borrowing done toward RD                                                                                                                                                                                                                                                                                                                                                                                                |
| 3     | Inventory to sales             | Inventory / sales                                                                                           | 存货 (c81) / sales                                                                                                                                                  | #inventory-to-sales                             | Negative                    | Share of inventory over sales. Larger values indicates share of unsold or not consumed items. large values is a demonstration of tighter credit constraint                                                                                                                                                                                                                                                                                                                                                                                     |
| 4     | % receivable                   | receivable account / current asset                                                                          | 应收帐款 (c80) / cuasset                                                                                                                                            | #account-receivable #current-asset              | Negative                    | Share of receivable over current asset. Larger value indicates longer time before collecting the money from the customers                                                                                                                                                                                                                                                                                                                                                                                                                      |
| 5     | Liabilities over asset         | (Short-Tern Debt + Long-Term Debt)/total asset                                                              | 1- (流动负债合计 (c95) + 长期负债合计 (c97)) / toasset                                                                                                              | #total-debt-to-total-assets                     | Negative                    | Share of liabilities over total asset. Larger value indicates assets that are financed by external creditors                                                                                                                                                                                                                                                                                                                                                                                                                                   |
| 6     | working capital requirement    | Inventory + Accounts receivable - Accounts payable                                                          | 存货 (c81) + 应收帐款 (c80) - 应付帐款  (c96)                                                                                                                       | #working-capital                                | Negative                    | Working Capital Requirement is the amount of money needed to finance the gap between disbursements (payments to suppliers) and receipts (payments from customers). Larger values indicate the amount of money needed to meet the debt.                                                                                                                                                                                                                                                                                                         |
| 7     | % cash                         | Current asset - cash / current asset                                                                        | (cuasset- 其中：短期投资 (c79) - 应收帐款 (c80) - 存货 (c81) - 其中：产成品 (c82)) /current asset                                                                   | #current-asset #cash                            | Positive                    | Share of cash asset over current asset. Larger values indicate more cash in hand.                                                                                                                                                                                                                                                                                                                                                                                                                                                              |
| 8     | cash ratio                     | (Cash + marketable securities)/current liabilities                                                          | 1-(cuasset - 其中：短期投资 (c79) - 应收帐款 (c80) - 存货 (c81) - 其中：产成品 (c82))/ 流动负债合计 (c95)                                                           | #cash-asset #cash-ratio                         | Positive                    | Cash divided by liabilities. A portion of short-term debt that can be financed by cash. A larger value indicates the company generates enough cash to cope with the short term debt                                                                                                                                                                                                                                                                                                                                                            |
| 9     | Working capital                | Current asset - current liabilities                                                                         | cuasset- 流动负债合计 (c95)                                                                                                                                         | #working-capital-requirement                    | Positive                    | Difference between current asset and current liabilities. Larger value indicates that assets are enough to cope with the short term need                                                                                                                                                                                                                                                                                                                                                                                                       |
| 10    | current ratio                  | Current asset /current liabilities                                                                          | cuasset/流动负债合计 (c95)                                                                                                                                          | #current-ratio                                  | Ambiguous                   | Asset divided by liabilities. Values above 1 indicate there are more assets than liabilities. There are two effects on the liquidity constraint. Larger values imply the company has more liquidity, hence they may be less dependent on the formal financial market. By analogy, the financial market prefers to invest or provide money to the more liquid company (reduce the risk default)                                                                                                                                                 |
| 11    | Quick ratio                    | (Current asset - Inventory)/current liabilities                                                             | (cuasset -  其中：短期投资 (c79) - 应收帐款 (c80) - 存货 (c81)) / 流动负债合计 (c95)                                                                                | #quick-ratio                                    | Ambiguous                   | The quick ratio is a measure of liquidity. The higher the more liquid the company is. To improve the ratio, the company should reduce the account receivable (reduce payment time) and increase the account payable (negotiate payment term). There are two effects on the liquidity constraint. Larger values imply the company has more liquidity, hence they may be less dependent on the formal financial market. By analogy, the financial market prefers to invest or provide money to the more liquid company (reduce the risk default) |
| 12    | Return on Asset                | Net income / Total assets                                                                                   | sales - (主营业务成本 (c108) + 营业费用 (c113) + 管理费用 (c114) + 财产保险费 (c116) + 劳动、失业保险费 (c118)+ 财务费用 (c124) + 本年应付工资总额 (wage)) /toasset | #return-on-asset                                | Ambiguous                   | Net income over total asset. Capacity of an asset to generate income. Larger value indicates that asset are used in an efficiente way to generate income                                                                                                                                                                                                                                                                                                                                                                                       |
| 13    | Asset Turnover Ratio           | Total sales / ((delta total asset)/2)                                                                       | 全年营业收入合计 (c64) /($\Delta$ toasset/2)                                                                                                                        | #asset-turnover-ratio                           | Ambiguous                   | Sales divided by the average changes in total asset. Larger value indicates better efficiency at using asset to generate revenue                                                                                                                                                                                                                                                                                                                                                                                                               |
| 14    | Asset tangibility              | Total fixed assets - Intangible assets                                                                      | tofixed - 无形资产 (c92)                                                                                                                                            | #asset-tangibility                              | Ambiguous                   | Difference between fixed sset and intangible asset. Larger value indicates more collateral, hence higher borrowing capacity                                                                                                                                                                                                                                                                                                                                                                                                                    |
| 15    | Account payable to total asset | (delta account payable)/ (delta total asset)                                                                | ($\Delta$ 应付帐款  (c96))/ ($\Delta$$ (toasset))                                                                                                                   | #change-account-paybable-to-change-total-assets | Ambiguous (favour positive) | Variation of account payable over variation total asset. If the nominator larger than the denominator, it means the account payable grew larger than an asset, or more time is given to pay back the supplier relative to the total asset.  Say differently,  companies can more easily access buyer or supplier trade credit, they may be less dependent on the formal financial market                                                                                                                                                       |
<!-- #endregion -->

<!-- #region kernel="SoS" -->
## Tables

Estimate the following equations for each industry

**Table 1: Determinant of SO2 emission: so2 and financial ratio**

$$ \begin{aligned} \text{SO2}{cit} &= \alpha \text{Financial ratio}_{ci} + \text{X}_{cit} + \gamma{t} + \epsilon_{cit} \end{aligned} $$

**Table 2: Variation of SO2 emission: so2 and and financial ratio**

$$ \begin{aligned} \text{SO2}{cit} &= \alpha \text{Financial ratio}_{ci} \times \text{Period} + \text{X}_{cit} + \gamma{t} + \gamma{c} + \epsilon_{cit} \end{aligned} $$

**Table 3: Baseline Estimate, so2 and financial ratio**

$$ \begin{aligned} \text{SO2}{cit} &= \alpha \text{Financial ratio}_{ci} \times \text{Period} \times \text{policy mandate}_c  + \text{X}_{cit} + \gamma{t} + \gamma{c} + \epsilon_{cit} \end{aligned} $$

<!-- #endregion -->

```sos kernel="R"
industries <- df_final %>% 
group_by(short) %>% 
summarise(credit_constraint = unique(credit_constraint)) %>%
arrange(desc(credit_constraint))
industries
```

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
df_final <- df_final %>% mutate(q_tso2_mandate_c = ntile(tso2_mandate_c, 3))
df_final %>% 
mutate(q_tso2_mandate_c = ntile(tso2_mandate_c, 3)) %>% 
select(tso2_mandate_c, q_tso2_mandate_c, ) %>%
distinct() %>% 
arrange(q_tso2_mandate_c, tso2_mandate_c)
```

<!-- #region kernel="R" -->
Test filter policy mandate
<!-- #endregion -->

```sos kernel="R"
#summary(felm(log(tso2) ~ 
            #credit_constraint * period * tso2_mandate_c +
#            std_rd_intensity_ci * period +
#            log(output) + log(employment) + log(capital)
#            |fe_c_t + fe_t_i + fe_c_i|0 | geocode4_corr, df_final %>% filter(q_tso2_mandate_c == 3),
#            exactDOF = TRUE))
```

```sos kernel="R"
%get folder
t <- 1
for (i in 1:3){
    
    #sector_name <- industries[i, 'short']$short
    
    title_1 = paste0("Determinant of SO2 emission, so2 and financial ratio quantile",i)
    path_1 = paste0(folder,"/table_",t ,".txt")
    t_1 <- felm(log(tso2) ~ 
            #credit_constraint * period * tso2_mandate_c +
            std_rd_intensity_ci  +
            log(output) + log(employment) + log(capital)
            |fe_c_t + fe_t_i|0 | geocode4_corr, df_final %>% filter(q_tso2_mandate_c == i),
            exactDOF = TRUE)

    t_2 <- felm(log(tso2) ~ 
                #credit_constraint * period * tso2_mandate_c +
                std_inventory_to_sales_ci   +
                log(output) + log(employment) + log(capital)
                | fe_c_t + fe_t_i|0 | geocode4_corr, df_final %>% filter(q_tso2_mandate_c == i),
                exactDOF = TRUE)
    
    t_3 <- felm(log(tso2) ~ 
                #credit_constraint * period * tso2_mandate_c +
                std_receivable_curasset_ci  +
                log(output) + log(employment) + log(capital)
                |fe_c_t + fe_t_i|0 | geocode4_corr, df_final %>% filter(q_tso2_mandate_c == i),
                exactDOF = TRUE)
    
    t_4 <- felm(log(tso2) ~ 
                 #credit_constraint * period * tso2_mandate_c +
                 std_liabilities_assets_ci  +
                log(output) + log(employment) + log(capital)
                | fe_c_t + fe_t_i|0 | geocode4_corr, df_final %>% filter(q_tso2_mandate_c == i),
                exactDOF = TRUE)
    
    t_5 <- felm(log(tso2) ~ 
                 #credit_constraint * period * tso2_mandate_c +
                 std_working_capital_requirement_ci  +
                log(output) + log(employment) + log(capital)
                | fe_c_t + fe_t_i|0 | geocode4_corr, df_final %>% filter(q_tso2_mandate_c == i),
                exactDOF = TRUE)

    t_6 <- felm(log(tso2) ~ 
                #credit_constraint * period * tso2_mandate_c +
                std_cash_over_curasset_ci +
                log(output) + log(employment) + log(capital)
                | fe_c_t + fe_t_i |0 | geocode4_corr, df_final %>% filter(q_tso2_mandate_c == i),
                exactDOF = TRUE)
    
    t_7 <- felm(log(tso2) ~ 
                #credit_constraint * period * tso2_mandate_c +
                std_cash_ratio_ci   +
                log(output) + log(employment) + log(capital)
                | fe_c_t + fe_t_i|0 | geocode4_corr, df_final %>% filter(q_tso2_mandate_c == i),
                exactDOF = TRUE)
    
    t_8 <- felm(log(tso2) ~ 
                #credit_constraint * period * tso2_mandate_c +
                std_working_capital_ci   +
                log(output) + log(employment) + log(capital)
                | fe_c_t + fe_t_i |0 | geocode4_corr, df_final %>% filter(q_tso2_mandate_c == i),
                exactDOF = TRUE)

    t_9 <- felm(log(tso2) ~ 
                #credit_constraint * period * tso2_mandate_c +
                std_current_ratio_ci  +
                log(output) + log(employment) + log(capital)
                | fe_c_t + fe_t_i|0 | geocode4_corr, df_final %>% filter(q_tso2_mandate_c == i),
                exactDOF = TRUE)

    t_10 <- felm(log(tso2) ~ 
                #credit_constraint * period * tso2_mandate_c +
                std_quick_ratio_ci  +
                log(output) + log(employment) + log(capital)
                |fe_c_t + fe_t_i|0 | geocode4_corr, df_final %>% filter(q_tso2_mandate_c == i),
                exactDOF = TRUE)

    
    t_11 <- felm(log(tso2) ~ 
                #credit_constraint * period * tso2_mandate_c +
                std_return_on_asset_ci   +
                log(output) + log(employment) + log(capital)
                | fe_c_t + fe_t_i |0 | geocode4_corr, df_final %>% filter(q_tso2_mandate_c == i),
                exactDOF = TRUE)

    t_12 <- felm(log(tso2) ~ 
                 #credit_constraint * period * tso2_mandate_c +
                 std_sales_assets_ci  +
                log(output) + log(employment) + log(capital)
                | fe_c_t + fe_t_i |0 | geocode4_corr, df_final %>% filter(q_tso2_mandate_c == i),
                exactDOF = TRUE)

    t_13 <- felm(log(tso2) ~ 
                 #credit_constraint * period * tso2_mandate_c +
                 std_asset_tangibility_ci +
                log(output) + log(employment) + log(capital)
                | fe_c_t + fe_t_i|0 | geocode4_corr, df_final %>% filter(q_tso2_mandate_c == i),
                exactDOF = TRUE)

    t_14 <- felm(log(tso2) ~ 
                 #credit_constraint * period * tso2_mandate_c +
                 std_account_paybable_to_asset_ci  +
                log(output) + log(employment) + log(capital)
                |fe_c_t + fe_t_i|0 | geocode4_corr, df_final %>% filter(q_tso2_mandate_c == i),
                exactDOF = TRUE)

    dep <- "Dependent variable: SO2 emission"
    fe1 <- list(
        c("city-Time","Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes"),
        c("Time-industry", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes")
        #c("City", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes")
                 )

    table_1 <- go_latex(list(
        t_1, t_2, t_3, t_4, t_5, t_6, t_7, t_8, t_9, t_10, t_11, t_12, t_13, t_14
    ),
        title=title_1,
        dep_var = dep,
        addFE=fe1,
        save=TRUE,
        note = FALSE,
        name=path_1
    ) 
    
    #### Table 2
    
    title_2 = paste0("Variation of SO2 emission, so2 and and financial ratio  quantile",i)
    path_2 = paste0(folder,"/table_",t + 1 ,".txt")
    
    t_1 <- felm(log(tso2) ~ 
            #credit_constraint * period * tso2_mandate_c +
            std_rd_intensity_ci * period +
            log(output) + log(employment) + log(capital)
            |fe_c_t + fe_t_i + fe_c_i |0 | geocode4_corr, df_final %>% filter(q_tso2_mandate_c == i),
            exactDOF = TRUE)

    t_2 <- felm(log(tso2) ~ 
                #credit_constraint * period * tso2_mandate_c +
                std_inventory_to_sales_ci * period  +
                log(output) + log(employment) + log(capital)
                | fe_c_t + fe_t_i + fe_c_i |0 | geocode4_corr, df_final %>% filter(q_tso2_mandate_c == i),
                exactDOF = TRUE)
    
    t_3 <- felm(log(tso2) ~ 
                #credit_constraint * period * tso2_mandate_c +
                std_receivable_curasset_ci * period +
                log(output) + log(employment) + log(capital)
                |fe_c_t + fe_t_i + fe_c_i |0 | geocode4_corr, df_final %>% filter(q_tso2_mandate_c == i),
                exactDOF = TRUE)
    
    t_4 <- felm(log(tso2) ~ 
                 #credit_constraint * period * tso2_mandate_c +
                 std_liabilities_assets_ci * period +
                log(output) + log(employment) + log(capital)
                | fe_c_t + fe_t_i + fe_c_i |0 | geocode4_corr, df_final %>% filter(q_tso2_mandate_c == i),
                exactDOF = TRUE)
    
    t_5 <- felm(log(tso2) ~ 
                 #credit_constraint * period * tso2_mandate_c +
                 std_working_capital_requirement_ci * period +
                log(output) + log(employment) + log(capital)
                | fe_c_t + fe_t_i + fe_c_i |0 | geocode4_corr, df_final %>% filter(q_tso2_mandate_c == i),
                exactDOF = TRUE)

    t_6 <- felm(log(tso2) ~ 
                #credit_constraint * period * tso2_mandate_c +
                std_cash_over_curasset_ci * period +
                log(output) + log(employment) + log(capital)
                | fe_c_t + fe_t_i + fe_c_i |0 | geocode4_corr, df_final %>% filter(q_tso2_mandate_c == i),
                exactDOF = TRUE)
    
    t_7 <- felm(log(tso2) ~ 
                #credit_constraint * period * tso2_mandate_c +
                std_cash_ratio_ci * period +
                log(output) + log(employment) + log(capital)
                | fe_c_t + fe_t_i + fe_c_i |0 | geocode4_corr, df_final %>% filter(q_tso2_mandate_c == i),
                exactDOF = TRUE)
    
    t_8 <- felm(log(tso2) ~ 
                #credit_constraint * period * tso2_mandate_c +
                std_working_capital_ci * period +
                log(output) + log(employment) + log(capital)
                | fe_c_t + fe_t_i + fe_c_i |0 | geocode4_corr, df_final %>% filter(q_tso2_mandate_c == i),
                exactDOF = TRUE)

    t_9 <- felm(log(tso2) ~ 
                #credit_constraint * period * tso2_mandate_c +
                std_current_ratio_ci * period +
                log(output) + log(employment) + log(capital)
                | fe_c_t + fe_t_i + fe_c_i |0 | geocode4_corr, df_final %>% filter(q_tso2_mandate_c == i),
                exactDOF = TRUE)

    t_10 <- felm(log(tso2) ~ 
                #credit_constraint * period * tso2_mandate_c +
                std_quick_ratio_ci * period +
                log(output) + log(employment) + log(capital)
                |fe_c_t + fe_t_i + fe_c_i|0 | geocode4_corr, df_final %>% filter(q_tso2_mandate_c == i),
                exactDOF = TRUE)

    
    t_11 <- felm(log(tso2) ~ 
                #credit_constraint * period * tso2_mandate_c +
                std_return_on_asset_ci * period +
                log(output) + log(employment) + log(capital)
                | fe_c_t + fe_t_i + fe_c_i |0 | geocode4_corr, df_final %>% filter(q_tso2_mandate_c == i),
                exactDOF = TRUE)

    t_12 <- felm(log(tso2) ~ 
                 #credit_constraint * period * tso2_mandate_c +
                 std_sales_assets_ci* period +
                log(output) + log(employment) + log(capital)
                | fe_c_t + fe_t_i + fe_c_i |0 | geocode4_corr, df_final %>% filter(q_tso2_mandate_c == i),
                exactDOF = TRUE)

    t_13 <- felm(log(tso2) ~ 
                 #credit_constraint * period * tso2_mandate_c +
                 std_asset_tangibility_ci * period+
                log(output) + log(employment) + log(capital)
                | fe_c_t + fe_t_i + fe_c_i|0 | geocode4_corr, df_final %>% filter(q_tso2_mandate_c == i),
                exactDOF = TRUE)

    t_14 <- felm(log(tso2) ~ 
                 #credit_constraint * period * tso2_mandate_c +
                 std_account_paybable_to_asset_ci * period +
                log(output) + log(employment) + log(capital)
                |fe_c_t + fe_t_i + fe_c_i |0 | geocode4_corr, df_final %>% filter(q_tso2_mandate_c == i),
                exactDOF = TRUE)

    dep <- "Dependent variable: SO2 emission"
    fe1 <- list(
        c("City-Time","Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes"),
        c("Indutry-time", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes"),
        c("City-Indutry", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes")
                 )
    
    table_1 <- go_latex(list(
        t_1, t_2, t_3, t_4, t_5, t_6, t_7, t_8, t_9, t_10, t_11, t_12, t_13, t_14
    ),
        title=title_2,
        dep_var = dep,
        addFE=fe1,
        save=TRUE,
        note = FALSE,
        name=path_2
    )


    t <- t+2
       
}
```

```sos kernel="SoS"
tbe1  = "This table estimates eq(3). " \
"Heteroskedasticity-robust standard errors" \
"clustered at the product level appear inparentheses."\
"\sym{*} Significance at the 10\%, \sym{**} Significance at the 5\%, \sym{***} Significance at the 1\%."

multicolumn ={
    'Negative': 1,
    'Positive': 9,
    #'Ambiguous': 10,
}

reorder = {
    # Old, New
    17:3, ## Working capital
    18:5,
    19:7,
    20:9,
    21:11,
    22:13,
    23:15,
    24:17,
    25:19,
    26:21,
    27:23,
    28:25,
    29:27,
    30:29
}

#multi_lines_dep = '(city/product/trade regime/year)'
#new_r = ['& test1', 'test2']
for i in range(1, 7):
    print('\n\nRank {} in term of credit constraint\n\n'.format(i))
    lb.beautify(table_number = i,
                    #reorder_var = reorder,
                    #multi_lines_dep = multi_lines_dep,
                    #new_row= new_r,
                    #multicolumn = multicolumn,
                    table_nte = tbe1,
                    jupyter_preview = True,
                    resolution = 280,
                    folder = folder)
```

<!-- #region kernel="R" -->
Sector by sector
<!-- #endregion -->

```sos kernel="R"
%get folder
t <- 1
for (i in 1:nrow(industries)){
    
    sector_name <- industries[i, 'short']$short
    
    title_1 = paste0("Determinant of SO2 emission, so2 and financial ratio ",sector_name)
    path_1 = paste0(folder,"/table_",t ,".txt")
    t_1 <- felm(log(tso2) ~ 
            #credit_constraint * period * tso2_mandate_c +
            std_rd_intensity_ci  +
            log(output) + log(employment) + log(capital)
            |year |0 | geocode4_corr, df_final %>% filter(short == sector_name),
            exactDOF = TRUE)

    t_2 <- felm(log(tso2) ~ 
                #credit_constraint * period * tso2_mandate_c +
                std_inventory_to_sales_ci   +
                log(output) + log(employment) + log(capital)
                | year |0 | geocode4_corr, df_final %>% filter(short == sector_name),
                exactDOF = TRUE)
    
    t_3 <- felm(log(tso2) ~ 
                #credit_constraint * period * tso2_mandate_c +
                std_receivable_curasset_ci  +
                log(output) + log(employment) + log(capital)
                |year |0 | geocode4_corr, df_final %>% filter(short == sector_name),
                exactDOF = TRUE)
    
    t_4 <- felm(log(tso2) ~ 
                 #credit_constraint * period * tso2_mandate_c +
                 std_liabilities_assets_ci  +
                log(output) + log(employment) + log(capital)
                | year |0 | geocode4_corr, df_final %>% filter(short == sector_name),
                exactDOF = TRUE)
    
    t_5 <- felm(log(tso2) ~ 
                 #credit_constraint * period * tso2_mandate_c +
                 std_working_capital_requirement_ci  +
                log(output) + log(employment) + log(capital)
                | year |0 | geocode4_corr, df_final %>% filter(short == sector_name),
                exactDOF = TRUE)

    t_6 <- felm(log(tso2) ~ 
                #credit_constraint * period * tso2_mandate_c +
                std_cash_over_curasset_ci +
                log(output) + log(employment) + log(capital)
                | year |0 | geocode4_corr, df_final %>% filter(short == sector_name),
                exactDOF = TRUE)
    
    t_7 <- felm(log(tso2) ~ 
                #credit_constraint * period * tso2_mandate_c +
                std_cash_ratio_ci   +
                log(output) + log(employment) + log(capital)
                | year |0 | geocode4_corr, df_final %>% filter(short == sector_name),
                exactDOF = TRUE)
    
    t_8 <- felm(log(tso2) ~ 
                #credit_constraint * period * tso2_mandate_c +
                std_working_capital_ci   +
                log(output) + log(employment) + log(capital)
                | year |0 | geocode4_corr, df_final %>% filter(short == sector_name),
                exactDOF = TRUE)

    t_9 <- felm(log(tso2) ~ 
                #credit_constraint * period * tso2_mandate_c +
                std_current_ratio_ci  +
                log(output) + log(employment) + log(capital)
                | year |0 | geocode4_corr, df_final %>% filter(short == sector_name),
                exactDOF = TRUE)

    t_10 <- felm(log(tso2) ~ 
                #credit_constraint * period * tso2_mandate_c +
                std_quick_ratio_ci  +
                log(output) + log(employment) + log(capital)
                |year |0 | geocode4_corr, df_final %>% filter(short == sector_name),
                exactDOF = TRUE)

    
    t_11 <- felm(log(tso2) ~ 
                #credit_constraint * period * tso2_mandate_c +
                std_return_on_asset_ci   +
                log(output) + log(employment) + log(capital)
                | year |0 | geocode4_corr, df_final %>% filter(short ==  sector_name),
                exactDOF = TRUE)

    t_12 <- felm(log(tso2) ~ 
                 #credit_constraint * period * tso2_mandate_c +
                 std_sales_assets_ci  +
                log(output) + log(employment) + log(capital)
                | year |0 | geocode4_corr, df_final %>% filter(short == sector_name),
                exactDOF = TRUE)

    t_13 <- felm(log(tso2) ~ 
                 #credit_constraint * period * tso2_mandate_c +
                 std_asset_tangibility_ci +
                log(output) + log(employment) + log(capital)
                | year |0 | geocode4_corr, df_final %>% filter(short == sector_name),
                exactDOF = TRUE)

    t_14 <- felm(log(tso2) ~ 
                 #credit_constraint * period * tso2_mandate_c +
                 std_account_paybable_to_asset_ci  +
                log(output) + log(employment) + log(capital)
                |year |0 | geocode4_corr, df_final %>% filter(short == sector_name),
                exactDOF = TRUE)

    dep <- "Dependent variable: SO2 emission"
    fe1 <- list(
        c("Time","Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes")
        #c("Time-industry", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes"),
        #c("City-Time", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes")
                 )

    table_1 <- go_latex(list(
        t_1, t_2, t_3, t_4, t_5, t_6, t_7, t_8, t_9, t_10, t_11, t_12, t_13, t_14
    ),
        title=title_1,
        dep_var = dep,
        addFE=fe1,
        save=TRUE,
        note = FALSE,
        name=path_1
    ) 
    
    #### Table 2
    
    title_2 = paste0("Variation of SO2 emission, so2 and and financial ratio  ",sector_name)
    path_2 = paste0(folder,"/table_",t +1 ,".txt")
    
    t_1 <- felm(log(tso2) ~ 
            #credit_constraint * period * tso2_mandate_c +
            std_rd_intensity_ci * period +
            log(output) + log(employment) + log(capital)
            |geocode4_corr + year |0 | geocode4_corr, df_final %>% filter(short == sector_name),
            exactDOF = TRUE)

    t_2 <- felm(log(tso2) ~ 
                #credit_constraint * period * tso2_mandate_c +
                std_inventory_to_sales_ci * period  +
                log(output) + log(employment) + log(capital)
                | geocode4_corr + year |0 | geocode4_corr, df_final %>% filter(short == sector_name),
                exactDOF = TRUE)
    
    t_3 <- felm(log(tso2) ~ 
                #credit_constraint * period * tso2_mandate_c +
                std_receivable_curasset_ci * period +
                log(output) + log(employment) + log(capital)
                |geocode4_corr + year |0 | geocode4_corr, df_final %>% filter(short == sector_name),
                exactDOF = TRUE)
    
    t_4 <- felm(log(tso2) ~ 
                 #credit_constraint * period * tso2_mandate_c +
                 std_liabilities_assets_ci * period +
                log(output) + log(employment) + log(capital)
                | geocode4_corr + year |0 | geocode4_corr, df_final %>% filter(short == sector_name),
                exactDOF = TRUE)
    
    t_5 <- felm(log(tso2) ~ 
                 #credit_constraint * period * tso2_mandate_c +
                 std_working_capital_requirement_ci * period +
                log(output) + log(employment) + log(capital)
                | geocode4_corr + year |0 | geocode4_corr, df_final %>% filter(short == sector_name),
                exactDOF = TRUE)

    t_6 <- felm(log(tso2) ~ 
                #credit_constraint * period * tso2_mandate_c +
                std_cash_over_curasset_ci * period +
                log(output) + log(employment) + log(capital)
                | geocode4_corr + year |0 | geocode4_corr, df_final %>% filter(short == sector_name),
                exactDOF = TRUE)
    
    t_7 <- felm(log(tso2) ~ 
                #credit_constraint * period * tso2_mandate_c +
                std_cash_ratio_ci * period +
                log(output) + log(employment) + log(capital)
                | geocode4_corr + year |0 | geocode4_corr, df_final %>% filter(short == sector_name),
                exactDOF = TRUE)
    
    t_8 <- felm(log(tso2) ~ 
                #credit_constraint * period * tso2_mandate_c +
                std_working_capital_ci * period +
                log(output) + log(employment) + log(capital)
                | geocode4_corr + year |0 | geocode4_corr, df_final %>% filter(short == sector_name),
                exactDOF = TRUE)

    t_9 <- felm(log(tso2) ~ 
                #credit_constraint * period * tso2_mandate_c +
                std_current_ratio_ci * period +
                log(output) + log(employment) + log(capital)
                | geocode4_corr + year |0 | geocode4_corr, df_final %>% filter(short == sector_name),
                exactDOF = TRUE)

    t_10 <- felm(log(tso2) ~ 
                #credit_constraint * period * tso2_mandate_c +
                std_quick_ratio_ci * period +
                log(output) + log(employment) + log(capital)
                |geocode4_corr + year |0 | geocode4_corr, df_final %>% filter(short == sector_name),
                exactDOF = TRUE)

    
    t_11 <- felm(log(tso2) ~ 
                #credit_constraint * period * tso2_mandate_c +
                std_return_on_asset_ci * period +
                log(output) + log(employment) + log(capital)
                | geocode4_corr + year |0 | geocode4_corr, df_final %>% filter(short ==  sector_name),
                exactDOF = TRUE)

    t_12 <- felm(log(tso2) ~ 
                 #credit_constraint * period * tso2_mandate_c +
                 std_sales_assets_ci* period +
                log(output) + log(employment) + log(capital)
                | geocode4_corr + year |0 | geocode4_corr, df_final %>% filter(short == sector_name),
                exactDOF = TRUE)

    t_13 <- felm(log(tso2) ~ 
                 #credit_constraint * period * tso2_mandate_c +
                 std_asset_tangibility_ci * period+
                log(output) + log(employment) + log(capital)
                | geocode4_corr + year |0 | geocode4_corr, df_final %>% filter(short == sector_name),
                exactDOF = TRUE)

    t_14 <- felm(log(tso2) ~ 
                 #credit_constraint * period * tso2_mandate_c +
                 std_account_paybable_to_asset_ci * period +
                log(output) + log(employment) + log(capital)
                |geocode4_corr + year |0 | geocode4_corr, df_final %>% filter(short == sector_name),
                exactDOF = TRUE)

    dep <- "Dependent variable: SO2 emission"
    fe1 <- list(
        c("Time","Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes"),
        c("City", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes")
        #c("City-Time", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes")
                 )

    table_1 <- go_latex(list(
        t_1, t_2, t_3, t_4, t_5, t_6, t_7, t_8, t_9, t_10, t_11, t_12, t_13, t_14
    ),
        title=title_2,
        dep_var = dep,
        addFE=fe1,
        save=TRUE,
        note = FALSE,
        name=path_2
    ) 
    
    
     #### Table 3
    
    title_3 = paste0("Baseline Estimate, so2 and financial ratio ",sector_name)
    path_3 = paste0(folder,"/table_",t +2,".txt")
    
    t_1 <- felm(log(tso2) ~ 
            #credit_constraint * period * tso2_mandate_c +
            std_rd_intensity_ci * period * tso2_mandate_c +
            log(output) + log(employment) + log(capital)
            |geocode4_corr + year |0 | geocode4_corr, df_final %>% filter(short == sector_name),
            exactDOF = TRUE)

    t_2 <- felm(log(tso2) ~ 
                #credit_constraint * period * tso2_mandate_c +
                std_inventory_to_sales_ci * period * tso2_mandate_c +
                log(output) + log(employment) + log(capital)
                | geocode4_corr + year |0 | geocode4_corr, df_final %>% filter(short == sector_name),
                exactDOF = TRUE)
    
    t_3 <- felm(log(tso2) ~ 
                #credit_constraint * period * tso2_mandate_c +
                std_receivable_curasset_ci * period * tso2_mandate_c+
                log(output) + log(employment) + log(capital)
                |geocode4_corr + year |0 | geocode4_corr, df_final %>% filter(short == sector_name),
                exactDOF = TRUE)
    
    t_4 <- felm(log(tso2) ~ 
                 #credit_constraint * period * tso2_mandate_c +
                 std_liabilities_assets_ci * period * tso2_mandate_c+
                log(output) + log(employment) + log(capital)
                | geocode4_corr + year |0 | geocode4_corr, df_final %>% filter(short == sector_name),
                exactDOF = TRUE)
    
    t_5 <- felm(log(tso2) ~ 
                 #credit_constraint * period * tso2_mandate_c +
                 std_working_capital_requirement_ci * period * tso2_mandate_c+
                log(output) + log(employment) + log(capital)
                | geocode4_corr + year |0 | geocode4_corr, df_final %>% filter(short == sector_name),
                exactDOF = TRUE)

    t_6 <- felm(log(tso2) ~ 
                #credit_constraint * period * tso2_mandate_c +
                std_cash_over_curasset_ci * period * tso2_mandate_c+
                log(output) + log(employment) + log(capital)
                | geocode4_corr + year |0 | geocode4_corr, df_final %>% filter(short == sector_name),
                exactDOF = TRUE)
    
    t_7 <- felm(log(tso2) ~ 
                #credit_constraint * period * tso2_mandate_c +
                std_cash_ratio_ci * period * tso2_mandate_c  +
                log(output) + log(employment) + log(capital)
                | geocode4_corr + year |0 | geocode4_corr, df_final %>% filter(short == sector_name),
                exactDOF = TRUE)
    
    t_8 <- felm(log(tso2) ~ 
                #credit_constraint * period * tso2_mandate_c +
                std_working_capital_ci * period * tso2_mandate_c+
                log(output) + log(employment) + log(capital)
                | geocode4_corr + year |0 | geocode4_corr, df_final %>% filter(short == sector_name),
                exactDOF = TRUE)

    t_9 <- felm(log(tso2) ~ 
                #credit_constraint * period * tso2_mandate_c +
                std_current_ratio_ci * period * tso2_mandate_c+
                log(output) + log(employment) + log(capital)
                | geocode4_corr + year |0 | geocode4_corr, df_final %>% filter(short == sector_name),
                exactDOF = TRUE)

    t_10 <- felm(log(tso2) ~ 
                #credit_constraint * period * tso2_mandate_c +
                std_quick_ratio_ci * period * tso2_mandate_c+
                log(output) + log(employment) + log(capital)
                |geocode4_corr + year |0 | geocode4_corr, df_final %>% filter(short == sector_name),
                exactDOF = TRUE)

    
    t_11 <- felm(log(tso2) ~ 
                #credit_constraint * period * tso2_mandate_c +
                std_return_on_asset_ci * period * tso2_mandate_c+
                log(output) + log(employment) + log(capital)
                | geocode4_corr + year |0 | geocode4_corr, df_final %>% filter(short ==  sector_name),
                exactDOF = TRUE)

    t_12 <- felm(log(tso2) ~ 
                 #credit_constraint * period * tso2_mandate_c +
                 std_sales_assets_ci* period * tso2_mandate_c+
                log(output) + log(employment) + log(capital)
                | geocode4_corr + year |0 | geocode4_corr, df_final %>% filter(short == sector_name),
                exactDOF = TRUE)

    t_13 <- felm(log(tso2) ~ 
                 #credit_constraint * period * tso2_mandate_c +
                 std_asset_tangibility_ci * period* tso2_mandate_c+
                log(output) + log(employment) + log(capital)
                | geocode4_corr + year |0 | geocode4_corr, df_final %>% filter(short == sector_name),
                exactDOF = TRUE)

    t_14 <- felm(log(tso2) ~ 
                 #credit_constraint * period * tso2_mandate_c +
                 std_account_paybable_to_asset_ci * period * tso2_mandate_c+
                log(output) + log(employment) + log(capital)
                |geocode4_corr + year |0 | geocode4_corr, df_final %>% filter(short == sector_name),
                exactDOF = TRUE)

    dep <- "Dependent variable: SO2 emission"
    fe1 <- list(
        c("Time","Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes"),
        c("City", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes")
        #c("City-Time", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes", "Yes")
                 )

    table_1 <- go_latex(list(
        t_1, t_2, t_3, t_4, t_5, t_6, t_7, t_8, t_9, t_10, t_11, t_12, t_13, t_14
    ),
        title=title_3,
        dep_var = dep,
        addFE=fe1,
        save=TRUE,
        note = FALSE,
        name=path_3
    ) 
    
    t <- t+3
       
}
```

```sos kernel="SoS"
tbe1  = "This table estimates eq(3). " \
"Heteroskedasticity-robust standard errors" \
"clustered at the product level appear inparentheses."\
"\sym{*} Significance at the 10\%, \sym{**} Significance at the 5\%, \sym{***} Significance at the 1\%."

multicolumn ={
    'Negative': 1,
    'Positive': 9,
    #'Ambiguous': 10,
}

reorder = {
    # Old, New
    17:3, ## Working capital
    18:5,
    19:7,
    20:9,
    21:11,
    22:13,
    23:15,
    24:17,
    25:19,
    26:21,
    27:23,
    28:25,
    29:27,
    30:29
}

#multi_lines_dep = '(city/product/trade regime/year)'
#new_r = ['& test1', 'test2']
for i in range(1, 88):
    print('\n\nRank {} in term of credit constraint\n\n'.format(i))
    if i % 3 == 0:
        lb.beautify(table_number = i,
                    reorder_var = reorder,
                    #multi_lines_dep = multi_lines_dep,
                    #new_row= new_r,
                    #multicolumn = multicolumn,
                    table_nte = tbe1,
                    jupyter_preview = True,
                    resolution = 280,
                    folder = folder)
    else:
        lb.beautify(table_number = i,
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
Remove table
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
create_report(extension = "html", keep_code = False, notebookname = "02_so2_fin_ratio_city_sector_by_sector.ipynb")
```
