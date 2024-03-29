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
# US Name

Stylised fact asset tangibility 

# Business needs 

Find stylised fact investment biased toward asset tangibility

## Description

### Objective 


Stylised fact asset tangibility:Find stylised fact investment biased toward asset tangibility

#### Scatterplot

1. Compute a scatterplot aggregated at the city-industy-year 
  1. x-axis: 
    1. Asset tangible
    2. TFP
  2. y-axis: SO2 emission
2. Compute a scatterplot aggregated at the city-industy-year 
  1. x-axis: 
    1. Asset tangible
    2. TFP
  2. y-axis: SO2 emission
  3. Color by city SOE vs Private
  4. Color by industry LARGE vs SMALL

#### Table

# Metadata

- Key: oql78mzsh26385x
- Epic: Statistical analysis
- US: Asset tangibility and tfp
- Task tag: #data-analysis, #tfp, #asset-tangibility
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

```python kernel="SoS"
from awsPy.aws_authorization import aws_connector
from awsPy.aws_s3 import service_s3
from awsPy.aws_glue import service_glue
from pathlib import Path
import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
import os, shutil, json

#import tex2pix
#from PyPDF2 import PdfFileMerger
#from wand.image import Image as WImage

path = os.getcwd()
parent_path = str(Path(path).parent.parent)


name_credential = 'financial_dep_SO2_accessKeys.csv'
region = 'eu-west-2'
bucket = 'datalake-london'
path_cred = "{0}/creds/{1}".format(parent_path, name_credential)
```

```python kernel="SoS"
con = aws_connector.aws_instantiate(credential = path_cred,
                                       region = region)
client= con.client_boto()
s3 = service_s3.connect_S3(client = client,
                      bucket = bucket, verbose = False)
glue = service_glue.connect_glue(client = client) 
```

```python kernel="SoS"
pandas_setting = True
if pandas_setting:
    #cm = sns.light_palette("green", as_cmap=True)
    pd.set_option('display.max_columns', None)
    pd.set_option('display.max_colwidth', None)
```

```python kernel="SoS" nteract={"transient": {"deleting": false}}
os.environ['KMP_DUPLICATE_LIB_OK']='True'

```

<!-- #region kernel="SoS" -->
## Scatterplot

1. Compute a scatterplot aggregated at the city-industy-year 
  1. x-axis: 
    1. Asset tangible
    2. TFP
  2. y-axis: SO2 emission
2. Compute a scatterplot aggregated at the city-industy-year 
  1. x-axis: 
    1. Asset tangible
    2. TFP
  2. y-axis: SO2 emission
  3. Color by city SOE vs Private
  4. Color by industry LARGE vs SMALL
<!-- #endregion -->

<!-- #region kernel="SoS" -->
#### Raw scatterplot

1. Compute the average Asset tangible and TFP using firm-level data
2. Merge with SO2 emission on city-industry-year
3. Plot results
<!-- #endregion -->

```python kernel="SoS"
db = 'fin_dep_pollution_baseline_city'
```

```python kernel="SoS"
query = """
SELECT 
  fin_dep_pollution_baseline_city.year, 
  fin_dep_pollution_baseline_city.geocode4_corr, 
  fin_dep_pollution_baseline_city.ind2, 
  short,
  tso2, 
  ln(tso2) AS log_tso2,
  asset_tangibility_tot_asset,
  ln(asset_tangibility_tot_asset) AS log_asset_tangibility_tot_asset,
  total_asset,
  ln(total_asset) AS log_total_asset,
  lag_cashflow_to_tangible, 
  ln(lag_cashflow_to_tangible) AS log_lag_cashflow_to_tangible,
  lag_current_ratio,
  ln(lag_current_ratio) AS log_lag_current_ratio,
  lag_liabilities_tot_asset,
  ln(lag_liabilities_tot_asset) AS log_lag_liabilities_tot_asset,
  lag_sales_tot_asset,
  ln(lag_sales_tot_asset) AS log_lag_sales_tot_asset,
  tfp_cit, 
  ln(tfp_cit) AS log_tfp_cit,
  sum_rd,
  avg_rd,
  dominated_output_soe_c, 
  element_at(dominated_output_i, .5) as dominated_output_i,
  lower_location,
  larger_location,
  tcz,
  spz,
  credit_constraint,
  supply_all_credit,
  supply_long_term_credit,
  CASE WHEN credit_constraint > -0.47 THEN 'EXTERNAL_CONSTRAINT' ELSE 'NO_EXTERNAL_CONSTRAINT' END AS d_credit_constraint,
  CASE WHEN supply_all_credit < 0.99 THEN 'INTERNAL_CONSTRAINT' ELSE 'NO_INTERNAL_CONSTRAINT' END AS d_supply_all_credit,
  CASE WHEN supply_long_term_credit < 2.3255813 THEN 'INTERNAL_CONSTRAINT' ELSE 'NO_INTERNAL_CONSTRAINT' END AS d_supply_long_term_credit
  
FROM 
  environment.fin_dep_pollution_baseline_city 
  INNER JOIN (
    SELECT 
      year, 
      indu_2, 
      geocode4_corr, 
      SUM(rd_tot_asset) AS sum_rd,
      AVG(rd_tot_asset) AS avg_rd
    FROM 
      firms_survey.asif_tfp_credit_constraint 
    GROUP BY 
      year, 
      indu_2, 
      geocode4_corr
  ) AS agg ON fin_dep_pollution_baseline_city.year = agg.year 
  AND fin_dep_pollution_baseline_city.geocode4_corr = agg.geocode4_corr 
  AND fin_dep_pollution_baseline_city.ind2 = agg.indu_2 
WHERE 
  fin_dep_pollution_baseline_city.year in (
    '2001', '2002', '2003', '2004', '2005', 
    '2006', '2007'
  )
"""
df = (
    s3.run_query(
    query=query,
    database=db,
    s3_output="SQL_OUTPUT_ATHENA",
    filename="scatter_plot_1",  # Add filename to print dataframe
    # destination_key="SQL_OUTPUT_ATHENA/CSV",  # Use it temporarily
    # dtype=dtypes,
    )
)
df.head()
```

```python kernel="SoS"
df.shape
```

<!-- #region kernel="SoS" -->
Load credit constraint
<!-- #endregion -->

```python kernel="SoS"
query_cc = """
SELECT DISTINCT(short), AVG(credit_constraint) as credit_constraint
FROM environment.fin_dep_pollution_baseline_city 
GROUP BY short
ORDER BY credit_constraint
"""
df_cc = (
    s3.run_query(
    query=query_cc,
    database=db,
    s3_output="SQL_OUTPUT_ATHENA",
    filename="scatter_plot_2",  # Add filename to print dataframe
    # destination_key="SQL_OUTPUT_ATHENA/CSV",  # Use it temporarily
    # dtype=dtypes,
    )
)
df_cc
```

```python kernel="SoS"
def generate_plots(df, 
                   x_title = 'Asset tangibility',
                   x = 'log_asset_tangibility_tot_asset', 
                   industry = ['Tobacco', 'Smelting ferrous Metals']
                  ):
    """
    industry-> List of two items. First items be less constraints than second items
    """
    sns.set_style("white")
    ### plot 1
    sns.lmplot(x=x,
           y="log_tso2",
           data=df
          )
    plt.xlabel(x_title)
    plt.ylabel('SO2 emission')
    plt.title('Relationship between {} and SO2 emission'.format(x_title))
    #plt.savefig("fig_2.png",
    #       bbox_inches='tight',
    #        dpi=600)
    
    ### Plot 2
    sns.lmplot(x=x,
           y="log_tso2",
           hue="dominated_output_soe_c",
           data=df
          )
    plt.xlabel(x_title)
    plt.ylabel('SO2 emission')
    plt.title('Relationship between {} and SO2 emission, by city ownership'.format(x_title))
    
    ### Plot 3
    sns.lmplot(x=x,
           y="log_tso2",
           hue="dominated_output_i",
           data=df
          )
    plt.xlabel(x_title)
    plt.ylabel('SO2 emission')
    plt.title('Relationship between {} and SO2 emission, by industry size'.format(x_title))
    
    ### Plot 4
    sns.lmplot(x=x,
           y="log_tso2",
           hue="d_credit_constraint",
           data=df
          )
    plt.xlabel(x_title)
    plt.ylabel('SO2 emission')
    plt.title('Relationship between {} and SO2 emission, by external finance'.format(x_title))
    
    ### Plot 5
    sns.lmplot(x=x,
           y="log_tso2",
           hue="short",
           data=df.loc[lambda x: x['short'].isin(industry)]
          )
    plt.xlabel(x_title)
    plt.ylabel('SO2 emission')
    plt.title('Relationship between {} and SO2 emission, by external finance'.format(x_title))
    
```

<!-- #region kernel="SoS" -->
## SO2
<!-- #endregion -->

<!-- #region kernel="SoS" -->
## Plots and statistical in paper

-  SO2 emission in China from 2000 to 2010
    - Code here: https://github.com/thomaspernet/pollution_credit_constraint/blob/main/02_data_analysis/02_statistical_tables/00_final_publishable_tables_figures.md#figure-1-replicate
- Log asset tangibility vs log SO2 emissions
- Log of internal finance vs log SO2 emissions
- 
<!-- #endregion -->

```python
from GoogleDrivePy.google_drive import connect_drive
from GoogleDrivePy.google_authorization import authorization_service
import seaborn as sns;
import matplotlib.pyplot as plt

```

### figure 1

```python
for d in ['token.pickle', 'service.json']:
    s3.download_file(key = "CREDS/pollution_credit_constraint/creds/{}".format(d),
                 path_local = os.path.join(parent_path, "creds"))
```

```python
os.path.join(
    parent_path, "creds", "service.json")
```

```python
auth = authorization_service.get_authorization(
    #path_credential_gcp=os.path.join(parent_path, "creds", "service.json"),
    path_credential_drive=os.path.join(parent_path, "creds"),
    verbose=False,
    # scope = ['https://www.googleapis.com/auth/spreadsheets.readonly',
    # "https://www.googleapis.com/auth/drive"]
)
gd_auth = auth.authorization_drive(path_secret=os.path.join(
    parent_path, "creds", "credentials.json"))
drive = connect_drive.drive_operations(gd_auth)
```

```python
FILENAME_SPREADSHEET = "so2-emissions_china"
spreadsheet_id = drive.find_file_id(FILENAME_SPREADSHEET, to_print=False)
sheetName = 'so2-emissions_china.csv'
var = (
    drive.upload_data_from_spreadsheet(
        sheetID=spreadsheet_id,
        sheetName=sheetName,
        to_dataframe=True)
    .assign(
    so2 = lambda x: (pd.to_numeric(x['SO2(10.000 tons)'])/100).round(0).astype(int)
    )
)
var
```

```python
fig, ax = plt.subplots(figsize=(10, 8))
ax.plot(var['Year'], var['so2'])
ax.axvline(x='2006', c='red')
ax.spines['right'].set_visible(False)
ax.spines['top'].set_visible(False)
ax.annotate('Introduction of the\n 11th FYP', xy=('2006', 26), xytext=('2008', 26),
            arrowprops=dict(facecolor='black', shrink=0.1),
            )
plt.xlabel('Year')
# Set y-axis label
plt.ylabel('SO2 (millions tons)')
plt.xticks(rotation=30)
#plt.title('SO2 emission in China from 2000 to 2010')
#plt.show()
plt.savefig("Figures/FIGURE_1.png",
            bbox_inches='tight',
            dpi=600
           )
```

<!-- #region kernel="SoS" -->
### figure 2
<!-- #endregion -->

```python kernel="SoS"
query = """
SELECT 
  year, 
  cityen, 
  sum_.province_en, 
  tcz, 
  larger_location,
  lower_location, 
  sum_tso2 
FROM 
  (
    SELECT 
      year, 
      cityen, 
      TRIM(
        UPPER(province_en)
      ) as province_en, 
      tcz, 
      SUM(tso2) as sum_tso2 
    FROM 
      environment.fin_dep_pollution_baseline_city 
    GROUP BY 
      year, 
      cityen, 
      province_en, 
      tcz
  ) as sum_ 
  LEFT JOIN (
    SELECT 
      UPPER(province_en) as province_en, 
      larger_location,
      lower_location 
    FROM 
      chinese_lookup.geo_chinese_province_location
  ) as city on sum_.province_en = city.province_en

"""
df_1 = (
    s3.run_query(
    query=query,
    database=db,
    s3_output="SQL_OUTPUT_ATHENA",
    filename="scatter_plot_2",  # Add filename to print dataframe
    # destination_key="SQL_OUTPUT_ATHENA/CSV",  # Use it temporarily
    # dtype=dtypes,
    )
)
df_1 = df_1.fillna("Western")
```

```python kernel="SoS"
df_tcz = (df_1
     .assign(sum_so2 = lambda x: x['sum_tso2']/1000000000)
 .replace({'tcz': {0: 'No TCZ cities', 1:'TCZ cities'}})
 #.loc[lambda x: x['year'].isin([2004, 2005, 2006, 2007])]
 .groupby(['tcz','lower_location', 'year'])[['sum_so2']]
 .sum()
          .assign(pct_change = lambda x: x.groupby(['tcz', "lower_location"])['sum_so2'].transform("pct_change"))
 .reset_index()
 #.stack()
 #.unstack([0, 1])
 
)
df_tcz.head()
#sns.lineplot(data=df_tcz, x="year", y="pct_change", hue="tcz")
```

```python kernel="SoS"
chart = sns.relplot(
    data=df_tcz,
    x="year",
    y="pct_change",
    hue="lower_location",
    col = 'tcz',
    kind="line"
)
chart.set_axis_labels(x_var="year", y_var="Percentage change SO2 emission")
chart.map(plt.axhline, y=0, ls='--', c='red')
chart.set_titles("{col_name}")
chart._legend.set_title("Locations")
chart.set_axis_labels(x_var="Year", y_var="Percentage change SO2")
chart.fig.set_figwidth(10)
chart.fig.set_figheight(8)

plt.savefig("Figures/FIGURE_2.png",
            bbox_inches='tight',
            dpi=600)
```

### figure 3

```python kernel="SoS"
sns.set_style("white")
    ### plot 1
chart = sns.lmplot(x="log_asset_tangibility_tot_asset",
           y="log_tso2",
           data=df.loc[lambda x: 
                      x['log_asset_tangibility_tot_asset'] > -4]
          )
chart.fig.set_figwidth(10)
chart.fig.set_figheight(8)
plt.xlabel("log Asset tangibility")
plt.ylabel('log SO2 emissions')
plt.savefig("Figures/FIGURE_3.png",
            bbox_inches='tight',
            dpi=600)
```

### figure 4

```python kernel="SoS"
fig_dims = (15, 10)
fig, (ax1, ax2) = plt.subplots(ncols=2, sharey=False, figsize=fig_dims)

sns.regplot(
    x="log_lag_cashflow_to_tangible",
    y="log_tso2",
    data=(
        df[
            ["log_tso2", "lag_cashflow_to_tangible", "log_lag_cashflow_to_tangible"]
        ].loc[
            lambda x: x["lag_cashflow_to_tangible"]
            > 0 & ~x["lag_cashflow_to_tangible"].isin([np.nan])
        ]
    ),
    ax=ax1,
)
ax1.set_xlabel("log lag Cashflow")
ax1.set_ylabel("log SO2 emissions")
sns.regplot(
    x="log_lag_current_ratio",
    y="log_tso2",
    data=(
        df[["log_tso2", "lag_current_ratio", "log_lag_current_ratio"]].loc[
            lambda x: x["lag_current_ratio"]
            > 0 & ~x["lag_current_ratio"].isin([np.nan])
        ]
    ),
    ax=ax2,
)
ax2.set_xlabel("log lag Current ratio")
ax2.set_ylabel("log SO2 emissions")
# plt.xlabel(x_title)
#fig.suptitle('Relationship between internal finance and SO2 emission')
plt.savefig("Figures/FIGURE_4.png",
            bbox_inches='tight',
            dpi=600)
```

<!-- #region kernel="SoS" -->
### Asset tangible
<!-- #endregion -->

```python kernel="SoS"
generate_plots(df.loc[lambda x: 
                      x['log_asset_tangibility_tot_asset'] > -6], 
                   x_title = 'Asset tangibility',
                   x = 'log_asset_tangibility_tot_asset', 
                   industry = ['Tobacco', 'Smelting ferrous Metals']
                  )
```

<!-- #region kernel="SoS" -->
### Cashflow
<!-- #endregion -->

```python kernel="SoS"
generate_plots(
    (
        df
        .loc[
            lambda x: x["lag_cashflow_to_tangible"]
            > 0 & ~x["lag_cashflow_to_tangible"].isin([np.nan])
        ]
    ),
    x_title="Cashflow",
    x="log_lag_cashflow_to_tangible",
    industry=["Tobacco", "Smelting ferrous Metals"],
)
```

<!-- #region kernel="SoS" -->
### Current ratio
<!-- #endregion -->

```python kernel="SoS"
generate_plots( (
        df
    .loc[
            lambda x: x["lag_current_ratio"]
            > 0 & ~x["lag_current_ratio"].isin([np.nan])
        ]
    ), 
                   x_title = 'Current ratio',
                   x = 'log_lag_current_ratio', 
                   industry = ['Tobacco', 'Smelting ferrous Metals']
                  )
```

<!-- #region kernel="SoS" -->
### Liabilities to asset
<!-- #endregion -->

```python kernel="SoS"
generate_plots((
        df
    .loc[
            lambda x: x["lag_liabilities_tot_asset"]
            > 0 & ~x["lag_liabilities_tot_asset"].isin([np.nan])
        ]
    ), 
                   x_title = 'Liabilities to asset',
                   x = 'log_lag_liabilities_tot_asset', 
                   industry = ['Tobacco', 'Smelting ferrous Metals']
                  )
```

<!-- #region kernel="SoS" -->
#### RD
<!-- #endregion -->

```python kernel="SoS"
generate_plots(df, 
                   x_title = 'R&D',
                   x = 'avg_rd', 
                   industry = ['Tobacco', 'Smelting ferrous Metals']
                  )
```

<!-- #region kernel="SoS" -->
#### TFP 
<!-- #endregion -->

```python kernel="SoS"
generate_plots(df, 
                   x_title = 'TFP',
                   x = 'log_tfp_cit', 
                   industry = ['Tobacco', 'Smelting ferrous Metals']
                  )
```

<!-- #region kernel="SoS" -->
## Side by side
<!-- #endregion -->

```python kernel="SoS"
fig_dims = (15, 10)
fig, (ax1, ax2) = plt.subplots(ncols=2, sharey=False, figsize=fig_dims)

sns.regplot(
    x="log_lag_cashflow_to_tangible",
    y="log_tso2",
    data=(
        df[
            ["log_tso2", "lag_cashflow_to_tangible", "log_lag_cashflow_to_tangible"]
        ].loc[
            lambda x: x["lag_cashflow_to_tangible"]
            > 0 & ~x["lag_cashflow_to_tangible"].isin([np.nan])
        ]
    ),
    ax=ax1,
)
ax1.set_xlabel("Lag Cashflow")
ax1.set_ylabel("SO2 emission")
sns.regplot(
    x="log_lag_current_ratio",
    y="log_tso2",
    data=(
        df[["log_tso2", "lag_current_ratio", "log_lag_current_ratio"]].loc[
            lambda x: x["lag_current_ratio"]
            > 0 & ~x["lag_current_ratio"].isin([np.nan])
        ]
    ),
    ax=ax2,
)
ax2.set_xlabel("Lag Current ratio")
ax2.set_ylabel("SO2 emission")
# plt.xlabel(x_title)
fig.suptitle('Relationship between internal finance and SO2 emission')
```

<!-- #region kernel="SoS" -->
## Asset tangible
<!-- #endregion -->

<!-- #region kernel="SoS" -->
### Cashflow
<!-- #endregion -->

```python kernel="SoS"
path_local = os.path.join(str(Path(path).parent.parent), 
                              "00_data_catalogue/temporary_local_data",
                         'df_asif_tfp_credit_constraint.csv')
df_tfp = (
    pd.read_csv(path_local)
    .assign(
        log_tfp_op = lambda x: np.log(x['tfp_op']), 
        log_current_ratio = lambda x: np.log(x['current_ratio']), 
        log_liabilities_tot_asset = lambda x: np.log(x['liabilities_tot_asset']), 
        log_asset_tangibility_tot_asset = lambda x: np.log(x['asset_tangibility_tot_asset']), 
        log_cashflow_to_tangible = lambda x: np.log(x['cashflow_to_tangible'])
    )
)
```

```python kernel="SoS"
sns.lmplot(x="log_cashflow_to_tangible",
           y="log_asset_tangibility_tot_asset",
           data=df_tfp
          )
plt.xlabel('Cashflow')
plt.ylabel('Asset tangible')
plt.title('Relationship between {} and TFP'.format('Asset tangible'))
```

<!-- #region kernel="SoS" -->
## RD
<!-- #endregion -->

```python kernel="SoS"
sns.lmplot(x="log_cashflow_to_tangible",
           y="rd_tot_asset",
           data=df_tfp.loc[lambda x: 
                           (x['year'] >2004) &
                           (x['rd_tot_asset'] > 0)
                           &
                           (x['rd_tot_asset'] < 1.5)
                          ]
          )
plt.xlabel('Cashflow')
plt.ylabel('RD')
plt.title('Relationship between {} and RD'.format('Cashflow'))
```

<!-- #region kernel="SoS" -->
## TFP
<!-- #endregion -->

<!-- #region kernel="SoS" -->
### Cashflow
<!-- #endregion -->

```python kernel="SoS"
sns.lmplot(x="log_cashflow_to_tangible",
           y="log_tfp_op",
           data=df_tfp
          )
plt.xlabel('Cashflow')
plt.ylabel('TFP')
plt.title('Relationship between {} and TFP'.format('Cashflow'))
```

<!-- #region kernel="SoS" -->
## Table
<!-- #endregion -->

```python kernel="SoS"
path_local = os.path.join(str(Path(path).parent.parent), 
                              "00_data_catalogue/temporary_local_data",
                         'df_fin_dep_pollution_baseline_city.csv')
df_so2 = (
    pd.read_csv(path_local)
)
```

```python kernel="SoS"
df_table = pd.concat(
    [
        pd.concat(
            [
                (
                    df_so2.assign(
                        tso2=lambda x: x["tso2"] / 1000000,
                        sales=lambda x: x["lag_sales_tot_asset"] / 1000000,
                        total_asset=lambda x: x["total_asset"] / 1000000,
                    )
                    .agg(
                        {
                            "tso2": ["mean", "std"],
                            "asset_tangibility_tot_asset": ["mean", "std"],
                            "lag_sales_tot_asset": ["mean", "std"],
                            "total_asset": ["mean", "std"],
                            "lag_cashflow_to_tangible": ["mean", "std"],
                            "lag_current_ratio": ["mean", "std"],
                            "lag_liabilities_tot_asset": ["mean", "std"],
                            "tfp_cit": ["mean", "std"],
                            # "sum_rd": ["mean", "std"],
                        }
                    )
                    .T.assign(
                        full_sample=lambda x: np.round(x["mean"], 2).astype(str)
                        + " ("
                        + np.round(x["std"], 2).astype(str)
                        + ")"
                    )
                    .reindex(columns=["full_sample"])
                    .rename(columns={"full_sample": "ALL"})
                )
            ],
            axis=1,
            keys=["Full Sample"],
        ),
        pd.concat(
            [
                (
                    df_so2
                    .assign(
                        tso2=lambda x: x["tso2"] / 1000000,
                        sales=lambda x: x["lag_sales_tot_asset"] / 1000000,
                        total_asset=lambda x: x["total_asset"] / 1000000,
                        dominated_output_i = lambda x: 
                        np.where(
        x['dominated_output_i'].str.extract(r"(?<=0.5\=)(.*?)(?=\,)") == 'true', True, False
        )
                    )
                    .groupby(["dominated_output_i"])
                    .agg(
                        {
                            "tso2": ["mean", "std"],
                            "asset_tangibility_tot_asset": ["mean", "std"],
                            "lag_sales_tot_asset": ["mean", "std"],
                            "total_asset": ["mean", "std"],
                            "lag_cashflow_to_tangible": ["mean", "std"],
                            "lag_current_ratio": ["mean", "std"],
                            "lag_liabilities_tot_asset": ["mean", "std"],
                            "tfp_cit": ["mean", "std"],
                            # "sum_rd": ["mean", "std"],
                        }
                    )
                    .T.unstack(-1)
                    .assign(
                        industry_false=lambda x: np.round(x[(False, "mean")], 2).astype(
                            str
                        )
                        + " ("
                        + np.round(x[(False, "std")], 2).astype(str)
                        + ")",
                        industry_true=lambda x: np.round(x[(True, "mean")], 2).astype(
                            str
                        )
                        + " ("
                        + np.round(x[(True, "std")], 2).astype(str)
                        + ")",
                    )
                    .droplevel(axis=1, level=1)
                    .iloc[:, 4:]
                    # .reindex(columns = ['industry_false', 'industry_true'])
                ).rename(columns={"industry_false": "SMALL", "industry_true": "LARGE"})
            ],
            axis=1,
            keys=["Industry"],
        ),
        pd.concat(
            [
                pd.concat(
                    [
                        (
                            df_so2
                            .assign(
                                tso2=lambda x: x["tso2"] / 1000000,
                                sales=lambda x: x["lag_sales_tot_asset"] / 1000000,
                                total_asset=lambda x: x["total_asset"] / 1000000,
                            )
                            .groupby(["dominated_output_soe_c"])
                            .agg(
                                {
                                    "tso2": ["mean", "std"],
                                    "asset_tangibility_tot_asset": ["mean", "std"],
                                    "lag_sales_tot_asset": ["mean", "std"],
                                    "total_asset": ["mean", "std"],
                                    "lag_cashflow_to_tangible": ["mean", "std"],
                                    "lag_current_ratio": ["mean", "std"],
                                    "lag_liabilities_tot_asset": ["mean", "std"],
                                    "tfp_cit": ["mean", "std"],
                                    # "sum_rd": ["mean", "std"],
                                }
                            )
                            .T.unstack(-1)
                            .assign(
                                soe_false=lambda x: np.round(
                                    x[(False, "mean")], 2
                                ).astype(str)
                                + " ("
                                + np.round(x[(False, "std")], 2).astype(str)
                                + ")",
                                soe_true=lambda x: np.round(
                                    x[(True, "mean")], 2
                                ).astype(str)
                                + " ("
                                + np.round(x[(True, "std")], 2).astype(str)
                                + ")",
                            )
                            .droplevel(axis=1, level=1)
                            .iloc[:, 4:]
                            .rename(columns={"soe_false": "PRIVATE", "soe_true": "SOE"})
                        ),
                        (
                            df_so2
                            .assign(
                                tso2=lambda x: x["tso2"] / 1000000,
                                sales=lambda x: x["lag_sales_tot_asset"] / 1000000,
                                total_asset=lambda x: x["total_asset"] / 1000000,
                            )
                            .groupby(["tcz"])
                            .agg(
                                {
                                    "tso2": ["mean", "std"],
                                    "asset_tangibility_tot_asset": ["mean", "std"],
                                    "lag_sales_tot_asset": ["mean", "std"],
                                    "total_asset": ["mean", "std"],
                                    "lag_cashflow_to_tangible": ["mean", "std"],
                                    "lag_current_ratio": ["mean", "std"],
                                    "lag_liabilities_tot_asset": ["mean", "std"],
                                    "tfp_cit": ["mean", "std"],
                                    # "sum_rd": ["mean", "std"],
                                }
                            )
                            .T.unstack(-1)
                            .assign(
                                tcz_false=lambda x: np.round(x[(0, "mean")], 2).astype(
                                    str
                                )
                                + " ("
                                + np.round(x[(0, "std")], 2).astype(str)
                                + ")",
                                tcz_true=lambda x: np.round(x[(1, "mean")], 2).astype(
                                    str
                                )
                                + " ("
                                + np.round(x[(1, "std")], 2).astype(str)
                                + ")",
                            )
                            .droplevel(axis=1, level=1)
                            .iloc[:, 4:]
                            .rename(columns={"tcz_false": "NO TCZ", "tcz_true": "TCZ"})
                        ),
                    ],
                    axis=1,
                )
            ],
            axis=1,
            keys=["City"],
        ),
    ],
    axis=1,
).rename(
    index={
        "tso2": "SO2",
        "asset_tangibility_tot_asset": "asset tangibility",
        "total_asset": "total asset",
        "lag_cashflow_to_tangible": "cashflow",
        "lag_current_ratio": "current ratio",
        "lag_liabilities_tot_asset": "liabilities to asset",
        "tfp_cit": "TFP",
        "lag_sales_tot_asset": "sales to asset",
        # "sum_rd": "RD",
    }
)
df_latex = df_table.to_latex()
# df_table
```

```python kernel="SoS"
folder = 'Tables'
```

```python kernel="SoS"
title = "Summary statistic"
tb_note = """
The information about the SO2 level is collected using various editions of the China Environment Statistics Yearbook and is reported in millions of tons.
cashflow is measured as net income + depreciation over asset;
current ratio is measured as current asset over current liabilities. Cashflow, current ratio, sales to asset and liabilities to asset are lagged by one year.
TFP is computed the Olley-Parkes algorithm. 
An industry is labelled as large (small) when the city-industrial output share above (below) a critical threshold, for instance the 50th decile.
(Non-)SOE-dominated cities refers to cities where the output share of SOEs is (below) above a critical threshold, for instance the 60th decile.
The list of TCZ is provided by the State Council, 1998. “Official Reply to the State Council Concerning
Acid Rain Control Areas and Sulfur Dioxide Pollution Control Areas”.
Standard deviation is reported in parenthesis
"""
```

```python kernel="SoS"
table_number = 1
with open('{}/table_{}.tex'.format(folder,table_number), 'w') as fout:
    for i in range(len( df_latex)):
        if i ==0:
            header= "\documentclass[preview]{standalone} \n\\usepackage[utf8]{inputenc}\n" \
            "\\usepackage{booktabs,caption,threeparttable, siunitx, adjustbox}\n\n" \
            "\\begin{document}"
            top =  '\n\\begin{adjustbox}{width=\\textwidth, totalheight=\\textheight-2\\baselineskip,keepaspectratio}\n'
            table_top = "\n\\begin{table}[!htbp] \centering"
            table_title = "\n\caption{%s}\n" % title
            
            fout.write(header)
            fout.write(table_top)
            fout.write(table_title)
            fout.write(top)
           
        fout.write( df_latex[i])
    
    bottom =  '\n\\end{adjustbox}\n'
    tb_note_top = "\n\\begin{tablenotes}\n\small\n\item"
    table_bottom = "\n\end{table}"
    footer = "\n\n\\end{document}"
    tb_note_bottom = "\n\end{tablenotes}"
    fout.write(bottom)
    fout.write(tb_note_top)
    fout.write(tb_note)
    fout.write(tb_note_bottom)
    fout.write(table_bottom)
    fout.write(footer)
 
f = open('{}/table_{}.tex'.format(folder,table_number))
r = tex2pix.Renderer(f, runbibtex=False)
r.mkpdf('{}/table_{}.pdf'.format(folder,table_number))
img = WImage(filename='{}/table_{}.pdf'.format(folder,table_number),
resolution = 200)
display(img)
```

```python kernel="SoS"
import os
path = os.path.join(folder)
if os.path.exists(folder) == False:
        os.mkdir(folder)
for ext in ['.txt', '.tex', '.pdf']:
    x = [a for a in os.listdir(folder) if a.endswith(ext)]
    [os.remove(os.path.join(folder, i)) for i in x]
```

<!-- #region kernel="SoS" -->
### Distribution by industry
<!-- #endregion -->

```python kernel="SoS"
df_industry = (
    df_so2
    .assign(
        tso2=lambda x: x["tso2"] / 1000000,
        sales=lambda x: x["sales_tot_asset"] / 1000000,
        total_asset=lambda x: x["total_asset"] / 1000000,
    )
    .groupby("short")
    .agg(
        {
            "tso2": ["mean", "std"],
            "asset_tangibility_tot_asset": ["mean", "std"],
            "lag_cashflow_to_tangible": ["mean", "std"],
            "lag_current_ratio": ["mean", "std"],
            "lag_liabilities_tot_asset": ["mean", "std"],
            #"tfp_cit": ["mean", "std"],
            #"sum_rd": ["mean", "std"],
        }
    )
    .assign(
        tso2_=lambda x: np.round(x[("tso2", "mean")], 2).astype(str)
        + " ("
        + np.round(x[("tso2", "std")], 2).astype(str)
        + ")",
        asset_tangibility_tot_asset_=lambda x: np.round(
            x[("asset_tangibility_tot_asset", "mean")], 2
        ).astype(str)
        + " ("
        + np.round(x[("asset_tangibility_tot_asset", "std")], 2).astype(str)
        + ")",
        cashflow_to_tangible_=lambda x: np.round(
            x[("lag_cashflow_to_tangible", "mean")], 2
        ).astype(str)
        + " ("
        + np.round(x[("lag_cashflow_to_tangible", "std")], 2).astype(str)
        + ")",
        current_ratio_=lambda x: np.round(x[("lag_current_ratio", "mean")], 2).astype(str)
        + " ("
        + np.round(x[("lag_current_ratio", "std")], 2).astype(str)
        + ")",
        liabilities_tot_asset_=lambda x: np.round(
            x[("lag_liabilities_tot_asset", "mean")], 2
        ).astype(str)
        + " ("
        + np.round(x[("lag_liabilities_tot_asset", "std")], 2).astype(str)
        + ")",
        #tfp_cit_=lambda x: np.round(x[("tfp_cit", "mean")], 2).astype(str)
        #+ " ("
        #+ np.round(x[("tfp_cit", "std")], 2).astype(str)
        #+ ")",
    )
    .iloc[:, 10:]
    .rename(
        columns={
            "tso2_": "SO2",
            "asset_tangibility_tot_asset_": "asset tangibility",
            "cashflow_to_tangible_": "cashflow",
            "current_ratio_": "current ratio",
            "liabilities_tot_asset_": "liabilities to asset",
            #"tfp_cit_": "TFP",
        }
    )
    .rename_axis(index={'short': 'Industry'})
)
df_latex = df_industry.to_latex()
#df_industry
```

```python kernel="SoS"
title = "Average main variables by industry"
tb_note = """
The information about the SO2 level is collected using various editions of the China Environment Statistics Yearbook and is reported in millions of tons.
cashflow is measured as net income + depreciation over asset;
current ratio is measured as current asset over current liabilities. 
Cashflow, current ratio, sales to asset and liabilities to asset are lagged by one year.
Standard deviation is reported in parenthesis
"""
```

```python kernel="SoS"
table_number = 1
with open('{}/table_{}.tex'.format(folder,table_number), 'w') as fout:
    for i in range(len( df_latex)):
        if i ==0:
            header= "\documentclass[preview]{standalone} \n\\usepackage[utf8]{inputenc}\n" \
            "\\usepackage{booktabs,caption,threeparttable, siunitx, adjustbox}\n\n" \
            "\\begin{document}"
            top =  '\n\\begin{adjustbox}{width=\\textwidth, totalheight=\\textheight-2\\baselineskip,keepaspectratio}\n'
            table_top = "\n\\begin{table}[!htbp] \centering"
            table_title = "\n\caption{%s}\n" % title
            
            fout.write(header)
            fout.write(table_top)
            fout.write(table_title)
            fout.write(top)
           
        fout.write( df_latex[i])
    
    bottom =  '\n\\end{adjustbox}\n'
    tb_note_top = "\n\\begin{tablenotes}\n\small\n\item"
    table_bottom = "\n\end{table}"
    footer = "\n\n\\end{document}"
    tb_note_bottom = "\n\end{tablenotes}"
    fout.write(bottom)
    fout.write(tb_note_top)
    fout.write(tb_note)
    fout.write(tb_note_bottom)
    fout.write(table_bottom)
    fout.write(footer)
 
f = open('{}/table_{}.tex'.format(folder,table_number))
r = tex2pix.Renderer(f, runbibtex=False)
r.mkpdf('{}/table_{}.pdf'.format(folder,table_number))
img = WImage(filename='{}/table_{}.pdf'.format(folder,table_number),
resolution = 200)
display(img)
```

```python kernel="SoS"
import os
path = os.path.join(folder)
if os.path.exists(folder) == False:
        os.mkdir(folder)
for ext in ['.txt', '.tex', '.pdf']:
    x = [a for a in os.listdir(folder) if a.endswith(ext)]
    [os.remove(os.path.join(folder, i)) for i in x]
```

<!-- #region kernel="SoS" nteract={"transient": {"deleting": false}} -->
# Generate reports
<!-- #endregion -->

```python kernel="python3" nteract={"transient": {"deleting": false}} outputExpanded=false
import os, time, shutil, urllib, ipykernel, json
from pathlib import Path
from notebook import notebookapp
```

```python kernel="python3" nteract={"transient": {"deleting": false}} outputExpanded=false
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

```python kernel="python3" nteract={"transient": {"deleting": false}} outputExpanded=false
create_report(extension = "html", keep_code = False, notebookname = '00_asset_tangibility_tfp.ipynb')
```
