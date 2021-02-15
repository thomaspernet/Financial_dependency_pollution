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

```sos kernel="SoS"
from awsPy.aws_authorization import aws_connector
from awsPy.aws_s3 import service_s3
from awsPy.aws_glue import service_glue
from pathlib import Path
import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
import os, shutil, json

import tex2pix
from PyPDF2 import PdfFileMerger
from wand.image import Image as WImage

path = os.getcwd()
parent_path = str(Path(path).parent.parent)


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

```sos kernel="SoS"
db = 'fin_dep_pollution_baseline_city'
```

```sos kernel="SoS"
query = """
SELECT 
  fin_dep_pollution_baseline_city.year, 
  fin_dep_pollution_baseline_city.geocode4_corr, 
  fin_dep_pollution_baseline_city.ind2, 
  tso2, 
  avg_tangible, 
  median_tangible, 
  avg_tfp, 
  median_tfp,
  avg_rd,
  median_rd,
  sales_tot_asset,
  total_asset,
  employment,
  capital,
  current_asset,
  current_ratio,
  liabilities_tot_asset,
  asset_tangibility_tot_asset,
  cashflow_to_tangible, 
  dominated_output_soe_c, 
  element_at(dominated_output_i, .5) as dominated_output_i,
  lower_location,
  larger_location,
  tcz,
  spz
  
FROM 
  environment.fin_dep_pollution_baseline_city 
  INNER JOIN (
    SELECT 
      year, 
      indu_2, 
      geocode4_corr, 
      AVG(asset_tangibility_tot_asset) AS avg_tangible, 
      approx_percentile(asset_tangibility_tot_asset,.5) AS median_tangible, 
      AVG(tfp_op) AS avg_tfp, 
      approx_percentile(tfp_op,.5) AS median_tfp,
      AVG(rd_tot_asset) AS avg_rd, 
      approx_percentile(rd_tot_asset,.5) AS median_rd
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
    .assign(
    log_tso2= lambda x: np.log(x['tso2']),
    log_avg_tangible=lambda x: np.log(x['avg_tangible']),
    log_median_tangible=lambda x: np.log(x['median_tangible']),
    log_avg_tfp=lambda x: np.log(x['avg_tfp']),
    log_median_tfp=lambda x: np.log(x['median_tfp'])
    )
)
df.head()
```

```sos kernel="SoS"
df.shape
```

<!-- #region kernel="SoS" -->
## Asset tangible
<!-- #endregion -->

```sos kernel="SoS"
sns.set_style("white")
#beingsaved.set_rasterized(True)
sns.lmplot(x="log_avg_tangible",
           y="log_tso2",
           data=df.loc[lambda x: x['log_avg_tangible'] > -6])
#plt.title('Relationship between Financial dependencies and variation of SO2 between 2005 and 2007, in log')
# Set x-axis label
plt.xlabel('Asset tangibility')
# Set y-axis label
plt.ylabel('SO2 emission')
plt.title('Relationship between Asset tangibility and SO2 emission')
#plt.savefig("fig_2.png",
#            bbox_inches='tight',
#            dpi=600)
```

```sos kernel="SoS"
sns.set_style("white")
#beingsaved.set_rasterized(True)
sns.lmplot(x="log_avg_tangible",
           y="log_tso2",
           hue="dominated_output_soe_c",
           data=df.loc[lambda x: x['log_avg_tangible'] > -6]
          )
#plt.title('Relationship between Financial dependencies and variation of SO2 between 2005 and 2007, in log')
# Set x-axis label
plt.xlabel('Asset tangibility')
# Set y-axis label
plt.ylabel('SO2 emission')
plt.title('Relationship between Asset tangibility and SO2 emission, by city ownership')
#plt.savefig("fig_2.png",
#            bbox_inches='tight',
#            dpi=600)
```

```sos kernel="SoS"
sns.set_style("white")
#beingsaved.set_rasterized(True)
sns.lmplot(x="log_avg_tangible",
           y="log_tso2",
           hue="dominated_output_i",
           data=df.loc[lambda x: x['log_avg_tangible'] > -6]
          )
#plt.title('Relationship between Financial dependencies and variation of SO2 between 2005 and 2007, in log')
# Set x-axis label
plt.xlabel('Asset tangibility')
# Set y-axis label
plt.ylabel('SO2 emission')
plt.title('Relationship between Asset tangibility and SO2 emission, by industry size')
#plt.savefig("fig_2.png",
#            bbox_inches='tight',
#            dpi=600)
```

<!-- #region kernel="SoS" -->
#### RD
<!-- #endregion -->

```sos kernel="SoS"
sns.set_style("white")
#beingsaved.set_rasterized(True)
sns.lmplot(x="avg_rd",
           y="log_tso2",
           data=df.loc[lambda x: x['year'] > 2004])
#plt.title('Relationship between Financial dependencies and variation of SO2 between 2005 and 2007, in log')
# Set x-axis label
plt.xlabel('R&D')
# Set y-axis label
plt.ylabel('SO2 emission')
plt.title('Relationship between RD and SO2 emission')
#plt.savefig("fig_2.png",
#            bbox_inches='tight',
#            dpi=600)
```

```sos kernel="SoS"
sns.set_style("white")
#beingsaved.set_rasterized(True)
sns.lmplot(x="avg_rd",
           y="log_tso2",
           hue="dominated_output_soe_c",
           data=df.loc[lambda x: x['year'] > 2004])
#plt.title('Relationship between Financial dependencies and variation of SO2 between 2005 and 2007, in log')
# Set x-axis label
plt.xlabel('R&D')
# Set y-axis label
plt.ylabel('SO2 emission')
plt.title('Relationship between RD and SO2 emission, by city ownership')
#plt.savefig("fig_2.png",
#            bbox_inches='tight',
#            dpi=600)
```

```sos kernel="SoS"
sns.set_style("white")
#beingsaved.set_rasterized(True)
sns.lmplot(x="avg_rd",
           y="log_tso2",
           hue="dominated_output_i",
           data=df.loc[lambda x: x['year'] > 2004])
#plt.title('Relationship between Financial dependencies and variation of SO2 between 2005 and 2007, in log')
# Set x-axis label
plt.xlabel('R&D')
# Set y-axis label
plt.ylabel('SO2 emission')
plt.title('Relationship between RD and SO2 emission, by industry size')
#plt.savefig("fig_2.png",
#            bbox_inches='tight',
#            dpi=600)
```

<!-- #region kernel="SoS" -->
#### TFP 
<!-- #endregion -->

```sos kernel="SoS"
sns.set_style("white")
#beingsaved.set_rasterized(True)
sns.lmplot(x="log_avg_tfp",
           y="log_tso2",
           data=df#.loc[lambda x: x['log_avg_tangible'] > -6]
          )
#plt.title('Relationship between Financial dependencies and variation of SO2 between 2005 and 2007, in log')
# Set x-axis label
plt.xlabel('TFP')
# Set y-axis label
plt.ylabel('SO2 emission')
plt.title('Relationship between TFP and SO2 emission')
#plt.savefig("fig_2.png",
#            bbox_inches='tight',
#            dpi=600)
```

```sos kernel="SoS"
sns.set_style("white")
#beingsaved.set_rasterized(True)
sns.lmplot(x="log_avg_tfp",
           y="log_tso2",
           hue="dominated_output_soe_c",
           data=df#.loc[lambda x: x['log_avg_tangible'] > -6]
          )
#plt.title('Relationship between Financial dependencies and variation of SO2 between 2005 and 2007, in log')
# Set x-axis label
plt.xlabel('TFP')
# Set y-axis label
plt.ylabel('SO2 emission')
plt.title('Relationship between TFP and SO2 emission, by city ownership')
#plt.savefig("fig_2.png",
#            bbox_inches='tight',
#            dpi=600)
```

```sos kernel="SoS"
sns.set_style("white")
#beingsaved.set_rasterized(True)
sns.lmplot(x="log_avg_tfp",
           y="log_tso2",
           hue="dominated_output_i",
           data=df#.loc[lambda x: x['log_avg_tangible'] > -6]
          )
#plt.title('Relationship between Financial dependencies and variation of SO2 between 2005 and 2007, in log')
# Set x-axis label
plt.xlabel('TFP')
# Set y-axis label
plt.ylabel('SO2 emission')
plt.title('Relationship between TFP and SO2 emission, by industry size')
#plt.savefig("fig_2.png",
#            bbox_inches='tight',
#            dpi=600)
```

<!-- #region kernel="SoS" -->
## Table
<!-- #endregion -->

```sos kernel="SoS"
df_table = (
    pd.concat(
    [
        pd.concat(
            [
                (
                    df.assign(
                        tso2=lambda x: x["tso2"] / 1000000,
                        sales=lambda x: x["sales_tot_asset"] / 1000000,
                        total_asset=lambda x: x["total_asset"] / 1000000,
                    )
                    .agg(
                        {
                            "tso2": ["mean", "std"],
                            "asset_tangibility_tot_asset": ["mean", "std"],
                            "sales_tot_asset": ["mean", "std"],
                            "total_asset": ["mean", "std"],
                            "cashflow_to_tangible": ["mean", "std"],
                            "current_ratio": ["mean", "std"],
                            "liabilities_tot_asset": ["mean", "std"],
                            "avg_tfp": ["mean", "std"],
                            "avg_rd": ["mean", "std"],
                        }
                    )
                    .T.assign(
                        full_sample=lambda x: np.round(x["mean"], 2).astype(str)
                        + " ("
                        + np.round(x["std"], 2).astype(str)
                        + ")"
                    )
                    .reindex(columns=["full_sample"])
                    .rename(columns={"full_sample": ""})
                )
            ],
            axis=1,
            keys=["Full Sample"],
        ),
        pd.concat(
            [
                (
                    df.assign(
                        tso2=lambda x: x["tso2"] / 1000000,
                        sales=lambda x: x["sales_tot_asset"] / 1000000,
                        total_asset=lambda x: x["total_asset"] / 1000000,
                    )
                    .groupby(["dominated_output_i"])
                    .agg(
                        {
                            "tso2": ["mean", "std"],
                            "asset_tangibility_tot_asset": ["mean", "std"],
                            "sales_tot_asset": ["mean", "std"],
                            "total_asset": ["mean", "std"],
                            "cashflow_to_tangible": ["mean", "std"],
                            "current_ratio": ["mean", "std"],
                            "liabilities_tot_asset": ["mean", "std"],
                            "avg_tfp": ["mean", "std"],
                            "avg_rd": ["mean", "std"],
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
                            df.assign(
                                tso2=lambda x: x["tso2"] / 1000000,
                                sales=lambda x: x["sales_tot_asset"] / 1000000,
                                total_asset=lambda x: x["total_asset"] / 1000000,
                            )
                            .groupby(["dominated_output_soe_c"])
                            .agg(
                                {
                                    "tso2": ["mean", "std"],
                                    "asset_tangibility_tot_asset": ["mean", "std"],
                                    "sales_tot_asset": ["mean", "std"],
                                    "total_asset": ["mean", "std"],
                                    "cashflow_to_tangible": ["mean", "std"],
                                    "current_ratio": ["mean", "std"],
                                    "liabilities_tot_asset": ["mean", "std"],
                                    "avg_tfp": ["mean", "std"],
                                    "avg_rd": ["mean", "std"],
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
                            df.assign(
                                tso2=lambda x: x["tso2"] / 1000000,
                                sales=lambda x: x["sales_tot_asset"] / 1000000,
                                total_asset=lambda x: x["total_asset"] / 1000000,
                            )
                            .groupby(["tcz"])
                            .agg(
                                {
                                    "tso2": ["mean", "std"],
                                    "asset_tangibility_tot_asset": ["mean", "std"],
                                    "sales_tot_asset": ["mean", "std"],
                                    "total_asset": ["mean", "std"],
                                    "cashflow_to_tangible": ["mean", "std"],
                                    "current_ratio": ["mean", "std"],
                                    "liabilities_tot_asset": ["mean", "std"],
                                    "avg_tfp": ["mean", "std"],
                                    "avg_rd": ["mean", "std"],
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
)
.rename(index={
    "tso2": "SO2",
    "asset_tangibility_tot_asset": "asset tangibility",
    "sales_tot_asset": "sales to asset",
    "total_asset": "total asset",
    "cashflow_to_tangible": "cashflow",
    "current_ratio": "current ratio",
    "liabilities_tot_asset": "liabilities to asset",
    "avg_tfp": "TFP",
    "avg_rd": "RD",
})
)
df_latex = df_table.to_latex()
#df_table
```

```sos kernel="SoS"
folder = 'Tables'
table_number = 1
```

```sos kernel="SoS"
with open('{}/table_{}.tex'.format(folder,table_number), 'w') as fout:
    for i in range(len( df_latex)):
        if i ==0:
            header= "\documentclass[preview]{standalone} \n\\usepackage[utf8]{inputenc}\n" \
            "\\usepackage{booktabs,caption,threeparttable, siunitx, adjustbox}\n\n" \
            "\\begin{document}"
            top =  '\n\\begin{adjustbox}{width=\\textwidth, totalheight=\\textheight-2\\baselineskip,keepaspectratio}\n'
            fout.write(header)
            fout.write(top)
        fout.write( df_latex[i])
    
    bottom =  '\n\\end{adjustbox}\n'
    footer = "\n\n\\end{document}"
    fout.write(bottom)
    fout.write(footer)
 
f = open('{}/table_{}.tex'.format(folder,table_number))
r = tex2pix.Renderer(f, runbibtex=False)
r.mkpdf('{}/table_{}.pdf'.format(folder,table_number))
img = WImage(filename='{}/table_{}.pdf'.format(folder,table_number),
resolution = 200)
display(img)
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
create_report(extension = "html", keep_code = False, notebookname = '00_asset_tangibility_tfp.ipynb')
```
