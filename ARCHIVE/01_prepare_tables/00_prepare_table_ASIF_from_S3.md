---
jupyter:
  jupytext:
    formats: ipynb,md
    text_representation:
      extension: .md
      format_name: markdown
      format_version: '1.2'
      jupytext_version: 1.4.2
  kernel_info:
    name: python3
  kernelspec:
    display_name: Python 3
    language: python
    name: python3
---

# replicate notebook to prepare ASIF panel data 

# Objective(s)

Replicate the following notebook https://github.com/thomaspernet/SBC_pollution_China/blob/master/Data_preprocessing/00_Asif_raw_to_csv_preprocessing.md to prepare the data, and use more variables. If 2008 and 2009 don’t have the varialbe we need, exclude them

* Need to use:
  * 'stata_year_1998.csv', 
  * 'stata_year_1999.csv',
  *  'stata_year_2000.csv',
  *  'stata_year_2001.csv',
  * 'stata_year_2002.csv', 
  * 'stata_year_2003.csv',
  *  'stata_year_2004.csv',
  * 'stata_year_2005.csv', 
  *  'stata_year_2006.csv',
  *  'stata_year_2007.csv',
  *  'stata_year_2008.csv',
  * 'stata_year_2009.csv'
The data need to be saved in the S3, PREPARED folder

# Metadata

* Key: wbg66hgcz25127p
* Parent key (for update parent):  
* Notebook US Parent (i.e the one to update): 
* Child key: 
* Notebook US Child: 
 * Epic: Epic 1
* US: US 2
* Date Begin: 11/14/2020
* Duration Task: 0
* Description: Prepare the ASIF panel data using the previous notebook and add all possible variables from 1998 to 2009
* Step type: Prepare table
* Status: Active
* Source URL: US 02 Prepare ASIF
* Task type: Jupyter Notebook
* Users: Thomas Pernet
* Watchers: Thomas Pernet
* User Account: https://468786073381.signin.aws.amazon.com/console
* Estimated Log points: 5
* Task tag: #asif,#data-preparation
* Toggl Tag: #data-preparation
* Meetings:  
* Presentation:  
* Email Information:  
  * thread: Number of threads: 0(Default 0, to avoid display email)
  *  

# Input Cloud Storage [AWS/GCP]

## Table/file

* Origin: 
* S3
* Name: 
* DATA/FIRMS/RAW_DATA
* Github: 
  * 

## Destination Output/Delivery

### Table/file

* Origin: 
* S3
* Name:
* DATA/FIRMS/PREPARED
* GitHub:
* https://github.com/thomaspernet/Financial_dependency_pollution/blob/master/01_data_preprocessing/01_prepare_tables/00_prepare_table_ASIF_from_S3.md
* URL: 
  * chinese-data/DATA/FIRMS/PREPARED


# Connexion server

```python
from awsPy.aws_authorization import aws_connector
from awsPy.aws_s3 import service_s3
from awsPy.aws_glue import service_glue
from pathlib import Path
import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
import os, shutil, json
import sidetable


path = os.getcwd()
parent_path = str(Path(path).parent.parent)


name_credential = 'financial_dep_SO2_accessKeys.csv'
region = 'eu-west-3'
bucket = 'chinese-data'
path_cred = "{0}/creds/{1}".format(parent_path, name_credential)
```

```python
con = aws_connector.aws_instantiate(credential = path_cred,
                                       region = region)
client= con.client_boto()
s3 = service_s3.connect_S3(client = client,
                      bucket = bucket, verbose = False)
glue = service_glue.connect_glue(client = client,
                      bucket = bucket)
```

```python
pandas_setting = True
if pandas_setting:
    cm = sns.light_palette("green", as_cmap=True)
    pd.set_option('display.max_columns', None)
    pd.set_option('display.max_colwidth', None)
```

# Prepare query POC

Once you are satisfied by the jobs, create a new US in Coda to move the queries to the ETL. As a reminder, the POC is meant to test the queries, modify or improve it. A second notebook in the root of the folder `01_prepare_tables` is generated so that you can add the steps to the ETL. 

Basically, the life of a query is:

1. Create a test notebook (in POC subfolder)
    2. Move the queries to the second (in the root folder)
        3. Loop 1 and 2 if needed
2. In the deployment process, only queries in the root folder are used

# Download data locally

First of all, load the data locally. Use the function `list_all_files_with_prefix` to parse all the files in a given folder. Change the prefix to the name of the folder in which the data are located.

```python
prefix = 'DATA/FIRMS/RAW_DATA'
LOCAL_PATH_CATALOGUE= os.path.join(str(Path(path).parent.parent),
                                          '00_data_catalogue'
                                     )
LOCAL_PATH_CONFIG_FILE = os.path.join(str(Path(path).parent.parent),
                                          '00_data_catalogue',
                                          'temporary_local_data'
                                     )
```

```python
to_download = False
if to_download:
    FILES_TO_UPLOAD = s3.list_all_files_with_prefix(prefix=prefix)
    list(
        map(
            lambda x:
            s3.download_file(key=x, path_local=LOCAL_PATH_CONFIG_FILE),
            FILES_TO_UPLOAD
        )
    )
```

# Steps 


# Table `Raw Asif Dataset`

- Table name: `XX`


Variables:

From [Brandt paper- Challenges of working with the Chinese NBS firm-level data](https://docs.google.com/file/d/16agSbxO7cYuEn1v2bvw16ZRAx9gg7-Zm/edit)

Online files are availble [here](https://feb.kuleuven.be/public/u0044468//CHINA/appendix/)

![](https://camo.githubusercontent.com/7ae30cf56f052176f0dab3c2ff75969fd5b2690f354532ef960577b59194f0cc/68747470733a2f2f64796e616c6973742e696f2f752f674e346d35584f6e30566a6f744659706975576b724a5f64)

## Original data

The original paper has the following obs before cleaning

![](https://camo.githubusercontent.com/19e7bb249cce6c5d91739691845bc887017c79779169b778d4c0d416f7486a23/68747470733a2f2f64796e616c6973742e696f2f752f5956364a644d7146754a44786c673174793265774b695f2d)

We compare the raw data we have with the raw data from the paper

1998-2007

```python
from tqdm.notebook import tqdm
```

```python
df_raw = pd.DataFrame()
for year in tqdm(range(1998, 2008, 1)):
    #print(year)
    path = os.path.join(LOCAL_PATH_CONFIG_FILE, 'stata_year_{0}.csv'.format(year))
    df_ = pd.read_csv(
        path
        ,
        usecols = ['id'],
        )
    df_['year'] = year
    
    df_raw = df_raw.append(df_, sort = False)
```

2008-2009

2008 does not have firm's ID

```python
df_2008 = pd.read_csv(
       os.path.join(LOCAL_PATH_CONFIG_FILE, 'stata_year_2008.csv'),
        )
df_2008['id'] = df_2008.index
df_2008['year'] = '2008'
df_2008 = df_2008[['year', 'id']]
df_raw = df_raw.append(df_2008, sort = False)
```

```python
df_2009 = pd.read_csv(
         os.path.join(LOCAL_PATH_CONFIG_FILE, 'stata_year_2009.csv'),
    usecols = ['法人代码'],
        )
df_2009['year'] = '2009'
df_2009 = df_2009.rename(columns={'法人代码': "id"})
df_raw = df_raw.append(df_2009, sort = False)
```

```python
def compare_raw_Brandt(raw):
    """
    function to compare the raw count
    from Brandt paper
    """

    df_raw = raw.dropna(subset=["id"])
    df_raw_count = df_raw.groupby("year")["id"].count()
    
    if len(df_raw_count.index) == 10:
        s = [165118,162033,162883,169030,
             181557,196222,279092,271835,
             301961,336768,#327853,np.nan,
                    ]
    elif len(df_raw_count.index) == 11:
        s = [165118,162033,162883,169030,
             181557,196222,279092,271835,
             301961,336768,327853#,np.nan,
                    ]
    else:
        s = [165118,162033,162883,169030,
             181557,196222,279092,271835,
             301961,336768,327853,np.nan,
                    ]

    temp = (
        pd.concat(
            [
                pd.Series(
                    s
                    ,
                    index=df_raw_count.index,
                    name="Brandt",
                ),
                df_raw_count,
            ],
            axis=1,
        )
        # .reset_index()
        # .apply(lambda x: x['Brandt'] - x['id'], axis = 1)
    )
    
    temp['diff'] = temp['Brandt'] - temp['id']
    
    #temp.iloc[:,[2]].plot(kind='bar')
    
    return temp.iloc[:,:2].plot(kind='bar',  figsize=(10,8))
```

```python
compare_raw_Brandt(raw= df_raw)
```

## Prepare Data

### 1998 - 2007

We specify the type for each variable so that we avoid the warning DtypeWarning: Columns (X) have mixed types.

We load equity share, output, employement, export, fa_original, fa_net, profit a_dep, c_dep, wage, input as object because so observations have formating issue (infinity, NaN, especially for year 2005, 2006, 2007). We tackle it in the prepareData function.

The program runs for about 12 minutes to clean up the data

```python
test = pd.read_csv(
       os.path.join(LOCAL_PATH_CONFIG_FILE, 'stata_year_1998.csv', low_memory=False),
        )
```

```python
test.head()
```

Check for duplicates


# Analysis

The notebook file already contains code to analyse the dataset. It contains codes to count the number of observations for a given variables, for a group and a pair of group. It also has queries to provide the distribution for a single column, for a group and a pair of group. The queries are available in the key `ANALYSIS`


## Categorical Description

During the categorical analysis, we wil count the number of observations for a given group and for a pair.

**Count obs by group**

- Index: primary group
- nb_obs: Number of observations per primary group value
- percentage: Percentage of observation per primary group value over the total number of observations

Returns the top 20 only


# FILENAME 1

```python
path_file = os.path.join(LOCAL_PATH_CONFIG_FILE, os.path.split(FILES_TO_UPLOAD[0])[1])
df_test = pd.read_csv(path_file)
```

Get the values fior each object

```python
dic_ = {'var': [],
       'count':[],
       'values': []}
for v in df_test.select_dtypes(include='object').columns:
    cat = df_test[v].nunique()
    value_cat  = df_test[v].unique()
    dic_['var'].append(v)
    dic_['count'].append(cat)
    dic_['values'].append(value_cat)
(pd.DataFrame(dic_)
 .sort_values(by = ['count'], ascending = False)
 .set_index('var')
)
```

Number of missing values

```python
(
    pd.concat([
    df_test.isna().sum().sort_values().rename("count"),
    (df_test.isna().sum().sort_values()/len(df_test)).rename("pct")
    ], axis = 1
    ).loc[lambda x: x['count']!=0]
    .style
    .format("{0:,.2%}", subset=["pct"], na_rep="-")
    .bar(subset=["count"], color="#d65f5f")
)
```

Frequency 

```python
for objects in list(df_test.select_dtypes(include=["string", "object"]).columns):
    df_count = df_test.stb.freq([objects])
    if df_count.shape[0] > 20:
        df_count = df_count.iloc[:20, :]
    display(
        (
            df_count.reset_index(drop=True)
            .style
            .format(
                "{0:,.2%}", subset=["Percent", "Cumulative Percent"], na_rep="-"
            )
            .bar(subset=["Cumulative Percent"], color="#d65f5f")
        )
    )
```

## Count obs by one key pair

You need to pass the primary group in the cell below

- Index: primary group
- Columns: Secondary key -> All the categorical variables in the dataset
- nb_obs: Number of observations per primary group value
- Total: Total number of observations per primary group value (sum by row)
- percentage: Percentage of observations per primary group value over the total number of observations per primary group value (sum by row)

Returns the top 20 only

```python
primary_key = ""
```

```python
for objects in list(df_test.select_dtypes(include=["string", "object"]).columns):
    if objects not in [primary_key]:
        df_count = df_test.stb.freq([objects])
        if df_count.shape[0] > 20:
            df_count = df_count.iloc[:20, :]
        display(
            (
                df_test.stb.freq([primary_key, objects])
                .set_index([primary_key, objects])
                .drop(columns=['Cumulative Count', 'Cumulative Percent'])
                .iloc[:20, :]
                .unstack(-1)
                .style
                .format(
                    "{0:,.2%}", subset=["Percent"], na_rep="-"
                )
                .format(
                    "{0:,.2f}", subset=["Count"], na_rep="-"
                )
                .background_gradient(
                    cmap=sns.light_palette("green", as_cmap=True), subset=("Count")
                )

            )
        )
```

## Continuous description

There are three possibilities to show the ditribution of a continuous variables:

- Display the percentile
- Display the percentile, with one primary key
- Display the percentile, with one primary key, and a secondary key

```python
(
    df_test
    .describe()
    .style.format("{0:.2f}")
)
```

### 2. Display the percentile, with one primary key

The primary key will be passed to all the continuous variables

- index: 
    - Primary group
    - Percentile [.25, .50, .75, .95, .90] per primary group value
- Columns: Secondary group
- Heatmap is colored based on the row, ie darker blue indicates larger values for a given row

```python
primary_key = ""
```

```python
for objects in list(df_test.select_dtypes(exclude=["string", "object", 'boolean', 'datetime64[ns]']).columns):
    if objects not in [primary_key]:
        
        print("\nDistribution of {} by {}\n".format(objects, primary_key))
        
        display(
            (
                df_test
                .groupby(primary_key)
                .describe()[objects]
                .sort_values(by='count', ascending=False)
                .iloc[:20, :]
                .style.format("{0:.2f}")
            )
        )
```

## Statistical Analysis

In this section, we are going to perform:

- Chi square test
- Anova test

To see if there is any dependence between the primary key, and the other variables.

Each statistic is saved in the folder `statistical_analysis`

### Chi square test

There are two types of chi-square tests. Both use the chi-square statistic and distribution for different purposes:

- A chi-square goodness of fit test determines if a sample data matches a population. For more details on this type, see: Goodness of Fit Test.
- A chi-square test for independence compares two variables in a contingency table to see if they are related. In a more general sense, it tests to see whether distributions of categorical variables differ from each another.
    - A very small chi square test statistic means that your observed data fits your expected data extremely well. In other words, there is a relationship.
    - A very large chi square test statistic means that the data does not fit very well. In other words, there isn’t a relationship
    
The formula for the chi-square statistic used in the chi square test is:

$$
\chi_{c}^{2}=\sum \frac{\left(O_{i}-E_{i}\right)^{2}}{E_{i}}
$$

The subscript $c$ are the degrees of freedom. $O$ is your observed value and $E$ is your expected value.

A low value for chi-square means there is a high correlation between your two sets of data. In theory, if your observed and expected values were equal ("no difference") then chi-square would be zero — an event that is unlikely to happen in real life

### Anova

An ANOVA test is a way to find out if survey or experiment results are significant. In other words, they help you to figure out if you need to reject the null hypothesis or accept the alternate hypothesis.

Basically, you’re testing groups to see if there’s a difference between them. Examples of when you might want to test different groups:

- A group of psychiatric patients are trying three different therapies: counseling, medication and biofeedback. You want to see if one therapy is better than the others.
- A manufacturer has two different processes to make light bulbs. They want to know if one process is better than the other.
Students from different colleges take the same exam. You want to see if one college outperforms the other.

Source: 

- [Chi-square](https://www.statisticshowto.com/probability-and-statistics/chi-square/)
- [Anova](https://www.statisticshowto.com/probability-and-statistics/hypothesis-testing/anova/)

```python
from scipy.stats import chi2_contingency
from scipy.stats import chi2
from statsmodels.stats.multicomp import MultiComparison
import scipy.stats as stats
```

A README is automatically generated, and is available at:

```python
os.path.join("https://github.com", github_owner, github_owner, "tree/master/00_data_catalogue/statistical_analysis")
```

By default, use 10% probability

```python
proba = .9
```

```python
dic_tables = {}

to_include_cat = []
to_include_cont = []

feat_obj = list(df_test.select_dtypes(include=['object']))
feat_cont = list(df_test.select_dtypes(
    exclude=["string", "object", 'boolean', 'datetime64[ns]']))

readme_chi_square_middle_1 = """

# Chi square

"""

readme_anova_middle_1 = """

# Anova

"""

# CHI SQUARE

for col in feat_obj:
    table = pd.crosstab(df_test[primary_key],
                        df_test[col],
                        margins=False)
    if table.shape[1] > 1:
        stat, p, dof, expected = chi2_contingency(table)
        critical = chi2.ppf(proba, dof)

        if abs(stat) >= critical:
            to_include_cat.append('PO Sub Type')
            result = 'Dependent (reject H0)'
            to_include_cat.append(col)
        else:
            result = 'Independent (fail to reject H0)'

        dic_results = {
            'test': 'Chi Square',
            'primary_key': primary_key,
            'secondary_key': col,
            'statistic': stat,
            'p_value': p,
            'dof': dof,
            'critical': critical,
            'result': result
        }

        dic_tables[col] = dic_results

        # Tables
        total_obs = table.sum(axis=0).sum()
        cont_table = (
            table.assign(total_rows=lambda x: x.sum(axis=1))
            .append(table.sum(axis=0).rename('total_columns'))
            .fillna(total_obs)
        )
        dic_contengency = {

            'contengency': cont_table.to_json(),
            'pearson_residual': ((table - expected) / np.sqrt(expected)).to_json(),
            'pct_row': (table.apply(lambda r: r / r.sum(), axis=1)).to_json(),
            'pct_columns': (table.apply(lambda r: r / r.sum(), axis=0)).to_json(),
            'pct_total': (table.apply(lambda r: r / total_obs)).to_json()
        }

        path_name = os.path.join(
            LOCAL_PATH_CATALOGUE, "statistical_analysis", 'chi-square', col.replace('/', ''))
        with open('{}.json'.format(path_name), "w") as outfile:
            json.dump(dic_contengency, outfile)

        if cont_table.shape[1] > 20:
            cont_table = cont_table.iloc[:, np.r_[:10, -10:-1, -1]]
            is_full = 'Troncated, only first/last 10 columns'
        else:
            is_full = 'Full table'
        readme_chi_square_middle_2 = """

### {0}

- Results between {0} and {1}: {2}
- Contengency table ({4}):

{3}

        """.format(col, primary_key, result, cont_table.to_markdown(), is_full)

        readme_chi_square_middle_1 += readme_chi_square_middle_2

for col in feat_cont:
    result = df_test.groupby(primary_key)[col].apply(list)
    F, p = stats.f_oneway(*result)
    if p <= 1 - proba:
        result = 'Dependent (fail to reject H0)'
        to_include_cont.append(col)
        
    else:
        result = 'Independent (reject H0)'

    dic_results = {
        'test': 'Anova',
        'primary_key': primary_key,
        'secondary_key': col,
        'statistic': F,
        'p_value': p,
        'result': result
    }

    dic_tables[col] = dic_results

    readme_anova_middle_2 = """
    
### {0}
    
- Results between {0} and {1}: {2}
    
    """.format(col, primary_key, result)

    readme_anova_middle_1 += readme_anova_middle_2
    
full_table = (
    pd.DataFrame(dic_tables).T
    .sort_values(by = ['test', 'result'])
    .assign(
        statistic = lambda x: np.round(x['statistic'].astype('float'), 2),
        dof = lambda x: np.round(x['dof'].astype('float'), 2),
        critical = lambda x: np.round(x['critical'].astype('float'), 2),
        p_value = lambda x: np.round(x['p_value'].astype('float'), 2),
    )
    #
)

readme_top = """
# Statistical Analysis 

The primary key is {0}

The full results are listed below:

{1}

List of relevant variables:

""".format(primary_key, full_table.fillna('-').to_markdown())

# Save README
to_include = to_include_cat + to_include_cont
for i, val in enumerate(to_include):
    relevant_var = "{}. {}\n".format(i+1, val)
    readme_top += relevant_var

path_readme = os.path.join(
    LOCAL_PATH_CATALOGUE, 'statistical_analysis', "README.md")
with open(path_readme, "w") as outfile:
    outfile.write(readme_top + readme_chi_square_middle_1 +
                  readme_anova_middle_1)
```

```python
(
    full_table
    .style
                .format(
                    "{0:,.2%}", subset=["p_value"], na_rep="-"
                )
                .format(
                    "{0:,.2f}", subset=["statistic", "dof", 'critical'], na_rep="-"
                )
)
```

To visualize in more detail the contency table, you can use the function `contengency_table`. The function parses the folder `statistical_analysis/chi-square`. Five tables are generated:

- Contengency table full: contengency
- Pearson contribution: pearson_residual
- Centengency table percentage row-wise: pct_row
- Centengency table percentage column-wise: pct_columns
- Centengency table percentage full: pct_total

```python
def read_contengency(filename, option='contengency', style=True):
    """
    Read the contengency table
    filename: Filename to load, including `.json`. 
    Check the folder `statistical_analysis/chi-square`  to get the name
    """
    path_name = os.path.join(LOCAL_PATH_CATALOGUE,
                             "statistical_analysis", 'chi-square', filename)

    with open(path_name, 'r') as fp:
        table = json.load(fp)

    if option in ['pct_row', 'pct_columns', 'pct_total']:

        table = pd.read_json(table[option])

        if style:
            table = (
                table
                .style
                .format(
                    "{0:,.2%}", na_rep="-"
                )
                .background_gradient(
                    cmap=sns.light_palette("green", as_cmap=True)
                )
            )

        return table
    else:
        table = pd.read_json(table[option])
        if style:
            table = (table.style
                     .background_gradient(
                         cmap=sns.light_palette("green", as_cmap=True)
                     )
                     )
        return table
```

```python
filename = ''
read_contengency(filename, option = 'pct_columns')
```

## Graphs

- Heatmap
- Diverging bar
- Scatter plot
- Correspondance analysis

### 
- heatmap, code by [Seaborn](https://seaborn.pydata.org/examples/many_pairwise_correlations.html)

```python
sns.set_theme(style="white")

# Generate a large random dataset
d = df_test.select_dtypes(
    exclude=["string", "object", 'boolean', 'datetime64[ns]'])

# Compute the correlation matrix
corr = d.corr()

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

## Scatterplot

From the correlation plot above, pick up a $y$ variables.

We only plot the variables that succeed the Anova test, minus the $y$ var

```python
y_var = ''
for i, col in enumerate(to_include_cont):
    if col != y_var:
        #plt.figure(i)
        f, ax = plt.subplots(figsize=(7, 7))
        ax.set(xscale="log", yscale="log")
        (sns.regplot(x=col, y=y_var, data=df_test, ax=ax, scatter_kws={"s": 100})
         .set_title('Scatterplot between {} and {}'.format(y_var, col))
        )
```

### Diverging bars

The diverging bar plot is plotting for the variables to succeed the Anova test

```python
for col  in to_include_cont:
    df_test_ = df_test.groupby([primary_key])[col].mean().reset_index()
    df_test_['mean'] = (df_test_[col] - df_test_[col].mean())/df_test_[col].std()
    df_test_['colors'] = ['red' if x < 0 else 'green' for x in df_test_['mean']]
    df_test_.sort_values('mean', inplace=True)
    df_test_.reset_index(inplace=True)
    # Draw plot
    plt.figure(figsize=(14, 10), dpi=80)
    plt.hlines(y=df_test_.index, xmin=0, xmax=df_test_['mean'],
               color=df_test_['colors'], alpha=0.4, linewidth=5)
    # Decorations
    text = "Diverging Bars of {} within {} ".format(col, primary_key)
    plt.gca().set(ylabel=primary_key, xlabel=col)
    plt.yticks(df_test_.index, df_test_[primary_key], fontsize=12)
    plt.title(text, fontdict={'size': 20})
    plt.grid(linestyle='--', alpha=0.5)
    plt.show()
```

### Correspondance analysis

We created a Python library to make a correspondance analysis. Please, refers to [https://github.com/thomaspernet/Correspondence_analysis](https://github.com/thomaspernet/Correspondence_analysis/blob/master/CorrespondenceAnalysisPy/correspondence_analysis_computation/ca_compute.py) for the codes

```python
from CorrespondenceAnalysisPy.correspondence_analysis_computation import ca_compute
```

```python
for var in to_include_cat:
    name = '{}.json'.format(var)
    try:
        tb = read_contengency(filename=name, option='contengency', style=False)
        ca = ca_compute.compute_ca(
            (
                tb
                .iloc[:-1, :-1]
            )
        )
        ca_computed = ca.correspondance_analysis()
        fig_2 = ca_compute.row_focus_coordinates(
            df_x=ca_computed['pc_rows'],
            df_y=ca_computed['pc_columns'],
            variance_explained=ca_computed['variance_explained'],
            export_data=True)
    except:
        pass
```

```python jupyter={"source_hidden": true}

ca = ca_compute.compute_ca(
(
    read_contengency(filename = 'PO Sub Type.json', option = 'contengency', style = False)
    .iloc[:-1, :-1]
)
)
ca_computed = ca.correspondance_analysis()
fig_2 = ca_compute.row_focus_coordinates(
                df_x=ca_computed['pc_rows'],
                df_y=ca_computed['pc_columns'],
                variance_explained=ca_computed['variance_explained'],
                export_data=True)
```

<!-- #region nteract={"transient": {"deleting": false}} -->
# Generate reports
<!-- #endregion -->

```python nteract={"transient": {"deleting": false}} outputExpanded=false
import os, time, shutil, urllib, ipykernel, json
from pathlib import Path
from notebook import notebookapp
```

```python nteract={"transient": {"deleting": false}} outputExpanded=false
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

```python nteract={"transient": {"deleting": false}} outputExpanded=false
create_report(extension = "html", keep_code = False)
```
