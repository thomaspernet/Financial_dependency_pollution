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

# NOTEBOOK NAME FROM CODA TASK

Copy paste from Coda to fill the information



# Connexion server

```python
from awsPy.aws_authorization import aws_connector
from awsPy.aws_s3 import service_s3
from pathlib import Path
import pandas as pd
import numpy as np
import seaborn as sns
import os, shutil

path = os.getcwd()
parent_path = str(Path(path).parent.parent.parent)


name_credential = 'XXX_credentials.csv'
region = ''
bucket = ''
path_cred = "{0}/creds/{1}".format(parent_path, name_credential)
```

```python
con = aws_connector.aws_instantiate(credential = path_cred,
                                       region = region)
client= con.client_boto()
s3 = service_s3.connect_S3(client = client,
                      bucket = bucket, verbose = False)
```

```python
pandas_setting = True
if pandas_setting:
    cm = sns.light_palette("green", as_cmap=True)
    pd.set_option('display.max_columns', None)
    pd.set_option('display.max_colwidth', None)
```

```python
import csv
creds = []
with open(path_cred, 'r') as file:
    reader = csv.reader(file)
    for row in reader:
        creds.append(row)
```

```python
from dask_cloudprovider import FargateCluster
cluster = FargateCluster(
    aws_access_key_id=creds[1][0], 
    aws_secret_access_key=creds[1][1],   
                         skip_cleanup=True,
    scheduler_timeout = '10 minutes',
    n_workers=1,
    scheduler_cpu = 4096,
    scheduler_mem= 24576,
    worker_cpu = 4096,
    worker_mem = 24576,
                         #threads_per_worker=4,
                         image='thomaspernet/dask-container:py-38'
                         #thomaspernet/dask_fargate
                         #rsignell/pangeo-worker:2020-01-23b
    
                        )
#cluster.scale(5)
client = Client(cluster)
client
```

```python
#### X train
X_train= (
    dd.read_csv('s3://',
               storage_options={'anon': False,
                                 "key":creds[1][0],
                                "secret":creds[1][1]},
                low_memory=False,
               #dtype=dtypes
               )   
    #.categorize()
)
```

```python nteract={"transient": {"deleting": false}} outputExpanded=false
pd.set_option('display.max_columns', None)
```

```python nteract={"transient": {"deleting": false}} outputExpanded=false

```

## Analysis 1: 

```python
import matplotlib.pyplot as plt
import seaborn as sns
#import echotorch as etnn
from sklearn.metrics import mean_squared_error, mean_absolute_error
import tensorflow as tf
from sklearn.preprocessing import MinMaxScaler
from datetime import datetime, timedelta
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
create_report(extension = "html")
```
