# Data Preprocessing

The data preparation is composed by three steps:

1. Download the raw data
   - Folder `00_download_data_from_`
2. Prepare the raw data
   1. Folder `01_prepare_tables` 
      1. A subfolder `00_POC_prepare_tables` is available to make some tests before finalizing the preparation of the raw data -> Push the queries to the ETL json file
3. Prepare the data for the model
   1. Folder `02_prepare_tables_model` 



The methodology to prepare the ETL is broken down into two parts: first of all, we write and test the queries (Python or SQL). Then, we move the codes to the pipeline (ETL). 

The first part happens in all notebooks located in subfolder with a prefix `POC` . Choose the Notebook you need, `from_S3` or `from_Athena` . The purpose of those notebooks is to write consistent codes to prepare the ETL **but** with a high level of details. A notebook should not contains tons of codes, rather a notebook should be view as one brick to finalise the ETL. In fact, each notebook is linked to an US, and an US should not last more than 3/4 days. In a case of a very long US, with a very long notebook, you loose the spirit of the method, and increase the possibility of making an error. Hence, the subfolder `POC` can have one or more notebooks, depending on the number of steps. 

Secondly, you can move the codes in the notebook(s) located at the root. These notebooks are consistent with the ETL and not meant for testing. Inside these notebook, you will find a way to add your comments to Glue, generate a `README` with the data catalog (in the folder `00_data_catalogue` ). More specifically, these notebooks are cleaner version of the ETL, and the codes are stored in a JSON files so that it is easier to move them to deployment. You can create one or more notebook, depending on the size of the ETL. If the ETL has many steps, it is recommended to generate one notebook per step (mirroring the `POC`  subfolder), but if the ETL is simple and one or two steps needed, one notebook should be enough. Once again, the creation of those notebooks are linked to an US, and validate the ETL steps (i.e. add the step to the ETL graph and documentation). 



In a nutshell:

1. Create a notebook in the `POC`subfolder to tests your ETL, codes
   1. Link this notebook to the root folder to create a cleaner version of the ETL
      1. Repeat 1 and 2 until the ETL is finalised 



The ETL makes use of a JSON file with three keys, one for the creation of tables, one for the transformation of tables and a last key for analysing the tables. This JSON file is used to bring the queries to the ETL, say differently in production. All queries in the JSON files are made to be deployed in the workflow. 

Before to push the queries to the JSON files, we strongly recommend you to create and validate the queries in notebooks saved in the subfolder `00_POC_prepare_tables`. Each notebook is referenced by an US. Therefore, there is a first batch of US meant to design and validate the queries. A second batch of US has the objective of bringing the queries to the JSON files (i.e. the ETL). These notebooks will be in the root directory, not in the POC subdirectory. 



Each child folder has a folder named `Reports` to host HTML report from the notebook. It is recommended to saved the notebook in `md` format for versioning purpose, reset the notebook to avoid overload the notebook and generate an HTML (with or without the code) to ease the reading.



# Data Preparation Workflow

Use the content of the page `ETL and model` in Coda.