# Prepare Data

Information related to the source of data



## Prepare ASIF data to S3

- Description: 

Prepare (cleaning  & removing unwanted rows) ASIF data using Athena and save output to S3 + Glue. 

- Tables: 

[DATA/ECON/FIRM_SURVEY/ASIF_CHINA/PREPARED](https://coda.io/d/_dY_ZokB7AWF#DATALAKE_tuQRD/r118&modal=true)[asif_firms_prepared](https://coda.io/d/_dY_ZokB7AWF#DATALAKE_tuQRD/r119&modal=true)

- Github URL (parents): https://github.com/thomaspernet/Financial_dependency_pollution/blob/master/01_data_preprocessing/01_prepare_tables/00_prepare_asif.md

### Explanation

1.  Keeping year 1998 to 2007
2. Clean citycode → Only 4 digits
3. Clean setup → replace to Null all values with a length lower/higher than 4 
4. Remove rows when cic is unknown
5. Remove firms ID not matching digit format
6. Remove duplicates by firm-year

