# Transform Data

Information related to the source of data

## Transform ASIF data by constructing financial variables raw data data to S3

- Description: 

Transform (creating financial variables) ASIF data using Athena and save output to S3 + Glue. 

- Tables: 

[DATA/ECON/FIRM_SURVEY/ASIF_CHINA/TRANSFORMED/FINANCIAL_RATIO](https://coda.io/d/_dY_ZokB7AWF#DATALAKE_tuQRD/r120&modal=true)[asif_city_industry_financial_ratio](https://coda.io/d/_dY_ZokB7AWF#DATALAKE_tuQRD/r121&modal=true)

- Github URL (parents): https://github.com/thomaspernet/Financial_dependency_pollution/blob/master/01_data_preprocessing/02_transform_tables/00_asif_financial_ratio.md

### Explanation

1.  Construct financial ratio at the city-industry-year, city-industry, industry level
2.  Add consistent city code to avoid duplicates

## Transform (creating time break variables) financial ratio data and merging pollution tables to S3

- Description: 

Transform (creating time-break variables and fixed effect) asif_financial_ratio data and merging pollution, industry and city mandate tables using Athena and save output to S3 + Glue. 

- Tables: 

[DATA/ENVIRONMENT/CHINA/FYP/FINANCIAL_CONTRAINT/PAPER_FYP_FINANCE_POL/BASELINE](https://coda.io/d/_dY_ZokB7AWF#DATALAKE_tuQRD/r122&modal=true)[fin_dep_pollution_baseline](https://coda.io/d/_dY_ZokB7AWF#DATALAKE_tuQRD/r123&modal=true)

- Github URL (parents): https://github.com/thomaspernet/Financial_dependency_pollution/blob/master/01_data_preprocessing/02_transform_tables/01_fin_dep_pol_baseline.md

### Explanation

1.  Merge financial ratio with the pollution, city mandate, tcz table
2. Filter year 2001 to 2007 and add time break
3.  Create fixed effect 