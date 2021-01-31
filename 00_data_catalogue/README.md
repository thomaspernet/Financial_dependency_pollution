
# Data Catalogue



## Table of Content

    
- [asif_unzip_data_csv](https://github.com/thomaspernet/Financial_dependency_pollution/tree/master/00_data_catalogue#table-asif_unzip_data_csv)
- [ind_cic_2_name](https://github.com/thomaspernet/Financial_dependency_pollution/tree/master/00_data_catalogue#table-ind_cic_2_name)
- [china_city_code_normalised](https://github.com/thomaspernet/Financial_dependency_pollution/tree/master/00_data_catalogue#table-china_city_code_normalised)
- [china_city_reduction_mandate](https://github.com/thomaspernet/Financial_dependency_pollution/tree/master/00_data_catalogue#table-china_city_reduction_mandate)
- [china_city_sector_pollution](https://github.com/thomaspernet/Financial_dependency_pollution/tree/master/00_data_catalogue#table-china_city_sector_pollution)
- [geo_chinese_province_location](https://github.com/thomaspernet/Financial_dependency_pollution/tree/master/00_data_catalogue#table-geo_chinese_province_location)
- [china_city_tcz_spz](https://github.com/thomaspernet/Financial_dependency_pollution/tree/master/00_data_catalogue#table-china_city_tcz_spz)
- [china_credit_constraint](https://github.com/thomaspernet/Financial_dependency_pollution/tree/master/00_data_catalogue#table-china_credit_constraint)
- [asif_firms_prepared](https://github.com/thomaspernet/Financial_dependency_pollution/tree/master/00_data_catalogue#table-asif_firms_prepared)
- [asif_industry_financial_ratio_city](https://github.com/thomaspernet/Financial_dependency_pollution/tree/master/00_data_catalogue#table-asif_industry_financial_ratio_city)
- [china_sector_pollution_threshold](https://github.com/thomaspernet/Financial_dependency_pollution/tree/master/00_data_catalogue#table-china_sector_pollution_threshold)
- [asif_industry_financial_ratio_industry](https://github.com/thomaspernet/Financial_dependency_pollution/tree/master/00_data_catalogue#table-asif_industry_financial_ratio_industry)
- [fin_dep_pollution_baseline_industry](https://github.com/thomaspernet/Financial_dependency_pollution/tree/master/00_data_catalogue#table-fin_dep_pollution_baseline_industry)
- [asif_financial_ratio_baseline_firm](https://github.com/thomaspernet/Financial_dependency_pollution/tree/master/00_data_catalogue#table-asif_financial_ratio_baseline_firm)
- [asif_industry_characteristics_ownership](https://github.com/thomaspernet/Financial_dependency_pollution/tree/master/00_data_catalogue#table-asif_industry_characteristics_ownership)
- [asif_tfp_credit_constraint](https://github.com/thomaspernet/Financial_dependency_pollution/tree/master/00_data_catalogue#table-asif_tfp_credit_constraint)
- [fin_dep_pollution_baseline_city](https://github.com/thomaspernet/Financial_dependency_pollution/tree/master/00_data_catalogue#table-fin_dep_pollution_baseline_city)

    

## Table asif_unzip_data_csv

- Database: firms_survey
- S3uri: `s3://datalake-datascience/DATA/ECON/FIRM_SURVEY/ASIF_CHINA/UNZIP_DATA_CSV`
- Partitition: []

|     | Name           | Type   | Comment                                                                                        |
|----:|:---------------|:-------|:-----------------------------------------------------------------------------------------------|
|   0 | firm           | string | firm ID                                                                                        |
|   1 | year           | string | balance sheet year                                                                             |
|   2 | export         | int    | Where: export delivery value                                                                   |
|   3 | dq             | string | Province county code                                                                           |
|   4 | name           | string | Corporate Name                                                                                 |
|   5 | town           | string | Township (town)                                                                                |
|   6 | village        | string | Street (village), house number                                                                 |
|   7 | street         | string | Street office                                                                                  |
|   8 | c15            | string | Community (neighborhood) and village committee                                                 |
|   9 | zip            | string | Zip code                                                                                       |
|  10 | product1_      | string | The main products (1)                                                                          |
|  11 | c26            | string | The main products (2)                                                                          |
|  12 | c27            | string | The main products (3)                                                                          |
|  13 | cic            | string | Industry Code                                                                                  |
|  14 | type           | string | Registration Type                                                                              |
|  15 | c44            | string | Holding case                                                                                   |
|  16 | c45            | string | Affiliation                                                                                    |
|  17 | setup          | string | year of creation                                                                               |
|  18 | c47            | int    | Opening (established) time - months                                                            |
|  19 | c60            | int    | Code-scale enterprises                                                                         |
|  20 | c61            | int    | Light and heavy industrial codes                                                               |
|  21 | employ         | int    | Employement                                                                                    |
|  22 | c69            | int    | Industrial output (constant prices)                                                            |
|  23 | output         | int    | Industrial output value (current prices)                                                       |
|  24 | new_product    | int    | Where: New product output value                                                                |
|  25 | c74            | int    | Industrial sales output value (current prices)                                                 |
|  26 | addval         | int    | Value added                                                                                    |
|  27 | cuasset        | int    | Current asset                                                                                  |
|  28 | c80            | int    | Accounts Receivable                                                                            |
|  29 | c81            | int    | stock                                                                                          |
|  30 | c82            | int    | Wherein: Finished                                                                              |
|  31 | c83            | int    | The average balance of current assets                                                          |
|  32 | c84            | int    | Long-term investment                                                                           |
|  33 | c85            | int    | Total fixed assets (deprecated)                                                                |
|  34 | tofixed        | int    | Total fixed assets (to use)                                                                    |
|  35 | c87            | int    | Where: the production and operation with                                                       |
|  36 | todepre        | int    | total depreciation                                                                             |
|  37 | cudepre        | int    | current depreciation                                                                           |
|  38 | netfixed       | int    | net fixed asset                                                                                |
|  39 | c91            | int    | Intangible and Deferred                                                                        |
|  40 | c92            | int    | Intangible assets                                                                              |
|  41 | toasset        | int    | Total asset                                                                                    |
|  42 | c95            | int    | Total current liabilities                                                                      |
|  43 | c97            | int    | Total long-term liabilities                                                                    |
|  44 | c98            | int    | Total Liabilities                                                                              |
|  45 | c99            | int    | Total shareholders' equity                                                                     |
|  46 | captal         | int    | Capital                                                                                        |
|  47 | e_state        | int    | Where: National Capital                                                                        |
|  48 | e_collective   | int    | Collective capital                                                                             |
|  49 | e_legal_person | int    | Corporate capital                                                                              |
|  50 | e_individual   | int    | Personal Capital                                                                               |
|  51 | e_HMT          | int    | Hong Kong, Macao and Taiwan capital                                                            |
|  52 | e_foreign      | int    | Foreign capital                                                                                |
|  53 | sales          | int    | Sales revenue                                                                                  |
|  54 | c108           | int    | Main business cost                                                                             |
|  55 | c113           | int    | Operating expenses                                                                             |
|  56 | c110           | int    | The main business tax and surcharges                                                           |
|  57 | c111           | int    | sales profit                                                                                   |
|  58 | c114           | int    | Management fees                                                                                |
|  59 | c115           | int    | Where: Taxes                                                                                   |
|  60 | c116           | int    | Property insurance                                                                             |
|  61 | c118           | int    | Labor, unemployment insurance                                                                  |
|  62 | c124           | int    | Financial expenses                                                                             |
|  63 | c125           | int    | Of which: Interest expense                                                                     |
|  64 | profit         | int    | operating profit                                                                               |
|  65 | c128           | int    | Subsidy income                                                                                 |
|  66 | c131           | int    | The total profit                                                                               |
|  67 | c132           | int    | Total loss                                                                                     |
|  68 | c133           | int    | Total profit and tax                                                                           |
|  69 | c134           | int    | Income tax payable                                                                             |
|  70 | c136           | int    | Profit payable                                                                                 |
|  71 | wage           | int    | Wages of the year                                                                              |
|  72 | c140           | int    | Where: Main Wages of business                                                                  |
|  73 | c141           | int    | This year the total amount of welfare payable (credit accumulated amount)                      |
|  74 | c142           | int    | Where: Main business cope with the total welfare                                               |
|  75 | c143           | int    | Payable VAT                                                                                    |
|  76 | c144           | int    | Input tax year                                                                                 |
|  77 | c145           | int    | Output tax year                                                                                |
|  78 | midput         | int    | intermediate inputs                                                                            |
|  79 | c62            | int    | Persons Employed Total Total _                                                                 |
|  80 | c147           | int    | Taxes (processing)                                                                             |
|  81 | c64            | int    | Total annual revenue                                                                           |
|  82 | c65            | int    | Where: Main business revenue                                                                   |
|  83 | c93            | int    | Total assets 318                                                                               |
|  84 | c16            | int    | Provinces                                                                                      |
|  85 | c9             | string | State / Province / State)                                                                      |
|  86 | c10            | string | The (autonomous regions and municipalities, prefectures and leagues)                           |
|  87 | c11            | string | Counties (districts, cities, flag)                                                             |
|  88 | c17            | string | Area Code                                                                                      |
|  89 | c29            | string | Registration (or approval) authority - the administrative department for industry and commerce |
|  90 | c167           | string | Founded union situation                                                                        |
|  91 | c168           | int    | unionized                                                                                      |
|  92 | v90            | int    | number_unionized                                                                               |
|  93 | c79            | int    | short_term_investments                                                                         |
|  94 | c96            | int    | accounts_Payable                                                                               |
|  95 | c117           | int    | travel                                                                                         |
|  96 | c119           | int    | union_funds                                                                                    |
|  97 | c121           | int    | administrative_expenses                                                                        |
|  98 | trainfee       | int    | trainfee                                                                                       |
|  99 | c123           | int    | sewage_charges                                                                                 |
| 100 | c127           | int    | investment_income                                                                              |
| 101 | c135           | int    | advertising_fee                                                                                |
| 102 | c120           | int    | pension_medical_insurance                                                                      |
| 103 | c138           | int    | housing_fund                                                                                   |
| 104 | c148           | int    | direct_aterial                                                                                 |
| 105 | c149           | int    | intermediate_inputs                                                                            |
| 106 | c150           | int    | middle_management_fees_investment                                                              |
| 107 | c151           | int    | operating_expenses                                                                             |
| 108 | rdfee          | int    | rdfee                                                                                          |
| 109 | c156           | string | Light and heavy industrial Name                                                                |
| 110 | c157           | string | Scale enterprises name                                                                         |
| 111 | citycode       | string | City code clean by Zhao Ruili using adress                                                     |
| 112 | prov           | string |                                                                                                |

    

## Table ind_cic_2_name

- Database: chinese_lookup
- S3uri: `s3://datalake-datascience/DATA/ECON/LOOKUP_DATA/CIC_2_NAME`
- Partitition: []

|    | Name          | Type   | Comment                      |
|---:|:--------------|:-------|:-----------------------------|
|  0 | cic           | string | 2 digits industry code       |
|  1 | industry_name | string | 2 digits industry name       |
|  2 | short         | string | 2 digits industry short name |

    

## Table china_city_code_normalised

- Database: chinese_lookup
- S3uri: `s3://datalake-datascience/DATA/ECON/LOOKUP_DATA/CITY_CODE_NORMALISED`
- Partitition: []

|    | Name           | Type   | Comment                          |
|---:|:---------------|:-------|:---------------------------------|
|  0 | extra_code     | string | All available city codes         |
|  1 | geocode4_corr  | string | Normalised city code             |
|  2 | citycn         | string | City name in Chinese             |
|  3 | cityen         | string | City name in English             |
|  4 | citycn_correct | string | City name in Chinese, normalized |
|  5 | cityen_correct | string | City name in English, normalized |
|  6 | province_cn    | string | Province name in Chinese         |
|  7 | province_en    | string | Province name in English         |

    

## Table china_city_reduction_mandate

- Database: policy
- S3uri: `s3://datalake-datascience/DATA/ENVIRONMENT/CHINA/FYP/CITY_REDUCTION_MANDATE`
- Partitition: []

|    | Name                      | Type   | Comment                              |
|---:|:--------------------------|:-------|:-------------------------------------|
|  0 | citycn                    | string | City name in Chinese                 |
|  1 | cityen                    | string | City name in English                 |
|  2 | prov2013                  | string | Province name in Chinese             |
|  3 | province                  | string | Province name in English             |
|  4 | so2_05_city_reconstructed | float  | SO2 emission in 2005                 |
|  5 | so2_obj_2010              | float  | SO2 emission in 2010, objective      |
|  6 | tso2_mandate_c            | float  | city reduction mandate in tonnes     |
|  7 | so2_perc_reduction_c      | float  | city reduction mandate in percentage |
|  8 | ttoutput                  | float  | total output by city                 |
|  9 | in_10_000_tonnes          | float  | city reduction mandate in 10k tonnes |

    

## Table china_city_sector_pollution

- Database: environment
- S3uri: `s3://datalake-datascience/DATA/ENVIRONMENT/CHINA/CITY_SECTOR_POLLUTION`
- Partitition: []

|    | Name              | Type   | Comment                                                                 |
|---:|:------------------|:-------|:------------------------------------------------------------------------|
|  0 | year              | string |                                                                         |
|  1 | prov2013          | string | Province name in Chinese                                                |
|  2 | provinces         | string | Province name in English                                                |
|  3 | citycode          | string | citycode refers to geocode4corr                                         |
|  4 | citycn            | string | City name in Chinese                                                    |
|  5 | cityen            | string | City name in English                                                    |
|  6 | indus_code        | string | 4 digits industry code                                                  |
|  7 | ind2              | string | 2 digits industry code                                                  |
|  8 | ttoutput          | float  | Total output city sector                                                |
|  9 | twaste_water      | int    | Total waste water city sector                                           |
| 10 | tcod              | float  | Total COD city sector                                                   |
| 11 | tammonia_nitrogen | float  | Total Ammonia Nitrogen city sector                                      |
| 12 | twaste_gas        | int    | Total Waste gas city sector                                             |
| 13 | tso2              | int    | Total so2 city sector                                                   |
| 14 | tnox              | int    | Total NOx city sector                                                   |
| 15 | tsmoke_dust       | int    | Total smoke dust city sector                                            |
| 16 | tsoot             | int    | Total soot city sector                                                  |
| 17 | lower_location    | string | Location city. one of Coastal, Central, Northwest, Northeast, Southwest |
| 18 | larger_location   | string | Location city. one of Eastern, Central, Western                         |
| 19 | coastal           | string | City is bordered by sea or not                                          |

    

## Table geo_chinese_province_location

- Database: chinese_lookup
- S3uri: `s3://datalake-datascience/DATA/ECON/LOOKUP_DATA/CHINESE_PROVINCE_LOCATION`
- Partitition: []

|    | Name            | Type   | Comment                                                                 |
|---:|:----------------|:-------|:------------------------------------------------------------------------|
|  0 | prov2013        | string | Province name,  in Chinese                                              |
|  1 | province_en     | string | Province name,  in English                                              |
|  2 | lower_location  | string | Location city. one of Coastal, Central, Northwest, Northeast, Southwest |
|  3 | larger_location | string | Location city. one of Eastern, Central, Western                         |
|  4 | coastal         | string | City is bordered by sea or not                                          |

    

## Table china_city_tcz_spz

- Database: policy
- S3uri: `s3://datalake-datascience/DATA/ECON/POLICY/CHINA/STRUCTURAL_TRANSFORMATION/CITY_TARGET/TCZ_SPZ`
- Partitition: []

|    | Name          | Type   | Comment                         |
|---:|:--------------|:-------|:--------------------------------|
|  0 | province      | string | Province name                   |
|  1 | city          | string | City name                       |
|  2 | geocode4_corr | string | City code                       |
|  3 | tcz           | string | Two control zone policy city    |
|  4 | spz           | string | Special policy zone policy city |

    

## Table china_credit_constraint

- Database: industry
- S3uri: `s3://datalake-datascience/DATA/ECON/INDUSTRY/ADDITIONAL_DATA/CHINA/CIC/CREDIT_CONSTRAINT`
- Partitition: []

|    | Name                | Type   | Comment                      |
|---:|:--------------------|:-------|:-----------------------------|
|  0 | cic                 | string | 2 digits industry short name |
|  1 | industry_name       | string | 2 digits industry short name |
|  2 | financial_dep_china | float  | Financial dependency metric  |

    

## Table asif_firms_prepared

- Database: firms_survey
- S3uri: `s3://datalake-datascience/DATA/ECON/FIRM_SURVEY/ASIF_CHINA/PREPARED`
- Partitition: ['firm', 'year']

|     | Name           | Type   | Comment                                                                                        |
|----:|:---------------|:-------|:-----------------------------------------------------------------------------------------------|
|   0 | firm           | string | firm ID                                                                                        |
|   1 | year           | string | balance sheet year                                                                             |
|   2 | export         | int    | Where: export delivery value                                                                   |
|   3 | dq             | string | Province county code                                                                           |
|   4 | name           | string | Corporate Name                                                                                 |
|   5 | town           | string | Township (town)                                                                                |
|   6 | village        | string | Street (village), house number                                                                 |
|   7 | street         | string | Street office                                                                                  |
|   8 | c15            | string | Community (neighborhood) and village committee                                                 |
|   9 | zip            | string | Zip code                                                                                       |
|  10 | product1_      | string | The main products (1)                                                                          |
|  11 | c26            | string | The main products (2)                                                                          |
|  12 | c27            | string | The main products (3)                                                                          |
|  13 | cic            | string | Industry Code                                                                                  |
|  14 | type           | string | Registration Type                                                                              |
|  15 | c44            | string | Holding case                                                                                   |
|  16 | c45            | string | Affiliation                                                                                    |
|  17 | setup          | string | year of creation                                                                               |
|  18 | c47            | int    | Opening (established) time - months                                                            |
|  19 | c60            | int    | Code-scale enterprises                                                                         |
|  20 | c61            | int    | Light and heavy industrial codes                                                               |
|  21 | employ         | int    | Employement                                                                                    |
|  22 | c69            | int    | Industrial output (constant prices)                                                            |
|  23 | output         | int    | Industrial output value (current prices)                                                       |
|  24 | new_product    | int    | Where: New product output value                                                                |
|  25 | c74            | int    | Industrial sales output value (current prices)                                                 |
|  26 | addval         | int    | Value added                                                                                    |
|  27 | cuasset        | int    | Current asset                                                                                  |
|  28 | c80            | int    | Accounts Receivable                                                                            |
|  29 | c81            | int    | stock                                                                                          |
|  30 | c82            | int    | Wherein: Finished                                                                              |
|  31 | c83            | int    | The average balance of current assets                                                          |
|  32 | c84            | int    | Long-term investment                                                                           |
|  33 | c85            | int    | Total fixed assets (deprecated)                                                                |
|  34 | tofixed        | int    | Total fixed assets (to use)                                                                    |
|  35 | c87            | int    | Where: the production and operation with                                                       |
|  36 | todepre        | int    | total depreciation                                                                             |
|  37 | cudepre        | int    | current depreciation                                                                           |
|  38 | netfixed       | int    | net fixed asset                                                                                |
|  39 | c91            | int    | Intangible and Deferred                                                                        |
|  40 | c92            | int    | Intangible assets                                                                              |
|  41 | toasset        | int    | Total asset                                                                                    |
|  42 | c95            | int    | Total current liabilities                                                                      |
|  43 | c97            | int    | Total long-term liabilities                                                                    |
|  44 | c98            | int    | Total Liabilities                                                                              |
|  45 | c99            | int    | Total shareholders' equity                                                                     |
|  46 | captal         | int    | Capital                                                                                        |
|  47 | e_state        | int    | Where: National Capital                                                                        |
|  48 | e_collective   | int    | Collective capital                                                                             |
|  49 | e_legal_person | int    | Corporate capital                                                                              |
|  50 | e_individual   | int    | Personal Capital                                                                               |
|  51 | e_hmt          | bigint | nan                                                                                            |
|  52 | e_foreign      | int    | Foreign capital                                                                                |
|  53 | sales          | int    | Sales revenue                                                                                  |
|  54 | c108           | int    | Main business cost                                                                             |
|  55 | c113           | int    | Operating expenses                                                                             |
|  56 | c110           | int    | The main business tax and surcharges                                                           |
|  57 | c111           | int    | sales profit                                                                                   |
|  58 | c114           | int    | Management fees                                                                                |
|  59 | c115           | int    | Where: Taxes                                                                                   |
|  60 | c116           | int    | Property insurance                                                                             |
|  61 | c118           | int    | Labor, unemployment insurance                                                                  |
|  62 | c124           | int    | Financial expenses                                                                             |
|  63 | c125           | int    | Of which: Interest expense                                                                     |
|  64 | profit         | int    | operating profit                                                                               |
|  65 | c128           | int    | Subsidy income                                                                                 |
|  66 | c131           | int    | The total profit                                                                               |
|  67 | c132           | int    | Total loss                                                                                     |
|  68 | c133           | int    | Total profit and tax                                                                           |
|  69 | c134           | int    | Income tax payable                                                                             |
|  70 | c136           | int    | Profit payable                                                                                 |
|  71 | wage           | int    | Wages of the year                                                                              |
|  72 | c140           | int    | Where: Main Wages of business                                                                  |
|  73 | c141           | int    | This year the total amount of welfare payable (credit accumulated amount)                      |
|  74 | c142           | int    | Where: Main business cope with the total welfare                                               |
|  75 | c143           | int    | Payable VAT                                                                                    |
|  76 | c144           | int    | Input tax year                                                                                 |
|  77 | c145           | int    | Output tax year                                                                                |
|  78 | midput         | int    | intermediate inputs                                                                            |
|  79 | c62            | int    | Persons Employed Total Total _                                                                 |
|  80 | c147           | int    | Taxes (processing)                                                                             |
|  81 | c64            | int    | Total annual revenue                                                                           |
|  82 | c65            | int    | Where: Main business revenue                                                                   |
|  83 | c93            | int    | Total assets 318                                                                               |
|  84 | c16            | int    | Provinces                                                                                      |
|  85 | c9             | string | State / Province / State)                                                                      |
|  86 | c10            | string | The (autonomous regions and municipalities, prefectures and leagues)                           |
|  87 | c11            | string | Counties (districts, cities, flag)                                                             |
|  88 | c17            | string | Area Code                                                                                      |
|  89 | c29            | string | Registration (or approval) authority - the administrative department for industry and commerce |
|  90 | c167           | string | Founded union situation                                                                        |
|  91 | c168           | int    | unionized                                                                                      |
|  92 | v90            | int    | number_unionized                                                                               |
|  93 | c79            | int    | short_term_investments                                                                         |
|  94 | c96            | int    | accounts_Payable                                                                               |
|  95 | c117           | int    | travel                                                                                         |
|  96 | c119           | int    | union_funds                                                                                    |
|  97 | c121           | int    | administrative_expenses                                                                        |
|  98 | trainfee       | int    | trainfee                                                                                       |
|  99 | c123           | int    | sewage_charges                                                                                 |
| 100 | c127           | int    | investment_income                                                                              |
| 101 | c135           | int    | advertising_fee                                                                                |
| 102 | c120           | int    | pension_medical_insurance                                                                      |
| 103 | c138           | int    | housing_fund                                                                                   |
| 104 | c148           | int    | direct_aterial                                                                                 |
| 105 | c149           | int    | intermediate_inputs                                                                            |
| 106 | c150           | int    | middle_management_fees_investment                                                              |
| 107 | c151           | int    | operating_expenses                                                                             |
| 108 | rdfee          | int    | rdfee                                                                                          |
| 109 | c156           | string | Light and heavy industrial Name                                                                |
| 110 | c157           | string | Scale enterprises name                                                                         |
| 111 | citycode       | string | City code clean by Zhao Ruili using adress                                                     |
| 112 | prov           | string |                                                                                                |

    

## Table asif_industry_financial_ratio_city

- Database: firms_survey
- S3uri: `s3://datalake-datascience/DATA/ECON/FIRM_SURVEY/ASIF_CHINA/TRANSFORMED/FINANCIAL_RATIO/CITY`
- Partitition: ['province_en', 'geocode4_corr', 'indu_2', 'year']

|    | Name                        | Type          | Comment                                                                                                        |
|---:|:----------------------------|:--------------|:---------------------------------------------------------------------------------------------------------------|
|  0 | output                      | decimal(16,5) | Output                                                                                                         |
|  1 | employment                  | decimal(16,5) | employment                                                                                                     |
|  2 | capital                     | decimal(16,5) | capital                                                                                                        |
|  3 | current_asset               | int           | current asset                                                                                                  |
|  4 | net_fixed_asset             | int           | total net fixed asset                                                                                          |
|  5 | error                       | int           | difference between cuasset+tofixed and total liabilities +equity. Error makes the balance sheet equation right |
|  6 | total_liabilities           | int           | total adjusted liabilities                                                                                     |
|  7 | total_asset                 | int           | total adjusted asset                                                                                           |
|  8 | current_liabilities         | int           | current liabilities                                                                                            |
|  9 | lt_liabilities              | int           | long term liabilities                                                                                          |
| 10 | from_asif_tot_liabilities   | int           | total liabilities from asif not constructed                                                                    |
| 11 | total_right                 | int           | Adjusted right part balance sheet                                                                              |
| 12 | intangible                  | int           | intangible asset measured as the sum of intangibles variables                                                  |
| 13 | tangible                    | int           | tangible asset measured as the difference between total fixed asset minus intangible asset                     |
| 14 | cashflow                    | int           | cash flow                                                                                                      |
| 15 | sales                       | decimal(16,5) | sales                                                                                                          |
| 16 | current_ratio               | decimal(21,5) | current ratio cuasset/流动负债合计 (c95)                                                                       |
| 17 | lag_current_ratio           | decimal(21,5) | lag value of current ratio                                                                                     |
| 18 | quick_ratio                 | decimal(21,5) | quick ratio (cuasset-存货 (c81) ) / 流动负债合计 (c95)                                                         |
| 19 | lag_quick_ratio             | decimal(21,5) | lag value of quick ratio                                                                                       |
| 20 | liabilities_tot_asset       | decimal(21,5) | liabilities to total asset                                                                                     |
| 21 | lag_liabilities_tot_asset   | decimal(21,5) | lag value of liabilities to asset                                                                              |
| 22 | sales_tot_asset             | decimal(21,5) | sales to total asset                                                                                           |
| 23 | lag_sales_tot_asset         | decimal(21,5) | lag value of sales to asset                                                                                    |
| 24 | investment_tot_asset        | decimal(21,5) | investment to total asset                                                                                      |
| 25 | rd_tot_asset                | decimal(21,5) | rd to total asset                                                                                              |
| 26 | asset_tangibility_tot_asset | decimal(21,5) | asset tangibility to total asset                                                                               |
| 27 | cashflow_tot_asset          | decimal(21,5) | cashflow to total asset                                                                                        |
| 28 | lag_cashflow_tot_asset      | decimal(21,5) | lag value of cashflow to total asset                                                                           |
| 29 | cashflow_to_tangible        | decimal(21,5) | cashflow to tangible asset                                                                                     |
| 30 | lag_cashflow_to_tangible    | decimal(21,5) | lag value of cashflow to tangible asset                                                                        |
| 31 | return_to_sale              | decimal(21,5) |                                                                                                                |
| 32 | lag_return_to_sale          | decimal(21,5) | lag value of return to sale                                                                                    |
| 33 | coverage_ratio              | decimal(21,5) | net income(c131) /total interest payments                                                                      |
| 34 | liquidity                   | decimal(21,5) | current assets-current liabilities/total assets                                                                |

    

## Table china_sector_pollution_threshold

- Database: environment
- S3uri: `s3://datalake-datascience/DATA/ENVIRONMENT/CHINA/SECTOR_POLLUTION_THRESHOLD`
- Partitition: ['year', 'polluted_di']

|    | Name          | Type       | Comment                                                                           |
|---:|:--------------|:-----------|:----------------------------------------------------------------------------------|
|  0 | year          | string     |                                                                                   |
|  1 | ind2          | string     |                                                                                   |
|  2 | tso2          | bigint     |                                                                                   |
|  3 | pct_75_tso2   | bigint     | Yearly 75th percentile of SO2                                                     |
|  4 | avg_tso2      | double     | Yearly average of SO2                                                             |
|  5 | mdn_tso2      | bigint     | Yearly median of SO2                                                              |
|  6 | polluted_di   | varchar(5) | Sectors with values above Yearly 75th percentile of SO2 label as ABOVE else BELOW |
|  7 | polluted_mi   | varchar(5) | Sectors with values above Yearly average of SO2 label as ABOVE else BELOW         |
|  8 | polluted_mei  | varchar(5) | Sectors with values above Yearly median of SO2 label as ABOVE else BELOW          |
|  9 | polluted_thre | varchar(5) | Sectors with values above 68070.78  of SO2 label as ABOVE else BELOW              |

    

## Table asif_industry_financial_ratio_industry

- Database: firms_survey
- S3uri: `s3://datalake-datascience/DATA/ECON/FIRM_SURVEY/ASIF_CHINA/TRANSFORMED/FINANCIAL_RATIO/INDUSTRY`
- Partitition: ['indu_2']

|    | Name                              | Type   | Comment                                                                                                                                                             |
|---:|:----------------------------------|:-------|:--------------------------------------------------------------------------------------------------------------------------------------------------------------------|
|  0 | indu_2                            | string | Two digits industry. If length cic equals to 3, then add 0 to indu_2                                                                                                |
|  1 | receivable_curasset_i             | double | 应收帐款 (c80) / cuasset                                                                                                                                            |
|  2 | std_receivable_curasset_i         | double | standaridzed values (x - x mean) / std)                                                                                                                             |
|  3 | cash_over_curasset_i              | double | (其中：短期投资 (c79) - 应收帐款 (c80) - 存货 (c81) - 其中：产成品 (c82)) /current asset                                                                            |
|  4 | std_cash_over_curasset_i          | double | standaridzed values (x - x mean) / std)                                                                                                                             |
|  5 | working_capital_i                 | double | cuasset- 流动负债合计 (c95)                                                                                                                                         |
|  6 | std_working_capital_i             | double | standaridzed values (x - x mean) / std)                                                                                                                             |
|  7 | working_capital_requirement_i     | double | 存货 (c81) + 应收帐款 (c80) - 应付帐款  (c96)                                                                                                                       |
|  8 | std_working_capital_requirement_i | double | standaridzed values (x - x mean) / std)                                                                                                                             |
|  9 | current_ratio_i                   | double | cuasset/流动负债合计 (c95)                                                                                                                                          |
| 10 | std_current_ratio_i               | double | standaridzed values (x - x mean) / std)                                                                                                                             |
| 11 | quick_ratio_i                     | double | (cuasset -  其中：短期投资 (c79) - 应收帐款 (c80) - 存货 (c81)) / 流动负债合计 (c95)                                                                                |
| 12 | std_quick_ratio_i                 | double | standaridzed values (x - x mean) / std)                                                                                                                             |
| 13 | cash_ratio_i                      | double | (1 - cuasset - 其中：短期投资 (c79) - 应收帐款 (c80) - 存货 (c81) - 其中：产成品 (c82))/ 流动负债合计 (c95)                                                         |
| 14 | std_cash_ratio_i                  | double | standaridzed values (x - x mean) / std)                                                                                                                             |
| 15 | liabilities_assets_i              | double | (流动负债合计 (c95) + 长期负债合计 (c97)) / toasset                                                                                                                 |
| 16 | std_liabilities_assets_i          | double | standaridzed values (x - x mean) / std)                                                                                                                             |
| 17 | return_on_asset_i                 | double | sales - (主营业务成本 (c108) + 营业费用 (c113) + 管理费用 (c114) + 财产保险费 (c116) + 劳动、失业保险费 (c118)+ 财务费用 (c124) + 本年应付工资总额 (wage)) /toasset |
| 18 | std_return_on_asset_i             | double | standaridzed values (x - x mean) / std)                                                                                                                             |
| 19 | sales_assets_i                    | double | 全年营业收入合计 (c64) /(\Delta toasset/2)                                                                                                                          |
| 20 | std_sales_assets_i                | double | standaridzed values (x - x mean) / std)                                                                                                                             |
| 21 | sales_assets_andersen_i           | double | Sales over asset                                                                                                                                                    |
| 22 | std_sales_assets_andersen_i       | double | standaridzed values (x - x mean) / std)                                                                                                                             |
| 23 | account_paybable_to_asset_i       | double | (\Delta 应付帐款  (c96))/ (\Delta (toasset))                                                                                                                        |
| 24 | std_account_paybable_to_asset_i   | double | standaridzed values (x - x mean) / std)                                                                                                                             |
| 25 | asset_tangibility_i               | double | Total fixed assets - Intangible assets                                                                                                                              |
| 26 | std_asset_tangibility_i           | double | standaridzed values (x - x mean) / std)                                                                                                                             |
| 27 | rd_intensity_i                    | double | rdfee/全年营业收入合计 (c64)                                                                                                                                        |
| 28 | std_rd_intensity_i                | double | standaridzed values (x - x mean) / std)                                                                                                                             |
| 29 | inventory_to_sales_i              | double | 存货 (c81) / sales                                                                                                                                                  |
| 30 | std_inventory_to_sales_i          | double | standaridzed values (x - x mean) / std)                                                                                                                             |

    

## Table fin_dep_pollution_baseline_industry

- Database: environment
- S3uri: `s3://datalake-datascience/DATA/ENVIRONMENT/CHINA/FYP/FINANCIAL_CONTRAINT/PAPER_FYP_FINANCE_POL/BASELINE/INDUSTRY`
- Partitition: ['geocode4_corr', 'year', 'ind2']

|    | Name                              | Type          | Comment                                                                                                                                                             |
|---:|:----------------------------------|:--------------|:--------------------------------------------------------------------------------------------------------------------------------------------------------------------|
|  0 | year                              | string        | year from 2001 to 2007                                                                                                                                              |
|  1 | period                            | varchar(5)    | False if year before 2005 included, True if year 2006 and 2007                                                                                                      |
|  2 | provinces                         | string        |                                                                                                                                                                     |
|  3 | cityen                            | string        |                                                                                                                                                                     |
|  4 | geocode4_corr                     | string        |                                                                                                                                                                     |
|  5 | tcz                               | string        | Two control zone policy city                                                                                                                                        |
|  6 | spz                               | string        | Special policy zone policy city                                                                                                                                     |
|  7 | ind2                              | string        | 2 digits industry                                                                                                                                                   |
|  8 | short                             | string        |                                                                                                                                                                     |
|  9 | polluted_di                       | varchar(5)    | Sectors with values above Yearly 75th percentile of SO2 label as ABOVE else BELOW                                                                                   |
| 10 | polluted_mi                       | varchar(5)    | Sectors with values above Yearly average of SO2 label as ABOVE else BELOW                                                                                           |
| 11 | polluted_mei                      | varchar(5)    | Sectors with values above Yearly median of SO2 label as ABOVE else BELOW                                                                                            |
| 12 | tso2                              | bigint        | Total so2 city sector. Filtered values above  4863 (5% of the distribution)                                                                                         |
| 13 | so2_intensity                     | decimal(21,5) | SO2 divided by output                                                                                                                                               |
| 14 | tso2_mandate_c                    | float         | city reduction mandate in tonnes                                                                                                                                    |
| 15 | in_10_000_tonnes                  | float         | city reduction mandate in 10k tonnes                                                                                                                                |
| 16 | output                            | decimal(16,5) | Output                                                                                                                                                              |
| 17 | employment                        | decimal(16,5) | Employemnt                                                                                                                                                          |
| 18 | sales                             | decimal(16,5) | Sales                                                                                                                                                               |
| 19 | capital                           | decimal(16,5) | Capital                                                                                                                                                             |
| 20 | total_asset                       | decimal(16,5) | Total asset                                                                                                                                                         |
| 21 | credit_constraint                 | float         | Financial dependency. From paper https://www.sciencedirect.com/science/article/pii/S0147596715000311                                                                |
| 22 | receivable_curasset_i             | double        | 应收帐款 (c80) / cuasset                                                                                                                                            |
| 23 | std_receivable_curasset_i         | double        | standaridzed values (x - x mean) / std)                                                                                                                             |
| 24 | cash_over_curasset_i              | double        | (其中：短期投资 (c79) - 应收帐款 (c80) - 存货 (c81) - 其中：产成品 (c82)) /current asset                                                                            |
| 25 | std_cash_over_curasset_i          | double        | standaridzed values (x - x mean) / std)                                                                                                                             |
| 26 | working_capital_i                 | double        | cuasset- 流动负债合计 (c95)                                                                                                                                         |
| 27 | std_working_capital_i             | double        | standaridzed values (x - x mean) / std)                                                                                                                             |
| 28 | working_capital_requirement_i     | double        | 存货 (c81) + 应收帐款 (c80) - 应付帐款  (c96)                                                                                                                       |
| 29 | std_working_capital_requirement_i | double        | standaridzed values (x - x mean) / std)                                                                                                                             |
| 30 | current_ratio_i                   | double        | cuasset/流动负债合计 (c95)                                                                                                                                          |
| 31 | std_current_ratio_i               | double        | standaridzed values (x - x mean) / std)                                                                                                                             |
| 32 | quick_ratio_i                     | double        | (cuasset-存货 (c81) ) / 流动负债合计 (c95)                                                                                                                          |
| 33 | std_quick_ratio_i                 | double        | standaridzed values (x - x mean) / std)                                                                                                                             |
| 34 | cash_ratio_i                      | double        | (cuasset -  其中：短期投资 (c79) - 应收帐款 (c80) - 存货 (c81)/ 流动负债合计 (c95)                                                                                  |
| 35 | std_cash_ratio_i                  | double        | standaridzed values (x - x mean) / std)                                                                                                                             |
| 36 | liabilities_assets_i              | double        | (流动负债合计 (c95) + 长期负债合计 (c97)) / toasset                                                                                                                 |
| 37 | std_liabilities_assets_i          | double        | standaridzed values (x - x mean) / std)                                                                                                                             |
| 38 | reverse_liabilities_assets_i      | double        | 1-liabilities_assets_i                                                                                                                                              |
| 39 | std_reverse_liabilities_assets_i  | double        | 1 - standaridzed values (x - x mean) / std)                                                                                                                         |
| 40 | return_on_asset_i                 | double        | sales - (主营业务成本 (c108) + 营业费用 (c113) + 管理费用 (c114) + 财产保险费 (c116) + 劳动、失业保险费 (c118)+ 财务费用 (c124) + 本年应付工资总额 (wage)) /toasset |
| 41 | std_return_on_asset_i             | double        | standaridzed values (x - x mean) / std)                                                                                                                             |
| 42 | sales_assets_i                    | double        | 全年营业收入合计 (c64) /(\Delta toasset/2)                                                                                                                          |
| 43 | std_sales_assets_i                | double        | standaridzed values (x - x mean) / std)                                                                                                                             |
| 44 | sales_assets_andersen_i           | double        | 全年营业收入合计 (c64) /(toasset)                                                                                                                                   |
| 45 | std_sales_assets_andersen_i       | double        | standaridzed values (x - x mean) / std)                                                                                                                             |
| 46 | account_paybable_to_asset_i       | double        | (\Delta 应付帐款  (c96))/ (\Delta (toasset))                                                                                                                        |
| 47 | std_account_paybable_to_asset_i   | double        | standaridzed values (x - x mean) / std)                                                                                                                             |
| 48 | asset_tangibility_i               | double        | Total fixed assets - Intangible assets                                                                                                                              |
| 49 | std_asset_tangibility_i           | double        | standaridzed values (x - x mean) / std)                                                                                                                             |
| 50 | rd_intensity_i                    | double        | rdfee/全年营业收入合计 (c64)                                                                                                                                        |
| 51 | std_rd_intensity_i                | double        | standaridzed values (x - x mean) / std)                                                                                                                             |
| 52 | inventory_to_sales_i              | double        | 存货 (c81) / sales                                                                                                                                                  |
| 53 | std_inventory_to_sales_i          | double        | standaridzed values (x - x mean) / std)                                                                                                                             |
| 54 | lower_location                    | string        | Location city. one of Coastal, Central, Northwest, Northeast, Southwest                                                                                             |
| 55 | larger_location                   | string        | Location city. one of Eastern, Central, Western                                                                                                                     |
| 56 | coastal                           | string        | City is bordered by sea or not                                                                                                                                      |
| 57 | fe_c_i                            | bigint        | City industry fixed effect                                                                                                                                          |
| 58 | fe_t_i                            | bigint        | year industry fixed effect                                                                                                                                          |
| 59 | fe_c_t                            | bigint        | city industry fixed effect                                                                                                                                          |

    

## Table asif_financial_ratio_baseline_firm

- Database: firms_survey
- S3uri: `s3://datalake-datascience/DATA/ECON/FIRM_SURVEY/ASIF_CHINA/TRANSFORMED/FINANCIAL_RATIO/FIRM`
- Partitition: ['firm', 'year', 'cic', 'geocode4_corr']

|    | Name                        | Type                | Comment                                                                                                                                                             |
|---:|:----------------------------|:--------------------|:--------------------------------------------------------------------------------------------------------------------------------------------------------------------|
|  0 | firm                        | string              | Firms ID                                                                                                                                                            |
|  1 | year                        | string              |                                                                                                                                                                     |
|  2 | period                      | varchar(5)          | if year prior to 2006 then False else true. Indicate break from 10 and 11 FYP                                                                                       |
|  3 | cic                         | string              | 4 digits industry code                                                                                                                                              |
|  4 | indu_2                      | string              | Two digits industry. If length cic equals to 3, then add 0 to indu_2                                                                                                |
|  5 | short                       | string              | Industry short description                                                                                                                                          |
|  6 | geocode4_corr               | string              | city code                                                                                                                                                           |
|  7 | tcz                         | string              | Two control zone policy                                                                                                                                             |
|  8 | spz                         | string              | Special policy zone                                                                                                                                                 |
|  9 | ownership                   | string              | Firms ownership                                                                                                                                                     |
| 10 | soe_vs_pri                  | varchar(7)          | SOE vs PRIVATE                                                                                                                                                      |
| 11 | for_vs_dom                  | varchar(8)          | FOREIGN vs DOMESTICT if ownership is HTM then FOREIGN                                                                                                               |
| 12 | tso2_mandate_c              | float               | city reduction mandate in tonnes                                                                                                                                    |
| 13 | in_10_000_tonnes            | float               | city reduction mandate in 10k tonnes                                                                                                                                |
| 14 | output                      | decimal(16,5)       | Output                                                                                                                                                              |
| 15 | employment                  | decimal(16,5)       | employment                                                                                                                                                          |
| 16 | capital                     | decimal(16,5)       | capital                                                                                                                                                             |
| 17 | sales                       | decimal(16,5)       | sales                                                                                                                                                               |
| 18 | total_asset                 | decimal(16,5)       | Total asset                                                                                                                                                         |
| 19 | credit_constraint           | float               | Financial dependency. From paper https://www.sciencedirect.com/science/article/pii/S0147596715000311                                                                |
| 20 | d_credit_constraint         | varchar(5)          | Sectors financially dependant when above median                                                                                                                     |
| 21 | asset_tangibility_fcit      | decimal(16,5)       | Total fixed assets - Intangible assets                                                                                                                              |
| 22 | cash_over_totasset_fcit     | decimal(21,5)       | cuasset - short_term_investment - c80 - c81 - c82 divided by toasset                                                                                                |
| 23 | lag_cash_over_totasset_fcit | decimal(21,5)       | lag cash over total asset                                                                                                                                           |
| 24 | current_ratio_fcit          | decimal(21,5)       | cuasset/流动负债合计 (c95)                                                                                                                                          |
| 25 | lag_current_ratio_fcit      | decimal(21,5)       | lag current ratio                                                                                                                                                   |
| 26 | quick_ratio_fcit            | decimal(21,5)       | (cuasset-存货 (c81) ) / 流动负债合计 (c95)                                                                                                                          |
| 27 | lag_quick_ratio_fcit        | decimal(21,5)       | lag quick ratio                                                                                                                                                     |
| 28 | liabilities_assets_fcit     | decimal(21,5)       | (流动负债合计 (c95) + 长期负债合计 (c97)) / toasset                                                                                                                 |
| 29 | lag_liabilities_assets_fcit | decimal(21,5)       | lag liabilities over total asset                                                                                                                                    |
| 30 | sales_assets_andersen_fcit  | decimal(21,5)       | Sales divided by total asset                                                                                                                                        |
| 31 | return_on_asset_fcit        | decimal(21,5)       | sales - (主营业务成本 (c108) + 营业费用 (c113) + 管理费用 (c114) + 财产保险费 (c116) + 劳动、失业保险费 (c118)+ 财务费用 (c124) + 本年应付工资总额 (wage)) /toasset |
| 32 | avg_size_asset_f            | varchar(5)          | if firm s asset tangibility average is above average of firm s average then firm is large                                                                           |
| 33 | avg_size_output_f           | varchar(5)          | if firm s ouptut average is above average of firm s average then firm is large                                                                                      |
| 34 | avg_employment_f            | varchar(5)          | if firm s employment average is above average of firm s average then firm is large                                                                                  |
| 35 | avg_size_capital_f          | varchar(5)          | if firm s capital average is above average of firm s average then firm is large                                                                                     |
| 36 | avg_sales_f                 | varchar(5)          | if firm s sale is above average of firm s average then firm is large                                                                                                |
| 37 | size_asset_fci              | map<double,boolean> | if firm s asset tangibility average is above average of firm city industry s decile then firm is large                                                              |
| 38 | size_asset_fc               | map<double,boolean> | if firm s asset tangibility average is above average of firm s city decile then firm is large                                                                       |
| 39 | size_asset_fi               | map<double,boolean> | if firm s asset tangibility average is above average of firm s industry decile then firm is large                                                                   |
| 40 | size_output_fci             | map<double,boolean> | if firm s ouptut average is above average of firm s city industry decile then firm is large                                                                         |
| 41 | size_output_fc              | map<double,boolean> | if firm s ouptut average is above average of firm s city decile then firm is large                                                                                  |
| 42 | size_output_fi              | map<double,boolean> | if firm s ouptut average is above average of firm s industry decile then firm is large                                                                              |
| 43 | size_employment_fci         | map<double,boolean> | if firm s employment average is above average of firm s city industry decile then firm is large                                                                     |
| 44 | size_employment_fc          | map<double,boolean> | if firm s employment average is above average of firm s city decile then firm is large                                                                              |
| 45 | size_employment_fi          | map<double,boolean> | if firm s employment average is above average of firm s industry decile then firm is large                                                                          |
| 46 | size_capital_fci            | map<double,boolean> | if firm s capital average is above average of firm s city industry decile then firm is large                                                                        |
| 47 | size_capital_fc             | map<double,boolean> | if firm s capital average is above average of firm s city decile then firm is large                                                                                 |
| 48 | size_capital_fi             | map<double,boolean> | if firm s capital average is above average of firm s industry decile then firm is large                                                                             |
| 49 | size_sales_fci              | map<double,boolean> | if firm s sale is above average of firm s city industry decile then firm is large                                                                                   |
| 50 | size_sales_fc               | map<double,boolean> | if firm s sale is above average of firm s city decile then firm is large                                                                                            |
| 51 | size_sales_fi               | map<double,boolean> | if firm s sale is above average of firm s industry decile then firm is large                                                                                        |
| 52 | fe_c_i                      | bigint              | City industry fixed effect                                                                                                                                          |
| 53 | fe_t_i                      | bigint              | year industry fixed effect                                                                                                                                          |
| 54 | fe_c_t                      | bigint              | city industry fixed effect                                                                                                                                          |

    

## Table asif_industry_characteristics_ownership

- Database: firms_survey
- S3uri: `s3://datalake-datascience/DATA/ECON/FIRM_SURVEY/ASIF_CHINA/TRANSFORMED/INDUSTRY_CHARACTERISTICS/OWNERSHIP`
- Partitition: ['geocode4_corr', 'indu_2']

|    | Name                       | Type                | Comment                                                                                              |
|---:|:---------------------------|:--------------------|:-----------------------------------------------------------------------------------------------------|
|  0 | indu_2                     | string              |                                                                                                      |
|  1 | geocode4_corr              | string              | city code                                                                                            |
|  2 | dominated_output_i         | map<double,boolean> | map with information dominated industry knowing percentile .5, .75, .9, .95 of output                |
|  3 | dominated_employment_i     | map<double,boolean> | map with information on dominated industry knowing percentile .5, .75, .9, .95 of employment         |
|  4 | dominated_capital_i        | map<double,boolean> | map with information on dominated industry knowing percentile .5, .75, .9, .95 of capital            |
|  5 | dominated_sales_i          | map<double,boolean> | map with information on SOE dominated industry knowing percentile .5, .75, .9, .95 of sales          |
|  6 | dominated_output_soe_i     | map<double,boolean> | map with information on SOE dominated industry knowing percentile .5, .75, .9, .95 of output         |
|  7 | dominated_employment_soe_i | map<double,boolean> | map with information on SOE dominated industry knowing percentile .5, .75, .9, .95 of employment     |
|  8 | dominated_sales_soe_i      | map<double,boolean> | map with information on SOE dominated industry knowing percentile .5, .75, .9, .95 of sales          |
|  9 | dominated_capital_soe_i    | map<double,boolean> | map with information on SOE dominated industry knowing percentile .5, .75, .9, .95 of capital        |
| 10 | dominated_output_for_i     | map<double,boolean> | map with information on foreign dominated industry knowing percentile .5, .75, .9, .95 of output     |
| 11 | dominated_employment_for_i | map<double,boolean> | map with information on foreign dominated industry knowing percentile .5, .75, .9, .95 of employment |
| 12 | dominated_sales_for_i      | map<double,boolean> | map with information on foreign dominated industry knowing percentile .5, .75, .9, .95 of sales      |
| 13 | dominated_capital_for_i    | map<double,boolean> | map with information on foreign dominated industry knowing percentile .5, .75, .9, .95 of capital    |

    

## Table asif_tfp_credit_constraint

- Database: firms_survey
- S3uri: `s3://datalake-datascience/DATA/ECON/FIRM_SURVEY/ASIF_CHINA/TRANSFORMED/TFP/CREDIT_CONSTRAINT`
- Partitition: ['firm', 'year', 'cic', 'geocode4_corr']

|    | Name                        | Type                | Comment                                                                                                                                                       |
|---:|:----------------------------|:--------------------|:--------------------------------------------------------------------------------------------------------------------------------------------------------------|
|  0 | firm                        | string              | firm ID                                                                                                                                                       |
|  1 | year                        | string              | year                                                                                                                                                          |
|  2 | period                      | varchar(5)          | if year prior to 2006 then False else true. Indicate break from 10 and 11 FYP                                                                                 |
|  3 | cic                         | string              | 4 digits industry code                                                                                                                                        |
|  4 | indu_2                      | string              | Two digits industry. If length cic equals to 3, then add 0 to indu_2                                                                                          |
|  5 | short                       | string              | Industry short description                                                                                                                                    |
|  6 | geocode4_corr               | string              | city code                                                                                                                                                     |
|  7 | tcz                         | string              | Two control zone policy                                                                                                                                       |
|  8 | spz                         | string              | Special policy zone                                                                                                                                           |
|  9 | ownership                   | string              | Firms ownership                                                                                                                                               |
| 10 | soe_vs_pri                  | varchar(7)          | SOE vs PRIVATE                                                                                                                                                |
| 11 | for_vs_dom                  | varchar(8)          | FOREIGN vs DOMESTICT if ownership is HTM then FOREIGN                                                                                                         |
| 12 | tso2_mandate_c              | float               | city reduction mandate in tonnes                                                                                                                              |
| 13 | in_10_000_tonnes            | float               | city reduction mandate in 10k tonnes                                                                                                                          |
| 14 | output                      | decimal(16,5)       | Output                                                                                                                                                        |
| 15 | employment                  | decimal(16,5)       | employment                                                                                                                                                    |
| 16 | capital                     | decimal(16,5)       | capital                                                                                                                                                       |
| 17 | current_asset               | int                 | current asset                                                                                                                                                 |
| 18 | net_fixed_asset             | int                 | total net fixed asset                                                                                                                                         |
| 19 | error                       | int                 | difference between cuasset+tofixed and total liabilities +equity. Error makes the balance sheet equation right                                                |
| 20 | total_liabilities           | int                 | total adjusted liabilities                                                                                                                                    |
| 21 | total_asset                 | int                 | total adjusted asset                                                                                                                                          |
| 22 | current_liabilities         | int                 | current liabilities                                                                                                                                           |
| 23 | lt_liabilities              | int                 | long term liabilities                                                                                                                                         |
| 24 | from_asif_tot_liabilities   | int                 | total liabilities from asif not constructed                                                                                                                   |
| 25 | total_right                 | int                 | Adjusted right part balance sheet                                                                                                                             |
| 26 | intangible                  | int                 | intangible asset measured as the sum of intangibles variables                                                                                                 |
| 27 | tangible                    | int                 | tangible asset measured as the difference between total fixed asset minus intangible asset                                                                    |
| 28 | c91                         | int                 | Intangible and Deferred                                                                                                                                       |
| 29 | c92                         | int                 | Intangible assets                                                                                                                                             |
| 30 | cashflow                    | int                 | cash flow                                                                                                                                                     |
| 31 | sales                       | decimal(16,5)       | sales                                                                                                                                                         |
| 32 | tfp_op                      | double              | TFP. Computed from https://github.com/thomaspernet/Financial_dependency_pollution/blob/master/01_data_preprocessing/02_transform_tables/05_tfp_computation.md |
| 33 | credit_constraint           | float               | Financial dependency. From paper https://www.sciencedirect.com/science/article/pii/S0147596715000311                                                          |
| 34 | supply_all_credit           | double              | total credit over gdp province                                                                                                                                |
| 35 | supply_long_term_credit     | float               | total long term credit over gdp province                                                                                                                      |
| 36 | current_ratio               | decimal(21,5)       | current ratio cuasset/流动负债合计 (c95)                                                                                                                      |
| 37 | quick_ratio                 | decimal(21,5)       | quick ratio (cuasset-存货 (c81) ) / 流动负债合计 (c95)                                                                                                        |
| 38 | liabilities_tot_asset       | decimal(21,5)       | liabilities to total asset                                                                                                                                    |
| 39 | sales_tot_asset             | decimal(21,5)       | sales to total asset                                                                                                                                          |
| 40 | investment_tot_asset        | decimal(21,5)       | investment to total asset                                                                                                                                     |
| 41 | rd_tot_asset                | decimal(21,5)       | rd to total asset                                                                                                                                             |
| 42 | asset_tangibility_tot_asset | decimal(21,5)       | asset tangibility to total asset                                                                                                                              |
| 43 | cashflow_tot_asset          | decimal(21,5)       | cashflow to total asset                                                                                                                                       |
| 44 | cashflow_to_tangible        | decimal(21,5)       | cashflow to tangible asset                                                                                                                                    |
| 45 | return_to_sale              | decimal(21,5)       |                                                                                                                                                               |
| 46 | export_to_sale              | decimal(21,5)       | overseas turnover / sales                                                                                                                                     |
| 47 | labor_productivity          | decimal(21,5)       | real sales/number of employees.                                                                                                                               |
| 48 | labor_capital               | decimal(21,5)       | Labor / tangible asset                                                                                                                                        |
| 49 | age                         | decimal(17,5)       | current year – firms year of establishment                                                                                                                    |
| 50 | coverage_ratio              | decimal(21,5)       | net income(c131) /total interest payments                                                                                                                     |
| 51 | liquidity                   | decimal(21,5)       | current assets-current liabilities/total assets                                                                                                               |
| 52 | avg_size_asset_f            | varchar(5)          | if firm s asset tangibility average is above average of firm s average then firm is large                                                                     |
| 53 | avg_size_output_f           | varchar(5)          | if firm s ouptut average is above average of firm s average then firm is large                                                                                |
| 54 | avg_employment_f            | varchar(5)          | if firm s employment average is above average of firm s average then firm is large                                                                            |
| 55 | avg_size_capital_f          | varchar(5)          | if firm s capital average is above average of firm s average then firm is large                                                                               |
| 56 | avg_sales_f                 | varchar(5)          | if firm s sale is above average of firm s average then firm is large                                                                                          |
| 57 | size_asset_fci              | map<double,boolean> | if firm s asset tangibility average is above average of firm city industry s decile then firm is large                                                        |
| 58 | size_asset_fc               | map<double,boolean> | if firm s asset tangibility average is above average of firm s city decile then firm is large                                                                 |
| 59 | size_asset_fi               | map<double,boolean> | if firm s asset tangibility average is above average of firm s industry decile then firm is large                                                             |
| 60 | size_output_fci             | map<double,boolean> | if firm s ouptut average is above average of firm s city industry decile then firm is large                                                                   |
| 61 | size_output_fc              | map<double,boolean> | if firm s ouptut average is above average of firm s city decile then firm is large                                                                            |
| 62 | size_output_fi              | map<double,boolean> | if firm s ouptut average is above average of firm s industry decile then firm is large                                                                        |
| 63 | size_employment_fci         | map<double,boolean> | if firm s employment average is above average of firm s city industry decile then firm is large                                                               |
| 64 | size_employment_fc          | map<double,boolean> | if firm s employment average is above average of firm s city decile then firm is large                                                                        |
| 65 | size_employment_fi          | map<double,boolean> | if firm s employment average is above average of firm s industry decile then firm is large                                                                    |
| 66 | size_capital_fci            | map<double,boolean> | if firm s capital average is above average of firm s city industry decile then firm is large                                                                  |
| 67 | size_capital_fc             | map<double,boolean> | if firm s capital average is above average of firm s city decile then firm is large                                                                           |
| 68 | size_capital_fi             | map<double,boolean> | if firm s capital average is above average of firm s industry decile then firm is large                                                                       |
| 69 | size_sales_fci              | map<double,boolean> | if firm s sale is above average of firm s city industry decile then firm is large                                                                             |
| 70 | size_sales_fc               | map<double,boolean> | if firm s sale is above average of firm s city decile then firm is large                                                                                      |
| 71 | size_sales_fi               | map<double,boolean> | if firm s sale is above average of firm s industry decile then firm is large                                                                                  |
| 72 | fe_c_i                      | bigint              | City industry fixed effect                                                                                                                                    |
| 73 | fe_t_i                      | bigint              | year industry fixed effect                                                                                                                                    |
| 74 | fe_c_t                      | bigint              | city industry fixed effect                                                                                                                                    |

    

## Table fin_dep_pollution_baseline_city

- Database: environment
- S3uri: `s3://datalake-datascience/DATA/ENVIRONMENT/CHINA/FYP/FINANCIAL_CONTRAINT/PAPER_FYP_FINANCE_POL/BASELINE/CITY`
- Partitition: ['province_en', 'geocode4_corr', 'indu_2', 'year']

|    | Name                        | Type                | Comment                                                                                                                                                                                                   |
|---:|:----------------------------|:--------------------|:----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
|  0 | year                        | string              | year from 2001 to 2007                                                                                                                                                                                    |
|  1 | period                      | varchar(5)          | False if year before 2005 included, True if year 2006 and 2007                                                                                                                                            |
|  2 | provinces                   | string              |                                                                                                                                                                                                           |
|  3 | cityen                      | string              |                                                                                                                                                                                                           |
|  4 | geocode4_corr               | string              |                                                                                                                                                                                                           |
|  5 | tcz                         | string              | Two control zone policy city                                                                                                                                                                              |
|  6 | spz                         | string              | Special policy zone policy city                                                                                                                                                                           |
|  7 | ind2                        | string              | 2 digits industry                                                                                                                                                                                         |
|  8 | short                       | string              |                                                                                                                                                                                                           |
|  9 | polluted_di                 | varchar(5)          | Sectors with values above Yearly 75th percentile of SO2 label as ABOVE else BELOW                                                                                                                         |
| 10 | polluted_mi                 | varchar(5)          | Sectors with values above Yearly average of SO2 label as ABOVE else BELOW                                                                                                                                 |
| 11 | polluted_mei                | varchar(5)          | Sectors with values above Yearly median of SO2 label as ABOVE else BELOW                                                                                                                                  |
| 12 | tso2                        | bigint              | Total so2 city sector. Filtered values above  4863 (5% of the distribution)                                                                                                                               |
| 13 | so2_intensity               | decimal(21,5)       | SO2 divided by output                                                                                                                                                                                     |
| 14 | tso2_mandate_c              | float               | city reduction mandate in tonnes                                                                                                                                                                          |
| 15 | above_threshold_mandate     | map<double,boolean> | Policy mandate above percentile .5, .75, .9, .95                                                                                                                                                          |
| 16 | above_average_mandate       | varchar(5)          | Policy mandate above national average                                                                                                                                                                     |
| 17 | in_10_000_tonnes            | float               | city reduction mandate in 10k tonnes                                                                                                                                                                      |
| 18 | tfp_cit                     | double              | TFP at the city industry level. From https://github.com/thomaspernet/Financial_dependency_pollution/blob/master/01_data_preprocessing/02_transform_tables/05_tfp_computation.md#table-asif_tfp_firm_level |
| 19 | credit_constraint           | float               | Financial dependency. From paper https://www.sciencedirect.com/science/article/pii/S0147596715000311"                                                                                                     |
| 20 | supply_all_credit           | double              | province external supply of credit                                                                                                                                                                        |
| 21 | supply_long_term_credit     | float               | province external supply of long term credit                                                                                                                                                              |
| 22 | output                      | decimal(16,5)       | Output                                                                                                                                                                                                    |
| 23 | employment                  | decimal(16,5)       | Employemnt                                                                                                                                                                                                |
| 24 | sales                       | decimal(16,5)       | Sales                                                                                                                                                                                                     |
| 25 | capital                     | decimal(16,5)       | Capital                                                                                                                                                                                                   |
| 26 | current_asset               | int                 | current asset                                                                                                                                                                                             |
| 27 | tofixed                     | bigint              | total fixed asset                                                                                                                                                                                         |
| 28 | total_liabilities           | int                 | total liabilities                                                                                                                                                                                         |
| 29 | total_asset                 | int                 | total asset                                                                                                                                                                                               |
| 30 | tangible                    | int                 | tangible asset                                                                                                                                                                                            |
| 31 | cashflow                    | int                 | cashflow                                                                                                                                                                                                  |
| 32 | current_ratio               | decimal(21,5)       | current ratio                                                                                                                                                                                             |
| 33 | lag_current_ratio           | decimal(21,5)       | lag value of current ratio                                                                                                                                                                                |
| 34 | liabilities_tot_asset       | decimal(21,5)       | liabilities to total asset                                                                                                                                                                                |
| 35 | lag_liabilities_tot_asset   | decimal(21,5)       | lag value of liabilities to asset                                                                                                                                                                         |
| 36 | sales_tot_asset             | decimal(21,5)       | sales to total asset                                                                                                                                                                                      |
| 37 | lag_sales_tot_asset         | decimal(21,5)       | lag value of sales to asset                                                                                                                                                                               |
| 38 | asset_tangibility_tot_asset | decimal(21,5)       | asset tangibility tot total asset                                                                                                                                                                         |
| 39 | cashflow_to_tangible        | decimal(21,5)       | cashflow to tangible asset                                                                                                                                                                                |
| 40 | lag_cashflow_to_tangible    | decimal(21,5)       | lag value of cashflow to tangible asset                                                                                                                                                                   |
| 41 | return_to_sale              | decimal(21,5)       | return to sale                                                                                                                                                                                            |
| 42 | lag_return_to_sale          | decimal(21,5)       | lag value of return to sale                                                                                                                                                                               |
| 43 | coastal                     | string              | City is bordered by sea or not                                                                                                                                                                            |
| 44 | dominated_output_soe_c      | boolean             | SOE dominated city of output. If true, then SOEs dominated city                                                                                                                                           |
| 45 | dominated_employment_soe_c  | boolean             | SOE dominated city of employment. If true, then SOEs dominated city                                                                                                                                       |
| 46 | dominated_sales_soe_c       | boolean             | SOE dominated city of sales. If true, then SOEs dominated city                                                                                                                                            |
| 47 | dominated_capital_soe_c     | boolean             | SOE dominated city of capital. If true, then SOEs dominated city                                                                                                                                          |
| 48 | dominated_output_for_c      | boolean             | foreign dominated city of output. If true, then foreign dominated city                                                                                                                                    |
| 49 | dominated_employment_for_c  | boolean             | foreign dominated city of employment. If true, then foreign dominated city                                                                                                                                |
| 50 | dominated_sales_for_c       | boolean             | foreign dominated cityof sales. If true, then foreign dominated city                                                                                                                                      |
| 51 | dominated_capital_for_c     | boolean             | foreign dominated city of capital. If true, then foreign dominated city                                                                                                                                   |
| 52 | dominated_output_i          | map<double,boolean> | map with information dominated industry knowing percentile .5, .75, .9, .95 of output                                                                                                                     |
| 53 | dominated_employment_i      | map<double,boolean> | map with information on dominated industry knowing percentile .5, .75, .9, .95 of employment                                                                                                              |
| 54 | dominated_capital_i         | map<double,boolean> | map with information on dominated industry knowing percentile .5, .75, .9, .95 of capital                                                                                                                 |
| 55 | dominated_sales_i           | map<double,boolean> | map with information on SOE dominated industry knowing percentile .5, .75, .9, .95 of sales                                                                                                               |
| 56 | dominated_output_soe_i      | map<double,boolean> | map with information on SOE dominated industry knowing percentile .5, .75, .9, .95 of output                                                                                                              |
| 57 | dominated_employment_soe_i  | map<double,boolean> | map with information on SOE dominated industry knowing percentile .5, .75, .9, .95 of employment                                                                                                          |
| 58 | dominated_sales_soe_i       | map<double,boolean> | map with information on SOE dominated industry knowing percentile .5, .75, .9, .95 of sales                                                                                                               |
| 59 | dominated_capital_soe_i     | map<double,boolean> | map with information on SOE dominated industry knowing percentile .5, .75, .9, .95 of capital                                                                                                             |
| 60 | dominated_output_for_i      | map<double,boolean> | map with information on foreign dominated industry knowing percentile .5, .75, .9, .95 of output                                                                                                          |
| 61 | dominated_employment_for_i  | map<double,boolean> | map with information on foreign dominated industry knowing percentile .5, .75, .9, .95 of employment                                                                                                      |
| 62 | dominated_sales_for_i       | map<double,boolean> | map with information on foreign dominated industry knowing percentile .5, .75, .9, .95 of sales                                                                                                           |
| 63 | dominated_capital_for_i     | map<double,boolean> | map with information on foreign dominated industry knowing percentile .5, .75, .9, .95 of capital                                                                                                         |
| 64 | fe_c_i                      | bigint              | City industry fixed effect                                                                                                                                                                                |
| 65 | fe_t_i                      | bigint              | year industry fixed effect                                                                                                                                                                                |
| 66 | fe_c_t                      | bigint              | city industry fixed effect                                                                                                                                                                                |

    