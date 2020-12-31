
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
- [fin_dep_pollution_baseline_city](https://github.com/thomaspernet/Financial_dependency_pollution/tree/master/00_data_catalogue#table-fin_dep_pollution_baseline_city)
- [china_sector_pollution_threshold](https://github.com/thomaspernet/Financial_dependency_pollution/tree/master/00_data_catalogue#table-china_sector_pollution_threshold)
- [asif_industry_financial_ratio_industry](https://github.com/thomaspernet/Financial_dependency_pollution/tree/master/00_data_catalogue#table-asif_industry_financial_ratio_industry)
- [fin_dep_pollution_baseline_industry](https://github.com/thomaspernet/Financial_dependency_pollution/tree/master/00_data_catalogue#table-fin_dep_pollution_baseline_industry)

    

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
- Partitition: ['geocode4_corr', 'indu_2']

|    | Name                               | Type   | Comment                                                                                                                                                             |
|---:|:-----------------------------------|:-------|:--------------------------------------------------------------------------------------------------------------------------------------------------------------------|
|  0 | indu_2                             | string | Two digits industry. If length cic equals to 3, then add 0 to indu_2                                                                                                |
|  1 | geocode4_corr                      | string |                                                                                                                                                                     |
|  2 | receivable_curasset_ci             | double | 应收帐款 (c80) / cuasset                                                                                                                                            |
|  3 | std_receivable_curasset_ci         | double | standaridzed values (x - x mean) / std)                                                                                                                             |
|  4 | cash_over_curasset_ci              | double | (其中：短期投资 (c79) - 应收帐款 (c80) - 存货 (c81) - 其中：产成品 (c82)) /current asset                                                                            |
|  5 | std_cash_over_curasset_ci          | double | standaridzed values (x - x mean) / std)                                                                                                                             |
|  6 | working_capital_ci                 | double | cuasset- 流动负债合计 (c95)                                                                                                                                         |
|  7 | std_working_capital_ci             | double | standaridzed values (x - x mean) / std)                                                                                                                             |
|  8 | working_capital_requirement_ci     | double | 存货 (c81) + 应收帐款 (c80) - 应付帐款  (c96)                                                                                                                       |
|  9 | std_working_capital_requirement_ci | double | standaridzed values (x - x mean) / std)                                                                                                                             |
| 10 | current_ratio_ci                   | double | cuasset/流动负债合计 (c95)                                                                                                                                          |
| 11 | std_current_ratio_ci               | double | standaridzed values (x - x mean) / std)                                                                                                                             |
| 12 | quick_ratio_ci                     | double | (cuasset -  其中：短期投资 (c79) - 应收帐款 (c80) - 存货 (c81)) / 流动负债合计 (c95)                                                                                |
| 13 | std_quick_ratio_ci                 | double | standaridzed values (x - x mean) / std)                                                                                                                             |
| 14 | cash_ratio_ci                      | double | (cuasset - 其中：短期投资 (c79) - 应收帐款 (c80) - 存货 (c81) - 其中：产成品 (c82))/ 流动负债合计 (c95)                                                             |
| 15 | std_cash_ratio_ci                  | double | standaridzed values (x - x mean) / std)                                                                                                                             |
| 16 | liabilities_assets_ci              | double | (流动负债合计 (c95) + 长期负债合计 (c97)) / toasset                                                                                                                 |
| 17 | std_liabilities_assets_ci          | double | standaridzed values (x - x mean) / std)                                                                                                                             |
| 18 | return_on_asset_ci                 | double | sales - (主营业务成本 (c108) + 营业费用 (c113) + 管理费用 (c114) + 财产保险费 (c116) + 劳动、失业保险费 (c118)+ 财务费用 (c124) + 本年应付工资总额 (wage)) /toasset |
| 19 | std_return_on_asset_ci             | double | standaridzed values (x - x mean) / std)                                                                                                                             |
| 20 | sales_assets_ci                    | double | 全年营业收入合计 (c64) /(\Delta toasset/2)                                                                                                                          |
| 21 | std_sales_assets_ci                | double | standaridzed values (x - x mean) / std)                                                                                                                             |
| 22 | account_paybable_to_asset_ci       | double | (\Delta 应付帐款  (c96))/ (\Delta (toasset))                                                                                                                        |
| 23 | std_account_paybable_to_asset_ci   | double | standaridzed values (x - x mean) / std)                                                                                                                             |
| 24 | asset_tangibility_ci               | double | Total fixed assets - Intangible assets                                                                                                                              |
| 25 | std_asset_tangibility_ci           | double | standaridzed values (x - x mean) / std)                                                                                                                             |
| 26 | rd_intensity_ci                    | double | rdfee/全年营业收入合计 (c64)                                                                                                                                        |
| 27 | std_rd_intensity_ci                | double | standaridzed values (x - x mean) / std)                                                                                                                             |
| 28 | inventory_to_sales_ci              | double | 存货 (c81) / sales                                                                                                                                                  |
| 29 | std_inventory_to_sales_ci          | double | standaridzed values (x - x mean) / std)                                                                                                                             |

    

## Table fin_dep_pollution_baseline_city

- Database: environment
- S3uri: `s3://datalake-datascience/DATA/ENVIRONMENT/CHINA/FYP/FINANCIAL_CONTRAINT/PAPER_FYP_FINANCE_POL/BASELINE/CITY`
- Partitition: ['geocode4_corr', 'year', 'ind2']

|    | Name                               | Type          | Comment                                                                                                                                                             |
|---:|:-----------------------------------|:--------------|:--------------------------------------------------------------------------------------------------------------------------------------------------------------------|
|  0 | year                               | string        | year from 2001 to 2007                                                                                                                                              |
|  1 | period                             | varchar(5)    | False if year before 2005 included, True if year 2006 and 2007                                                                                                      |
|  2 | provinces                          | string        |                                                                                                                                                                     |
|  3 | cityen                             | string        |                                                                                                                                                                     |
|  4 | geocode4_corr                      | string        |                                                                                                                                                                     |
|  5 | tcz                                | string        | Two control zone policy city                                                                                                                                        |
|  6 | spz                                | string        | Special policy zone policy city                                                                                                                                     |
|  7 | ind2                               | string        | 2 digits industry                                                                                                                                                   |
|  8 | short                              | string        |                                                                                                                                                                     |
|  9 | polluted_di                        | varchar(5)    | Sectors with values above Yearly 75th percentile of SO2 label as ABOVE else BELOW                                                                                   |
| 10 | polluted_mi                        | varchar(5)    | Sectors with values above Yearly average of SO2 label as ABOVE else BELOW                                                                                           |
| 11 | polluted_mei                       | varchar(5)    | Sectors with values above Yearly median of SO2 label as ABOVE else BELOW                                                                                            |
| 12 | tso2                               | bigint        | Total so2 city sector. Filtered values above  4863 (5% of the distribution)                                                                                         |
| 13 | so2_intensity                      | decimal(21,5) | SO2 divided by output                                                                                                                                               |
| 14 | tso2_mandate_c                     | float         | city reduction mandate in tonnes                                                                                                                                    |
| 15 | in_10_000_tonnes                   | float         | city reduction mandate in 10k tonnes                                                                                                                                |
| 16 | output                             | decimal(16,5) | Output                                                                                                                                                              |
| 17 | employment                         | decimal(16,5) | Employemnt                                                                                                                                                          |
| 18 | sales                              | decimal(16,5) | Sales                                                                                                                                                               |
| 19 | capital                            | decimal(16,5) | Capital                                                                                                                                                             |
| 20 | credit_constraint                  | float         | Financial dependency. From paper https://www.sciencedirect.com/science/article/pii/S0147596715000311                                                                |
| 21 | receivable_curasset_ci             | double        | 应收帐款 (c80) / cuasset                                                                                                                                            |
| 22 | std_receivable_curasset_ci         | double        | standaridzed values (x - x mean) / std)                                                                                                                             |
| 23 | cash_over_curasset_ci              | double        | (其中：短期投资 (c79) - 应收帐款 (c80) - 存货 (c81) - 其中：产成品 (c82)) /current asset                                                                            |
| 24 | std_cash_over_curasset_ci          | double        | standaridzed values (x - x mean) / std)                                                                                                                             |
| 25 | working_capital_ci                 | double        | cuasset- 流动负债合计 (c95)                                                                                                                                         |
| 26 | std_working_capital_ci             | double        | standaridzed values (x - x mean) / std)                                                                                                                             |
| 27 | working_capital_requirement_ci     | double        | 存货 (c81) + 应收帐款 (c80) - 应付帐款  (c96)                                                                                                                       |
| 28 | std_working_capital_requirement_ci | double        | standaridzed values (x - x mean) / std)                                                                                                                             |
| 29 | current_ratio_ci                   | double        | cuasset/流动负债合计 (c95)                                                                                                                                          |
| 30 | std_current_ratio_ci               | double        | standaridzed values (x - x mean) / std)                                                                                                                             |
| 31 | quick_ratio_ci                     | double        | (cuasset-存货 (c81) ) / 流动负债合计 (c95)                                                                                                                          |
| 32 | std_quick_ratio_ci                 | double        | standaridzed values (x - x mean) / std)                                                                                                                             |
| 33 | cash_ratio_ci                      | double        | (cuasset -  其中：短期投资 (c79) - 应收帐款 (c80) - 存货 (c81)/ 流动负债合计 (c95)                                                                                  |
| 34 | std_cash_ratio_ci                  | double        | standaridzed values (x - x mean) / std)                                                                                                                             |
| 35 | liabilities_assets_ci              | double        | (流动负债合计 (c95) + 长期负债合计 (c97)) / toasset                                                                                                                 |
| 36 | std_liabilities_assets_ci          | double        | standaridzed values (x - x mean) / std)                                                                                                                             |
| 37 | return_on_asset_ci                 | double        | sales - (主营业务成本 (c108) + 营业费用 (c113) + 管理费用 (c114) + 财产保险费 (c116) + 劳动、失业保险费 (c118)+ 财务费用 (c124) + 本年应付工资总额 (wage)) /toasset |
| 38 | std_return_on_asset_ci             | double        | standaridzed values (x - x mean) / std)                                                                                                                             |
| 39 | sales_assets_ci                    | double        | 全年营业收入合计 (c64) /(\Delta toasset/2)                                                                                                                          |
| 40 | std_sales_assets_ci                | double        | standaridzed values (x - x mean) / std)                                                                                                                             |
| 41 | account_paybable_to_asset_ci       | double        | (\Delta 应付帐款  (c96))/ (\Delta (toasset))                                                                                                                        |
| 42 | std_account_paybable_to_asset_ci   | double        | standaridzed values (x - x mean) / std)                                                                                                                             |
| 43 | asset_tangibility_ci               | double        | Total fixed assets - Intangible assets                                                                                                                              |
| 44 | std_asset_tangibility_ci           | double        | standaridzed values (x - x mean) / std)                                                                                                                             |
| 45 | rd_intensity_ci                    | double        | rdfee/全年营业收入合计 (c64)                                                                                                                                        |
| 46 | std_rd_intensity_ci                | double        | standaridzed values (x - x mean) / std)                                                                                                                             |
| 47 | inventory_to_sales_ci              | double        | 存货 (c81) / sales                                                                                                                                                  |
| 48 | std_inventory_to_sales_ci          | double        | standaridzed values (x - x mean) / std)                                                                                                                             |
| 49 | lower_location                     | string        | Location city. one of Coastal, Central, Northwest, Northeast, Southwest                                                                                             |
| 50 | larger_location                    | string        | Location city. one of Eastern, Central, Western                                                                                                                     |
| 51 | coastal                            | string        | City is bordered by sea or not                                                                                                                                      |
| 52 | fe_c_i                             | bigint        | City industry fixed effect                                                                                                                                          |
| 53 | fe_t_i                             | bigint        | year industry fixed effect                                                                                                                                          |
| 54 | fe_c_t                             | bigint        | city industry fixed effect                                                                                                                                          |

    

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
| 13 | cash_ratio_i                      | double | (cuasset - 其中：短期投资 (c79) - 应收帐款 (c80) - 存货 (c81) - 其中：产成品 (c82))/ 流动负债合计 (c95)                                                             |
| 14 | std_cash_ratio_i                  | double | standaridzed values (x - x mean) / std)                                                                                                                             |
| 15 | liabilities_assets_i              | double | (流动负债合计 (c95) + 长期负债合计 (c97)) / toasset                                                                                                                 |
| 16 | std_liabilities_assets_i          | double | standaridzed values (x - x mean) / std)                                                                                                                             |
| 17 | return_on_asset_i                 | double | sales - (主营业务成本 (c108) + 营业费用 (c113) + 管理费用 (c114) + 财产保险费 (c116) + 劳动、失业保险费 (c118)+ 财务费用 (c124) + 本年应付工资总额 (wage)) /toasset |
| 18 | std_return_on_asset_i             | double | standaridzed values (x - x mean) / std)                                                                                                                             |
| 19 | sales_assets_i                    | double | 全年营业收入合计 (c64) /(\Delta toasset/2)                                                                                                                          |
| 20 | std_sales_assets_i                | double | standaridzed values (x - x mean) / std)                                                                                                                             |
| 21 | account_paybable_to_asset_i       | double | (\Delta 应付帐款  (c96))/ (\Delta (toasset))                                                                                                                        |
| 22 | std_account_paybable_to_asset_i   | double | standaridzed values (x - x mean) / std)                                                                                                                             |
| 23 | asset_tangibility_i               | double | Total fixed assets - Intangible assets                                                                                                                              |
| 24 | std_asset_tangibility_i           | double | standaridzed values (x - x mean) / std)                                                                                                                             |
| 25 | rd_intensity_i                    | double | rdfee/全年营业收入合计 (c64)                                                                                                                                        |
| 26 | std_rd_intensity_i                | double | standaridzed values (x - x mean) / std)                                                                                                                             |
| 27 | inventory_to_sales_i              | double | 存货 (c81) / sales                                                                                                                                                  |
| 28 | std_inventory_to_sales_i          | double | standaridzed values (x - x mean) / std)                                                                                                                             |

    

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
| 20 | credit_constraint                 | float         | Financial dependency. From paper https://www.sciencedirect.com/science/article/pii/S0147596715000311                                                                |
| 21 | receivable_curasset_i             | double        | 应收帐款 (c80) / cuasset                                                                                                                                            |
| 22 | std_receivable_curasset_i         | double        | standaridzed values (x - x mean) / std)                                                                                                                             |
| 23 | cash_over_curasset_i              | double        | (其中：短期投资 (c79) - 应收帐款 (c80) - 存货 (c81) - 其中：产成品 (c82)) /current asset                                                                            |
| 24 | std_cash_over_curasset_i          | double        | standaridzed values (x - x mean) / std)                                                                                                                             |
| 25 | working_capital_i                 | double        | cuasset- 流动负债合计 (c95)                                                                                                                                         |
| 26 | std_working_capital_i             | double        | standaridzed values (x - x mean) / std)                                                                                                                             |
| 27 | working_capital_requirement_i     | double        | 存货 (c81) + 应收帐款 (c80) - 应付帐款  (c96)                                                                                                                       |
| 28 | std_working_capital_requirement_i | double        | standaridzed values (x - x mean) / std)                                                                                                                             |
| 29 | current_ratio_i                   | double        | cuasset/流动负债合计 (c95)                                                                                                                                          |
| 30 | std_current_ratio_i               | double        | standaridzed values (x - x mean) / std)                                                                                                                             |
| 31 | quick_ratio_i                     | double        | (cuasset-存货 (c81) ) / 流动负债合计 (c95)                                                                                                                          |
| 32 | std_quick_ratio_i                 | double        | standaridzed values (x - x mean) / std)                                                                                                                             |
| 33 | cash_ratio_i                      | double        | (cuasset -  其中：短期投资 (c79) - 应收帐款 (c80) - 存货 (c81)/ 流动负债合计 (c95)                                                                                  |
| 34 | std_cash_ratio_i                  | double        | standaridzed values (x - x mean) / std)                                                                                                                             |
| 35 | liabilities_assets_i              | double        | (流动负债合计 (c95) + 长期负债合计 (c97)) / toasset                                                                                                                 |
| 36 | std_liabilities_assets_i          | double        | standaridzed values (x - x mean) / std)                                                                                                                             |
| 37 | return_on_asset_i                 | double        | sales - (主营业务成本 (c108) + 营业费用 (c113) + 管理费用 (c114) + 财产保险费 (c116) + 劳动、失业保险费 (c118)+ 财务费用 (c124) + 本年应付工资总额 (wage)) /toasset |
| 38 | std_return_on_asset_i             | double        | standaridzed values (x - x mean) / std)                                                                                                                             |
| 39 | sales_assets_i                    | double        | 全年营业收入合计 (c64) /(\Delta toasset/2)                                                                                                                          |
| 40 | std_sales_assets_i                | double        | standaridzed values (x - x mean) / std)                                                                                                                             |
| 41 | account_paybable_to_asset_i       | double        | (\Delta 应付帐款  (c96))/ (\Delta (toasset))                                                                                                                        |
| 42 | std_account_paybable_to_asset_i   | double        | standaridzed values (x - x mean) / std)                                                                                                                             |
| 43 | asset_tangibility_i               | double        | Total fixed assets - Intangible assets                                                                                                                              |
| 44 | std_asset_tangibility_i           | double        | standaridzed values (x - x mean) / std)                                                                                                                             |
| 45 | rd_intensity_i                    | double        | rdfee/全年营业收入合计 (c64)                                                                                                                                        |
| 46 | std_rd_intensity_i                | double        | standaridzed values (x - x mean) / std)                                                                                                                             |
| 47 | inventory_to_sales_i              | double        | 存货 (c81) / sales                                                                                                                                                  |
| 48 | std_inventory_to_sales_i          | double        | standaridzed values (x - x mean) / std)                                                                                                                             |
| 49 | lower_location                    | string        | Location city. one of Coastal, Central, Northwest, Northeast, Southwest                                                                                             |
| 50 | larger_location                   | string        | Location city. one of Eastern, Central, Western                                                                                                                     |
| 51 | coastal                           | string        | City is bordered by sea or not                                                                                                                                      |
| 52 | fe_c_i                            | bigint        | City industry fixed effect                                                                                                                                          |
| 53 | fe_t_i                            | bigint        | year industry fixed effect                                                                                                                                          |
| 54 | fe_c_t                            | bigint        | city industry fixed effect                                                                                                                                          |

    