# ASIF TFP CREDIT CONSTRAINT

Transform asif tfp firm level and others data by merging asif firms prepared, china credit constraint, ind cic 2 name, china city code normalised, china tcz spz, china city reduction mandate data by constructing asset_tangibility, cash_over_total_asset, sales over asset Andersen method, current ratio (Compute proxy for credit constraint , Construct main variable asset tangibility) to asif tfp credit constraint

* **[asif_tfp_credit_constraint](https://github.com/thomaspernet/Financial_dependency_pollution/blob/master/01_data_preprocessing/02_transform_tables/09_asif_tfp_firm_baseline.md)**: 
Transform asif tfp firm level and others data by merging asif firms prepared, china credit constraint, ind cic 2 name, china city code normalised, china tcz spz, china city reduction mandate data by constructing asset_tangibility, cash_over_total_asset, sales over asset Andersen method, current ratio (Compute proxy for credit constraint , Construct main variable asset tangibility) to asif tfp credit constraint

    * TRANSFORMATION
        * [asif_tfp_firm_level](https://github.com/thomaspernet/Financial_dependency_pollution/blob/master/01_data_preprocessing/02_transform_tables/05_tfp_computation.md): 
Compute TFP using Olley and Pakes approach at the firm level

            * PREPARATION
                * [asif_firms_prepared](https://github.com/thomaspernet/Financial_dependency_pollution/blob/master/01_data_preprocessing/01_prepare_tables/00_prepare_asif.md): 
Prepare ASIF raw data by removing unconsistent year format, industry and birth year

                    * CREATION
                        * [asif_unzip_data_csv](https://github.com/thomaspernet/Financial_dependency_pollution/blob/master/01_data_preprocessing/00_download_data_from/ASIF_PANEL/firm_asif.py): Create Firms survey ASIF panel data from STATA
            * CREATION
                * [china_city_code_normalised](https://github.com/thomaspernet/Financial_dependency_pollution/blob/master/01_data_preprocessing/00_download_data_from/CITY_CODE_CORRESPONDANCE/city_code_correspondance.py): Create consistent city code 
    * CREATION
        * [china_city_code_normalised](https://github.com/thomaspernet/Financial_dependency_pollution/blob/master/01_data_preprocessing/00_download_data_from/CITY_CODE_CORRESPONDANCE/city_code_correspondance.py): Create consistent city code 
        * [china_city_sector_pollution](https://github.com/thomaspernet/Financial_dependency_pollution/blob/master/01_data_preprocessing/00_download_data_from/CITY_SECTOR_POLLUTION/city_sector_pollution.py): Create China city sector pollution
        * [china_city_reduction_mandate](https://github.com/thomaspernet/Financial_dependency_pollution/blob/master/01_data_preprocessing/00_download_data_from/CITY_REDUCTION_MANDATE/city_reduction_mandate.py): Create city reduction mandate policy 6th FYP
        * [china_city_tcz_spz](https://github.com/thomaspernet/Financial_dependency_pollution/blob/master/01_data_preprocessing/00_download_data_from/TCZ_SPZ/tcz_spz_policy.py): Create Control zone policy city
        * [ind_cic_2_name](https://github.com/thomaspernet/Financial_dependency_pollution/blob/master/01_data_preprocessing/00_download_data_from/CIC_NAME/cic_industry_name.py): Create industry name 2 digits
        * [province_credit_constraint](None): None
        * [china_credit_constraint](https://github.com/thomaspernet/Financial_dependency_pollution/blob/master/01_data_preprocessing/00_download_data_from/CIC_CREDIT_CONSTRAINT/financial_dependency.py): Create financial dependency at industry name 2 digits

### ETL diagrams



![](https://raw.githubusercontent.com/thomaspernet/Financial_dependency_pollution/master/utils/IMAGES/asif_tfp_credit_constraint.jpg)

# FIN DEP POLLUTION BASELINE CITY

Transform (creating time-break variables and fixed effect) asif_financial_ratio data and merging pollution, industry and city mandate tables
with financial ratio at the city level

* **[fin_dep_pollution_baseline_city](https://github.com/thomaspernet/Financial_dependency_pollution/blob/master/01_data_preprocessing/02_transform_tables/04_fin_dep_pol_baseline_city.md)**: 
Transform (creating time-break variables and fixed effect) asif_financial_ratio data and merging pollution, industry and city mandate tables
with financial ratio at the city level

    * TRANSFORMATION
        * [asif_industry_financial_ratio_city](https://github.com/thomaspernet/Financial_dependency_pollution/blob/master/01_data_preprocessing/02_transform_tables/03_asif_financial_ratio_city.md): 
Compute the financial ratio by city-industry

            * PREPARATION
                * [asif_firms_prepared](https://github.com/thomaspernet/Financial_dependency_pollution/blob/master/01_data_preprocessing/01_prepare_tables/00_prepare_asif.md): 
Prepare ASIF raw data by removing unconsistent year format, industry and birth year

                    * CREATION
                        * [asif_unzip_data_csv](https://github.com/thomaspernet/Financial_dependency_pollution/blob/master/01_data_preprocessing/00_download_data_from/ASIF_PANEL/firm_asif.py): Create Firms survey ASIF panel data from STATA
            * CREATION
                * [china_city_code_normalised](https://github.com/thomaspernet/Financial_dependency_pollution/blob/master/01_data_preprocessing/00_download_data_from/CITY_CODE_CORRESPONDANCE/city_code_correspondance.py): Create consistent city code 
    * TRANSFORMATION
        * [china_sector_pollution_threshold](https://github.com/thomaspernet/Financial_dependency_pollution/blob/master/01_data_preprocessing/02_transform_tables/02_so2_polluted_sectors.md): 
 Yearly Rank sectors based on SO2 emissionsand label them as ABOVE or BELOW

            * CREATION
                * [china_city_sector_pollution](https://github.com/thomaspernet/Financial_dependency_pollution/blob/master/01_data_preprocessing/00_download_data_from/CITY_SECTOR_POLLUTION/city_sector_pollution.py): Create China city sector pollution
    * TRANSFORMATION
        * [china_city_sector_year_pollution_threshold](https://github.com/thomaspernet/Financial_dependency_pollution/blob/master/01_data_preprocessing/02_transform_tables/10_city_sector_year_polluted_sectors.md): 
Transform china city sector pollution and others data by merging china city code normalised 
data by constructing polluted sectors city-industry-year (Compute polluted sectors at 
decile .5, .75, .80, .85, .90 and .95 at the city-industry-year level) 
to china city sector year pollution threshold

            * CREATION
                * [china_city_sector_pollution](https://github.com/thomaspernet/Financial_dependency_pollution/blob/master/01_data_preprocessing/00_download_data_from/CITY_SECTOR_POLLUTION/city_sector_pollution.py): Create China city sector pollution
            * CREATION
                * [china_city_code_normalised](https://github.com/thomaspernet/Financial_dependency_pollution/blob/master/01_data_preprocessing/00_download_data_from/CITY_CODE_CORRESPONDANCE/city_code_correspondance.py): Create consistent city code 
    * TRANSFORMATION
        * [asif_tfp_firm_level](https://github.com/thomaspernet/Financial_dependency_pollution/blob/master/01_data_preprocessing/02_transform_tables/05_tfp_computation.md): 
Compute TFP using Olley and Pakes approach at the firm level

            * PREPARATION
                * [asif_firms_prepared](https://github.com/thomaspernet/Financial_dependency_pollution/blob/master/01_data_preprocessing/01_prepare_tables/00_prepare_asif.md): 
Prepare ASIF raw data by removing unconsistent year format, industry and birth year

                    * CREATION
                        * [asif_unzip_data_csv](https://github.com/thomaspernet/Financial_dependency_pollution/blob/master/01_data_preprocessing/00_download_data_from/ASIF_PANEL/firm_asif.py): Create Firms survey ASIF panel data from STATA
            * CREATION
                * [china_city_code_normalised](https://github.com/thomaspernet/Financial_dependency_pollution/blob/master/01_data_preprocessing/00_download_data_from/CITY_CODE_CORRESPONDANCE/city_code_correspondance.py): Create consistent city code 
    * TRANSFORMATION
        * [asif_industry_characteristics_ownership](https://github.com/thomaspernet/Financial_dependency_pollution/blob/master/01_data_preprocessing/02_transform_tables/08_dominated_industry_ownership.md): 
Transform asif firms prepared data by merging china city code normalised data by constructing foreign_vs_domestic, foreign_size, domestic_size, private_size, public_size, soe_vs_private (create dominated industry by ownership (public-private, foreign-domestic) using industry size) to asif industry characteristics  ownership

            * PREPARATION
                * [asif_firms_prepared](https://github.com/thomaspernet/Financial_dependency_pollution/blob/master/01_data_preprocessing/01_prepare_tables/00_prepare_asif.md): 
Prepare ASIF raw data by removing unconsistent year format, industry and birth year

                    * CREATION
                        * [asif_unzip_data_csv](https://github.com/thomaspernet/Financial_dependency_pollution/blob/master/01_data_preprocessing/00_download_data_from/ASIF_PANEL/firm_asif.py): Create Firms survey ASIF panel data from STATA
            * CREATION
                * [china_city_code_normalised](https://github.com/thomaspernet/Financial_dependency_pollution/blob/master/01_data_preprocessing/00_download_data_from/CITY_CODE_CORRESPONDANCE/city_code_correspondance.py): Create consistent city code 
    * TRANSFORMATION
        * [asif_city_characteristics_ownership](https://github.com/thomaspernet/Financial_dependency_pollution/blob/master/01_data_preprocessing/02_transform_tables/07_dominated_city_ownership.md): 
Transform asif firms prepared and others data by merging china city code normalised data by constructing domestic_size, foreign_size, private_size, public_size (create dominated city by ownership (public-private, foreign-domestic) using city size) to asif city characteristics ownership

            * PREPARATION
                * [asif_firms_prepared](https://github.com/thomaspernet/Financial_dependency_pollution/blob/master/01_data_preprocessing/01_prepare_tables/00_prepare_asif.md): 
Prepare ASIF raw data by removing unconsistent year format, industry and birth year

                    * CREATION
                        * [asif_unzip_data_csv](https://github.com/thomaspernet/Financial_dependency_pollution/blob/master/01_data_preprocessing/00_download_data_from/ASIF_PANEL/firm_asif.py): Create Firms survey ASIF panel data from STATA
            * CREATION
                * [china_city_code_normalised](https://github.com/thomaspernet/Financial_dependency_pollution/blob/master/01_data_preprocessing/00_download_data_from/CITY_CODE_CORRESPONDANCE/city_code_correspondance.py): Create consistent city code 
    * CREATION
        * [china_city_sector_pollution](https://github.com/thomaspernet/Financial_dependency_pollution/blob/master/01_data_preprocessing/00_download_data_from/CITY_SECTOR_POLLUTION/city_sector_pollution.py): Create China city sector pollution
        * [china_city_code_normalised](https://github.com/thomaspernet/Financial_dependency_pollution/blob/master/01_data_preprocessing/00_download_data_from/CITY_CODE_CORRESPONDANCE/city_code_correspondance.py): Create consistent city code 
        * [china_city_reduction_mandate](https://github.com/thomaspernet/Financial_dependency_pollution/blob/master/01_data_preprocessing/00_download_data_from/CITY_REDUCTION_MANDATE/city_reduction_mandate.py): Create city reduction mandate policy 6th FYP
        * [china_city_tcz_spz](https://github.com/thomaspernet/Financial_dependency_pollution/blob/master/01_data_preprocessing/00_download_data_from/TCZ_SPZ/tcz_spz_policy.py): Create Control zone policy city
        * [ind_cic_2_name](https://github.com/thomaspernet/Financial_dependency_pollution/blob/master/01_data_preprocessing/00_download_data_from/CIC_NAME/cic_industry_name.py): Create industry name 2 digits
        * [china_credit_constraint](https://github.com/thomaspernet/Financial_dependency_pollution/blob/master/01_data_preprocessing/00_download_data_from/CIC_CREDIT_CONSTRAINT/financial_dependency.py): Create financial dependency at industry name 2 digits
        * [province_credit_constraint](None): None

### ETL diagrams



![](https://raw.githubusercontent.com/thomaspernet/Financial_dependency_pollution/master/utils/IMAGES/fin_dep_pollution_baseline_city.jpg)

