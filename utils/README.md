# ASIF TFP CREDIT CONSTRAINT

Transform asif tfp firm level and others data by merging asif firms prepared, china credit constraint, ind cic 2 name, china city code normalised, china tcz spz, china city reduction mandate data by constructing asset_tangibility, cash_over_total_asset, sales over asset Andersen method, current ratio (Compute proxy for credit constraint , Construct main variable asset tangibility) to asif tfp credit constraint

* **[asif_tfp_credit_constraint](https://github.com/thomaspernet/Financial_dependency_pollution/blob/master/01_data_preprocessing/02_transform_tables/09_asif_tfp_firm_baseline.md)**
    * TRANSFORMATION
        * asif_tfp_firm_level
            * PREPARATION
                * [asif_firms_prepared](https://github.com/thomaspernet/Financial_dependency_pollution/blob/master/01_data_preprocessing/01_prepare_tables/00_prepare_asif.md)
                    * CREATION
                        * [asif_unzip_data_csv](https://github.com/thomaspernet/Financial_dependency_pollution/blob/master/01_data_preprocessing/00_download_data_from/ASIF_PANEL/firm_asif.py)
            * CREATION
                * [china_city_code_normalised](https://github.com/thomaspernet/Financial_dependency_pollution/blob/master/01_data_preprocessing/00_download_data_from/CITY_CODE_CORRESPONDANCE/city_code_correspondance.py)
    * CREATION
        * [china_city_code_normalised](https://github.com/thomaspernet/Financial_dependency_pollution/blob/master/01_data_preprocessing/00_download_data_from/CITY_CODE_CORRESPONDANCE/city_code_correspondance.py)
        * [china_city_sector_pollution](https://github.com/thomaspernet/Financial_dependency_pollution/blob/master/01_data_preprocessing/00_download_data_from/CITY_SECTOR_POLLUTION/city_sector_pollution.py)
        * [china_city_reduction_mandate](https://github.com/thomaspernet/Financial_dependency_pollution/blob/master/01_data_preprocessing/00_download_data_from/CITY_REDUCTION_MANDATE/city_reduction_mandate.py)
        * [china_city_tcz_spz](https://github.com/thomaspernet/Financial_dependency_pollution/blob/master/01_data_preprocessing/00_download_data_from/TCZ_SPZ/tcz_spz_policy.py)
        * [ind_cic_2_name](https://github.com/thomaspernet/Financial_dependency_pollution/blob/master/01_data_preprocessing/00_download_data_from/CIC_NAME/cic_industry_name.py)
        * [province_credit_constraint](None)
        * [china_credit_constraint](https://github.com/thomaspernet/Financial_dependency_pollution/blob/master/01_data_preprocessing/00_download_data_from/CIC_CREDIT_CONSTRAINT/financial_dependency.py)



# ETL diagrams


# fin_dep_pollution_baseline_city.jpg 

![](https://raw.githubusercontent.com/thomaspernet/Financial_dependency_pollution/master/utils/IMAGES/fin_dep_pollution_baseline_city.jpg)
# asif_tfp_credit_constraint.jpg 

![](https://raw.githubusercontent.com/thomaspernet/Financial_dependency_pollution/master/utils/IMAGES/asif_tfp_credit_constraint.jpg)