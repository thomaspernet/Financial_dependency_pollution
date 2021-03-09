
from diagrams import Cluster, Diagram
from diagrams.aws.compute import ECS
from diagrams.aws.database import Redshift, RDS
from diagrams.aws.integration import SQS
from diagrams.aws.storage import S3

with Diagram("FIN DEP POLLUTION BASELINE CITY", show=False, filename="/home/ec2-user/Financial_dependency_pollution/utils/IMAGES/fin_dep_pollution_baseline_city", outformat="jpg"):

     temp_1 = S3('china_city_sector_pollution')
     temp_2 = S3('china_city_code_normalised')
     temp_3 = S3('china_city_reduction_mandate')
     temp_4 = S3('china_city_tcz_spz')
     temp_5 = S3('ind_cic_2_name')
     temp_6 = S3('china_credit_constraint')
     temp_7 = S3('province_credit_constraint')
     temp_8 = ECS('asif_firms_prepared')
     temp_12 = SQS('asif_industry_financial_ratio_city')
     temp_13 = SQS('china_sector_pollution_threshold')
     temp_14 = SQS('asif_tfp_firm_level')
     temp_15 = SQS('asif_industry_characteristics_ownership')
     temp_16 = SQS('asif_city_characteristics_ownership')

     with Cluster("FINAL"):

         temp_final_1 = Redshift('fin_dep_pollution_baseline_city')


     temp_final_1 << temp_1
     temp_final_1 << temp_2
     temp_final_1 << temp_3
     temp_final_1 << temp_4
     temp_final_1 << temp_5
     temp_final_1 << temp_6
     temp_final_1 << temp_7
     temp_2 >>temp_8 >>temp_12 >> temp_final_1
     temp_1 >>temp_13 >> temp_final_1
     temp_2 >>temp_8 >>temp_14 >> temp_final_1
     temp_2 >>temp_8 >>temp_15 >> temp_final_1
     temp_2 >>temp_8 >>temp_16 >> temp_final_1
