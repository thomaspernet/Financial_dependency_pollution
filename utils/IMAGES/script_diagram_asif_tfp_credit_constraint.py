
from diagrams import Cluster, Diagram
from diagrams.aws.compute import ECS
from diagrams.aws.database import Redshift, RDS
from diagrams.aws.integration import SQS
from diagrams.aws.storage import S3

with Diagram("ASIF TFP CREDIT CONSTRAINT", show=False, filename="/Users/thomas/Google Drive/PROJECT/GITHUB/REPOSITORIES/Financial_dependency_pollution/utils/IMAGES/asif_tfp_credit_constraint", outformat="jpg"):

     temp_1 = S3('china_city_code_normalised')
     temp_2 = S3('china_city_sector_pollution')
     temp_3 = S3('china_city_reduction_mandate')
     temp_4 = S3('china_city_tcz_spz')
     temp_5 = S3('ind_cic_2_name')
     temp_6 = S3('province_credit_constraint')
     temp_7 = S3('china_credit_constraint')
     temp_8 = ECS('asif_firms_prepared')
     temp_9 = SQS('asif_tfp_firm_level')

     with Cluster("FINAL"):

         temp_final_1 = Redshift('asif_tfp_credit_constraint')


     temp_final_1 << temp_1
     temp_final_1 << temp_2
     temp_final_1 << temp_3
     temp_final_1 << temp_4
     temp_final_1 << temp_5
     temp_final_1 << temp_6
     temp_final_1 << temp_7
     temp_1 >>temp_8 >>temp_9 >> temp_final_1
