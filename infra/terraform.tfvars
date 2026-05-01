aws_region                = "us-east-1"
aws_profile               = "grupo1"
budget_name               = "zero-budget"
budget_limit_amount       = "0.01"
budget_limit_unit         = "USD"
budget_time_unit          = "MONTHLY"
budget_notification_email = "grupo1.soyhenry.de@gmail.com"
budget_threshold          = 0.01

datalake_bucket_name = "latam-sustainability-datalake"

data_engineers_group_name            = "data-engineers"
project_managers_group_name          = "project-manager"
data_engineers_logs_read_policy_name = "DataEngineersCloudWatchLogsReadPolicy"

data_engineers_users   = ["adrian.sosa", "luis.buruato", "mariana.gil", "martin.tedesco"]
project_managers_users = ["adrian.sosa"]

data_engineers_policy_arns = [
  "arn:aws:iam::aws:policy/AmazonAthenaFullAccess",
  "arn:aws:iam::aws:policy/AmazonEC2FullAccess",
  "arn:aws:iam::aws:policy/AmazonS3FullAccess",
  "arn:aws:iam::aws:policy/AWSGlueConsoleFullAccess"
]

project_managers_policy_arns = [
  "arn:aws:iam::aws:policy/AdministratorAccess",
  "arn:aws:iam::aws:policy/job-function/Billing",
  "arn:aws:iam::aws:policy/IAMFullAccess"
]

glue_database_name     = "latam_sustainable_tourism"
glue_service_role_name = "AWSGlueServiceRole-LatamSustainableTourism"

bronze_co2_crawler_name       = "latam-bronze-co2-crawler"
bronze_tourism_crawler_name   = "latam-bronze-tourism-crawler"
bronze_transport_crawler_name = "latam-bronze-transport-crawler"

silver_co2_crawler_name       = "latam-silver-co2-crawler"
silver_tourism_crawler_name   = "latam-silver-tourism-crawler"
silver_transport_crawler_name = "latam-silver-transport-crawler"

gold_dim_country_crawler_name            = "latam-gold-dim-country-crawler"
gold_fact_tourism_emissions_crawler_name = "latam-gold-fact-tourism-emissions-crawler"

bronze_co2_s3_target_path       = "s3://latam-sustainability-datalake/bronze/co2_emissions/"
bronze_tourism_s3_target_path   = "s3://latam-sustainability-datalake/bronze/tourism_arrivals/"
bronze_transport_s3_target_path = "s3://latam-sustainability-datalake/bronze/transport_mode/"

silver_co2_s3_target_path       = "s3://latam-sustainability-datalake/silver/co2_emissions/"
silver_tourism_s3_target_path   = "s3://latam-sustainability-datalake/silver/tourism_arrivals/"
silver_transport_s3_target_path = "s3://latam-sustainability-datalake/silver/transport_mode/"

gold_dim_country_s3_target_path            = "s3://latam-sustainability-datalake/gold/dim_country/"
gold_fact_tourism_emissions_s3_target_path = "s3://latam-sustainability-datalake/gold/fact_tourism_emissions/"

athena_workgroup_name = "latam-sustainable-tourism"
athena_results_prefix = "athena-results/"

# EC2 y EventBridge
ec2_role_name         = "LatamTourismEC2Role"
ec2_sg_name           = "latam-airflow-sg"
ec2_instance_name     = "latam-airflow-ec2"
ec2_instance_type     = "t3.small"
ec2_ami               = "ami-0ff290337e78c83bf"
aws_account_id        = "132425676374"
eventbridge_rule_name = "latam-airflow-monthly-start"
eventbridge_role_name = "LatamEventBridgeEC2Role"