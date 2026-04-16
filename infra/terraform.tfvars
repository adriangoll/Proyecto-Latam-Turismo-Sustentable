aws_region                = "us-east-1"
aws_profile               = "grupo1"
budget_name               = "zero-budget"
budget_limit_amount       = "0.01"
budget_limit_unit         = "USD"
budget_time_unit          = "MONTHLY"
budget_notification_email = "grupo1.soyhenry.de@gmail.com"
budget_threshold          = 0.01

datalake_bucket_name      = "latam-sustainability-datalake"

data_engineers_group_name   = "data-engineers"
project_managers_group_name = "project-manager"

data_engineers_users = ["adrian.sosa", "luis.buruato", "mariana.gil", "martin.tedesco"]
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

glue_database_name = "latam_sustainable_tourism"
glue_service_role_name = "AWSGlueServiceRole-LatamSustainableTourism"
glue_crawler_name      = "latam-sustainable-tourism-crawler"
crawler_s3_target_path = "s3://latam-sustainability-datalake/raw/"
