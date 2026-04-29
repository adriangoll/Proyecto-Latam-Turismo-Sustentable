resource "aws_budgets_budget" "zero_budget" {
  name         = var.budget_name
  budget_type  = "COST"
  limit_amount = var.budget_limit_amount
  limit_unit   = var.budget_limit_unit
  time_unit    = var.budget_time_unit

  notification {
    comparison_operator        = "GREATER_THAN"
    threshold                  = var.budget_threshold
    threshold_type             = "ABSOLUTE_VALUE"
    notification_type          = "ACTUAL"
    subscriber_email_addresses = [var.budget_notification_email]
  }
}

module "s3" {
  source               = "./modules/s3"
  datalake_bucket_name = var.datalake_bucket_name
}

module "iam" {
  source = "./modules/iam"

  data_engineers_group_name   = var.data_engineers_group_name
  project_managers_group_name = var.project_managers_group_name

  data_engineers_users   = var.data_engineers_users
  project_managers_users = var.project_managers_users

  data_engineers_policy_arns   = var.data_engineers_policy_arns
  project_managers_policy_arns = var.project_managers_policy_arns

  data_engineers_logs_read_policy_name = var.data_engineers_logs_read_policy_name
}

module "glue" {
  source                 = "./modules/glue"
  glue_database_name     = var.glue_database_name
  glue_service_role_name = var.glue_service_role_name
  datalake_bucket_name   = var.datalake_bucket_name

  bronze_co2_crawler_name       = var.bronze_co2_crawler_name
  bronze_tourism_crawler_name   = var.bronze_tourism_crawler_name
  bronze_transport_crawler_name = var.bronze_transport_crawler_name

  silver_co2_crawler_name       = var.silver_co2_crawler_name
  silver_tourism_crawler_name   = var.silver_tourism_crawler_name
  silver_transport_crawler_name = var.silver_transport_crawler_name

  gold_dim_country_crawler_name            = var.gold_dim_country_crawler_name
  gold_fact_tourism_emissions_crawler_name = var.gold_fact_tourism_emissions_crawler_name

  bronze_co2_s3_target_path       = var.bronze_co2_s3_target_path
  bronze_tourism_s3_target_path   = var.bronze_tourism_s3_target_path
  bronze_transport_s3_target_path = var.bronze_transport_s3_target_path

  silver_co2_s3_target_path       = var.silver_co2_s3_target_path
  silver_tourism_s3_target_path   = var.silver_tourism_s3_target_path
  silver_transport_s3_target_path = var.silver_transport_s3_target_path

  gold_dim_country_s3_target_path            = var.gold_dim_country_s3_target_path
  gold_fact_tourism_emissions_s3_target_path = var.gold_fact_tourism_emissions_s3_target_path

}

module "athena" {
  source = "./modules/athena"

  athena_workgroup_name = var.athena_workgroup_name
  datalake_bucket_name  = var.datalake_bucket_name
  athena_results_prefix = var.athena_results_prefix
}