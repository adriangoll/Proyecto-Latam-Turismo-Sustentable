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
}

module "glue" {
  source                 = "./modules/glue"
  glue_database_name     = var.glue_database_name
  glue_service_role_name = var.glue_service_role_name
  glue_crawler_name      = var.glue_crawler_name
  datalake_bucket_name   = var.datalake_bucket_name
  crawler_s3_target_path = var.crawler_s3_target_path
}
