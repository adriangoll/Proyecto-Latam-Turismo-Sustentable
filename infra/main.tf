resource "aws_budgets_budget" "zero_budget" {
	name = var.budget_name
	budget_type  = "COST"
	limit_amount = var.budget_limit_amount
	limit_unit = var.budget_limit_unit
	time_unit = var.budget_time_unit
  
    notification {
    comparison_operator        = "GREATER_THAN"
    threshold 				   = var.budget_threshold
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
}