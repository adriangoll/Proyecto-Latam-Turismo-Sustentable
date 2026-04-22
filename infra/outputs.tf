output "budget_name" {
  value = aws_budgets_budget.zero_budget.name
}

output "datalake_bucket_name" {
  value = module.s3.datalake_bucket_name
}

output "data_engineers_group_name" {
  value = module.iam.data_engineers_group_name
}

output "project_managers_group_name" {
  value = module.iam.project_managers_group_name
}

output "glue_database_name" {
  value = module.glue.glue_database_name
}

output "glue_service_role_name" {
  value = module.glue.glue_service_role_name
}

output "glue_service_role_arn" {
  value = module.glue.glue_service_role_arn
}

output "glue_crawler_name" {
  value = module.glue.glue_crawler_name
}

