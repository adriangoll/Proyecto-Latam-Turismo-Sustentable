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