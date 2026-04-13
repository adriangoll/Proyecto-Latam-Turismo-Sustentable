output "budget_name" {
  value = aws_budgets_budget.zero_budget.name
}

output "datalake_bucket_name" {
  value = module.s3.datalake_bucket_name
}