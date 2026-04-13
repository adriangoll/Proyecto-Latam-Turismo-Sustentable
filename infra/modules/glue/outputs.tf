output "glue_database_name" {
  value = aws_glue_catalog_database.this.name
}

output "glue_service_role_name" {
  value = aws_iam_role.glue_service_role.name
}

output "glue_service_role_arn" {
  value = aws_iam_role.glue_service_role.arn
}

output "glue_crawler_name" {
  value = aws_glue_crawler.this.name
}