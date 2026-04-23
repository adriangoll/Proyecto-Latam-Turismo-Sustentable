output "glue_database_name" {
  value = aws_glue_catalog_database.this.name
}

output "glue_service_role_name" {
  value = aws_iam_role.glue_service_role.name
}

output "glue_service_role_arn" {
  value = aws_iam_role.glue_service_role.arn
}

output "bronze_co2_crawler_name" {
  value = aws_glue_crawler.bronze_co2.name
}

output "bronze_tourism_crawler_name" {
  value = aws_glue_crawler.bronze_tourism.name
}

output "bronze_transport_crawler_name" {
  value = aws_glue_crawler.bronze_transport.name
}

output "silver_co2_crawler_name" {
  value = aws_glue_crawler.silver_co2.name
}

output "silver_tourism_crawler_name" {
  value = aws_glue_crawler.silver_tourism.name
}

output "silver_transport_crawler_name" {
  value = aws_glue_crawler.silver_transport.name
}