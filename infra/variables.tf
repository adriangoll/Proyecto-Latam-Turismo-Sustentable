variable "aws_region" {
  type = string
}

variable "aws_profile" {
  type = string
}

variable "budget_name" {
  type = string
}

variable "budget_limit_amount" {
  type = string
}

variable "budget_limit_unit" {
  type = string
}

variable "budget_time_unit" {
  type = string
}

variable "budget_notification_email" {
  type = string
}

variable "budget_threshold" {
  type = number
}

variable "datalake_bucket_name" {
  type = string
}

variable "data_engineers_group_name" {
  type = string
}

variable "project_managers_group_name" {
  type = string
}

variable "data_engineers_users" {
  type = list(string)
}

variable "project_managers_users" {
  type = list(string)
}

variable "data_engineers_policy_arns" {
  type = list(string)
}

variable "project_managers_policy_arns" {
  type = list(string)
}

variable "glue_database_name" {
  type = string
}

variable "glue_service_role_name" {
  type = string
}

variable "bronze_co2_crawler_name" {
  type = string
}

variable "bronze_tourism_crawler_name" {
  type = string
}

variable "bronze_transport_crawler_name" {
  type = string
}

variable "silver_co2_crawler_name" {
  type = string
}

variable "silver_tourism_crawler_name" {
  type = string
}

variable "silver_transport_crawler_name" {
  type = string
}

variable "bronze_co2_s3_target_path" {
  type = string
}

variable "bronze_tourism_s3_target_path" {
  type = string
}

variable "bronze_transport_s3_target_path" {
  type = string
}

variable "silver_co2_s3_target_path" {
  type = string
}

variable "silver_tourism_s3_target_path" {
  type = string
}

variable "silver_transport_s3_target_path" {
  type = string
}
