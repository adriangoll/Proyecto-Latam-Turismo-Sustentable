# -------------------------------------------------------
# Generales y budget
# -------------------------------------------------------

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

# -------------------------------------------------------
# IAM
# -------------------------------------------------------

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

variable "glue_service_role_name" {
  type = string
}

variable "data_engineers_logs_read_policy_name" {
  type = string
}

# -------------------------------------------------------
# s3
# -------------------------------------------------------

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

variable "gold_dim_country_s3_target_path" {
  type = string
}

variable "gold_fact_tourism_emissions_s3_target_path" {
  type = string
}

# -------------------------------------------------------
# Glue
# -------------------------------------------------------
variable "glue_database_name" {
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

variable "gold_dim_country_crawler_name" {
  type = string
}

variable "gold_fact_tourism_emissions_crawler_name" {
  type = string
}

# -------------------------------------------------------
# Athena
# -------------------------------------------------------

variable "athena_workgroup_name" {
  type = string
}

variable "athena_results_prefix" {
  type = string
}


# -------------------------------------------------------
# EC2 y EventBridge
# -------------------------------------------------------
variable "ec2_role_name" {
  type = string
}

variable "ec2_sg_name" {
  type = string
}

variable "ec2_instance_name" {
  type = string
}

variable "ec2_instance_type" {
  type = string
}

variable "ec2_ami" {
  type = string
}

variable "eventbridge_rule_name" {
  type = string
}

variable "eventbridge_role_name" {
  type = string
}

variable "aws_account_id" {
  type = string
}