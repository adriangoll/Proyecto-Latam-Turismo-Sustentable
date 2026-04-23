variable "glue_database_name" {
  type = string
}


variable "glue_service_role_name" {
  type = string
}

variable "datalake_bucket_name" {
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
