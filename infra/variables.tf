variable "aws_region" {
  type    = string
}

variable "aws_profile" {
  type    = string
}

variable "budget_name" {
  type    = string
}

variable "budget_limit_amount" {
  type    = string
}

variable "budget_limit_unit" {
  type    = string
}

variable "budget_time_unit" {
  type    = string
}

variable "budget_notification_email" {
  type    = string
}

variable "budget_threshold" {
  type    = number
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