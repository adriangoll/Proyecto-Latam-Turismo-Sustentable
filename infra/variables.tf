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