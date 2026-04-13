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
