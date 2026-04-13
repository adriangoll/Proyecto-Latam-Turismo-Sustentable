output "data_engineers_group_name" {
  value = data.aws_iam_group.data_engineers.group_name
}

output "project_managers_group_name" {
  value = data.aws_iam_group.project_managers.group_name
}