data "aws_iam_group" "data_engineers" {
  group_name = var.data_engineers_group_name
}

data "aws_iam_group" "project_managers" {
  group_name = var.project_managers_group_name
}

