data "aws_iam_group" "data_engineers" {
  group_name = var.data_engineers_group_name
}

data "aws_iam_group" "project_managers" {
  group_name = var.project_managers_group_name
}

resource "aws_iam_group_policy_attachment" "data_engineers" {
  for_each = toset(var.data_engineers_policy_arns)

  group      = data.aws_iam_group.data_engineers.group_name
  policy_arn = each.value
}

resource "aws_iam_group_policy_attachment" "project_managers" {
  for_each = toset(var.project_managers_policy_arns)

  group      = data.aws_iam_group.project_managers.group_name
  policy_arn = each.value
}