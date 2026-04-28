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

resource "aws_iam_policy" "data_engineers_logs_read" {
  name = var.data_engineers_logs_read_policy_name

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "logs:DescribeLogGroups",
          "logs:DescribeLogStreams",
          "logs:GetLogEvents",
          "logs:FilterLogEvents"
        ]
        Resource = "*"
      }
    ]
  })
}

resource "aws_iam_group_policy_attachment" "data_engineers_logs_read" {
  group      = data.aws_iam_group.data_engineers.group_name
  policy_arn = aws_iam_policy.data_engineers_logs_read.arn
}