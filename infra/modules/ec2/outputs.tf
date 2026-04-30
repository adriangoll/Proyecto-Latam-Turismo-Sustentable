output "ec2_instance_id" {
  value = aws_instance.airflow.id
}

output "ec2_public_ip" {
  value = aws_instance.airflow.public_ip
}

output "ec2_role_name" {
  value = aws_iam_role.ec2_role.name
}

output "ec2_sg_id" {
  value = aws_security_group.ec2_sg.id
}

output "eventbridge_rule_name" {
  value = aws_cloudwatch_event_rule.ec2_monthly_start.name
}