terraform {
  required_providers {
    aws = {
      source = "hashicorp/aws"
    }
  }
}

# --- IAM Role para la EC2 ---
data "aws_iam_policy_document" "ec2_assume_role" {
  statement {
    effect = "Allow"

    principals {
      type        = "Service"
      identifiers = ["ec2.amazonaws.com"]
    }

    actions = ["sts:AssumeRole"]
  }
}

resource "aws_iam_role" "ec2_role" {
  name               = var.ec2_role_name
  assume_role_policy = data.aws_iam_policy_document.ec2_assume_role.json
}

# Permiso SSM para conectarse sin SSH
resource "aws_iam_role_policy_attachment" "ec2_ssm" {
  role       = aws_iam_role.ec2_role.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore"
}

# Adjunta permisos de CloudWatch Logs al rol EC2 (necesario para awslogs de Docker)
resource "aws_iam_role_policy_attachment" "ec2_cloudwatch_logs" {
  role       = aws_iam_role.ec2_role.name
  policy_arn = "arn:aws:iam::aws:policy/CloudWatchLogsFullAccess"
}

# Permisos S3 sobre el datalake
data "aws_iam_policy_document" "ec2_s3_access" {
  statement {
    effect = "Allow"

    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:ListBucket"
    ]

    resources = [
      "arn:aws:s3:::${var.datalake_bucket_name}",
      "arn:aws:s3:::${var.datalake_bucket_name}/*"
    ]
  }

  statement {
    effect = "Allow"

    actions = [
      "ec2:StopInstances"
    ]

    resources = ["*"]

    condition {
      test     = "StringEquals"
      variable = "ec2:ResourceTag/Name"
      values   = [var.ec2_instance_name]
    }
  }

  statement {
    effect = "Allow"

    actions = [
      "ec2:DescribeInstances"
    ]

    resources = ["*"]
  }

  statement {
    effect = "Allow"

    actions = [
      "logs:CreateLogGroup",
      "logs:CreateLogStream",
      "logs:PutLogEvents",
      "logs:DescribeLogStreams"
    ]

    resources = [
      "arn:aws:logs:${var.aws_region}:${var.aws_account_id}:log-group:/latam-turismo/airflow",
      "arn:aws:logs:${var.aws_region}:${var.aws_account_id}:log-group:/latam-turismo/airflow:*"
    ]
  }
}

resource "aws_iam_role_policy" "ec2_s3_access" {
  name   = "${var.ec2_role_name}-s3-access"
  role   = aws_iam_role.ec2_role.id
  policy = data.aws_iam_policy_document.ec2_s3_access.json
}

resource "aws_iam_instance_profile" "ec2_profile" {
  name = var.ec2_role_name
  role = aws_iam_role.ec2_role.name
}

# --- Security Group ---
resource "aws_security_group" "ec2_sg" {
  name        = var.ec2_sg_name
  description = "Security group para Airflow en EC2"

  ingress {
    description = "Airflow UI"
    from_port   = 8080
    to_port     = 8080
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

# --- EC2 ---
resource "aws_instance" "airflow" {
  ami                         = var.ec2_ami
  instance_type               = var.ec2_instance_type
  iam_instance_profile        = aws_iam_instance_profile.ec2_profile.name
  vpc_security_group_ids      = [aws_security_group.ec2_sg.id]
  associate_public_ip_address = true

  tags = {
    Name = var.ec2_instance_name
  }

  user_data = <<-EOF
  #!/bin/bash
  set -e

  apt-get update -y
  apt-get install -y docker.io docker-compose git curl

  systemctl enable docker
  systemctl start docker

  cd /opt

  if [ ! -d "/opt/app" ]; then
    git clone https://github.com/adriangoll/Proyecto-Latam-Turismo-Sustentable.git app
  fi

  cd /opt/app/docker

  mkdir -p ../airflow/dags

  docker-compose run --rm airflow-init
  docker-compose up -d airflow-webserver airflow-scheduler postgres

  cat > /etc/systemd/system/airflow.service <<'SERVICE'
  [Unit]
  Description=Airflow Docker Compose
  After=docker.service
  Requires=docker.service

  [Service]
  Type=oneshot
  RemainAfterExit=true
  WorkingDirectory=/opt/app/docker
  ExecStart=/usr/bin/docker-compose up -d airflow-webserver airflow-scheduler postgres
  ExecStop=/usr/bin/docker-compose down
  TimeoutStartSec=0

  [Install]
  WantedBy=multi-user.target
  SERVICE

  systemctl daemon-reload
  systemctl enable airflow.service
EOF

}

# --- EventBridge para arrancar la EC2 mensualmente ---
resource "aws_cloudwatch_event_rule" "ec2_monthly_start" {
  name                = var.eventbridge_rule_name
  description         = "Arranca la EC2 de Airflow el dia 1 de cada mes"
  schedule_expression = "cron(0 0 1 * ? *)"
}

resource "aws_cloudwatch_event_target" "ec2_start_target" {
  rule     = aws_cloudwatch_event_rule.ec2_monthly_start.name
  arn      = "arn:aws:ssm:${var.aws_region}::automation-definition/AWS-StartEC2Instance:$DEFAULT"
  role_arn = aws_iam_role.eventbridge_role.arn

  input = jsonencode({
    InstanceId = [aws_instance.airflow.id]
  })
}

# --- IAM Role para EventBridge ---
data "aws_iam_policy_document" "eventbridge_assume_role" {
  statement {
    effect = "Allow"

    principals {
      type        = "Service"
      identifiers = ["events.amazonaws.com"]
    }

    actions = ["sts:AssumeRole"]
  }
}

resource "aws_iam_role" "eventbridge_role" {
  name               = var.eventbridge_role_name
  assume_role_policy = data.aws_iam_policy_document.eventbridge_assume_role.json
}

data "aws_iam_policy_document" "eventbridge_ec2_start" {
  statement {
    effect = "Allow"

    actions = [
      "ssm:StartAutomationExecution"
    ]

    resources = [
      "arn:aws:ssm:${var.aws_region}::automation-definition/AWS-StartEC2Instance:*"
    ]
  }

  statement {
    effect = "Allow"

    actions = [
      "ec2:StartInstances"
    ]

    resources = [
      "arn:aws:ec2:${var.aws_region}:${var.aws_account_id}:instance/*"
    ]
  }
}

resource "aws_iam_role_policy" "eventbridge_ec2_start" {
  name   = "${var.eventbridge_role_name}-ec2-start"
  role   = aws_iam_role.eventbridge_role.id
  policy = data.aws_iam_policy_document.eventbridge_ec2_start.json
}

# --- Grupo de logs en CloudWatch para Airflow (usado por Docker awslogs) ---
resource "aws_cloudwatch_log_group" "airflow" {
  name              = "/latam-turismo/airflow"
  retention_in_days = 7
}

resource "aws_cloudwatch_log_stream" "airflow_init" {
  name           = "init"
  log_group_name = aws_cloudwatch_log_group.airflow.name
}

resource "aws_cloudwatch_log_stream" "airflow_webserver" {
  name           = "webserver"
  log_group_name = aws_cloudwatch_log_group.airflow.name
}

resource "aws_cloudwatch_log_stream" "airflow_scheduler" {
  name           = "scheduler"
  log_group_name = aws_cloudwatch_log_group.airflow.name
}