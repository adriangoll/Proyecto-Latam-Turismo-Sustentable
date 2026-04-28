terraform {
  required_providers {
    aws = {
      source = "hashicorp/aws"
    }
  }
}

resource "aws_athena_workgroup" "this" {
  name = var.athena_workgroup_name

  configuration {
    result_configuration {
      output_location = "s3://${var.datalake_bucket_name}/${var.athena_results_prefix}"
    }
  }
}