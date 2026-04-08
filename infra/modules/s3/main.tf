resource "aws_s3_bucket" "bronze" {
  bucket = var.bronze_bucket_name
}

module "s3" {
  source             = "./modules/s3"
  bronze_bucket_name = "latam-sustainability-bronze"
}