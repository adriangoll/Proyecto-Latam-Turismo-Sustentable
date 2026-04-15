resource "aws_s3_bucket" "datalake" {
  bucket = var.datalake_bucket_name
}

resource "aws_s3_bucket_versioning" "datalake" {
  bucket = aws_s3_bucket.datalake.id

  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_object" "raw_prefix" {
  bucket  = aws_s3_bucket.datalake.id
  key     = "raw/"
  content = ""
}

resource "aws_s3_object" "bronze_prefix" {
  bucket  = aws_s3_bucket.datalake.id
  key     = "bronze/"
  content = ""
}

resource "aws_s3_object" "silver_prefix" {
  bucket  = aws_s3_bucket.datalake.id
  key     = "silver/"
  content = ""
}

resource "aws_s3_object" "gold_prefix" {
  bucket  = aws_s3_bucket.datalake.id
  key     = "gold/"
  content = ""
}

resource "aws_s3_object" "raw_UNWTO_co2_prefix" {
  bucket  = aws_s3_bucket.datalake.id
  key     = "raw/UNWTO_co2/"
  content = ""
}

resource "aws_s3_object" "bronze_co2_emissions_prefix" {
  bucket  = aws_s3_bucket.datalake.id
  key     = "bronze/co2_emissions/"
  content = ""
}
resource "aws_s3_object" "raw_worldbank_tourism_prefix" {
  bucket  = aws_s3_bucket.datalake.id
  key     = "raw/worldbank_tourism/"
  content = ""
}

resource "aws_s3_object" "raw_UNWTO_transport_prefix" {
  bucket  = aws_s3_bucket.datalake.id
  key     = "raw/UNWTO_transport/"
  content = ""
}

resource "aws_s3_object" "bronze_tourism_arrivals_prefix" {
  bucket  = aws_s3_bucket.datalake.id
  key     = "bronze/tourism_arrivals/"
  content = ""
}

resource "aws_s3_object" "bronze_transport_mode_prefix" {
  bucket  = aws_s3_bucket.datalake.id
  key     = "bronze/transport_mode/"
  content = ""
}