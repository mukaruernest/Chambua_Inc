terraform {
  required_providers {
    postgresql = {
      version = ">= 1.19.0"
      source  = "cyrilgdn/postgresql"
    }
  }
}
provider "aws" {
  region = "us-west-2" 
}
provider "postgresql" {
  host     = "34.89.230.185"
  port     = 5432
  username = "ernemuka4263"
  password = "vtpNpj4nTe"
 
}

resource "aws_s3_bucket" "export_bucket" {
  bucket = "d2b-internal-assessment-bucket"
  acl    = "public-read"  
}

data "postgresql_table" "agg_performing_product" {
  schema = "ernemuka4263_staging"
  name   = "agg_performing_product"
}

data "postgresql_table" "agg_public_holiday" {
  schema = "ernemuka4263_staging"
  name   = "agg_public_holiday"
}

data "postgresql_table" "agg_shipments" {
  schema = "ernemuka4263_staging"
  name   = "agg_shipments"
}

resource "aws_s3_bucket_object" "agg_performing_product" {
  bucket = aws_s3_bucket.export_bucket.id
  key    = "analytics_export/ernemuka4263/agg_performing_product.csv"
  source = data.postgresql_table.agg_performing_product.query_export_csv
}

resource "aws_s3_bucket_object" "agg_public_holiday" {
  bucket = aws_s3_bucket.export_bucket.id
  key    = "analytics_export/ernemuka4263/agg_public_holiday.csv"
  source = data.postgresql_table.agg_public_holiday.query_export_csv
}

resource "aws_s3_bucket_object" "agg_shipments" {
  bucket = aws_s3_bucket.export_bucket.id
  key    = "analytics_export/ernemuka4263/agg_shipments.csv"
  source = data.postgresql_table.agg_shipments.query_export_csv
}