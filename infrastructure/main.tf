provider "aws" {
  region = "us-east-1" # Specify your desired AWS region
}

resource "aws_s3_bucket" "data_lake" {
  bucket = "sentinel-amr-datalake-unique-name" # Must be a globally unique name
}

# CHANGE 1: Versioning is now a separate resource
resource "aws_s3_bucket_versioning" "data_lake_versioning" {
  bucket = aws_s3_bucket.data_lake.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_kinesis_stream" "telemetry_stream" {
  name        = "sentinel-telemetry-stream"
  shard_count = 1 # Start with one shard for initial development
}

resource "aws_kinesis_firehose_delivery_stream" "s3_delivery_stream" {
  name        = "sentinel-s3-delivery-stream"
  destination = "extended_s3"

  extended_s3_configuration {
    # ... (keep all existing settings like role_arn, bucket_arn, prefix, etc.) ...
    role_arn            = aws_iam_role.firehose_role.arn
    bucket_arn          = aws_s3_bucket.data_lake.arn
    prefix              = "raw/telemetry/year=!{timestamp:yyyy}/month=!{timestamp:MM}/day=!{timestamp:dd}/hour=!{timestamp:HH}/"
    error_output_prefix = "errors/"
    buffering_interval  = 300
    buffering_size      = 5

    # ADD THIS ENTIRE BLOCK
    processing_configuration {
      enabled = "true"
      processors {
        type = "AppendDelimiterToRecord"
        parameters {
          parameter_name  = "Delimiter"
          parameter_value = "\\n"
        }
      }
    }
  }

  kinesis_source_configuration {
    # ... (this part remains unchanged) ...
    kinesis_stream_arn = aws_kinesis_stream.telemetry_stream.arn
    role_arn           = aws_iam_role.firehose_role.arn
  }
}

# IAM Role that allows Firehose to read from Kinesis and write to S3
resource "aws_iam_role" "firehose_role" {
  name = "firehose-delivery-role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Action = "sts:AssumeRole",
        Effect = "Allow",
        Principal = {
          Service = "firehose.amazonaws.com"
        }
      }
    ]
  })
}

resource "aws_iam_policy" "firehose_policy" {
  name = "firehose-access-policy"
  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Action   = "s3:*"
        Effect   = "Allow"
        Resource = [
          aws_s3_bucket.data_lake.arn,
          "${aws_s3_bucket.data_lake.arn}/*"
        ]
      },
      {
        Action   = ["kinesis:DescribeStream", "kinesis:GetShardIterator", "kinesis:GetRecords"]
        Effect   = "Allow"
        Resource = aws_kinesis_stream.telemetry_stream.arn
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "firehose_attach" {
  role       = aws_iam_role.firehose_role.name
  policy_arn = aws_iam_policy.firehose_policy.arn
}

# ------------------------------------------------------------------
# AWS Glue Resources for Data Cataloging
# ------------------------------------------------------------------

resource "aws_glue_catalog_database" "amr_db" {
  name = "amr_fleet_database"
}

resource "aws_iam_role" "glue_crawler_role" {
  name = "glue-crawler-role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Action = "sts:AssumeRole",
        Effect = "Allow",
        Principal = {
          Service = "glue.amazonaws.com"
        }
      }
    ]
  })
}

# This policy grants the necessary permissions for the crawler
resource "aws_iam_policy" "glue_crawler_policy" {
  name = "glue-crawler-access-policy"
  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      # ADDED: This statement allows the crawler to LIST the contents of the bucket
      {
        Action   = "s3:ListBucket",
        Effect   = "Allow",
        Resource = aws_s3_bucket.data_lake.arn # Permission on the bucket itself
      },
      # UPDATED: This statement allows the crawler to READ objects
      {
        Action   = "s3:GetObject",
        Effect   = "Allow",
        Resource = "${aws_s3_bucket.data_lake.arn}/*" # Permission on the objects INSIDE the bucket
      },
      {
        Action   = "glue:*", # Broadened for simplicity, allows creating tables, partitions etc.
        Effect   = "Allow",
        Resource = [
          aws_glue_catalog_database.amr_db.arn,
          "arn:aws:glue:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:catalog",
          "arn:aws:glue:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:table/${aws_glue_catalog_database.amr_db.name}/*",
          "arn:aws:glue:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:userDefinedFunction/${aws_glue_catalog_database.amr_db.name}/*"
        ]
      }
    ]
  })
}

# Add these data sources at the top of your main.tf file to get region/account info
data "aws_region" "current" {}
data "aws_caller_identity" "current" {}

# Attach the policy to the role
resource "aws_iam_role_policy_attachment" "glue_attach" {
  role       = aws_iam_role.glue_crawler_role.name
  policy_arn = aws_iam_policy.glue_crawler_policy.arn
}


resource "aws_glue_crawler" "telemetry_crawler" {
  name          = "sentinel-telemetry-crawler"
  database_name = aws_glue_catalog_database.amr_db.name
  role          = aws_iam_role.glue_crawler_role.arn

  s3_target {
    path = "s3://${aws_s3_bucket.data_lake.bucket}/raw/telemetry/"
  }
}

# ------------------------------------------------------------------
# AWS Glue Trigger to run the crawler on a schedule
# ------------------------------------------------------------------

resource "aws_glue_trigger" "telemetry_trigger" {
  name          = "sentinel-hourly-trigger"
  type          = "SCHEDULED"
  # Run every hour at the 5-minute mark
  schedule      = "cron(5 * * * ? *)" 
  enabled       = true

  actions {
    crawler_name = aws_glue_crawler.telemetry_crawler.name
  }
}