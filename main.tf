terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "6.22.0"
    }
  }
}

provider "aws" {
  access_key                  = "test"
  secret_key                  = "test"
  region                      = "us-east-1"
  s3_use_path_style           = true
  skip_credentials_validation = true
  skip_metadata_api_check     = true
  skip_requesting_account_id  = true

  endpoints {
    s3  = "http://localstack:4566"
    sqs = "http://localstack:4566"
  }
}

resource "aws_s3_bucket" "ingestion_drop_zone" {
  bucket = "healthcare-ingestion-drop-zone"
}

# 1. Raw Event Queue (S3 -> Here)
resource "aws_sqs_queue" "s3_event_queue" {
  name = "s3-event-queue"
}

# 2. Celery Task Queue (Worker -> Here)
resource "aws_sqs_queue" "ingestion_queue" {
  name = "healthcare-ingestion-queue"
}

# Configure S3 to notify the Event Queue
resource "aws_s3_bucket_notification" "bucket_notification" {
  bucket = aws_s3_bucket.ingestion_drop_zone.id

  queue {
    queue_arn     = aws_sqs_queue.s3_event_queue.arn
    events        = ["s3:ObjectCreated:*"]
    filter_suffix = ".csv"
  }
  
  depends_on = [aws_sqs_queue_policy.s3_event_policy]
}

# Policy allowing S3 to write to s3-event-queue
resource "aws_sqs_queue_policy" "s3_event_policy" {
  queue_url = aws_sqs_queue.s3_event_queue.id

  policy = "{\"Version\":\"2012-10-17\",\"Statement\":[{\"Effect\":\"Allow\",\"Principal\":\"*\",\"Action\":\"sqs:SendMessage\",\"Resource\":\"${aws_sqs_queue.s3_event_queue.arn}\",\"Condition\":{\"ArnEquals\":{\"aws:SourceArn\":\"${aws_s3_bucket.ingestion_drop_zone.arn}\"}}}]}"
}
