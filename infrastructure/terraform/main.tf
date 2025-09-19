# main.tf - Main Terraform configuration for UKHSA Data Platform

terraform {
  required_version = ">= 1.0"
  
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
  
  backend "s3" {
    bucket = "ukhsa-terraform-state"
    key    = "data-platform/terraform.tfstate"
    region = "eu-west-2"
    encrypt = true
  }
}

provider "aws" {
  region = var.aws_region
  
  default_tags {
    tags = {
      Project     = "UKHSA-DA-DataPlatform"
      Environment = var.environment
      ManagedBy   = "Terraform"
      CostCenter  = "DataEngineering"
    }
  }
}

# ============================================================
# S3 Buckets Configuration
# ============================================================

# Landing Bucket for initial file uploads
resource "aws_s3_bucket" "landing" {
  bucket = "${var.project_prefix}-landing-${var.environment}"
  
  tags = {
    Name        = "Landing Bucket"
    Purpose     = "Initial file upload from UKHSA and DAs"
    DataClass   = "Sensitive"
  }
}

resource "aws_s3_bucket_versioning" "landing" {
  bucket = aws_s3_bucket.landing.id
  
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "landing" {
  bucket = aws_s3_bucket.landing.id
  
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "landing" {
  bucket = aws_s3_bucket.landing.id
  
  rule {
    id     = "delete-old-files"
    status = "Enabled"
    
    transition {
      days          = 30
      storage_class = "STANDARD_IA"
    }
    
    transition {
      days          = 90
      storage_class = "GLACIER"
    }
    
    expiration {
      days = 365
    }
  }
}

# Validated Data Bucket
resource "aws_s3_bucket" "validated" {
  bucket = "${var.project_prefix}-validated-${var.environment}"
  
  tags = {
    Name        = "Validated Data Bucket"
    Purpose     = "Store validated and PII-free data"
    DataClass   = "Internal"
  }
}

# Validation Logs Bucket
resource "aws_s3_bucket" "validation_logs" {
  bucket = "${var.project_prefix}-validation-logs-${var.environment}"
  
  tags = {
    Name        = "Validation Logs"
    Purpose     = "Audit trail for all validations"
    DataClass   = "Internal"
  }
}

# Archive Bucket for processed files
resource "aws_s3_bucket" "archive" {
  bucket = "${var.project_prefix}-archive-${var.environment}"
  
  tags = {
    Name        = "Archive Bucket"
    Purpose     = "Long-term storage of processed files"
    DataClass   = "Archive"
  }
}

# S3 Event Notifications for file uploads
resource "aws_s3_bucket_notification" "landing_notifications" {
  bucket = aws_s3_bucket.landing.id
  
  lambda_function {
    lambda_function_arn = aws_lambda_function.file_processor.arn
    events              = ["s3:ObjectCreated:*"]
    filter_prefix       = "uploads/"
    filter_suffix       = ""
  }
  
  depends_on = [aws_lambda_permission.allow_s3]
}

# ============================================================
# Lambda Functions
# ============================================================

# File Processor Lambda
resource "aws_lambda_function" "file_processor" {
  filename         = "lambda_packages/file_processor.zip"
  function_name    = "${var.project_prefix}-file-processor-${var.environment}"
  role            = aws_iam_role.lambda_role.arn
  handler         = "index.handler"
  source_code_hash = filebase64sha256("lambda_packages/file_processor.zip")
  runtime         = "python3.11"
  timeout         = 300
  memory_size     = 1024
  
  environment {
    variables = {
      VALIDATED_BUCKET = aws_s3_bucket.validated.id
      LOGS_BUCKET      = aws_s3_bucket.validation_logs.id
      MACIE_JOB_ID     = aws_macie2_classification_job.pii_scan.id
      SNS_TOPIC_ARN    = aws_sns_topic.validation_alerts.arn
    }
  }
  
  vpc_config {
    subnet_ids         = var.private_subnet_ids
    security_group_ids = [aws_security_group.lambda_sg.id]
  }
  
  tags = {
    Name = "File Processor"
    Type = "EventDriven"
  }
}

# Validation Lambda
resource "aws_lambda_function" "data_validator" {
  filename         = "lambda_packages/data_validator.zip"
  function_name    = "${var.project_prefix}-data-validator-${var.environment}"
  role            = aws_iam_role.lambda_role.arn
  handler         = "validator.handler"
  source_code_hash = filebase64sha256("lambda_packages/data_validator.zip")
  runtime         = "python3.11"
  timeout         = 300
  memory_size     = 2048
  
  layers = [aws_lambda_layer_version.pandas_layer.arn]
  
  environment {
    variables = {
      VALIDATION_RULES_TABLE = aws_dynamodb_table.validation_rules.id
      NOTIFICATION_TOPIC     = aws_sns_topic.validation_alerts.arn
      LOG_LEVEL             = "INFO"
    }
  }
  
  tags = {
    Name = "Data Validator"
    Type = "Validation"
  }
}

# Lambda Layer for common dependencies
resource "aws_lambda_layer_version" "pandas_layer" {
  filename            = "layers/pandas_layer.zip"
  layer_name          = "${var.project_prefix}-pandas-layer"
  compatible_runtimes = ["python3.11"]
  description         = "Layer containing pandas and other data processing libraries"
}

# Lambda Permissions
resource "aws_lambda_permission" "allow_s3" {
  statement_id  = "AllowExecutionFromS3"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.file_processor.function_name
  principal     = "s3.amazonaws.com"
  source_arn    = aws_s3_bucket.landing.arn
}

# ============================================================
# IAM Roles and Policies
# ============================================================

# Lambda Execution Role
resource "aws_iam_role" "lambda_role" {
  name = "${var.project_prefix}-lambda-role-${var.environment}"
  
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "lambda.amazonaws.com"
        }
      }
    ]
  })
}

# Lambda Policy
resource "aws_iam_role_policy" "lambda_policy" {
  name = "${var.project_prefix}-lambda-policy-${var.environment}"
  role = aws_iam_role.lambda_role.id
  
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:ListBucket"
        ]
        Resource = [
          aws_s3_bucket.landing.arn,
          "${aws_s3_bucket.landing.arn}/*",
          aws_s3_bucket.validated.arn,
          "${aws_s3_bucket.validated.arn}/*",
          aws_s3_bucket.validation_logs.arn,
          "${aws_s3_bucket.validation_logs.arn}/*"
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ]
        Resource = "arn:aws:logs:*:*:*"
      },
      {
        Effect = "Allow"
        Action = [
          "macie2:GetFindings",
          "macie2:GetClassificationJobResults"
        ]
        Resource = "*"
      },
      {
        Effect = "Allow"
        Action = [
          "sns:Publish"
        ]
        Resource = aws_sns_topic.validation_alerts.arn
      },
      {
        Effect = "Allow"
        Action = [
          "dynamodb:GetItem",
          "dynamodb:Query",
          "dynamodb:Scan"
        ]
        Resource = aws_dynamodb_table.validation_rules.arn
      }
    ]
  })
}

# Attach VPC policy for Lambda
resource "aws_iam_role_policy_attachment" "lambda_vpc" {
  role       = aws_iam_role.lambda_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaVPCAccessExecutionRole"
}

# ============================================================
# Glue Configuration
# ============================================================

# Glue Catalog Database
resource "aws_glue_catalog_database" "main" {
  name = "${var.project_prefix}_db_${var.environment}"
  
  description = "UKHSA Data Platform Glue Database"
}

# Glue Crawler for validated data
resource "aws_glue_crawler" "validated_data" {
  database_name = aws_glue_catalog_database.main.name
  name          = "${var.project_prefix}-validated-crawler-${var.environment}"
  role          = aws_iam_role.glue_role.arn
  
  s3_target {
    path = "s3://${aws_s3_bucket.validated.bucket}/data/"
  }
  
  schema_change_policy {
    delete_behavior = "LOG"
    update_behavior = "UPDATE_IN_DATABASE"
  }
  
  configuration = jsonencode({
    Version = 1.0
    CrawlerOutput = {
      Tables = {
        AddOrUpdateBehavior = "MergeNewColumns"
      }
    }
  })
}

# Glue Job for ETL
resource "aws_glue_job" "etl_job" {
  name     = "${var.project_prefix}-etl-job-${var.environment}"
  role_arn = aws_iam_role.glue_role.arn
  
  command {
    script_location = "s3://${aws_s3_bucket.glue_scripts.bucket}/scripts/etl_job.py"
    python_version  = "3"
  }
  
  default_arguments = {
    "--job-language"        = "python"
    "--job-bookmark-option" = "job-bookmark-enable"
    "--enable-metrics"      = "true"
    "--enable-spark-ui"     = "true"
    "--spark-event-logs-path" = "s3://${aws_s3_bucket.glue_scripts.bucket}/logs/"
  }
  
  max_retries  = 1
  timeout      = 60
  max_capacity = 10
  
  execution_property {
    max_concurrent_runs = 2
  }
}

# Glue Role
resource "aws_iam_role" "glue_role" {
  name = "${var.project_prefix}-glue-role-${var.environment}"
  
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "glue.amazonaws.com"
        }
      }
    ]
  })
}

# Attach Glue service policy
resource "aws_iam_role_policy_attachment" "glue_service" {
  role       = aws_iam_role.glue_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

# ============================================================
# Macie Configuration
# ============================================================

resource "aws_macie2_account" "main" {
  finding_publishing_frequency = "FIFTEEN_MINUTES"
  status                      = "ENABLED"
}

resource "aws_macie2_classification_job" "pii_scan" {
  name     = "${var.project_prefix}-pii-scan-${var.environment}"
  job_type = "ONE_TIME"
  
  s3_job_definition {
    bucket_definitions {
      account_id = data.aws_caller_identity.current.account_id
      buckets    = [aws_s3_bucket.landing.id]
    }
  }
  
  depends_on = [aws_macie2_account.main]
}

# Custom data identifier for NHS Number
resource "aws_macie2_custom_data_identifier" "nhs_number" {
  name        = "NHS-Number"
  description = "Detects NHS numbers"
  regex       = "[0-9]{3}\\s?[0-9]{3}\\s?[0-9]{4}"
  
  tags = {
    Type = "PII"
  }
}

# ============================================================
# CloudWatch and Monitoring
# ============================================================

# CloudWatch Log Groups
resource "aws_cloudwatch_log_group" "lambda_logs" {
  name              = "/aws/lambda/${var.project_prefix}-${var.environment}"
  retention_in_days = 30
  
  tags = {
    Environment = var.environment
    Application = "UKHSA-DataPlatform"
  }
}

# CloudWatch Dashboard
resource "aws_cloudwatch_dashboard" "main" {
  dashboard_name = "${var.project_prefix}-dashboard-${var.environment}"
  
  dashboard_body = jsonencode({
    widgets = [
      {
        type = "metric"
        properties = {
          metrics = [
            ["AWS/Lambda", "Invocations", { stat = "Sum" }],
            [".", "Errors", { stat = "Sum" }],
            [".", "Duration", { stat = "Average" }]
          ]
          period = 300
          stat   = "Average"
          region = var.aws_region
          title  = "Lambda Metrics"
        }
      },
      {
        type = "metric"
        properties = {
          metrics = [
            ["AWS/S3", "BucketSizeBytes", { stat = "Average" }],
            [".", "NumberOfObjects", { stat = "Average" }]
          ]
          period = 86400
          stat   = "Average"
          region = var.aws_region
          title  = "S3 Storage Metrics"
        }
      }
    ]
  })
}

# CloudWatch Alarms
resource "aws_cloudwatch_metric_alarm" "lambda_errors" {
  alarm_name          = "${var.project_prefix}-lambda-errors-${var.environment}"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = "2"
  metric_name        = "Errors"
  namespace          = "AWS/Lambda"
  period             = "300"
  statistic          = "Sum"
  threshold          = "5"
  alarm_description  = "This metric monitors lambda errors"
  alarm_actions      = [aws_sns_topic.alerts.arn]
  
  dimensions = {
    FunctionName = aws_lambda_function.file_processor.function_name
  }
}

# ============================================================
# SNS Topics for Notifications
# ============================================================

resource "aws_sns_topic" "validation_alerts" {
  name = "${var.project_prefix}-validation-alerts-${var.environment}"
  
  tags = {
    Purpose = "Validation failure notifications"
  }
}

resource "aws_sns_topic" "alerts" {
  name = "${var.project_prefix}-system-alerts-${var.environment}"
  
  tags = {
    Purpose = "System alerts and monitoring"
  }
}

resource "aws_sns_topic_subscription" "email_alerts" {
  topic_arn = aws_sns_topic.validation_alerts.arn
  protocol  = "email"
  endpoint  = var.alert_email
}

# ============================================================
# DynamoDB for Validation Rules
# ============================================================

resource "aws_dynamodb_table" "validation_rules" {
  name           = "${var.project_prefix}-validation-rules-${var.environment}"
  billing_mode   = "PAY_PER_REQUEST"
  hash_key       = "dataset_type"
  range_key      = "rule_id"
  
  attribute {
    name = "dataset_type"
    type = "S"
  }
  
  attribute {
    name = "rule_id"
    type = "S"
  }
  
  tags = {
    Name = "Validation Rules Table"
  }
}

# ============================================================
# Security Group for Lambda
# ============================================================

resource "aws_security_group" "lambda_sg" {
  name        = "${var.project_prefix}-lambda-sg-${var.environment}"
  description = "Security group for Lambda functions"
  vpc_id      = var.vpc_id
  
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
  
  tags = {
    Name = "${var.project_prefix}-lambda-sg"
  }
}

# ============================================================
# Athena Configuration
# ============================================================

resource "aws_athena_workgroup" "main" {
  name = "${var.project_prefix}-workgroup-${var.environment}"
  
  configuration {
    enforce_workgroup_configuration = true
    
    result_configuration {
      output_location = "s3://${aws_s3_bucket.athena_results.bucket}/results/"
      
      encryption_configuration {
        encryption_option = "SSE_S3"
      }
    }
    
    engine_version {
      selected_engine_version = "AUTO"
    }
  }
}

resource "aws_s3_bucket" "athena_results" {
  bucket = "${var.project_prefix}-athena-results-${var.environment}"
  
  tags = {
    Name    = "Athena Query Results"
    Purpose = "Store Athena query results"
  }
}

resource "aws_s3_bucket" "glue_scripts" {
  bucket = "${var.project_prefix}-glue-scripts-${var.environment}"
  
  tags = {
    Name    = "Glue Scripts"
    Purpose = "Store Glue ETL scripts"
  }
}

# ============================================================
# Data Sources
# ============================================================

data "aws_caller_identity" "current" {}
data "aws_region" "current" {}

# ============================================================
# Outputs
# ============================================================

output "landing_bucket_name" {
  value = aws_s3_bucket.landing.id
}

output "validated_bucket_name" {
  value = aws_s3_bucket.validated.id
}

output "lambda_function_names" {
  value = {
    file_processor  = aws_lambda_function.file_processor.function_name
    data_validator  = aws_lambda_function.data_validator.function_name
  }
}

output "glue_database_name" {
  value = aws_glue_catalog_database.main.name
}

output "athena_workgroup_name" {
  value = aws_athena_workgroup.main.name
}