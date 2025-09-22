
# Macie Configuration
resource "aws_macie2_account" "main" {
  count = var.enable_macie ? 1 : 0
  
  finding_publishing_frequency = var.environment == "prod" ? "FIFTEEN_MINUTES" : "ONE_HOUR"
  status                      = "ENABLED"
}

resource "aws_macie2_classification_job" "pii_scan" {
  count = var.enable_macie ? 1 : 0
  
  name     = "${var.project_prefix}-${var.environment}-pii-scan"
  job_type = "SCHEDULED"
  
  schedule_frequency {
    daily_schedule = true
  }
  
  s3_job_definition {
    bucket_definitions {
      account_id = var.account_id
      buckets    = [var.landing_bucket_id]
    }
  }
  
  depends_on = [aws_macie2_account.main]
}

resource "aws_macie2_custom_data_identifier" "nhs_number" {
  count = var.enable_macie ? 1 : 0
  
  name        = "NHS-Number"
  description = "Detects NHS numbers"
  regex       = "[0-9]{3}\\s?[0-9]{3}\\s?[0-9]{4}"
  
  tags = var.tags
}

resource "aws_macie2_findings_filter" "pii_filter" {
  count = var.enable_macie ? 1 : 0
  
  name        = "${var.project_prefix}-pii-filter"
  description = "Filter for PII findings"
  action      = "ARCHIVE"
  
  finding_criteria {
    criterion {
      field = "severity.description"
      eq    = ["HIGH", "CRITICAL"]
    }
  }
}

# CloudTrail Configuration
resource "aws_cloudtrail" "main" {
  count = var.enable_cloudtrail ? 1 : 0
  
  name                          = "${var.project_prefix}-${var.environment}-trail"
  s3_bucket_name               = var.cloudtrail_bucket_id
  include_global_service_events = true
  is_multi_region_trail        = true
  enable_log_file_validation   = true
  
  event_selector {
    read_write_type           = "All"
    include_management_events = true
    
    data_resource {
      type   = "AWS::S3::Object"
      values = ["arn:aws:s3:::*/*"]
    }
  }
  
  cloud_watch_logs_group_arn = "${aws_cloudwatch_log_group.cloudtrail[0].arn}:*"
  cloud_watch_logs_role_arn  = aws_iam_role.cloudtrail[0].arn
  
  tags = var.tags
}

resource "aws_cloudwatch_log_group" "cloudtrail" {
  count = var.enable_cloudtrail ? 1 : 0
  
  name              = "/aws/cloudtrail/${var.project_prefix}-${var.environment}"
  retention_in_days = 90
  
  tags = var.tags
}

resource "aws_iam_role" "cloudtrail" {
  count = var.enable_cloudtrail ? 1 : 0
  
  name = "${var.project_prefix}-${var.environment}-cloudtrail-role"
  
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = "sts:AssumeRole"
      Effect = "Allow"
      Principal = {
        Service = "cloudtrail.amazonaws.com"
      }
    }]
  })
}

# GuardDuty Configuration
resource "aws_guardduty_detector" "main" {
  count = var.enable_guardduty ? 1 : 0
  
  enable                       = true
  finding_publishing_frequency = var.environment == "prod" ? "FIFTEEN_MINUTES" : "ONE_HOUR"
  
  datasources {
    s3_logs {
      enable = true
    }
  }
  
  tags = var.tags
}

# Variables
variable "environment" {
  type = string
}

variable "project_prefix" {
  type = string
}

variable "enable_macie" {
  type    = bool
  default = false
}

variable "enable_cloudtrail" {
  type    = bool
  default = false
}

variable "enable_guardduty" {
  type    = bool
  default = false
}

variable "landing_bucket_id" {
  type = string
}

variable "macie_finding_bucket_id" {
  type = string
}

variable "cloudtrail_bucket_id" {
  type = string
}

variable "account_id" {
  type = string
}

variable "tags" {
  type    = map(string)
  default = {}
}

# Outputs
output "macie_job_id" {
  value = var.enable_macie ? aws_macie2_classification_job.pii_scan[0].id : ""
}

output "cloudtrail_arn" {
  value = var.enable_cloudtrail ? aws_cloudtrail.main[0].arn : ""
}

output "guardduty_detector_id" {
  value = var.enable_guardduty ? aws_guardduty_detector.main[0].id : ""
}

output "module_version" {
  value = "1.0.0"
}

