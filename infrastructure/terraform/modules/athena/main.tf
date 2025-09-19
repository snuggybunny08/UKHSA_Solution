
# ============================================================
# modules/athena/main.tf - Athena Module
# ============================================================

locals {
  workgroup_name = "${var.project_prefix}-${var.environment}-workgroup"
}

resource "aws_athena_workgroup" "main" {
  name = local.workgroup_name
  
  configuration {
    enforce_workgroup_configuration    = true
    publish_cloudwatch_metrics_enabled = true
    
    result_configuration {
      output_location = "s3://${var.results_bucket_id}/results/"
      
      encryption_configuration {
        encryption_option = "SSE_S3"
      }
    }
    
    engine_version {
      selected_engine_version = "AUTO"
    }
    
    bytes_scanned_cutoff_per_query = var.bytes_scanned_cutoff
  }
  
  tags = merge(
    var.tags,
    {
      Name = "Athena Workgroup"
      Type = "Analytics"
    }
  )
}

resource "aws_athena_named_query" "daily_summary" {
  name      = "${var.project_prefix}-daily-summary"
  workgroup = aws_athena_workgroup.main.name
  database  = var.glue_database_name
  
  query = <<EOF
SELECT 
  DATE(report_date) as date,
  region,
  SUM(case_count) as total_cases,
  SUM(death_count) as total_deaths,
  AVG(case_count) as avg_cases
FROM validated_data
WHERE report_date >= current_date - interval '30' day
GROUP BY DATE(report_date), region
ORDER BY date DESC, region
EOF
  
  description = "Daily summary of cases by region"
}

resource "aws_athena_data_catalog" "main" {
  name        = "${var.project_prefix}-${var.environment}-catalog"
  description = "Data catalog for UKHSA platform"
  type        = "GLUE"
  
  parameters = {
    "catalog-id" = data.aws_caller_identity.current.account_id
  }
  
  tags = var.tags
}

data "aws_caller_identity" "current" {}

# Variables
variable "environment" {
  type = string
}

variable "project_prefix" {
  type = string
}

variable "results_bucket_id" {
  type = string
}

variable "results_bucket_arn" {
  type = string
}

variable "glue_database_name" {
  type = string
}

variable "bytes_scanned_cutoff" {
  type    = number
  default = 10737418240
}

variable "tags" {
  type    = map(string)
  default = {}
}

# Outputs
output "workgroup_name" {
  value = aws_athena_workgroup.main.name
}

output "catalog_name" {
  value = aws_athena_data_catalog.main.name
}

output "module_version" {
  value = "1.0.0"
}
