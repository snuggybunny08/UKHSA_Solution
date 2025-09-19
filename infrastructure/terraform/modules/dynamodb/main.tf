# ============================================================
# modules/dynamodb/main.tf - DynamoDB Module
# ============================================================

locals {
  table_prefix = "${var.project_prefix}-${var.environment}"
}

resource "aws_dynamodb_table" "validation_rules" {
  name           = "${local.table_prefix}-validation-rules"
  billing_mode   = var.environment == "prod" ? "PROVISIONED" : "PAY_PER_REQUEST"
  
  # For provisioned mode
  read_capacity  = var.environment == "prod" ? 5 : null
  write_capacity = var.environment == "prod" ? 5 : null
  
  hash_key  = "dataset_type"
  range_key = "rule_id"
  
  attribute {
    name = "dataset_type"
    type = "S"
  }
  
  attribute {
    name = "rule_id"
    type = "S"
  }
  
  attribute {
    name = "rule_type"
    type = "S"
  }
  
  global_secondary_index {
    name            = "RuleTypeIndex"
    hash_key        = "rule_type"
    projection_type = "ALL"
    read_capacity   = var.environment == "prod" ? 5 : null
    write_capacity  = var.environment == "prod" ? 5 : null
  }
  
  point_in_time_recovery {
    enabled = var.enable_pitr
  }
  
  server_side_encryption {
    enabled = true
  }
  
  stream_enabled   = true
  stream_view_type = "NEW_AND_OLD_IMAGES"
  
  lifecycle {
    prevent_destroy = var.environment == "prod" ? true : false
  }
  
  tags = merge(
    var.tags,
    {
      Name = "Validation Rules Table"
      Type = "Configuration"
    }
  )
}

resource "aws_dynamodb_table" "audit_log" {
  name           = "${local.table_prefix}-audit-log"
  billing_mode   = "PAY_PER_REQUEST"
  
  hash_key  = "event_id"
  range_key = "timestamp"
  
  attribute {
    name = "event_id"
    type = "S"
  }
  
  attribute {
    name = "timestamp"
    type = "S"
  }
  
  attribute {
    name = "user_id"
    type = "S"
  }
  
  global_secondary_index {
    name            = "UserIndex"
    hash_key        = "user_id"
    range_key       = "timestamp"
    projection_type = "ALL"
  }
  
  ttl {
    attribute_name = "ttl"
    enabled        = true
  }
  
  point_in_time_recovery {
    enabled = var.enable_pitr
  }
  
  tags = merge(
    var.tags,
    {
      Name = "Audit Log Table"
      Type = "Logging"
    }
  )
}

# Variables
variable "environment" {
  description = "Environment name"
  type        = string
}

variable "project_prefix" {
  description = "Project prefix"
  type        = string
}

variable "enable_pitr" {
  description = "Enable point-in-time recovery"
  type        = bool
  default     = false
}

variable "tags" {
  description = "Tags to apply"
  type        = map(string)
  default     = {}
}

# Outputs
output "validation_rules_table_name" {
  value = aws_dynamodb_table.validation_rules.name
}

output "validation_rules_table_arn" {
  value = aws_dynamodb_table.validation_rules.arn
}

output "audit_log_table_name" {
  value = aws_dynamodb_table.audit_log.name
}

output "module_version" {
  value = "1.0.0"
}
