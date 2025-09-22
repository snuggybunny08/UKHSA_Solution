
variable "aws_region" {
  description = "AWS region for deployment"
  type        = string
  default     = "eu-west-2"
}

variable "environment" {
  description = "Environment name (dev, staging, prod)"
  type        = string
  validation {
    condition     = contains(["dev", "staging", "prod"], var.environment)
    error_message = "Environment must be dev, staging, or prod."
  }
}

variable "project_prefix" {
  description = "Prefix for all resources"
  type        = string
  default     = "ukhsa"
}

variable "vpc_id" {
  description = "VPC ID for Lambda functions"
  type        = string
}

variable "private_subnet_ids" {
  description = "Private subnet IDs for Lambda functions"
  type        = list(string)
}

variable "alert_email" {
  description = "Email address for alerts"
  type        = string
  sensitive   = true
}

variable "allowed_da_accounts" {
  description = "List of Devolved Administration AWS account IDs"
  type        = list(string)
  default     = []
}

variable "data_retention_days" {
  description = "Number of days to retain data in different stages"
  type = object({
    landing   = number
    validated = number
    processed = number
    archive   = number
  })
  default = {
    landing   = 30
    validated = 90
    processed = 180
    archive   = 2555  # 7 years
  }
}

variable "lambda_config" {
  description = "Lambda function configuration"
  type = object({
    timeout     = number
    memory_size = number
    reserved_concurrent_executions = number
  })
  default = {
    timeout     = 300
    memory_size = 1024
    reserved_concurrent_executions = 10
  }
}

variable "glue_config" {
  description = "Glue job configuration"
  type = object({
    max_capacity = number
    max_retries  = number
    timeout      = number
  })
  default = {
    max_capacity = 10
    max_retries  = 2
    timeout      = 60
  }
}

variable "tags" {
  description = "Common tags to apply to all resources"
  type        = map(string)
  default     = {}
}

variable "enable_macie" {
  description = "Enable AWS Macie for PII detection"
  type        = bool
  default     = true
}

variable "enable_cloudtrail" {
  description = "Enable AWS CloudTrail for audit logging"
  type        = bool
  default     = true
}

variable "enable_guardduty" {
  description = "Enable AWS GuardDuty for threat detection"
  type        = bool
  default     = false
}

