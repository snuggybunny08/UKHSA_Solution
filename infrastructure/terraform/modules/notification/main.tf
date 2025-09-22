
locals {
  topic_prefix = "${var.project_prefix}-${var.environment}"
}

resource "aws_sns_topic" "validation_alerts" {
  name = "${local.topic_prefix}-validation-alerts"
  
  kms_master_key_id = var.kms_key_id
  
  tags = merge(
    var.tags,
    {
      Name    = "Validation Alerts"
      Purpose = "Data validation notifications"
    }
  )
}

resource "aws_sns_topic" "system_alerts" {
  name = "${local.topic_prefix}-system-alerts"
  
  kms_master_key_id = var.kms_key_id
  
  tags = merge(
    var.tags,
    {
      Name    = "System Alerts"
      Purpose = "System monitoring alerts"
    }
  )
}

resource "aws_sns_topic_subscription" "validation_email" {
  for_each = toset(var.alert_email_addresses)
  
  topic_arn = aws_sns_topic.validation_alerts.arn
  protocol  = "email"
  endpoint  = each.value
}

resource "aws_sns_topic_subscription" "system_email" {
  for_each = toset(var.alert_email_addresses)
  
  topic_arn = aws_sns_topic.system_alerts.arn
  protocol  = "email"
  endpoint  = each.value
}

resource "aws_sns_topic_subscription" "slack" {
  count = var.enable_slack ? 1 : 0
  
  topic_arn = aws_sns_topic.system_alerts.arn
  protocol  = "https"
  endpoint  = var.slack_webhook_url
  
  raw_message_delivery = true
}

# Variables
variable "environment" {
  type = string
}

variable "project_prefix" {
  type = string
}

variable "alert_email_addresses" {
  type = list(string)
}

variable "enable_slack" {
  type    = bool
  default = false
}

variable "slack_webhook_url" {
  type    = string
  default = ""
}

variable "kms_key_id" {
  type    = string
  default = ""
}

variable "tags" {
  type    = map(string)
  default = {}
}

# Outputs
output "validation_alerts_topic_arn" {
  value = aws_sns_topic.validation_alerts.arn
}

output "system_alerts_topic_arn" {
  value = aws_sns_topic.system_alerts.arn
}

output "module_version" {
  value = "1.0.0"
}