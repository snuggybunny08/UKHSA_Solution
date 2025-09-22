
environment = "prod"
aws_region  = "eu-west-2"

vpc_id = "vpc-abcdef0123456789"  # Replace with actual VPC ID
private_subnet_ids = [
  "subnet-abcdef0123456789a",
  "subnet-abcdef0123456789b",
  "subnet-abcdef0123456789c"
]

alert_email = "prod-alerts@ukhsa.gov.uk"

# Production retention periods
data_retention_days = {
  landing   = 30
  validated = 90
  processed = 180
  archive   = 2555  # 7 years for compliance
}

# Production-grade Lambda resources
lambda_config = {
  timeout     = 300
  memory_size = 2048
  reserved_concurrent_executions = 20
}

# Production Glue resources
glue_config = {
  max_capacity = 20
  max_retries  = 3
  timeout      = 60
}

# List of DA AWS accounts (example)
allowed_da_accounts = [
  "123456789012",  # Scotland
  "234567890123",  # Wales
  "345678901234"   # Northern Ireland
]

tags = {
  Environment     = "prod"
  CostCenter      = "production"
  DataClass       = "sensitive"
  Compliance      = "gdpr,nhs"
  BackupSchedule  = "daily"
  DisasterRecovery = "enabled"
}

enable_macie      = true
enable_cloudtrail = true
enable_guardduty  = true