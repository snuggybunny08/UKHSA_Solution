

environment = "staging"
aws_region  = "eu-west-2"

vpc_id = "vpc-0987654321fedcba0"  # Replace with actual VPC ID
private_subnet_ids = [
  "subnet-0987654321fedcba0",
  "subnet-0987654321fedcba1"
]

alert_email = "staging-alerts@ukhsa.gov.uk"

data_retention_days = {
  landing   = 14
  validated = 60
  processed = 120
  archive   = 730  # 2 years
}

lambda_config = {
  timeout     = 180
  memory_size = 1024
  reserved_concurrent_executions = 5
}

glue_config = {
  max_capacity = 5
  max_retries  = 2
  timeout      = 45
}

tags = {
  Environment     = "staging"
  CostCenter      = "testing"
  DataClass       = "internal"
  Compliance      = "gdpr"
  BackupSchedule  = "weekly"
}

enable_macie      = true
enable_cloudtrail = true
enable_guardduty  = false

