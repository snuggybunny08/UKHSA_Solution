
# ============================================================
# environments/dev.tfvars - Development environment configuration
# ============================================================

environment = "dev"
aws_region  = "eu-west-2"

vpc_id = "vpc-0123456789abcdef0"  # Replace with actual VPC ID
private_subnet_ids = [
  "subnet-0123456789abcdef0",
  "subnet-0123456789abcdef1"
]

alert_email = "dev-alerts@ukhsa.gov.uk"

# Reduced retention for dev environment
data_retention_days = {
  landing   = 7
  validated = 30
  processed = 60
  archive   = 365
}

# Smaller Lambda resources for dev
lambda_config = {
  timeout     = 60
  memory_size = 512
  reserved_concurrent_executions = 2
}

# Smaller Glue resources for dev
glue_config = {
  max_capacity = 2
  max_retries  = 1
  timeout      = 30
}

tags = {
  Environment     = "dev"
  CostCenter      = "development"
  DataClass       = "test"
  Compliance      = "none"
  BackupSchedule  = "none"
}

enable_macie      = false  # Disabled in dev to save costs
enable_cloudtrail = true
enable_guardduty  = false
