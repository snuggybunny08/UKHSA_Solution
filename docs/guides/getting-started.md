
# ============================================================
# docs/guides/getting-started.md - Getting Started Guide
# ============================================================

# Getting Started with UKHSA Data Platform

## Quick Start

This guide will help you get the UKHSA Data Platform up and running in your AWS environment.

### Step 1: Setup AWS Account

1. Create or access your AWS account
2. Configure AWS CLI:
```bash
aws configure
AWS Access Key ID: <your-key>
AWS Secret Access Key: <your-secret>
Default region: eu-west-2
```

### Step 2: Clone Repository

```bash
git clone https://github.com/ukhsa/data-platform.git
cd data-platform
```

### Step 3: Install Dependencies

```bash
# Install Python dependencies
pip install -r requirements.txt

# Install Terraform
brew install terraform  # macOS
# or
sudo apt-get install terraform  # Ubuntu
```

### Step 4: Configure Environment

```bash
# Copy environment template
cp .env.example .env

# Edit .env with your settings
vim .env
```

### Step 5: Deploy Infrastructure

```bash
# Initialize Terraform
make terraform-init

# Deploy to development
make deploy ENV=dev
```

### Step 6: Upload Test Data

```bash
# Generate sample data
python scripts/data/generate-sample-data.py

# Upload to S3
aws s3 cp sample_data.csv s3://ukhsa-landing-dev/uploads/
```

### Step 7: Monitor Processing

- Check CloudWatch Dashboard
- Review Lambda logs
- Query processed data with Athena

## Next Steps

- Read the [Architecture Documentation](../architecture/README.md)
- Review [Validation Rules](validation-rules.md)
- Set up [Power BI Integration](power-bi-setup.md)

