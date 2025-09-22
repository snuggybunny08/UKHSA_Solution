
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



# ============================================================
# docs/guides/getting-started.md
# ============================================================

# Getting Started Guide

## Welcome to UKHSA Data Platform

This guide will help you get started with uploading and processing health surveillance data.

## Quick Start

### Step 1: Access the Platform

1. Ensure you have AWS credentials configured
2. Verify access to the landing S3 bucket

### Step 2: Prepare Your Data

Supported formats:
- CSV (.csv)
- Excel (.xls, .xlsx)
- XML (.xml)
- JSON (.json)

Required columns for communicable disease data:
- report_date
- region
- disease_name
- case_count

### Step 3: Upload Data

#### Using AWS CLI:
```bash
aws s3 cp yourfile.csv s3://ukhsa-landing-prod/uploads/
```

#### Using AWS Console:
1. Navigate to S3
2. Open ukhsa-landing-prod bucket
3. Click Upload
4. Select your file

### Step 4: Monitor Processing

1. Check CloudWatch dashboard
2. Wait for email notification
3. Query processed data using Athena

## Data Validation

Your data will be automatically validated for:
- Required columns
- Data types
- Value ranges
- PII content

## Best Practices

1. **File Naming**: Use descriptive names with dates
2. **File Size**: Keep files under 100MB
3. **Frequency**: Upload daily for best results
4. **Format**: Use CSV for best compatibility

## Support

- Email: data-platform@ukhsa.gov.uk
- Slack: #data-platform-support
- Documentation: [GitHub Wiki](https://github.com/ukhsa/data-platform/wiki)

