
# ============================================================
# docs/runbooks/deployment.md - Deployment Runbook
# ============================================================

# Deployment Runbook

## Prerequisites

- AWS CLI configured with appropriate credentials
- Terraform >= 1.5.0
- Python 3.11
- Make installed
- Git access to repository

## Pre-Deployment Checklist

- [ ] All tests passing in CI/CD
- [ ] Security scan completed
- [ ] Change request approved
- [ ] Rollback plan documented
- [ ] Team notification sent

## Deployment Steps

### 1. Prepare Environment

```bash
# Clone latest code
git pull origin main

# Install dependencies
make install

# Run tests
make test
```

### 2. Validate Configuration

```bash
# Check Terraform configuration
cd infrastructure
./scripts/validate.sh

# Review planned changes
make terraform-plan ENV=<environment>
```

### 3. Deploy Infrastructure

```bash
# Deploy to environment
make deploy ENV=<environment>

# For production, use manual approval
make deploy ENV=prod AUTO_APPROVE=false
```

### 4. Verify Deployment

```bash
# Run smoke tests
make test-smoke ENV=<environment>

# Check CloudWatch dashboard
make dashboard ENV=<environment>

# Verify Lambda functions
aws lambda list-functions --query "Functions[?contains(FunctionName, 'ukhsa')]"
```

### 5. Post-Deployment

- [ ] Update documentation
- [ ] Notify stakeholders
- [ ] Monitor metrics for 30 minutes
- [ ] Update change log

## Rollback Procedure

If issues are detected:

```bash
# Revert to previous version
git checkout <previous-tag>

# Re-deploy previous version
make deploy ENV=<environment>

# Verify rollback
make test-smoke ENV=<environment>
```

## Troubleshooting

### Lambda Timeout Issues
- Check CloudWatch Logs: `/aws/lambda/ukhsa-*`
- Increase timeout in `terraform/modules/lambda/main.tf`
- Review memory allocation

### S3 Permission Errors
- Verify IAM role policies
- Check bucket policies
- Review S3 event configurations

### Glue Job Failures
- Check Glue console for error details
- Review CloudWatch Logs: `/aws-glue/jobs/`
- Verify data schema compatibility

---
