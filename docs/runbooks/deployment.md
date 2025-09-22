
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




# ============================================================
# docs/runbooks/deployment.md
# ============================================================

# Deployment Runbook

## Prerequisites

1. AWS Account with appropriate IAM permissions
2. AWS CLI configured
3. Terraform >= 1.5.0 installed
4. Python 3.11+ installed
5. Git repository access

## Deployment Steps

### 1. Clone Repository

```bash
git clone https://github.com/ukhsa/data-platform.git
cd data-platform
```

### 2. Install Dependencies

```bash
make install
```

### 3. Configure Environment

```bash
cp .env.example .env
# Edit .env with appropriate values
```

### 4. Initialize Terraform

```bash
cd infrastructure/terraform
terraform init
```

### 5. Plan Deployment

```bash
terraform plan -var-file=environments/dev.tfvars -out=dev.tfplan
```

### 6. Review Plan

Verify:
- Resources to be created
- No unintended deletions
- Correct environment configuration

### 7. Apply Infrastructure

```bash
terraform apply dev.tfplan
```

### 8. Package and Deploy Lambdas

```bash
cd ../..
make package-lambdas
make deploy-lambdas ENV=dev
```

### 9. Upload Glue Scripts

```bash
make upload-glue ENV=dev
```

### 10. Verify Deployment

```bash
make test-smoke ENV=dev
```

## Post-Deployment

1. **Configure SNS Subscriptions**: Confirm email subscriptions
2. **Test File Upload**: Upload sample file to landing bucket
3. **Check CloudWatch Logs**: Verify no errors
4. **Access Dashboard**: Verify CloudWatch dashboard is accessible
5. **Document**: Update deployment log

## Rollback Procedure

If deployment fails:

```bash
# Revert to previous Terraform state
terraform workspace select dev
terraform state pull > current-state.json
terraform state push backup-state.json

# Or destroy and redeploy
terraform destroy -var-file=environments/dev.tfvars
```
