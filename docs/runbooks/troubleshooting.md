
# ============================================================
# docs/runbooks/troubleshooting.md
# ============================================================

# Troubleshooting Guide

## Common Issues

### 1. Lambda Function Timeout

**Symptoms**: 
- Function execution exceeds timeout
- CloudWatch shows timeout errors

**Resolution**:
```bash
# Increase timeout in Terraform
# infrastructure/terraform/modules/lambda/main.tf
timeout = 600  # Increase from 300

# Redeploy
terraform apply -target=module.lambda_functions
```

### 2. PII Detection False Positives

**Symptoms**:
- Valid files quarantined
- Macie reports false PII

**Resolution**:
1. Review Macie findings in AWS Console
2. Update custom data identifiers
3. Adjust detection sensitivity

### 3. Glue Job Failures

**Symptoms**:
- ETL jobs failing
- Data not appearing in processed bucket

**Check**:
```bash
# Check Glue job logs
aws glue get-job-runs --job-name ukhsa-main-etl-job-dev

# Check CloudWatch logs
aws logs tail /aws-glue/jobs/ukhsa-dev --follow
```

**Common Fixes**:
- Increase worker count
- Check S3 permissions
- Verify schema compatibility

### 4. Validation Failures

**Symptoms**:
- Files rejected during validation
- Notifications about validation errors

**Debug Steps**:
1. Check validation logs in S3
2. Review validation rules in DynamoDB
3. Test with minimal dataset

```python
# Test validation locally
python scripts/validate-local.py --file test.csv --rules default.json
```

### 5. Permission Denied Errors

**Symptoms**:
- Access denied in CloudWatch logs
- S3 operations failing

**Resolution**:
```bash
# Check IAM role policies
aws iam get-role-policy --role-name ukhsa-lambda-role-dev --policy-name lambda-policy

# Update if needed
terraform apply -target=module.iam
```

## Performance Issues

### Slow Processing

1. **Check Lambda memory**: Increase if needed
2. **Review Glue workers**: Scale up for large datasets
3. **Optimize queries**: Use partitioning in Athena

### High Costs

1. **Review S3 lifecycle policies**
2. **Check Lambda invocations**
3. **Optimize Glue job frequency**

## Monitoring Commands

```bash
# Check system health
make check-health ENV=dev

# View recent errors
aws logs filter-log-events \
  --log-group-name /aws/lambda/ukhsa-file-processor-dev \
  --filter-pattern ERROR \
  --start-time $(date -d '1 hour ago' +%s000)

# Check S3 bucket sizes
aws s3 ls s3://ukhsa-landing-dev --recursive --summarize
```
