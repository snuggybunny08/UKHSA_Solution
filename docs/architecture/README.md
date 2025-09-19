# ============================================================
# docs/architecture/README.md - Architecture Overview
# ============================================================

# UKHSA Data Platform - Architecture Documentation

## Overview

The UKHSA Data Platform is built on AWS serverless architecture, providing scalable, secure, and cost-effective data processing for health surveillance data.

## Architecture Principles

1. **Serverless First**: Minimize operational overhead
2. **Security by Design**: PII detection and encryption at every layer
3. **Event-Driven**: Reactive processing based on data arrival
4. **Modular**: Loosely coupled components
5. **Observable**: Comprehensive monitoring and logging

## System Components

### Data Ingestion Layer
- **S3 Landing Bucket**: Initial upload point with versioning
- **Lambda File Processor**: Validates and routes files
- **AWS Macie**: Automated PII scanning

### Processing Layer
- **Lambda Validator**: Business rule validation
- **AWS Glue**: ETL transformation
- **DynamoDB**: Validation rules storage

### Analytics Layer
- **AWS Athena**: SQL queries on processed data
- **Glue Data Catalog**: Metadata management
- **Power BI**: Visualization and reporting

### Security & Compliance
- **IAM Roles**: Least privilege access
- **KMS**: Encryption key management
- **CloudTrail**: Audit logging
- **Macie**: PII detection and classification

## Data Flow

```
1. Data Upload → S3 Landing Bucket
2. S3 Event → Lambda File Processor
3. File Validation & PII Check
4. Valid Files → Lambda Validator
5. Business Rules Validation
6. Clean Data → S3 Validated Bucket
7. Glue ETL → Data Transformation
8. Processed Data → S3 Processed Bucket
9. Athena/Power BI → Analytics & Reporting
```

## Scalability

- **Lambda Concurrency**: Up to 1000 concurrent executions
- **S3 Storage**: Unlimited capacity with lifecycle management
- **Glue Auto-scaling**: Dynamic worker allocation
- **DynamoDB On-Demand**: Automatic scaling

## Security Controls

- Encryption at rest (AES-256)
- Encryption in transit (TLS 1.2+)
- VPC isolation for compute resources
- Automated PII detection and remediation
- Multi-factor authentication
- Regular security scanning

## Monitoring & Observability

- CloudWatch Dashboards
- Custom metrics and alarms
- X-Ray distributed tracing
- CloudWatch Logs aggregation
- SNS notifications

## Disaster Recovery

- **RTO**: 1 hour (Production)
- **RPO**: 15 minutes (Production)
- Cross-region replication
- Automated backups
- Infrastructure as Code


