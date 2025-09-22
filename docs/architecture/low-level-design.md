
# ============================================================
# docs/architecture/low-level-design.md
# ============================================================

# UKHSA Data Platform - Low Level Design

## Detailed Component Design

### 1. S3 Bucket Structure

```
ukhsa-landing-{env}/
├── uploads/
│   ├── {year}/{month}/{day}/
│   │   └── {timestamp}_{filename}
├── quarantine/
│   └── {year}/{month}/{day}/
└── archive/

ukhsa-validated-{env}/
├── data/
│   ├── year={year}/month={month}/
│   │   └── {dataset_type}/
└── metadata/

ukhsa-processed-{env}/
├── aggregations/
│   ├── daily/
│   ├── weekly/
│   └── monthly/
└── reports/
```

### 2. Lambda Functions

#### File Processor Lambda
- **Runtime**: Python 3.11
- **Memory**: 1024 MB (dev), 2048 MB (prod)
- **Timeout**: 300 seconds
- **Triggers**: S3 ObjectCreated events
- **Environment Variables**:
  - VALIDATED_BUCKET
  - LOGS_BUCKET
  - MACIE_JOB_ID
  - SNS_TOPIC_ARN

#### Data Validator Lambda
- **Runtime**: Python 3.11
- **Memory**: 2048 MB
- **Timeout**: 600 seconds
- **Layers**: pandas-layer
- **Environment Variables**:
  - VALIDATION_RULES_TABLE
  - NOTIFICATION_TOPIC

### 3. DynamoDB Tables

#### validation_rules Table
- **Partition Key**: dataset_type (String)
- **Sort Key**: rule_id (String)
- **Attributes**:
  - rule_type
  - enabled
  - configuration (JSON)
- **Indexes**: RuleTypeIndex (GSI)

### 4. Glue ETL Jobs

#### Main ETL Job
- **Type**: Spark ETL
- **Workers**: 10 (G.1X)
- **Timeout**: 60 minutes
- **Schedule**: Every 2 hours (prod)
- **Parameters**:
  - SOURCE_BUCKET
  - TARGET_BUCKET
  - DATABASE_NAME

## Data Flow

1. **Upload**: File uploaded to S3 landing bucket
2. **Trigger**: S3 event triggers File Processor Lambda
3. **PII Scan**: Macie scans file for PII
4. **Validation**: Data Validator Lambda applies rules
5. **Transform**: Glue ETL job processes validated data
6. **Catalog**: Glue Crawler updates data catalog
7. **Query**: Athena enables SQL queries
8. **Visualize**: Power BI connects for reporting

## Error Handling

### Retry Strategy
- Lambda: 3 retries with exponential backoff
- Glue: 2 retries for transient failures
- Dead Letter Queue for persistent failures

### Monitoring
- CloudWatch Alarms for:
  - Lambda errors > 5 in 5 minutes
  - Glue job failures
  - S3 bucket size > 1TB
  - PII detection incidents

