# ============================================================
# docs/architecture/high-level-design.md
# ============================================================

# UKHSA Data Platform - High Level Design

## Executive Summary

The UKHSA Data Platform is a cloud-native, serverless solution designed to facilitate secure data sharing between the UK Health Security Agency and Devolved Administrations. Built on AWS, it provides automated PII detection, comprehensive validation, and real-time analytics capabilities.

## System Architecture

### Core Components

```
┌─────────────────────────────────────────────────────────────┐
│                         Users/DAs                           │
└──────────────────────┬──────────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────────┐
│                    Data Ingestion Layer                     │
│  ┌──────────┐    ┌──────────┐    ┌──────────────────┐     │
│  │    S3    │───▶│  Lambda  │───▶│     Macie        │     │
│  │  Landing │    │Processor │    │  PII Detection   │     │
│  └──────────┘    └──────────┘    └──────────────────┘     │
└─────────────────────────────────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────────┐
│                    Processing Layer                         │
│  ┌──────────┐    ┌──────────┐    ┌──────────────────┐     │
│  │  Lambda  │───▶│   Glue   │───▶│    DynamoDB      │     │
│  │Validator │    │   ETL    │    │ Validation Rules │     │
│  └──────────┘    └──────────┘    └──────────────────┘     │
└─────────────────────────────────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────────┐
│                    Analytics Layer                          │
│  ┌──────────┐    ┌──────────┐    ┌──────────────────┐     │
│  │  Athena  │───▶│   Glue   │───▶│    Power BI      │     │
│  │  Queries │    │ Catalog  │    │  Dashboards     │     │
│  └──────────┘    └──────────┘    └──────────────────┘     │
└─────────────────────────────────────────────────────────────┘
```

## Key Design Decisions

### 1. Serverless Architecture
- **Rationale**: Eliminates server management overhead, provides automatic scaling
- **Benefits**: Cost-effective, highly available, minimal maintenance

### 2. Event-Driven Processing
- **Rationale**: Process data as it arrives, reducing latency
- **Benefits**: Real-time validation, immediate PII detection

### 3. Multi-Layer Security
- **Rationale**: Defense in depth approach
- **Benefits**: Comprehensive protection, compliance with regulations

## Non-Functional Requirements

### Performance
- File processing: < 5 minutes for files up to 100MB
- Query response: < 5 seconds for standard queries
- Dashboard refresh: < 30 seconds

### Scalability
- Support 100+ concurrent file uploads
- Handle 10TB+ of data annually
- Support 1000+ queries per day

### Availability
- 99.9% uptime for production
- RPO: 15 minutes
- RTO: 1 hour

### Security
- Encryption at rest and in transit
- PII detection accuracy > 99%
- Full audit trail



