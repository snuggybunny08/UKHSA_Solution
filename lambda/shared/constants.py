
# ============================================================
# lambdas/shared/constants.py - Shared constants
# ============================================================

# File processing constants
MAX_FILE_SIZE = 100 * 1024 * 1024  # 100MB
SUPPORTED_FILE_FORMATS = {'.csv', '.xls', '.xlsx', '.xml', '.json'}
DEFAULT_ENCODING = 'utf-8'

# UK Regions
UK_REGIONS = [
    "London",
    "South East",
    "South West", 
    "East of England",
    "East Midlands",
    "West Midlands",
    "Yorkshire and the Humber",
    "North East",
    "North West",
    "Wales",
    "Scotland",
    "Northern Ireland"
]

# Dataset types
DATASET_TYPES = {
    'COMMUNICABLE_DISEASE': 'communicable_disease',
    'IMMUNISATION': 'immunisation',
    'SURVEILLANCE': 'surveillance',
    'LABORATORY': 'laboratory',
    'OUTBREAK': 'outbreak'
}

# Validation thresholds
VALIDATION_THRESHOLDS = {
    'null_percentage': 0.1,
    'duplicate_percentage': 0.05,
    'outlier_percentage': 0.02,
    'completeness': 0.95
}

# Error messages
ERROR_MESSAGES = {
    'INVALID_FORMAT': 'File format not supported',
    'FILE_TOO_LARGE': 'File exceeds maximum size limit',
    'PII_DETECTED': 'Personal identifiable information detected',
    'VALIDATION_FAILED': 'Data validation failed',
    'MISSING_COLUMNS': 'Required columns are missing',
    'INVALID_DATA_TYPE': 'Invalid data type in columns'
}
