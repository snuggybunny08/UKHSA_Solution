
# ============================================================
# scripts/data/generate-sample-data.py
# ============================================================

#!/usr/bin/env python3

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
import argparse

def generate_sample_data(rows=1000, dataset_type='communicable_disease'):
    """Generate sample data for testing"""
    
    # UK regions
    regions = [
        "London", "South East", "South West", "East of England",
        "East Midlands", "West Midlands", "Yorkshire and the Humber",
        "North East", "North West", "Wales", "Scotland", "Northern Ireland"
    ]
    
    # Generate dates
    end_date = datetime.now()
    start_date = end_date - timedelta(days=365)
    dates = pd.date_range(start=start_date, end=end_date, periods=rows)
    
    if dataset_type == 'communicable_disease':
        data = {
            'report_date': dates,
            'region': [random.choice(regions) for _ in range(rows)],
            'disease_name': ['COVID-19'] * rows,
            'case_count': np.random.poisson(100, rows),
            'death_count': np.random.poisson(5, rows),
            'hospitalization_count': np.random.poisson(20, rows),
            'age_group': [random.choice(['0-18', '19-35', '36-50', '51-65', '65+']) for _ in range(rows)],
            'gender': [random.choice(['M', 'F', 'Other']) for _ in range(rows)]
        }
    elif dataset_type == 'immunisation':
        data = {
            'vaccination_date': dates,
            'region': [random.choice(regions) for _ in range(rows)],
            'vaccine_type': [random.choice(['Pfizer', 'AstraZeneca', 'Moderna']) for _ in range(rows)],
            'dose_number': [random.choice([1, 2, 3]) for _ in range(rows)],
            'age_group': [random.choice(['0-18', '19-35', '36-50', '51-65', '65+']) for _ in range(rows)],
            'dose_count': np.random.poisson(500, rows)
        }
    else:
        raise ValueError(f"Unknown dataset type: {dataset_type}")
    
    df = pd.DataFrame(data)
    return df

def main():
    parser = argparse.ArgumentParser(description='Generate sample data for UKHSA platform')
    parser.add_argument('--rows', type=int, default=1000, help='Number of rows to generate')
    parser.add_argument('--type', default='communicable_disease', help='Dataset type')
    parser.add_argument('--output', default='sample_data.csv', help='Output filename')
    parser.add_argument('--format', default='csv', choices=['csv', 'json', 'parquet'], help='Output format')
    
    args = parser.parse_args()
    
    print(f"Generating {args.rows} rows of {args.type} data...")
    df = generate_sample_data(args.rows, args.type)
    
    # Save to file
    if args.format == 'csv':
        df.to_csv(args.output, index=False)
    elif args.format == 'json':
        df.to_json(args.output, orient='records', date_format='iso')
    elif args.format == 'parquet':
        df.to_parquet(args.output, index=False)
    
    print(f"Sample data saved to {args.output}")
    print(f"Shape: {df.shape}")
    print(f"Columns: {', '.join(df.columns)}")
    print("\nFirst 5 rows:")
    print(df.head())

if __name__ == "__main__":
    main()

