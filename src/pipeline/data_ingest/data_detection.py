from dataclasses import asdict, dataclass
import json

import requests
import xml.etree.ElementTree as ET
import zipfile
import io
from sqlalchemy import create_engine
import os


@dataclass
class BronzeIngestSummary:
    layer: str
    job: str
    window: str
    objects: int
    rows: dict

def find_s3_key_by_date(date_input, region):
    url = 'https://s3.amazonaws.com/tripdata/'
    response = requests.get(url)
    
    root = ET.fromstring(response.content)
    namespace = {'s3': 'http://s3.amazonaws.com/doc/2006-03-01/'}
    
    keys = [elem.text for elem in root.findall('.//s3:Key', namespace)]
    
    normalized_date = 'JC-'+date_input.replace('-', '') if region.lower() == 'jc' else date_input.replace('-', '')
    
    for key in keys:
        if key.startswith(normalized_date):
            return key
    
    year_only = date_input.split('-')[0]
    for key in keys:
        if key.startswith(year_only):
            return key
    
    return None

def find_files_by_month(zip_ref, target_month):
    """
    Find CSV files in zip matching target month (YYYY-MM format)
    """
    file_list = zip_ref.namelist()
    
    # Normalize target month: "2024-01" -> "202401"
    normalized_month = target_month.replace('-', '')
    
    root_files = []
    nested_files = []
    
    for file_path in file_list:
        # find only relevant csv files
        if file_path.endswith('/') or '__MACOSX' in file_path or not file_path.endswith('.csv'):
            continue
        
        if normalized_month not in file_path:
            continue

        parts = file_path.split('/')
        
        if len(parts) > 2:
            nested_files.append(file_path)
        else:
            root_files.append(file_path)

    if nested_files:
        return nested_files
    else:
        return root_files

def write_to_database(df, table_name="trips"):
    """Write DataFrame to PostgreSQL"""
    database_url = os.getenv('DATABASE_URL')
    if not database_url:
        raise ValueError("DATABASE_URL environment variable not set")

    if database_url.startswith('postgresql://'):
        database_url = database_url.replace('postgresql://', 'postgresql+psycopg://', 1)
    
    engine = create_engine(database_url)
    
    try:
        df.to_sql(table_name, engine, if_exists='append', index=False)
        print(f"✓ Successfully wrote {len(df)} rows to {table_name}")
    except Exception as e:
        print(f"✗ Error writing to database: {e}")
        raise

def read_files_for_month(url, target_month, region):
    """
    Download zip, find files matching month,
    read them and write to a consolidated CSV in /data/raw_bronze
    """
    response = requests.get(url)
    
    with zipfile.ZipFile(io.BytesIO(response.content)) as zip_ref:
        # Find matching files
        matching_files = find_files_by_month(zip_ref, target_month)
        
        if not matching_files:
            print(f"No files found for month {target_month}")
            return None
        
        print(f"Found {len(matching_files)} file(s) for {target_month}:")
        for f in matching_files:
            print(f"  - {f}")

        output_dir = "/app/data/raw_bronze"
        os.makedirs(output_dir, exist_ok=True)

        normalized_month = target_month.replace('-', '')
        output_file = f"{output_dir}/{region}-{normalized_month}-consolidated-tripdata.csv"
        
        row_count = 0
        header_written = False

        with open(output_file, 'w', newline='', encoding='utf-8') as outfile:
            for csv_file in matching_files:
                print(f"Reading {csv_file}...")
                
                with zip_ref.open(csv_file) as csv_data:
                    text_stream = io.TextIOWrapper(csv_data, encoding='utf-8')
                    
                    for line_num, line in enumerate(text_stream):
                        # Skip header on subsequent files
                        if line_num == 0 and header_written:
                            continue
                        
                        outfile.write(line)
                        
                        if line_num > 0:  # Don't count header
                            row_count += 1
                    
                    header_written = True
                    print(f"  Processed {row_count} total rows so far...")
        
        print(f"\n✓ Total rows written: {row_count}")
        print(f"✓ Saved consolidated CSV to {output_file}")

        bronze_summary = BronzeIngestSummary(
            layer="bronze",
            job=f"trips:{region.lower()}",
            window=target_month,
            objects=len(matching_files),
            rows=row_count
        )
        
        return bronze_summary

def ingest_to_bronze(region, target_month):
    """Load data from S3 and write to database"""
    key = find_s3_key_by_date(target_month, region)
    if not key:
        raise ValueError(f"No S3 key found for {target_month} in region {region}")
    
    url = f'https://s3.amazonaws.com/tripdata/{key}'
    print(f"Loading data from URL: {url}")
    
    bronze_report = read_files_for_month(url, target_month, region)
    with open(f"data/reports/bronze-{region.lower()}-{target_month.replace('-', '')}-report.json", 'w', encoding='utf-8') as f:
        json.dump(asdict(bronze_report), f, indent=4)

def inspect_bronze(region, window):
    """Inspect the bronze data for a given region and window"""
    report_file = f"data/reports/bronze-{region.lower()}-{window.replace('-', '')}-report.json"
    if not os.path.exists(report_file):
        raise FileNotFoundError(f"No report found for region={region}, window={window}")
    
    with open(report_file, 'r', encoding='utf-8') as f:
        report_data = json.load(f)
    
    print(json.dumps(report_data, indent=4))