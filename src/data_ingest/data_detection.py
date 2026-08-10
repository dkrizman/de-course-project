import requests
import xml.etree.ElementTree as ET
import zipfile
import io
import pandas as pd
from sqlalchemy import create_engine
import os

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

def read_files_for_month_pd(url, target_month, nrows=None):
    """
    Download zip, find files matching month, and read them.
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
        
        # Read all matching files
        dfs = []
        for csv_file in matching_files:
            print(f"Reading {csv_file}...")
            with zip_ref.open(csv_file) as csv_data:
                df = pd.read_csv(csv_data, 
                                 nrows=nrows,
                                 parse_dates=['started_at', 'ended_at'],
                                 dtype={
                                    'start_lat': 'float64',
                                    'start_lng': 'float64',
                                    'end_lat': 'float64',
                                    'end_lng': 'float64'
                                })
                # Drop rows with null values in required columns
                required_cols = ['ride_id', 'start_station_id', 'end_station_id', 
                               'started_at', 'ended_at', 'rideable_type']
                before = len(df)
                df = df.dropna(subset=required_cols)
                after = len(df)
                
                print(f"  Dropped {before - after} rows with null required columns")
                print(f"  Loaded {after} valid rows")
                

                dfs.append(df)
                print(f"  Loaded {len(df)} rows")
        
        # Combine all files
        if dfs:
            combined_df = pd.concat(dfs, ignore_index=True)
            print(f"\nTotal rows: {len(combined_df)}")
            return combined_df
        
        return None

def load_window_data(region, target_month):
    url = f'https://s3.amazonaws.com/tripdata/{find_s3_key_by_date(target_month, region)}'
    print(f"Loading data from URL: {url}")
    dataset = read_files_for_month_pd(url, target_month, nrows=1000)
    return dataset

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

def load_and_ingest(region, target_month):
    """Load data from S3 and write to database"""
    key = find_s3_key_by_date(target_month, region)
    if not key:
        raise ValueError(f"No S3 key found for {target_month} in region {region}")
    
    url = f'https://s3.amazonaws.com/tripdata/{key}'
    print(f"Loading data from URL: {url}")
    
    dataset = read_files_for_month_pd(url, target_month)
    if dataset is not None:
        write_to_database(dataset)
    else:
        print("No data loaded")