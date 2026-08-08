import requests
import xml.etree.ElementTree as ET
import zipfile
import io
import pandas as pd

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
                df = pd.read_csv(csv_data, nrows=nrows)
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