import pandas as pd
import gzip
import requests
import sqlite3
from pymongo import MongoClient
from urllib.parse import urlparse
from datetime import datetime
import os
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Constants
UPSTOX_URL = "https://assets.upstox.com/market-quote/instruments/exchange/NSE.csv.gz"
DHAN_URL = "https://images.dhan.co/api-data/api-scrip-master.csv"
OUTPUT_DIR = "output"
DB_NAME = "nse_instruments.db"
MONGO_URI = "mongodb://localhost:27017"
MONGO_DB = "market_data"
MONGO_COLLECTION = "upstox_nse"

def create_output_directory():
    """Create output directory if it doesn't exist."""
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    logger.info(f"Output directory ensured: {OUTPUT_DIR}")

def download_file(url, local_filename):
    """Download file from URL and save locally."""
    try:
        response = requests.get(url, stream=True)
        response.raise_for_status()
        with open(local_filename, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        logger.info(f"Downloaded file: {local_filename}")
        return local_filename
    except Exception as e:
        logger.error(f"Error downloading {url}: {e}")
        raise

def extract_upstox_data():
    """Extract and filter Upstox NSE Equity data."""
    local_file = os.path.join(OUTPUT_DIR, "upstox_nse.csv.gz")
    download_file(UPSTOX_URL, local_file)
    
    # Decompress and read
    with gzip.open(local_file, 'rt', encoding='utf-8') as f:
        df = pd.read_csv(f)
    
    # Log available columns and unique values for debugging
    logger.info(f"Upstox DataFrame columns: {list(df.columns)}")
    if 'exchange' in df.columns:
        logger.info(f"Unique exchange values: {df['exchange'].unique()}")
    if 'instrument_type' in df.columns:
        logger.info(f"Unique instrument_type values: {df['instrument_type'].unique()}")
    
    # Filter for NSE Equity
    try:
        df_filtered = df[(df['exchange'] == 'NSE_EQ') & (df['instrument_type'] == 'EQUITY')]
        if df_filtered.empty:
            logger.warning("No records found for exchange='NSE_EQ' and instrument_type='EQUITY'. Check filter values.")
            raise ValueError("No NSE Equity records found in Upstox data.")
    except KeyError as e:
        logger.error(f"Filter columns not found: {e}")
        logger.error(f"Available columns: {list(df.columns)}")
        raise
    
    # Define column mapping
    column_mapping = {
        'instrument_key': 'instrument_key',
        'short_name': 'tick_size',  # Using tick_size as fallback
        'name': 'name',
        'isin': 'ISIN',
        'trading_symbol': 'tradingsymbol'
    }
    
    # Check available columns
    available_columns = set(df_filtered.columns)
    selected_columns = []
    for output_col, input_col in column_mapping.items():
        if input_col in available_columns:
            selected_columns.append(input_col)
        else:
            logger.warning(f"Column '{input_col}' not found in Upstox data. Setting '{output_col}' to None.")
            df_filtered = df_filtered.copy()
            df_filtered.loc[:, output_col] = None
    
    # Select columns
    try:
        df_selected = df_filtered[selected_columns].copy()
        reverse_mapping = {v: k for k, v in column_mapping.items() if v in selected_columns}
        df_selected = df_selected.rename(columns=reverse_mapping)
    except KeyError as e:
        logger.error(f"Column selection failed: {e}")
        logger.error(f"Available columns: {list(df_filtered.columns)}")
        raise
    
    # Ensure all required columns
    required_columns = ['instrument_key', 'short_name', 'name', 'isin', 'trading_symbol']
    for col in required_columns:
        if col not in df_selected.columns:
            df_selected[col] = None
    
    df_selected['short_name'] = df_selected['short_name'].fillna(df_selected['name'])
    df_selected['exchange'] = 'NSE'
    df_selected['trading_symbol'] = df_selected['trading_symbol'].str.strip().str.upper()
    
    logger.info(f"Extracted {len(df_selected)} Upstox NSE Equity records")
    return df_selected

def extract_dhan_data():
    """Extract and filter Dhan NSE Equity data."""
    local_file = os.path.join(OUTPUT_DIR, "dhan_scrip.csv")
    download_file(DHAN_URL, local_file)
    
    # Read with low_memory=False to handle mixed types
    df = pd.read_csv(local_file, low_memory=False)
    
    # Log available columns and unique values for debugging
    logger.info(f"Dhan DataFrame columns: {list(df.columns)}")
    if 'SEM_EXM_EXCH_ID' in df.columns:
        logger.info(f"Unique SEM_EXM_EXCH_ID values: {df['SEM_EXM_EXCH_ID'].unique()}")
    if 'SEM_INSTRUMENT_NAME' in df.columns:
        logger.info(f"Unique SEM_INSTRUMENT_NAME values: {df['SEM_INSTRUMENT_NAME'].unique()}")
    
    # Filter for NSE Equity
    try:
        df_filtered = df[(df['SEM_EXM_EXCH_ID'] == 'NSE') & (df['SEM_INSTRUMENT_NAME'] == 'EQUITY')]
        if df_filtered.empty:
            logger.warning("No records found for SEM_EXM_EXCH_ID='NSE' and SEM_INSTRUMENT_NAME='EQUITY'. Check column values.")
    except KeyError as e:
        logger.error(f"Filter columns not found: {e}")
        logger.error(f"Available columns: {list(df.columns)}")
        raise
    
    # Define column mapping
    column_mapping = {
        'security_id': 'SEM_SMST_SECURITY_ID',
        'symbol_name': 'SM_SYMBOL_NAME',
        'trading_symbol': 'SEM_TRADING_SYMBOL'
    }
    
    # Check available columns
    available_columns = set(df_filtered.columns)
    selected_columns = []
    for output_col, input_col in column_mapping.items():
        if input_col in available_columns:
            selected_columns.append(input_col)
        else:
            logger.warning(f"Column '{input_col}' not found in Dhan data. Setting '{output_col}' to None.")
            df_filtered = df_filtered.copy()
            df_filtered.loc[:, output_col] = None
    
    # Select columns
    try:
        df_selected = df_filtered[selected_columns].copy()
        reverse_mapping = {v: k for k, v in column_mapping.items() if v in selected_columns}
        df_selected = df_selected.rename(columns=reverse_mapping)
    except KeyError as e:
        logger.error(f"Column selection failed: {e}")
        logger.error(f"Available columns: {list(df_filtered.columns)}")
        raise
    
    # Ensure all required columns
    required_columns = ['security_id', 'symbol_name', 'trading_symbol']
    for col in required_columns:
        if col not in df_selected.columns:
            df_selected[col] = None
    
    df_selected['exchange'] = 'NSE'
    df_selected['trading_symbol'] = df_selected['trading_symbol'].str.strip().str.upper()
    
    logger.info(f"Extracted {len(df_selected)} Dhan NSE Equity records")
    return df_selected

def load_to_mongodb(upstox_df):
    """Load Upstox data to MongoDB."""
    try:
        client = MongoClient(MONGO_URI)
        db = client[MONGO_DB]
        collection = db[MONGO_COLLECTION]
        
        # Convert DataFrame to list of dictionaries
        records = upstox_df.to_dict('records')
        
        # Upsert based on instrument_key
        for record in records:
            collection.update_one(
                {'instrument_key': record['instrument_key']},
                {'$set': record},
                upsert=True
            )
        
        logger.info(f"Loaded {len(records)} records to MongoDB")
        client.close()
    except Exception as e:
        logger.error(f"Error loading to MongoDB: {e}")
        raise

def load_to_sqlite(dhan_df):
    """Load Dhan data to SQLite."""
    try:
        conn = sqlite3.connect(DB_NAME)
        # Create table if not exists
        create_table_query = """
        CREATE TABLE IF NOT EXISTS dhan_nse (
            security_id TEXT PRIMARY KEY,
            symbol_name TEXT,
            trading_symbol TEXT,
            exchange TEXT
        )
        """
        conn.execute(create_table_query)
        
        # Upsert data
        dhan_df.to_sql('dhan_nse', conn, if_exists='replace', index=False)
        
        logger.info(f"Loaded {len(dhan_df)} records to SQLite")
        conn.commit()
        conn.close()
    except Exception as e:
        logger.error(f"Error loading to SQLite: {e}")
        raise

def compare_dataframes(upstox_df, dhan_df):
    """Compare Upstox and Dhan data and generate output CSVs."""
    # Check for duplicate trading_symbols
    upstox_duplicates = upstox_df[upstox_df['trading_symbol'].duplicated(keep=False)]
    dhan_duplicates = dhan_df[dhan_df['trading_symbol'].duplicated(keep=False)]
    logger.info(f"Duplicate trading_symbols in Upstox: {len(upstox_duplicates)}")
    logger.info(f"Duplicate trading_symbols in Dhan: {len(dhan_duplicates)}")
    
    # Merge DataFrames
    common_df = pd.merge(
        upstox_df, dhan_df,
        on='trading_symbol', how='inner',
        suffixes=('_upstox', '_dhan')
    )
    
    # Select fields for output
    common_output = common_df[[
        'exchange_upstox', 'instrument_key', 'symbol_name',
        'security_id', 'short_name', 'name', 'isin', 'trading_symbol'
    ]].rename(columns={'exchange_upstox': 'exchange'})
    
    # Find unique records
    only_upstox = upstox_df[~upstox_df['trading_symbol'].isin(dhan_df['trading_symbol'])]
    only_dhan = dhan_df[~dhan_df['trading_symbol'].isin(upstox_df['trading_symbol'])]
    
    # Save to CSVs
    common_output.to_csv(os.path.join(OUTPUT_DIR, 'common_stocks.csv'), index=False)
    only_upstox.to_csv(os.path.join(OUTPUT_DIR, 'only_in_upstox.csv'), index=False)
    only_dhan.to_csv(os.path.join(OUTPUT_DIR, 'only_in_dhan.csv'), index=False)
    
    logger.info(f"Generated CSVs: common_stocks ({len(common_output)}), "
                f"only_in_upstox ({len(only_upstox)}), only_in_dhan ({len(only_dhan)})")

def main():
    """Main ETL pipeline function."""
    try:
        create_output_directory()
        
        # Extract
        upstox_df = extract_upstox_data()
        dhan_df = extract_dhan_data()
        
        # Load
        load_to_mongodb(upstox_df)
        load_to_sqlite(dhan_df)
        
        # Compare and output
        compare_dataframes(upstox_df, dhan_df)
        
        logger.info("ETL pipeline completed successfully")
    except Exception as e:
        logger.error(f"ETL pipeline failed: {e}")
        raise

if __name__ == "__main__":
    main()