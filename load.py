from pymongo import MongoClient
from sqlalchemy import create_engine
from decouple import config
import pandas as pd
import sqlite3
import os

def load_to_mongodb(df):
    """Load Upstox data to MongoDB."""
    print("Loading Upstox data to MongoDB...")
    if df.empty:
        print("Warning: Upstox DataFrame is empty. No data will be loaded to MongoDB.")
        return
    
    client = MongoClient(config('MONGODB_URI'))
    db = client['market_data']
    collection = db['upstox_nse']
    
    # Convert DataFrame to list of dictionaries
    records = df.to_dict('records')
    
    # Upsert based on instrument_key
    for record in records:
        collection.update_one(
            {'instrument_key': record['instrument_key']},
            {'$set': record},
            upsert=True
        )
    print(f"Loaded {len(records)} records to MongoDB upstox_nse collection.")
    client.close()

def load_to_sql(df):
    """Load Dhan data to SQLite."""
    print("Loading Dhan data to SQLite...")
    if df.empty:
        print("Warning: Dhan DataFrame is empty. No data will be loaded to SQLite.")
        return
    
    # Define SQLite database path
    db_path = config('SQLITE_DB_PATH', default='nse.db')
    
    # Validate db_path
    if not db_path:
        raise ValueError("SQLITE_DB_PATH is empty. Please set a valid path in .env file.")
    
    # Debug: Print database path and current working directory
    print(f"SQLite DB Path: {os.path.abspath(db_path)}")
    print(f"Current Working Directory: {os.getcwd()}")
    
    # Create directory if db_path includes a directory component
    db_dir = os.path.dirname(db_path)
    if db_dir:
        os.makedirs(db_dir, exist_ok=True)
    
    # Check if file exists and is a valid SQLite database
    if os.path.exists(db_path):
        try:
            with sqlite3.connect(db_path) as conn:
                conn.execute("SELECT name FROM sqlite_master WHERE type='table';")
        except sqlite3.DatabaseError:
            print(f"Warning: {db_path} is not a valid SQLite database. Deleting and recreating.")
            os.remove(db_path)
    
    # Create SQLAlchemy engine for SQLite
    engine = create_engine(f'sqlite:///{db_path}')
    
    # Load data to SQLite, replacing the table if it exists
    df.to_sql('dhan_nse', engine, if_exists='replace', index=False, chunksize=1000)
    
    # Ensure table schema
    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS dhan_nse (
                exchange TEXT,
                instrument_key TEXT,
                symbol_name TEXT,
                security_id TEXT PRIMARY KEY,
                short_name TEXT,
                name TEXT,
                isin TEXT,
                trading_symbol TEXT UNIQUE
            )
        """)
        conn.commit()
    
    print(f"Loaded {len(df)} records to SQLite dhan_nse table.")
    engine.dispose()