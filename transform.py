import pandas as pd
import re

def normalize_trading_symbol(symbol):
    """Normalize trading symbol by removing unwanted characters and standardizing format."""
    if pd.isna(symbol) or not symbol:
        print(f"Warning: Encountered null or empty trading_symbol: {symbol}")
        return None
    raw_symbol = str(symbol)
    symbol = raw_symbol.strip().upper()
    # Remove common suffixes
    symbol = re.sub(r'-(EQ|BE|RE|SM|ST|PP|BL|BZ|IW|GS|GB|N[1-3])$', '', symbol)
    # Keep alphanumeric and &
    symbol = re.sub(r'[^\w&]', '', symbol)
    if not symbol:
        print(f"Warning: Normalized trading_symbol is empty (raw: {raw_symbol})")
        return None
    if symbol != raw_symbol:
        print(f"Normalized trading_symbol: {raw_symbol} -> {symbol}")
    return symbol

def transform_upstox_data(df):
    """Filter and transform Upstox data for NSE Equity instruments."""
    print("Transforming Upstox data for NSE Equity...")
    print("Raw Upstox DataFrame shape:", df.shape)
    print("Upstox columns:", df.columns.tolist())
    print("Unique exchange values:", df['exchange'].unique().tolist())
    print("Unique instrument_type values:", df['instrument_type'].unique().tolist())
    
    # Filter for NSE Equity variations
    df_filtered = df[(df['exchange'].str.contains('NSE', case=False, na=False)) & 
                     (df['instrument_type'].str.contains('EQ|EQUITY|STOCK|SHARE', case=False, na=False))]
    print(f"Filtered Upstox DataFrame shape: {df_filtered.shape}")
    
    if df_filtered.empty:
        print("Warning: No NSE Equity instruments found in Upstox data.")
        print("Sample Upstox rows (first 5):")
        print(df.head(5))
    
    # Define available columns
    available_columns = df_filtered.columns.tolist()
    required_columns = ['exchange', 'instrument_key', 'tradingsymbol', 'name']
    optional_columns = ['isin', 'short_name']
    
    # Select available required columns
    columns_to_select = [col for col in required_columns if col in available_columns]
    
    # Initialize transformed DataFrame
    df_transformed = df_filtered[columns_to_select].copy()
    
    # Normalize trading_symbol
    if 'tradingsymbol' in df_transformed.columns:
        df_transformed['tradingsymbol'] = df_transformed['tradingsymbol'].apply(normalize_trading_symbol)
    
    # Add symbol_name
    df_transformed['symbol_name'] = df_transformed['tradingsymbol'] if 'tradingsymbol' in df_transformed.columns else None
    
    # Add security_id
    df_transformed['security_id'] = None
    
    # Add optional columns
    for col in optional_columns:
        if col not in df_transformed.columns:
            df_transformed[col] = None
    
    # Rename columns
    df_transformed = df_transformed.rename(columns={'tradingsymbol': 'trading_symbol'})
    
    # Validate trading_symbol
    if 'trading_symbol' in df_transformed.columns:
        null_symbols = df_transformed['trading_symbol'].isna().sum()
        if null_symbols > 0:
            print(f"Warning: {null_symbols} null trading_symbol values in Upstox data.")
            df_transformed = df_transformed.dropna(subset=['trading_symbol'])
            print(f"Upstox DataFrame shape after removing nulls: {df_transformed.shape}")
        duplicates = df_transformed['trading_symbol'].duplicated(keep=False)
        if duplicates.sum() > 0:
            print(f"Warning: {duplicates.sum()} duplicate trading_symbol values in Upstox data.")
            print("Duplicate trading_symbols:", df_transformed[duplicates]['trading_symbol'].unique().tolist())
            print("Sample duplicate rows:\n", df_transformed[duplicates].head(5))
            df_transformed = df_transformed.drop_duplicates(subset=['trading_symbol'], keep='first')
            print(f"After removing duplicates, Upstox DataFrame shape: {df_transformed.shape}")
        print(f"Unique Upstox trading_symbol count: {df_transformed['trading_symbol'].nunique()}")
    
    # Reorder columns
    output_columns = ['exchange', 'instrument_key', 'symbol_name', 'security_id', 
                     'short_name', 'name', 'isin', 'trading_symbol']
    for col in output_columns:
        if col not in df_transformed.columns:
            df_transformed[col] = None
    
    print(f"Final Upstox transformed DataFrame shape: {df_transformed.shape}")
    if not df_transformed.empty:
        print("Sample Upstox transformed data:\n", df_transformed.head(5))
    return df_transformed[output_columns]

def transform_dhan_data(df):
    """Filter and transform Dhan data for NSE Equity instruments."""
    print("Transforming Dhan data for NSE Equity...")
    print("Raw Dhan DataFrame shape:", df.shape)
    print("Dhan columns:", df.columns.tolist())
    
    # Filter for NSE Equity
    df_filtered = df[(df['SEM_EXM_EXCH_ID'] == 'NSE') & (df['SEM_INSTRUMENT_NAME'] == 'EQUITY')]
    print(f"Filtered Dhan DataFrame shape: {df_filtered.shape}")
    
    if df_filtered.empty:
        print("Warning: No NSE Equity instruments found in Dhan data.")
    
    # Select and rename columns
    df_transformed = df_filtered[[
        'SEM_EXM_EXCH_ID', 'SM_SYMBOL_NAME', 'SEM_SMST_SECURITY_ID', 'SEM_TRADING_SYMBOL'
    ]].copy()
    
    # Normalize trading_symbol
    if 'SEM_TRADING_SYMBOL' in df_transformed.columns:
        df_transformed['SEM_TRADING_SYMBOL'] = df_transformed['SEM_TRADING_SYMBOL'].apply(normalize_trading_symbol)
    
    # Add missing columns
    df_transformed['instrument_key'] = None
    df_transformed['short_name'] = None
    df_transformed['name'] = None
    df_transformed['isin'] = None
    
    # Rename columns
    df_transformed = df_transformed.rename(columns={
        'SEM_EXM_EXCH_ID': 'exchange',
        'SM_SYMBOL_NAME': 'symbol_name',
        'SEM_SMST_SECURITY_ID': 'security_id',
        'SEM_TRADING_SYMBOL': 'trading_symbol'
    })
    
    # Validate trading_symbol
    if 'trading_symbol' in df_transformed.columns:
        null_symbols = df_transformed['trading_symbol'].isna().sum()
        if null_symbols > 0:
            print(f"Warning: {null_symbols} null trading_symbol values in Dhan data.")
            df_transformed = df_transformed.dropna(subset=['trading_symbol'])
            print(f"Dhan DataFrame shape after removing nulls: {df_transformed.shape}")
        duplicates = df_transformed['trading_symbol'].duplicated(keep=False)
        if duplicates.sum() > 0:
            print(f"Warning: {duplicates.sum()} duplicate trading_symbol values in Dhan data.")
            print("Duplicate trading_symbols:", df_transformed[duplicates]['trading_symbol'].unique().tolist())
            print("Sample duplicate rows:\n", df_transformed[duplicates].head(5))
            df_transformed = df_transformed.drop_duplicates(subset=['trading_symbol'], keep='first')
            print(f"After removing duplicates, Dhan DataFrame shape: {df_transformed.shape}")
        print(f"Unique Dhan trading_symbol count: {df_transformed['trading_symbol'].nunique()}")
    
    # Reorder columns
    output_columns = ['exchange', 'instrument_key', 'symbol_name', 'security_id', 
                      'short_name', 'name', 'isin', 'trading_symbol']
    print(f"Final Dhan transformed DataFrame shape: {df_transformed.shape}")
    if not df_transformed.empty:
        print("Sample Dhan transformed data:\n", df_transformed.head(5))
    return df_transformed[output_columns]