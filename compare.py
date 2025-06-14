import pandas as pd
import os

def compare_and_output(upstox_df, dhan_df):
    """Compare Upstox and Dhan data and generate CSV outputs."""
    print("Comparing Upstox and Dhan data...")
    print(f"Upstox DataFrame shape: {upstox_df.shape}")
    print(f"Dhan DataFrame shape: {dhan_df.shape}")
    
    # Validate inputs
    if upstox_df.empty or dhan_df.empty:
        print("Warning: One or both DataFrames are empty. Skipping comparison.")
        os.makedirs('output', exist_ok=True)
        upstox_df.to_csv('output/only_in_upstox.csv', index=False)
        dhan_df.to_csv('output/only_in_dhan.csv', index=False)
        pd.DataFrame().to_csv('output/common_stocks.csv', index=False)
        return
    
    # Debug: Check for nulls and duplicates
    if 'trading_symbol' in upstox_df.columns:
        upstox_nulls = upstox_df['trading_symbol'].isna().sum()
        upstox_duplicates = upstox_df['trading_symbol'].duplicated(keep=False).sum()
        print(f"Upstox trading_symbol nulls: {upstox_nulls}, duplicates: {upstox_duplicates}")
        if upstox_duplicates > 0:
            print("Upstox duplicate trading_symbols:", upstox_df[upstox_df['trading_symbol'].duplicated(keep=False)]['trading_symbol'].unique().tolist())
            print("Sample Upstox duplicate rows:\n", upstox_df[upstox_df['trading_symbol'].duplicated(keep=False)].head(5))
    if 'trading_symbol' in dhan_df.columns:
        dhan_nulls = dhan_df['trading_symbol'].isna().sum()
        dhan_duplicates = dhan_df['trading_symbol'].duplicated(keep=False).sum()
        print(f"Dhan trading_symbol nulls: {dhan_nulls}, duplicates: {dhan_duplicates}")
        if dhan_duplicates > 0:
            print("Dhan duplicate trading_symbols:", dhan_df[dhan_df['trading_symbol'].duplicated(keep=False)]['trading_symbol'].unique().tolist())
            print("Sample Dhan duplicate rows:\n", dhan_df[dhan_df['trading_symbol'].duplicated(keep=False)].head(5))
    
    # Debug: Print sample trading_symbol values
    if not upstox_df.empty:
        print("Sample Upstox trading_symbol values:", upstox_df['trading_symbol'].head(5).tolist())
    if not dhan_df.empty:
        print("Sample Dhan trading_symbol values:", dhan_df['trading_symbol'].head(5).tolist())
    
    # Copy DataFrames
    upstox_df = upstox_df.copy()
    dhan_df = dhan_df.copy()
    
    # Remove null trading_symbol rows
    if 'trading_symbol' in upstox_df.columns:
        upstox_df = upstox_df.dropna(subset=['trading_symbol'])
        print(f"Upstox DataFrame shape after removing nulls: {upstox_df.shape}")
    if 'trading_symbol' in dhan_df.columns:
        dhan_df = dhan_df.dropna(subset=['trading_symbol'])
        print(f"Dhan DataFrame shape after removing nulls: {dhan_df.shape}")
    
    # Remove duplicates
    if 'trading_symbol' in upstox_df.columns:
        upstox_df = upstox_df.drop_duplicates(subset=['trading_symbol'], keep='first')
        print(f"Upstox DataFrame shape after deduplication: {upstox_df.shape}")
        print(f"Unique Upstox trading_symbol count: {upstox_df['trading_symbol'].nunique()}")
    if 'trading_symbol' in dhan_df.columns:
        dhan_df = dhan_df.drop_duplicates(subset=['trading_symbol'], keep='first')
        print(f"Dhan DataFrame shape after deduplication: {dhan_df.shape}")
        print(f"Unique Dhan trading_symbol count: {dhan_df['trading_symbol'].nunique()}")
    
    # Debug: Compare trading_symbol sets
    upstox_symbols = set(upstox_df['trading_symbol'])
    dhan_symbols = set(dhan_df['trading_symbol'])
    common_symbols = upstox_symbols.intersection(dhan_symbols)
    only_upstox_symbols = upstox_symbols - dhan_symbols
    only_dhan_symbols = dhan_symbols - upstox_symbols
    print(f"Total unique Upstox symbols: {len(upstox_symbols)}")
    print(f"Total unique Dhan symbols: {len(dhan_symbols)}")
    print(f"Common symbols: {len(common_symbols)}")
    print(f"Only in Upstox symbols: {len(only_upstox_symbols)}")
    print(f"Only in Dhan symbols: {len(only_dhan_symbols)}")
    if only_upstox_symbols:
        print("Sample only Upstox symbols:", list(only_upstox_symbols)[:5])
    if only_dhan_symbols:
        print("Sample only Dhan symbols:", list(only_dhan_symbols)[:5])
    
    # Inner join for common stocks
    common_df = pd.merge(
        upstox_df, dhan_df, 
        on='trading_symbol', 
        how='inner', 
        suffixes=('_upstox', '_dhan')
    )
    print(f"Common stocks DataFrame shape: {common_df.shape}")
    
    # Validate common_df
    if len(common_df) > min(len(upstox_df), len(dhan_df)):
        print("Warning: Common stocks DataFrame larger than expected. Deduplicating...")
        duplicates = common_df['trading_symbol'].duplicated(keep=False)
        if duplicates.sum() > 0:
            print("Common duplicate trading_symbols:", common_df[duplicates]['trading_symbol'].unique().tolist())
            print("Sample common duplicate rows:\n", common_df[duplicates].head(5))
        common_df = common_df.drop_duplicates(subset=['trading_symbol'], keep='first')
        print(f"Common stocks DataFrame shape after deduplication: {common_df.shape}")
    
    # Combine fields
    if not common_df.empty:
        common_df['exchange'] = common_df['exchange_upstox']
        common_df['instrument_key'] = common_df['instrument_key_upstox']
        common_df['symbol_name'] = dhan_df['symbol_name']
        common_df['security_id'] = common_df['security_id_dhan']
        common_df['short_name'] = common_df['short_name_upstox']
        common_df['name'] = common_df['name_upstox']
        common_df['isin'] = common_df['isin_upstox']
        
        common_df = common_df[[
            'exchange', 'instrument_key', 'symbol_name', 'security_id',
            'short_name', 'name', 'isin', 'trading_symbol'
        ]]
    
    # Only in Upstox
    only_upstox_df = upstox_df[~upstox_df['trading_symbol'].isin(dhan_df['trading_symbol'])]
    print(f"Only in Upstox DataFrame shape: {only_upstox_df.shape}")
    if not only_upstox_df.empty:
        print("Sample unique Upstox symbols:", only_upstox_df['trading_symbol'].head(5).tolist())
    
    # Only in Dhan
    only_dhan_df = dhan_df[~dhan_df['trading_symbol'].isin(upstox_df['trading_symbol'])]
    print(f"Only in Dhan DataFrame shape: {only_dhan_df.shape}")
    if not only_dhan_df.empty:
        print("Sample unique Dhan symbols:", only_dhan_df['trading_symbol'].head(5).tolist())
    
    # Save to CSVs
    os.makedirs('output', exist_ok=True)
    common_df.to_csv('output/common_stocks.csv', index=False)
    only_upstox_df.to_csv('output/only_in_upstox.csv', index=False)
    only_dhan_df.to_csv('output/only_in_dhan.csv', index=False)
    
    print("CSV files generated: output/common_stocks.csv, output/only_in_upstox.csv, output/only_in_dhan.csv")