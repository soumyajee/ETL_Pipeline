import requests
import io
import logging
import gzip
import pandas as pd
UPSTOX_URL = "https://assets.upstox.com/market-quote/instruments/exchange/NSE.csv.gz"
DHAN_URL = "https://images.dhan.co/api-data/api-scrip-master.csv"

def extract_data():
    # Upstox
    logging.info(f"Downloading Upstox data from {UPSTOX_URL}")
    response = requests.get(UPSTOX_URL)
    response.raise_for_status()
    with gzip.open(io.BytesIO(response.content), 'rt', encoding='utf-8') as f:
        df_upstox = pd.read_csv(f)
    logging.info(f"Loaded Upstox data with {len(df_upstox)} records")

    # Dhan
    logging.info(f"Downloading Dhan data from {DHAN_URL}")
    response = requests.get(DHAN_URL)
    response.raise_for_status()
    df_dhan = pd.read_csv(io.StringIO(response.text), dtype={"SEM_SERIES": str, "SM_SYMBOL_NAME": str})
    logging.info(f"Loaded Dhan data with {len(df_dhan)} records")

    return df_upstox, df_dhan