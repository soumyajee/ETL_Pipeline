import pandas as pd
import requests
import gzip
import io

def download_file(url, is_gzipped=False):
    """Download file from URL and return content."""
    try:
        response = requests.get(url, stream=True)
        response.raise_for_status()
        content = response.content
        if is_gzipped:
            content = gzip.decompress(content)
        return content
    except requests.RequestException as e:
        print(f"Error downloading {url}: {e}")
        raise

def extract_upstox_data():
    """Extract Upstox NSE instrument data."""
    url = "https://assets.upstox.com/market-quote/instruments/exchange/NSE.csv.gz"
    print(f"Downloading Upstox data from {url}...")
    content = download_file(url, is_gzipped=True)
    df = pd.read_csv(io.StringIO(content.decode('utf-8')))
    print(f"Upstox raw data shape: {df.shape}")
    if df.empty:
        raise ValueError("Upstox dataset is empty.")
    return df

def extract_dhan_data():
    """Extract Dhan scrip master data."""
    url = "https://images.dhan.co/api-data/api-scrip-master.csv"
    print(f"Downloading Dhan data from {url}...")
    content = download_file(url)
    df = pd.read_csv(io.StringIO(content.decode('utf-8')))
    print(f"Dhan raw data shape: {df.shape}")
    if df.empty:
        raise ValueError("Dhan dataset is empty.")
    return df