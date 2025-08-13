import pandas as pd
import requests
import time
from datetime import datetime, timedelta

# --- Constants ---
KRAKEN_API_URL = "https://api.kraken.com/0/public/"
CSV_FILE_PATH = "kraken_usd_pairs_ohlc_history.csv"
DAYS_OF_HISTORY = 730  # 2 years
KRAKEN_MAX_RESULTS = 720 # The maximum number of candles per API request

def get_usd_pairs():
    """Fetches all USD trading pairs from Kraken."""
    try:
        response = requests.get(KRAKEN_API_URL + "AssetPairs")
        response.raise_for_status()
        data = response.json()
        if data['error']:
            raise Exception(f"Kraken API error: {data['error']}")
        
        # Filter for pairs that end in 'USD' and are not futures
        usd_pairs = [pair for pair in data['result'] if pair.endswith('USD') and '.d' not in pair]
        print(f"Found {len(usd_pairs)} USD pairs.")
        return usd_pairs
    except requests.exceptions.RequestException as e:
        print(f"Error fetching USD pairs: {e}")
        return []

def get_ohlc_data(pair, since=None):
    """
    Fetches OHLC data for a given pair, handling pagination.
    """
    all_ohlc_data = []
    since_timestamp = int(since) if since else 0

    while True:
        params = {'pair': pair, 'interval': 1440}  # 1440 = 1 day
        if since_timestamp:
            params['since'] = since_timestamp

        try:
            response = requests.get(KRAKEN_API_URL + "OHLC", params=par

