import pandas as pd
import requests
import time
from datetime import datetime, timedelta

# --- Constants ---
KRAKEN_API_URL = "https://api.kraken.com/0/public/"
CSV_FILE_PATH = "kraken_usd_pairs_ohlc_history.csv"
DAYS_OF_HISTORY = 730  # 2 years
KRAKEN_MAX_RESULTS = 720  # Max candles per API request

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
            response = requests.get(KRAKEN_API_URL + "OHLC", params=params)
            response.raise_for_status()
            data = response.json()
            if data['error']:
                print(f"Kraken API error for {pair}: {data['error']}")
                break

            pair_key = list(data['result'].keys())[0]
            ohlc_data = data['result'][pair_key]
            
            if not ohlc_data:
                break

            all_ohlc_data.extend(ohlc_data)
            since_timestamp = int(data['result']['last'])

            if len(ohlc_data) < KRAKEN_MAX_RESULTS:
                break

        except requests.exceptions.RequestException as e:
            print(f"Error fetching OHLC data for {pair}: {e}")
            break
        
        time.sleep(1)  # Be respectful of API rate limits

    return all_ohlc_data

def main():
    """Main function to update the crypto data."""
    usd_pairs = get_usd_pairs()
    if not usd_pairs:
        print("No USD pairs found. Exiting.")
        return

    all_data = []
    
    try:
        existing_df = pd.read_csv(CSV_FILE_PATH)
        existing_df['date'] = pd.to_datetime(existing_df['date'])
        last_update_timestamp = int(existing_df['date'].max().timestamp())
        print(f"Found existing data. Last update was on: {existing_df['date'].max().date()}")
    except FileNotFoundError:
        print("No existing data file found. Fetching full 2-year history.")
        existing_df = pd.DataFrame()
        two_years_ago = datetime.now() - timedelta(days=DAYS_OF_HISTORY)
        last_update_timestamp = int(two_years_ago.timestamp())

    for pair in usd_pairs:
        print(f"Processing {pair}...")
        
        if not existing_df.empty and pair not in existing_df['pair'].unique():
            print(f"New pair {pair} found. Fetching full history.")
            since_timestamp = int((datetime.now() - timedelta(days=DAYS_OF_HISTORY)).timestamp())
        else:
            since_timestamp = last_update_timestamp

        ohlc_data = get_ohlc_data(pair, since=since_timestamp)
        
        if ohlc_data:
            df = pd.DataFrame(
                ohlc_data, 
                columns=['time', 'open', 'high', 'low', 'close', 'vwap', 'volume', 'count']
            )
            df['date'] = pd.to_datetime(df['time'], unit='s')
            df['pair'] = pair
            
            numeric_cols = ['open', 'high', 'low', 'close', 'vwap', 'volume', 'count']
            df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric, errors='coerce')
            
            df = df[['pair', 'date', 'open', 'high', 'low', 'close', 'vwap', 'volume', 'count']]
            all_data.append(df)
        
        time.sleep(1)  # Be respectful of API rate limits

    if all_data:
        new_data_df = pd.concat(all_data, ignore_index=True)
        combined_df = pd.concat([existing_df, new_data_df], ignore_index=True)
        combined_df = combined_df.drop_duplicates(subset=['pair', 'date'], keep='last')
        combined_df = combined_df.sort_values(by=['pair', 'date']).reset_index(drop=True)
        
        combined_df.to_csv(CSV_FILE_PATH, index=False)
        print("CSV file updated successfully.")
    else:
        print("No new data to update.")

if __name__ == "__main__":
    main()
