for pair in usd_pairs:
    print(f"Processing {pair}...")
    
    # Check if the pair is new. If so, get its full history.
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
        
        # Keep all OHLC columns
        df = df[['pair', 'date', 'open', 'high', 'low', 'close', 'vwap', 'volume', 'count']]
        
        # Convert numeric columns
        numeric_cols = ['open', 'high', 'low', 'close', 'vwap', 'volume', 'count']
        df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric, errors='coerce')
        
        all_data.append(df)
    
    time.sleep(1) # API rate limit

if all_data:
    new_data_df = pd.concat(all_data, ignore_index=True)
    
    # Merge and deduplicate
    combined_df = pd.concat([existing_df, new_data_df], ignore_index=True)
    combined_df = combined_df.drop_duplicates(subset=['pair', 'date'], keep='last')
    combined_df = combined_df.sort_values(by=['pair', 'date']).reset_index(drop=True)
    
    combined_df.to_csv(CSV_FILE_PATH, index=False)
    print("CSV file updated successfully.")
else:
    print("No new data to update.")

