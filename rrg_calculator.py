import pandas as pd
import numpy as np
import json
from datetime import datetime

# --- CONFIGURATION ---
CSV_URL = 'https://raw.githubusercontent.com/wayfarer-2018/Kraken_data/main/kraken_usd_pairs_close_history.csv'
OUTPUT_JSON_PATH = 'rrg_data.json'
RRG_LOOKBACK = 10
MIN_HISTORY_REQUIRED = RRG_LOOKBACK * 2 # Minimum data points needed to start calculation

# --- ALIASES AND CATEGORIES (Should match the dashboard's config) ---
TICKER_ALIAS_MAP = {
    'XBT': 'BTC', 'XDG': 'DOGE', 'XXRP': 'XRP', 'XXLM': 'XLM',
    'XETC': 'ETC', 'XETH': 'ETH', 'XLTC': 'LTC', 'XMLN': 'MLN',
    'XREP': 'REP', 'DOT2': 'DOT', 'KSM2': 'KSM', 'FIL2': 'FIL'
}

CATEGORY_MAP = {
    'BTC': 'Layer 1 & Layer 2', 'ETH': 'Layer 1 & Layer 2', 'SOL': 'Layer 1 & Layer 2', 'ADA': 'Layer 1 & Layer 2', 'AVAX': 'Layer 1 & Layer 2', 
    'TRX': 'Layer 1 & Layer 2', 'NEAR': 'Layer 1 & Layer 2', 'ATOM': 'Layer 1 & Layer 2', 'ALGO': 'Layer 1 & Layer 2', 'XRP': 'Layer 1 & Layer 2', 
    'LTC': 'Layer 1 & Layer 2', 'BCH': 'Layer 1 & Layer 2', 'XLM': 'Layer 1 & Layer 2', 'ETC': 'Layer 1 & Layer 2', 'HBAR': 'Layer 1 & Layer 2', 
    'FTM': 'Layer 1 & Layer 2', 'XTZ': 'Layer 1 & Layer 2', 'EOS': 'Layer 1 & Layer 2', 'KAS': 'Layer 1 & Layer 2', 'MINA': 'Layer 1 & Layer 2',
    'SUI': 'Layer 1 & Layer 2', 'INJ': 'Layer 1 & Layer 2', 'WAXL': 'Layer 1 & Layer 2', 'WLD': 'Layer 1 & Layer 2', 
    'ZEC': 'Privacy', 'XMR': 'Privacy',
    'MATIC': 'Layer 1 & Layer 2', 'STX': 'Layer 1 & Layer 2', 'OP': 'Layer 1 & Layer 2', 'ARB': 'Layer 1 & Layer 2', 'IMX': 'Layer 1 & Layer 2',
    'FLOW': 'Layer 1 & Layer 2', 'FLR': 'Layer 1 & Layer 2', 'HNT': 'Layer 1 & Layer 2', 'JST': 'Layer 1 & Layer 2', 'JUNO': 'Layer 1 & Layer 2', 'LUNA': 'Layer 1 & Layer 2', 'LUNA2': 'Layer 1 & Layer 2', 'NANO': 'Layer 1 & Layer 2', 'POL': 'Layer 1 & Layer 2', 'QNT': 'Layer 1 & Layer 2', 'QTUM': 'Layer 1 & Layer 2', 'RUNE': 'Layer 1 & Layer 2', 'SC': 'Layer 1 & Layer 2', 'SEI': 'Layer 1 & Layer 2', 'SGB': 'Layer 1 & Layer 2', 'STRK': 'Layer 1 & Layer 2', 'TON': 'Layer 1 & Layer 2',

    'LINK': 'DeFi', 'UNI': 'DeFi', 'AAVE': 'DeFi', 'MKR': 'DeFi', 'LDO': 'DeFi', 'SNX': 'DeFi', 
    'CRV': 'DeFi', 'COMP': 'DeFi', 'SUSHI': 'DeFi', 'YFI': 'DeFi', 'ZRX': 'DeFi', '1INCH': 'DeFi',
    'KAVA': 'DeFi', 'VELODROME': 'DeFi', 'WELL': 'DeFi', 'WOO': 'DeFi', 'MLN': 'DeFi', 'REP': 'DeFi', 'ZEX': 'DeFi',
    'FARM': 'DeFi', 'FIS': 'DeFi', 'FXS': 'DeFi', 'GMX': 'DeFi', 'GMT': 'DeFi', 'JOE': 'DeFi', 'JTO': 'DeFi', 'JUP': 'DeFi', 'KNC': 'DeFi', 'KP3R': 'DeFi', 'LIT': 'DeFi', 'LPT': 'DeFi', 'LQTY': 'DeFi', 'LRC': 'DeFi', 'MIR': 'DeFi', 'MNT': 'DeFi', 'NMR': 'DeFi', 'OGN': 'DeFi', 'OM': 'DeFi', 'ORCA': 'DeFi', 'PENDLE': 'DeFi', 'PERP': 'DeFi', 'PYTH': 'DeFi', 'RAD': 'DeFi', 'RAY': 'DeFi', 'REN': 'DeFi', 'REQ': 'DeFi', 'RPL': 'DeFi', 'RSR': 'DeFi', 'SPELL': 'DeFi', 'STORJ': 'DeFi', 'SUPER': 'DeFi', 'SYN': 'DeFi', 'UMA': 'DeFi', 'UNFI': 'DeFi',

    'DOGE': 'Memecoin', 'SHIB': 'Memecoin', 'PEPE': 'Memecoin', 'WIF': 'Memecoin', 'BONK': 'Memecoin', 
    'FLOKI': 'Memecoin', 'VINE': 'Memecoin', 'WEN': 'Memecoin',
    'FARTCOIN': 'Memecoin', 'FWOG': 'Memecoin', 'GOAT': 'Memecoin', 'GRIFFAIN': 'Memecoin', 'GUN': 'Memecoin', 'HIPPO': 'Memecoin', 'HMSTR': 'Memecoin', 'JAILSTOOL': 'Memecoin', 'ME': 'Memecoin', 'MELANIA': 'Memecoin', 'MEW': 'Memecoin', 'MICHI': 'Memecoin', 'MOG': 'Memecoin', 'MUBARAK': 'Memecoin', 'PENGU': 'Memecoin', 'PONKE': 'Memecoin', 'POPCAT': 'Memecoin', 'SAMO': 'Memecoin', 'SNEK': 'Memecoin', 'TITCOIN': 'Memecoin', 'TOSHI': 'Memecoin', 'TREMP': 'Memecoin', 'TRUMP': 'Memecoin', 'TURBO': 'Memecoin',

    'ICP': 'Infrastructure & Services', 'FIL': 'Infrastructure & Services', 'AR': 'Infrastructure & Services', 'THETA': 'Infrastructure & Services',
    'DOT': 'Infrastructure & Services', 'KSM': 'Infrastructure & Services', 'WCT': 'Infrastructure & Services',
    'WIN': 'Infrastructure & Services', 'W': 'Infrastructure & Services', 'XCN': 'Infrastructure & Services', 'XRT': 'Infrastructure & Services',
    'ZEUS': 'Infrastructure & Services',
    'EWT': 'Infrastructure & Services', 'FLUX': 'Infrastructure & Services', 'GLMR': 'Infrastructure & Services', 'ICX': 'Infrastructure & Services', 'JASMY': 'Infrastructure & Services', 'KIN': 'Infrastructure & Services', 'MOVR': 'Infrastructure & Services', 'MXC': 'Infrastructure & Services', 'NTRN': 'Infrastructure & Services', 'NYM': 'Infrastructure & Services', 'POWR': 'Infrastructure & Services', 'SAGA': 'Infrastructure & Services', 'SDN': 'Infrastructure & Services', 'SWEAT': 'Infrastructure & Services', 'TRAC': 'Infrastructure & Services',
    
    'GRT': 'AI', 'RNDR': 'AI', 'RENDER': 'AI', 'FET': 'AI', 'AGIX': 'AI', 'OCEAN': 'AI', 'VANRY': 'AI', 'VIRTUAL': 'AI', 'ZEREBRO': 'AI', 'TAO': 'AI',

    'MANA': 'Gaming & Metaverse', 'SAND': 'Gaming & Metaverse', 'AXS': 'Gaming & Metaverse', 'GALA': 'Gaming & Metaverse', 'ENJ': 'Gaming & Metaverse',
    'CHZ': 'Gaming & Metaverse', 'VVV': 'Gaming & Metaverse', 'WAL': 'Gaming & Metaverse', 'YGG': 'Gaming & Metaverse',
    'GAL': 'Gaming & Metaverse', 'GARI': 'Gaming & Metaverse', 'GHST': 'Gaming & Metaverse', 'ILV': 'Gaming & Metaverse', 'IMX': 'Gaming & Metaverse', 'MASK': 'Gaming & Metaverse', 'MC': 'Gaming & Metaverse', 'PORTAL': 'Gaming & Metaverse', 'RARE': 'Gaming & Metaverse', 'RARI': 'Gaming & Metaverse', 'TLM': 'Gaming & Metaverse', 'TVK': 'Gaming & Metaverse'
}

def load_and_clean_data(url):
    """Loads CSV, normalizes tickers, and pivots data into a wide format."""
    df = pd.read_csv(url)
    df['date'] = pd.to_datetime(df['date'])
    
    # Clean and normalize the 'pair' column
    df['symbol'] = df['pair'].str.replace(r'ZUSD|USD|USDT|BUSD|BTC$', '', regex=True).str.upper()
    df['symbol'] = df['symbol'].replace(TICKER_ALIAS_MAP)
    
    # Pivot the table to have dates as index and symbols as columns
    price_df = df.pivot_table(index='date', columns='symbol', values='close', aggfunc='first')
    
    # Forward-fill missing values to handle gaps
    price_df.ffill(inplace=True)
    
    return price_df

def calculate_rrg(price_df):
    """Calculates RRG data for the entire history using the Dynamic Universe approach."""
    all_dates = price_df.index
    rrg_snapshots = {}

    for i in range(MIN_HISTORY_REQUIRED, len(all_dates)):
        current_date = all_dates[i]
        start_date = all_dates[i - MIN_HISTORY_REQUIRED]
        
        # 1. Determine the valid universe for this date
        # An asset is valid if it has no missing data in the lookback window
        historical_window = price_df[start_date:current_date]
        valid_assets = historical_window.dropna(axis=1).columns
        
        if len(valid_assets) < 2:
            continue

        # 2. Calculate the dynamic benchmark for this day's universe
        benchmark = price_df.loc[start_date:current_date, valid_assets].mean(axis=1)
        
        daily_data = {}
        
        # 3. Calculate RRG for each asset in the valid universe
        for asset in valid_assets:
            # Calculate Relative Strength
            rs = price_df.loc[start_date:current_date, asset] / benchmark
            
            # Calculate RS-Ratio (SMA of Relative Strength)
            rs_ratio_series = rs.rolling(window=RRG_LOOKBACK).mean()
            
            # Normalize RS-Ratio
            mean_rs_ratio = rs_ratio_series.mean()
            std_rs_ratio = rs_ratio_series.std()
            if std_rs_ratio == 0: continue
            
            normalized_rs_ratio = 100 + ((rs_ratio_series - mean_rs_ratio) / std_rs_ratio) * 10
            
            # Calculate RS-Momentum (Rate of Change of Normalized RS-Ratio)
            rs_momentum_series = (normalized_rs_ratio / normalized_rs_ratio.shift(RRG_LOOKBACK) - 1) * 100 + 100
            
            # Get the latest values for the current date
            current_ratio = normalized_rs_ratio.iloc[-1]
            current_momentum = rs_momentum_series.iloc[-1]

            if not np.isnan(current_ratio) and not np.isnan(current_momentum):
                daily_data[asset] = {
                    'x': round(current_ratio, 2),
                    'y': round(current_momentum, 2)
                }
        
        # Store the snapshot for the current date
        # Format date as YYYY-MM-DD for JSON key
        date_str = current_date.strftime('%Y-%m-%d')
        rrg_snapshots[date_str] = daily_data

    return rrg_snapshots

def main():
    """Main function to run the script."""
    print("Loading and cleaning data...")
    price_df = load_and_clean_data(CSV_URL)
    
    print("Calculating RRG snapshots for all dates (this may take a moment)...")
    rrg_data = calculate_rrg(price_df)
    
    # Prepare final JSON object
    final_output = {
        "assets": [
            {"symbol": symbol, "category": CATEGORY_MAP.get(symbol, "Other")} 
            for symbol in price_df.columns if CATEGORY_MAP.get(symbol, "Other") != "Other"
        ],
        "rrg_history": rrg_data
    }

    print(f"Saving data to {OUTPUT_JSON_PATH}...")
    with open(OUTPUT_JSON_PATH, 'w') as f:
        json.dump(final_output, f)
        
    print("Done!")

if __name__ == "__main__":
    main()
