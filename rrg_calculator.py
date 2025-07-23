import pandas as pd
import numpy as np
import json
from datetime import datetime
import os

# --- CONFIGURATION ---
CSV_URL = 'https://raw.githubusercontent.com/wayfarer-2018/Kraken_data/main/kraken_usd_pairs_close_history.csv'
OUTPUT_JSON_PATH = 'rrg_data.json'
RRG_LOOKBACK = 10
HOOK_LOOKBACK_PERIOD = 12
MIN_HISTORY_REQUIRED = RRG_LOOKBACK * 2 

# --- ALIASES AND CATEGORIES ---
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
    
    df['symbol'] = df['pair'].str.replace(r'ZUSD|USD|USDT|BUSD|BTC$', '', regex=True).str.upper()
    df['symbol'] = df['symbol'].replace(TICKER_ALIAS_MAP)
    
    price_df = df.pivot_table(index='date', columns='symbol', values='close', aggfunc='first')
    price_df.ffill(inplace=True)
    
    return price_df

def get_signal(point, is_hook=False):
    """Determines the signal based on RRG coordinates."""
    if is_hook:
        return 'HOOK'
    if point['x'] > 100 and point['y'] > 100:
        return 'HOLD'
    if point['x'] < 100 and point['y'] > 100:
        return 'BUY'
    return 'SELL'

def calculate_rrg_and_signals(price_df, start_date=None):
    """Calculates RRG data and signals for the entire history using the Dynamic Universe approach."""
    all_dates = price_df.index
    
    rrg_df = pd.DataFrame(index=all_dates, columns=pd.MultiIndex.from_product([price_df.columns, ['x', 'y']]))

    start_index = MIN_HISTORY_REQUIRED
    if start_date:
        try:
            start_index = all_dates.get_loc(start_date) + 1
        except KeyError:
            start_index = MIN_HISTORY_REQUIRED

    print(f"Starting RRG calculation from index {start_index}...")

    for i in range(start_index, len(all_dates)):
        current_date = all_dates[i]
        history_start_date = all_dates[i - MIN_HISTORY_REQUIRED]
        
        historical_window = price_df.loc[history_start_date:current_date]
        valid_assets = historical_window.dropna(axis=1).columns
        
        if len(valid_assets) < 2: continue

        benchmark = historical_window[valid_assets].mean(axis=1)
        
        for asset in valid_assets:
            rs = historical_window[asset] / benchmark
            rs_ratio_series = rs.rolling(window=RRG_LOOKBACK).mean()
            
            mean_rs_ratio = rs_ratio_series.mean()
            std_rs_ratio = rs_ratio_series.std()
            if std_rs_ratio == 0: continue
            
            normalized_rs_ratio = 100 + ((rs_ratio_series - mean_rs_ratio) / std_rs_ratio) * 10
            rs_momentum_series = (normalized_rs_ratio / normalized_rs_ratio.shift(RRG_LOOKBACK) - 1) * 100 + 100
            
            current_ratio = normalized_rs_ratio.iloc[-1]
            current_momentum = rs_momentum_series.iloc[-1]

            if not np.isnan(current_ratio) and not np.isnan(current_momentum):
                rrg_df.loc[current_date, (asset, 'x')] = current_ratio
                rrg_df.loc[current_date, (asset, 'y')] = current_momentum

    signals_df = pd.DataFrame(index=rrg_df.index, columns=price_df.columns)
    for asset in rrg_df.columns.get_level_values(0).unique():
        asset_df = rrg_df[asset].dropna()
        if asset_df.empty: continue

        # Hook Signal Calculation
        is_hook = pd.Series(False, index=asset_df.index)
        if len(asset_df) > HOOK_LOOKBACK_PERIOD:
            for j in range(HOOK_LOOKBACK_PERIOD, len(asset_df)):
                current_point = asset_df.iloc[j]
                lookback_point = asset_df.iloc[j - HOOK_LOOKBACK_PERIOD]
                history_slice = asset_df.iloc[j - HOOK_LOOKBACK_PERIOD : j]

                cond1 = lookback_point['x'] > 100 and lookback_point['y'] > 100
                cond3 = current_point['x'] > 100 and current_point['y'] > 100
                if not (cond1 and cond3): continue

                cond2 = (history_slice['y'] < 100).any()
                if not cond2: continue
                
                cond45 = current_point['y'] > lookback_point['y'] and current_point['x'] > lookback_point['x']
                if cond45:
                    is_hook.iloc[j] = True

        for date, point in asset_df.iterrows():
            signals_df.loc[date, asset] = get_signal(point.to_dict(), is_hook=is_hook.get(date, False))

    return rrg_df, signals_df

def calculate_market_breadth(signals_df):
    """Calculates the Net Breadth Oscillator and its EMAs."""
    buy_signals = (signals_df == 'BUY').sum(axis=1)
    sell_signals = (signals_df == 'SELL').sum(axis=1)
    net_breadth = buy_signals - sell_signals
    
    breadth_df = pd.DataFrame(index=signals_df.index)
    breadth_df['net'] = net_breadth
    breadth_df['fast_ema'] = net_breadth.ewm(span=9, adjust=False).mean()
    breadth_df['slow_ema'] = net_breadth.ewm(span=21, adjust=False).mean()
    
    return breadth_df

def main():
    """Main function to run the script."""
    print("Loading and cleaning data...")
    price_df = load_and_clean_data(CSV_URL)
    
    existing_data = {}
    last_processed_date = None
    
    if os.path.exists(OUTPUT_JSON_PATH):
        print(f"Found existing data file: {OUTPUT_JSON_PATH}")
        with open(OUTPUT_JSON_PATH, 'r') as f:
            existing_data = json.load(f)
        if existing_data.get("rrg_history"):
            last_processed_date = sorted(existing_data["rrg_history"].keys())[-1]
            print(f"Last processed date: {last_processed_date}")

    print("Calculating RRG and signals...")
    rrg_df, signals_df = calculate_rrg_and_signals(price_df, start_date=last_processed_date)

    print("Calculating market breadth...")
    breadth_df = calculate_market_breadth(signals_df)

    new_snapshots = {}
    for date, row in rrg_df.iterrows():
        date_str = date.strftime('%Y-%m-%d')
        if last_processed_date and date_str <= last_processed_date:
            continue
        
        daily_data = {}
        for asset in row.index.get_level_values(0).unique():
            if not pd.isna(row[(asset, 'x')]):
                daily_data[asset] = {
                    'x': round(row[(asset, 'x')], 2),
                    'y': round(row[(asset, 'y')], 2),
                    'signal': signals_df.loc[date, asset]
                }
        if daily_data:
            new_snapshots[date_str] = daily_data

    breadth_history = {
        date.strftime('%Y-%m-%d'): {
            'fast_ema': round(row['fast_ema'], 2),
            'slow_ema': round(row['slow_ema'], 2)
        } for date, row in breadth_df.dropna().iterrows()
    }

    if not new_snapshots:
        print("No new data to process. Exiting.")
        return

    if not existing_data:
         final_output = {
            "assets": [
                {"symbol": symbol, "category": CATEGORY_MAP.get(symbol, "Other")} 
                for symbol in price_df.columns if CATEGORY_MAP.get(symbol, "Other") != "Other"
            ],
            "rrg_history": new_snapshots,
            "market_breadth": breadth_history
        }
    else:
        existing_data["rrg_history"].update(new_snapshots)
        existing_data["market_breadth"] = breadth_history
        final_output = existing_data

    print(f"Saving updated data to {OUTPUT_JSON_PATH}...")
    with open(OUTPUT_JSON_PATH, 'w') as f:
        json.dump(final_output, f)
        
    print("Done!")

if __name__ == "__main__":
    main()

