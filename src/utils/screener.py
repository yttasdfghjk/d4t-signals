import ccxt
import pandas as pd
from datetime import datetime, timedelta
#from pycoingecko import CoinGeckoAPI
#import telegramtools

def get_tickers():
    tickers = [
        "ETH", "BTC","SOL","ICP","POL",
        "AVAX", "DOT", "LINK"
    ]
    return tickers
    
def get_static_watchlist():    
    ## get list from gdrive and format
    tickers = get_tickers()
    pairs = [t+"/USDT" for t in tickers]
    """
    tickers = [
        "ETH/USDT", "BTC/USDT","SOL/USDT","ICP/USDT","MATIC/USDT",
        "AVAX/USDT", "DOT/USDT", "LINK/USDT"
    ]
    """
    return pairs

def get_dynamic_watchlist():
    """
    returns watchlist generated from Coingecko
    """
    tickers = []
    #tickers.extend(get_cg_trend_tickers())
    #todo: read tickers from db

    return tickers

"""
def get_cg_trending_tickers():
    cg = CoinGeckoAPI()
    trend_dict = cg.get_search_trending()
    trending_tickers = []

    for row in trend_dict['coins']:
        print(row['item']['symbol'])
        trending_tickers.append(row['item']['symbol']+"/USDT")

    return trending_tickers
"""

def get_ohlc(ticker, timeframe, limit):     # typechecks?!
    
    ex = ccxt.mexc({
        'enableRateLimit' : True
    })
    #limit = 1000
    #ex = ccxt.mexc()

    days_to_subtract_dict = {
        '1m': 2/3,
        '5m': 2,
        '15m': 9,
        '1h': 40,
        '4h': 160,
        '1d': 1000
    }
    days_to_subtract = days_to_subtract_dict.get(timeframe)
    from_date = datetime.today() - timedelta(days=days_to_subtract)
    from_ts_string = from_date.strftime("%Y:%m:%d 00:00:00")
    from_ts_parsed = ex.parse8601(from_ts_string) 
    
    attempts = 0
    while attempts < 3:
        try:
            ohlcv = ex.fetch_ohlcv(ticker, timeframe, since=from_ts_parsed, limit=limit)
            # https://pandas.pydata.org/docs/user_guide/pyarrow.html --> pyarrow pandas backend
            df = pd.DataFrame(ohlcv, columns = ['Time','Open', 'High', 'Low', 'Close', 'Volume'])
            df["Ticker"] = ticker
            df['Time'] = pd.to_datetime(df.Time, unit='ms')
            df.set_index("Time", inplace=True)
            df['datetimes'] = df.index
            break
        except Exception as e:
            attempts += 1
            print("Attempt " + str(attempts)+ " failed for "+ ticker)
            pass
        
    return df

