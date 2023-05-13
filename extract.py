from binance import Client
import json
from datetime import datetime
import os
from dotenv import load_dotenv
# take environment variables from .env.
load_dotenv()  

API_KEY = os.getenv("API_KEY")
SECRET_KEY = os.getenv("SECRET_KEY")

interval_map = {
        '1m': Client.KLINE_INTERVAL_1MINUTE,
        '3m': Client.KLINE_INTERVAL_3MINUTE,
        '5m': Client.KLINE_INTERVAL_5MINUTE,
        '15m': Client.KLINE_INTERVAL_15MINUTE,
        '30m': Client.KLINE_INTERVAL_30MINUTE,
        '1h': Client.KLINE_INTERVAL_1HOUR,
        '2h': Client.KLINE_INTERVAL_2HOUR,
        '4h': Client.KLINE_INTERVAL_4HOUR,
        '6h': Client.KLINE_INTERVAL_6HOUR,
        '8h': Client.KLINE_INTERVAL_8HOUR,
        '12h': Client.KLINE_INTERVAL_12HOUR,
        '1d': Client.KLINE_INTERVAL_1DAY,
        '3d': Client.KLINE_INTERVAL_3DAY,
        '1w': Client.KLINE_INTERVAL_1WEEK,
        '1M': Client.KLINE_INTERVAL_1MONTH
    }

#GET API call data
def get_klines(symbol,interval_map):
    client = Client(API_KEY,SECRET_KEY,tld='us')
    klines = client.get_historical_klines(symbol,interval_map,"1 May, 2023",datetime.today().strftime('%Y-%m-%d %H:%M:%S'))
    return klines 
get_klines("BTCUSD",'12h')

#Convert data to json file.
def save_klines(symbol,interval_map):
    save_file = open(f"{symbol}_{interval_map}.json", "w")  
    json.dump(get_klines(symbol, interval_map), save_file, indent = 6)  
    save_file.close()
save_klines("BTCUSD", "12h")