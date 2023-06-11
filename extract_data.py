import csv
from binance import Client
from datetime import datetime
import os
from dotenv import load_dotenv

#Get configuration variables from .env implementation
load_dotenv()

#API keys
API_KEY = os.getenv("API_KEY")
SECRET_KEY = os.getenv("SECRET_KEY")

#Time interval 
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

#Get Kline data for a specific symbol and time frame
def get_klines(symbol, interval):
    client = Client(API_KEY, SECRET_KEY, tld='us')
    klines = client.get_historical_klines(
        symbol,
        interval,
        "28 May, 2023",
        datetime.today().strftime('%Y-%m-%d %H:%M:%S')
    )
    return klines

#Save Kline data to CSV file
def save_klines_to_csv(symbol, interval):
    klines = get_klines(symbol, interval)
    filename = f"{symbol}_{interval}.csv"
    with open(filename, "w", newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['open_time', 'open', 'high', 'low', 'close', 'volume', 'close_time', 'quote_asset_volume',
                         'num_trades', 'taker_base_vol', 'taker_quote_vol', 'ignore'])
        writer.writerows(klines)

#Save 12 hours of Kline data for BTCUSD to CSV file
save_klines_to_csv("BTCUSD", interval_map['1h'])
