import pandas as pd
import numpy as np
import yfinance as yf
import time
import mysql.connector
from datetime import datetime
import os

def getTickers(symbols):
    # tix = ' '.join(list(symbols.symbol))
    tix = ' '.join(symbols)
    tickers = yf.Tickers(tix)
    return tickers

def testTicker(tickers, symbol, path):
    info = tickers.tickers[symbol].info
    if(len(info) > 1):
        with open(f'{path}/ValidTickers.txt', 'a') as f:
            f.write(f"{symbol}\n")

if __name__ == '__main__':
    current_file_path = os.path.abspath(__file__)
    current_file_path = os.path.dirname(current_file_path)

    nyse = pd.read_csv(f'{current_file_path}/NYSE_and_NYSE_MKT_Trading_Units_Daily_File.csv')
    nyse.columns = ['Company', 'symbol', 'TU/TXN', 'Auction', 'Tape']
    nyse['symbol'] = nyse['symbol'].replace(pd.NA, 'NAN')

    symbols = nyse.symbol
    tickers = getTickers(symbols)
    for sym in symbols:
        print(sym)
        try:
            testTicker(tickers, sym, current_file_path)
        except:
            print(f"Failed: {sym}")