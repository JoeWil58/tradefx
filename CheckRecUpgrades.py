import mysql.connector
import pandas as pd
import numpy as np
import os
import yfinance as yf
import time
from datetime import datetime

cnxn = mysql.connector.connect(host=os.environ['PIDB_HOST'], user=os.environ['PIDB_USER'], password=os.environ['PIDB_USER_PASSWORD'])
cursor = cnxn.cursor()

def getSymbols():
    query = "SELECT t.symbol FROM tradefx.tickers2 t "
    df = pd.read_sql(query, cnxn)
    return df

def getTickers(symbols):
    tix = ' '.join(list(symbols.symbol))
    tickers = yf.Tickers(tix)
    return tickers

def checkUpgrades(tickers, symbol):
    upg = tickers.tickers[symbol].upgrades_downgrades
    if(len(upg)):
        current_date = pd.Timestamp('today').normalize()
        filt_upg = upg.loc[upg.index.normalize() == current_date].copy()
        if(len(filt_upg)):
            filt_upg['change'] = filt_upg.apply(lambda row: 1 if row.FromGrade != row.ToGrade else 0, axis=1)
            filt_upg = filt_upg.loc[filt_upg.change == 1]
            if(len(filt_upg)):
                print(f"!!!{symbol}!!!")
                for i in range(len(filt_upg)):
                    curr_row = filt_upg.iloc[i]
                    with open('UpgradesDowngrades.txt', 'a') as f:
                        f.write(f"{current_date.date()} {symbol}: {curr_row.FromGrade} -> {curr_row.ToGrade} ({curr_row.Firm})\n")

start = time.time()
symbols = getSymbols()
tickers = getTickers(symbols)
for sym in symbols['symbol']:
    try:
        print(sym)
        checkUpgrades(tickers, sym)
    except:
        print(f"{sym} has no upgrades/downgrades")

with open('UpgradesDowngrades.txt', 'a') as f:
    f.write("\n")
    f.write("\n")
end = time.time()
print(end - start)