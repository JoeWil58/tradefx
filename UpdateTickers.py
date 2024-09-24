import pandas as pd
import numpy as np
import yfinance as yf
import time
import mysql.connector
from datetime import datetime
import os

cnxn = mysql.connector.connect(host=os.environ['PIDB_HOST'], user=os.environ['PIDB_USER'], password=os.environ['PIDB_USER_PASSWORD'])
cursor = cnxn.cursor()

def getSymbols():
    query = "SELECT t.symbol FROM tradefx.tickers t "
    df = pd.read_sql(query, cnxn)
    return df

def getTickers(symbols):
    # tix = ' '.join(list(symbols.symbol))
    tix = ' '.join(symbols)
    tickers = yf.Tickers(tix)
    return tickers

def testTicker(tickers, symbol):
    info = tickers.tickers[symbol].info
    if(len(info) > 1):
        with open('ValidTickers.txt', 'a') as f:
            f.write(f"{symbol}\n")

def convert_dates(df, date_columns):
    for col in date_columns:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], unit='s').dt.date
    return df

def insertTicker(tickers, symbol, cols):
    info = tickers.tickers[symbol].info
    if info and 'symbol' in info:
        filt_info = {key: info[key] for key in cols if key in info}
        filt_df = pd.DataFrame.from_dict([filt_info])
        
        date_columns = ['exDividendDate', 'dateShortInterest', 'lastFiscalYearEnd', 
                        'nextFiscalYearEnd', 'mostRecentQuarter', 'lastSplitDate', 'lastDividendDate']
        filt_df = convert_dates(filt_df, date_columns)
        
        filt_df.replace([np.inf, -np.inf, "Infinity", "-Infinity", "NaN"], None, inplace=True)
        
        ins_cols = '`,`'.join(filt_df.columns)
        dup_key_str = ', '.join([f"{col} = VALUES({col})" for col in filt_df.columns if col != 'symbol'])
        
        for _, row in filt_df.iterrows():
            sql = f"INSERT INTO tradefx.`tickers2` (`{ins_cols}`) VALUES ({', '.join(['%s'] * len(row))}) ON DUPLICATE KEY UPDATE {dup_key_str}"
            cursor.execute(sql, tuple(row))
            cnxn.commit()

if __name__ == '__main__':
    current_file_path = os.path.abspath(__file__)
    current_file_path = os.path.dirname(current_file_path)
    with open(f'{current_file_path}/ValidTickers.txt') as f:
        lines = [line.rstrip() for line in f]
    f.close()

    tickers = getTickers(lines)

    query = "SELECT * FROM tradefx.tickers2 LIMIT 1"
    col_df = pd.read_sql(query, cnxn)
    cols = list(col_df.columns)

    failed = []
    for l in lines:
        try:
            print(l)
            insertTicker(tickers, l, cols)
        except:
            print(f"Failed: {l}")
            failed.append(l)