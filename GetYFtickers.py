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

def insertTicker(tickers, symbol, cols):
    info = tickers.tickers[symbol].info
    if info and 'symbol' in info:
        filt_info = {key: info[key] for key in cols if key in info}
        filt_df = pd.DataFrame(filt_info, index=[0])
        if 'exDividendDate' in filt_df.columns:
            filt_df['exDividendDate'] = pd.to_datetime(filt_df['exDividendDate'], unit='s').dt.date
        if 'dateShortInterest' in filt_df.columns:
            filt_df['dateShortInterest'] = pd.to_datetime(filt_df['dateShortInterest'], unit='s').dt.date
        if 'lastFiscalYearEnd' in filt_df.columns:
            filt_df['lastFiscalYearEnd'] = pd.to_datetime(filt_df['lastFiscalYearEnd'], unit='s').dt.date
        if 'nextFiscalYearEnd' in filt_df.columns:
            filt_df['nextFiscalYearEnd'] = pd.to_datetime(filt_df['nextFiscalYearEnd'], unit='s').dt.date
        if 'mostRecentQuarter' in filt_df.columns:
            filt_df['mostRecentQuarter'] = pd.to_datetime(filt_df['mostRecentQuarter'], unit='s').dt.date
        if 'lastSplitDate' in filt_df.columns:
            filt_df['lastSplitDate'] = pd.to_datetime(filt_df['lastSplitDate'], unit='s').dt.date
        if 'lastDividendDate' in filt_df.columns:
            filt_df['lastDividendDate'] = pd.to_datetime(filt_df['lastDividendDate'], unit='s').dt.date
        filt_df.replace([np.inf, -np.inf], None, inplace=True)
        filt_df.replace(["Infinity", "-Infinity"], None, inplace=True)
        filt_df.replace(["NaN"], None, inplace=True)
        ins_cols = '`,`'.join([str(i) for i in filt_df.columns.tolist()])
        for i,row in filt_df.iterrows():
            dup_key_str = ""
            for col in ins_cols:
                if col != 'symbol':
                    dup_key_str += f"{ins_cols} = VALUES({ins_cols}), "
            sql = f"INSERT INTO tradefx.`tickers2` (`" +ins_cols + "`) VALUES (" + "%s,"*(len(row)-1) + "%s) ON DUPLICATE KEY UPDATE "
            sql += dup_key_str
            cursor.execute(sql, tuple(row)) 
            # the connection is not autocommitted by default, so we must commit to save our # changes 
            cnxn.commit()

symbols = getSymbols()
tickers = getTickers(symbols)
for sym in symbols['symbol']:
    print(sym)
    try:
        testTicker(tickers, sym)
    except:
        print(f"Failed: {sym}")


with open('ValidTickers.txt') as f:
    lines = [line.rstrip() for line in f]
f.close()

tickers = getTickers(lines)

info_cols = []
for sym in lines:
    print(sym)
    cols = list(tickers.tickers[sym].info.keys())
    for col in cols:
        if col not in info_cols:
            info_cols.append(col)

remove_cols = ['address1','city','state','zip','country','phone','fax','website','longBusinessSummary','companyOfficers','governanceEpochDate','compensationAsOfEpochDate','irWebsite','maxAge','priceHint','firstTradeDateEpochUtc','timeZoneFullName','timeZoneShortName','messageBoardId','gmtOffSetMilliseconds','address2','industrySymbol','fundInceptionDate','address3','morningStarOverallRating','morningStarRiskRating']
for r in remove_cols:
    print(r)
    info_cols.remove(r)

col_types = {}
for sym in lines:
    print(sym)
    info = tickers.tickers[sym].info
    cols = list(info.keys())
    for col in cols:
        if col in info_cols and col not in col_types.keys():
            col_type = type(info[col])
            if col_type is int:
                col_types[col] = 'INT'
            elif col_type is float:
                col_types[col] = 'FLOAT'
            elif col_type is str:
                col_types[col] = 'VARCHAR(100)'

col_types['exDividendDate'] = 'DATETIME'
col_types['dateShortInterest'] = 'DATETIME'
col_types['lastFiscalYearEnd'] = 'DATETIME'
col_types['nextFiscalYearEnd'] = 'DATETIME'
col_types['mostRecentQuarter'] = 'DATETIME'
col_types['lastSplitDate'] = 'DATETIME'
col_types['lastDividendDate'] = 'DATETIME'

# for col in col_types:
#   query = f"ALTER TABLE tradefx.tickers2 ADD COLUMN IF NOT EXISTS {col} {col_types[col]} "
#   cursor.execute(query)
#   cnxn.commit()

query = "SELECT * FROM tradefx.tickers2 LIMIT 1"
col_df = pd.read_sql(query, cnxn)
cols = list(col_df.columns)

# cols = list(col_types.keys())
sym = 'AAPL'
insertTicker(tickers, sym, cols)

failed = []
for l in lines[6450:]:
    print(l)
    insertTicker(tickers, l, cols)

# failed = []
# for t in lines:
#     print(t)
#     try:
#         info = tickers.tickers[t].info
#     except:
#         print(f"{t}: FAILED")
#         failed.append(t)

# info = tickers.tickers['AAPL'].info
# filt_info = {key: info[key] for key in cols if key in info}
# filt_df = pd.DataFrame(filt_info, index=[0])
# filt_df['exDividendDate'] = pd.to_datetime(filt_df['exDividendDate'], unit='s').dt.date
# filt_df['dateShortInterest'] = pd.to_datetime(filt_df['dateShortInterest'], unit='s').dt.date
# filt_df['lastFiscalYearEnd'] = pd.to_datetime(filt_df['lastFiscalYearEnd'], unit='s').dt.date
# filt_df['nextFiscalYearEnd'] = pd.to_datetime(filt_df['nextFiscalYearEnd'], unit='s').dt.date
# filt_df['mostRecentQuarter'] = pd.to_datetime(filt_df['mostRecentQuarter'], unit='s').dt.date
# filt_df['lastSplitDate'] = pd.to_datetime(filt_df['lastSplitDate'], unit='s').dt.date
# filt_df['lastDividendDate'] = pd.to_datetime(filt_df['lastDividendDate'], unit='s').dt.date

# for col in cols:
#     try:
#         test = filt_df.iloc[0][col]
#         if type(test) == np.int64:
#             test_str  = str(test)
#             if(len(test_str) > 10):
#                 print(col)
#     except:
#         print(f"{col} does not exist")