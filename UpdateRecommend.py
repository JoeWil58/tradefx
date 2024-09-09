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

def getRecs(symbol, tickers):
    recs = tickers.tickers[symbol].recommendations
    recs['symbol'] = symbol
    recs['period'] = recs['period'].str.replace('-', '', regex=False)
    recs.columns = ['period', 'strong_buy', 'buy', 'hold', 'sell', 'strong_sell', 'symbol']
    return recs

def getOldRecs():
    query = f"SELECT * FROM tradefx.recommendations r "
    df = pd.read_sql(query, cnxn)
    return df

def checkRecChange(symbol, new_recs, old_recs):
    temp_new_recs = new_recs.copy()
    temp_old_recs = old_recs.copy()
    temp_new_recs['tot'] = temp_new_recs.strong_buy + temp_new_recs.buy + temp_new_recs.hold + temp_new_recs.sell + temp_new_recs.strong_sell
    temp_old_recs['tot'] = temp_old_recs.strong_buy + temp_old_recs.buy + temp_old_recs.hold + temp_old_recs.sell + temp_old_recs.strong_sell

    temp_new_recs['strong_buy_pct'] = temp_new_recs.strong_buy / temp_new_recs.tot
    temp_new_recs['buy_pct'] = temp_new_recs.buy / temp_new_recs.tot
    temp_new_recs['hold_pct'] = temp_new_recs.hold / temp_new_recs.tot
    temp_new_recs['sell_pct'] = temp_new_recs.sell / temp_new_recs.tot
    temp_new_recs['strong_sell_pct'] = temp_new_recs.strong_sell / temp_new_recs.tot

    temp_old_recs['strong_buy_pct'] = temp_old_recs.strong_buy / temp_old_recs.tot
    temp_old_recs['buy_pct'] = temp_old_recs.buy / temp_old_recs.tot
    temp_old_recs['hold_pct'] = temp_old_recs.hold / temp_old_recs.tot
    temp_old_recs['sell_pct'] = temp_old_recs.sell / temp_old_recs.tot
    temp_old_recs['strong_sell_pct'] = temp_old_recs.strong_sell / temp_old_recs.tot

    temp_new_recs_now = temp_new_recs.loc[temp_new_recs.period == '0m']
    temp_old_recs_now = temp_old_recs.loc[temp_old_recs.period == '0m']

    if(temp_new_recs_now.tot.iloc[0] > 0 and temp_old_recs_now.tot.iloc[0] > 0):
        temp_new_recs_max = temp_new_recs_now[['strong_buy_pct', 'buy_pct', 'hold_pct', 'sell_pct', 'strong_sell_pct']].iloc[0].idxmax()
        temp_old_recs_max = temp_old_recs_now[['strong_buy_pct', 'buy_pct', 'hold_pct', 'sell_pct', 'strong_sell_pct']].iloc[0].idxmax()

        if(temp_new_recs_max != temp_old_recs_max):
            with open('RecChanges.txt', 'a') as f:
                f.write(f"{symbol}: {temp_old_recs_max} -> {temp_new_recs_max}\n")

def updateRecs(symbol, new_recs, old_recs):
    try:
        checkRecChange(symbol, new_recs, old_recs)
    except:
        failed_rec_changes.append(symbol)
    ins_cols = '`,`'.join([str(i) for i in new_recs.columns.tolist()])
    for i,row in new_recs.iterrows():
        if(len(old_recs.loc[old_recs.period == row.period])):
            sql = f"UPDATE tradefx.recommendations SET strong_buy = {row.strong_buy}, buy = {row.buy}, hold = {row.hold}, sell = {row.sell}, strong_sell = {row.strong_sell} WHERE symbol = '{row.symbol}' AND period = '{row.period}' "
            cursor.execute(sql)
            cnxn.commit()
        else:
            sql = f"INSERT INTO tradefx.`recommendations` (`" +ins_cols + "`) VALUES (" + "%s,"*(len(row)-1) + "%s)"
            cursor.execute(sql, tuple(row)) 
            # the connection is not autocommitted by default, so we must commit to save our # changes 
            cnxn.commit()

start = time.time()
failed = []
failed_rec_changes = []
symbols = getSymbols()
tickers = getTickers(symbols)
old_recs_all = getOldRecs()
for sym in symbols['symbol']:
    try: 
        print(sym)
        new_recs = getRecs(sym, tickers)
        old_recs = old_recs_all.loc[old_recs_all.symbol == sym].copy()
        updateRecs(sym, new_recs, old_recs)
    except:
        failed.append(sym)
end = time.time()
print(end - start)