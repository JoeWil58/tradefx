import pandas as pd
import numpy as np
import requests
import mysql.connector
import time
from datetime import datetime
import matplotlib.pyplot as plt
import yfinance as yf
import re
import os

cnxn = mysql.connector.connect(host=os.environ['PIDB_HOST'], user=os.environ['PIDB_USER'], password=os.environ['PIDB_USER_PASSWORD'])
cursor = cnxn.cursor()

def getSymbols():
    # query = "SELECT t.symbol FROM tradefx.tickers t LEFT OUTER JOIN tradefx.fundamentals f ON f.symbol = t.symbol "
    # query += "WHERE t.symbol NOT REGEXP '[0-9,+,-,.]+' AND f.marketCap >= 0 "
    query = "SELECT t.symbol FROM tradefx.tickers2 t "
    df = pd.read_sql(query, cnxn)
    return df

def getTickers(symbols):
    tix = ' '.join(list(symbols.symbol))
    tickers = yf.Tickers(tix)
    return tickers

def getExistingReports(report_table):
    query = f"SELECT * FROM tradefx.{report_table}"
    df = pd.read_sql(query, cnxn)
    cols = df.columns
    return df[['symbol', 'report_date']], cols

def getAPIData(symbol, tickers, report_type):
    if(report_type == 'annual_cashflow'):
        return tickers.tickers[symbol].cashflow
    elif(report_type == 'quarterly_cashflow'):
        return tickers.tickers[symbol].quarterly_cashflow
    elif(report_type == 'annual_income'):
        return tickers.tickers[symbol].income_stmt
    elif(report_type == 'quarterly_income'):
        return tickers.tickers[symbol].quarterly_income_stmt
    elif(report_type == 'annual_balance'):
        return tickers.tickers[symbol].balance_sheet
    elif(report_type == 'quarterly_balance'):
        return tickers.tickers[symbol].quarterly_balance_sheet

def checkNewDate(api_data, db_data):
    api_dates = api_data.columns
    db_dates = db_data.report_date
    date_check = api_dates.isin(db_dates.tolist())
    if not all(date_check):
        new_date_idx = np.where(date_check == False)[0]
        return api_data[api_dates[new_date_idx]]
    else:
        return []
    
def pivotData(df, db_cols, symbol):
    df_T = df.T.reset_index()
    df_T.columns = ['_'.join(c.lower().split(' ')) for c in df_T.columns]
    df_T = df_T.rename(columns={"index": "report_date"})
    rm_cols = [c for c in df_T.columns if c not in db_cols]
    df_T = df_T.drop(rm_cols, axis=1)
    df_T['symbol'] = symbol
    df_T = df_T.replace({np.nan: None})
    return df_T

def insertNewReports(new_reports, report_type):
    ins_cols = '`,`'.join([str(i) for i in new_reports.columns.tolist()])
    for i,row in new_reports.iterrows():
        sql = f"INSERT INTO tradefx.`{report_type}` (`" +ins_cols + "`) VALUES (" + "%s,"*(len(row)-1) + "%s)"
        cursor.execute(sql, tuple(row)) 
        # the connection is not autocommitted by default, so we must commit to save our # changes 
        cnxn.commit()

if __name__=="__main__":
    failed = []
    symbols = getSymbols()
    tickers = getTickers(symbols)
    report_types = ['annual_cashflow', 'quarterly_cashflow', 'annual_balance', 'quarterly_balance', 'annual_income', 'quarterly_income']
    # report_types = ['quarterly_cashflow']
    for rtype in report_types:
        reports, cols = getExistingReports(rtype)
        for sym in symbols['symbol']:
            print(f"{sym} | {rtype}")
            try:
                api_data = getAPIData(sym, tickers, rtype)
                new_reports = checkNewDate(api_data, reports.loc[reports.symbol == sym])
                if len(new_reports):
                    print(sym, rtype)
                    push_data = pivotData(new_reports, cols, sym)
                    insertNewReports(push_data, rtype)
            except:
                failed.append((sym, rtype))