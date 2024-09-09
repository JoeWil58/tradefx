import pandas as pd
import numpy as np
import requests
import mysql.connector
import time
from datetime import datetime
import matplotlib.pyplot as plt

cnxn = mysql.connector.connect(host="localhost", user="root", password="")
cursor = cnxn.cursor()

key = ""

def getPriceHistory(api_key, ticker, period_type, period, freq_type, freq):
    req_string = f"https://api.tdameritrade.com/v1/marketdata/{ticker}/pricehistory?apikey={api_key}&periodType={period_type}&period={period}&frequencyType={freq_type}&frequency={freq}"
    prices = requests.get(req_string).json()
    return pd.DataFrame(prices["candles"])

letters = ["A","B","C","D","E","F","G","H","I","J","K","L","M","N","O","P","Q","R","S","T","U","V","W","X","Y","Z"]
def getTickers(letter):
    url = f"https://api.tdameritrade.com/v1/instruments?apikey={key}&symbol={letter}.*&projection=symbol-regex"
    tick_resposne = requests.get(url).json()
    tickers = pd.DataFrame(tick_resposne.values())
    return tickers.loc[(tickers.exchange.isin(['NASDAQ', 'NYSE'])) & (tickers.assetType == 'EQUITY')]

def getFundamentals(ticker, idx):
    url = f"https://api.tdameritrade.com/v1/instruments?apikey={key}&symbol={ticker}&projection=fundamental"
    response = requests.get(url).json()
    data = response[ticker]['fundamental']
    return pd.DataFrame(data, index=[idx])


assets = pd.DataFrame()
for letter in letters:
    currTicks = getTickers(letter)
    assets = pd.concat([assets, currTicks])
assets = assets.dropna()

assets.columns = ['cusip', 'symbol', 'description', 'exchange', 'asset_type']
assets = assets[['symbol', 'cusip', 'description', 'exchange', 'asset_type']]

for i in range(len(assets)):
    row = assets.iloc[i]
    push_str = "INSERT INTO tradefx.tickers (`symbol`, `cusip`, `description`, `exchange`, `asset_type`) VALUES ( %s, %s, %s, %s, %s )"
    cursor.execute(push_str, [row.symbol, row.cusip, row.description, row.exchange, row.asset_type])
    cnxn.commit()

start = time.time()
# fundamentals = pd.DataFrame()
failed = []

df = getFundamentals('AAPL', 0)
cols = list(df.columns)
cols.append('modifiedDate')
params = '%s, ' * len(cols)
params = params[:-2]
push_str = "INSERT INTO tradefx.fundamentals ("
for col in cols:
    push_str += f'`{col}`, '
push_str = push_str[:-2]
push_str += f") VALUES ({params}) "

# for i,symbol in enumerate(list(assets['symbol'])):
for i,symbol in enumerate(retry):
    if i != 0 and i % 115 == 0:
        time.sleep(60)
    try:
        df = getFundamentals(symbol, i)
        divDate = df.iloc[0].dividendDate
        divPayDate = df.iloc[0].dividendPayDate
        if(divDate == ' '):
            df['dividendDate'] = None
        if(divPayDate == ' '):
            df['dividendPayDate'] = None
        # fundamentals = pd.concat([fundamentals, df])
        vals = [df.iloc[0][c] for c in cols[:-1]]
        vals.append(datetime.now())
        cursor.execute(push_str, vals)
        cnxn.commit()
        # df.to_csv('/Users/joewilson/Documents/Finances/fundamentals.csv', mode='a', header=False)
    except:
        failed.append(symbol)
end = time.time()
print(end - start)
retry = failed


##########################################
# Testing
##########################################

query = ""
query += "SELECT f.* FROM tradefx.fundamentals f  "
query += "ORDER BY f.marketCap DESC LIMIT 1904 "
df = pd.read_sql(query, cnxn)

datacols = list(df.select_dtypes(include=['float64']).columns)
plotcols = [
    datacols[0:9],
    datacols[9:18],
    datacols[18:27],
    datacols[27:36],
    datacols[36:]
]

for cols in plotcols:
    currList = [
        cols[0:3],
        cols[3:6],
        cols[6:]
    ]
    fig,ax = plt.subplots(nrows=3, ncols=3, figsize=(10,6))
    for i in range(len(currList)):
        for j in range(len(currList[i])):
            mean = np.mean(df[currList[i][j]])
            std = np.mean(df[currList[i][j]])
            plotdf = df.loc[(df[currList[i][j]] >= mean - (2.5*std)) & (df[currList[i][j]] <= mean + (2.5*std))]
            ax[i][j].hist(plotdf[currList[i][j]], bins=20)
            ax[i][j].set_title(currList[i][j])
    fig.tight_layout()
    plt.show()



import yfinance as yf
nvda = yf.Ticker('NVDA')
nvda.info.keys()
nvda.history(period='1mo')

nvda.get_shares_full(start="2022-01-01", end=None)
qcf = nvda.quarterly_cashflow
acf = nvda.cashflow

qfcf = qcf.loc['Free Cash Flow'].iloc[::-1]
afcf = acf.loc['Free Cash Flow'].iloc[::-1]
qocf = qcf.loc['Operating Cash Flow'].iloc[::-1]
qicf = qcf.loc['Investing Cash Flow'].iloc[::-1]
qficf = qcf.loc['Financing Cash Flow'].iloc[::-1]
qcapex = qcf.loc['Capital Expenditure'].iloc[::-1]
qncf = qcf.loc['Changes In Cash'].iloc[::-1]
ancf = acf.loc['Changes In Cash'].iloc[::-1]

fig,ax = plt.subplots(figsize=(10,6))
# ax.plot(qocf.index, qocf, label='Operating Cash Flow')
# ax.plot(qicf.index, qicf, label='Investing Cash Flow')
# ax.plot(qficf.index, qficf, label='Financing Cash Flow')
ax.plot(qcapex.index, qcapex, label='Cap Ex')
ax.legend()
plt.show()

tickers = yf.Tickers('msft aapl goog')

query = "SELECT t.symbol FROM tradefx.tickers t LEFT OUTER JOIN tradefx.fundamentals f ON f.symbol = t.symbol "
query += "WHERE t.symbol NOT REGEXP '[0-9,+,-,.]+' AND f.marketCap < 10000 AND f.marketCap >= 5000 "
query += "AND t.symbol  NOT IN (SELECT ac.symbol FROM tradefx.annual_cashflow ac GROUP BY ac.symbol) "
df = pd.read_sql(query, cnxn)

tix = ' '.join(list(df.symbol))
tickers = yf.Tickers(tix)

symbols = list(tickers.tickers.keys())

annual_cols = []
quarterly_cols = []
for sym in symbols:
    qcf = list(tickers.tickers[sym].quarterly_cashflow.index)
    for col in qcf:
        if col not in quarterly_cols:
            quarterly_cols.append(col)

    acf = list(tickers.tickers[sym].cashflow.index)
    for col in acf:
        if col not in annual_cols:
            annual_cols.append(col)

annual_db_cols = ['_'.join(a.lower().split(' ')) for a in annual_cols]
annual_db_cols.insert(0, 'report_date')
annual_db_cols.insert(0, 'symbol')
quarterly_db_cols = ['_'.join(q.lower().split(' ')) for q in quarterly_cols]
quarterly_db_cols.insert(0, 'report_date')
quarterly_db_cols.insert(0, 'symbol')

annual_df = pd.DataFrame(columns=annual_db_cols)
quarterly_df = pd.DataFrame(columns=quarterly_db_cols)

annual_df.to_csv('/Users/joewilson/Documents/Finances/annual_cf.csv')
quarterly_df.to_csv('/Users/joewilson/Documents/Finances/quarterly_cf.csv')

query = 'SELECT * FROM tradefx.annual_cashflow a limit 1'
adf = pd.read_sql(query, cnxn)
acols = list(adf.columns)

query = 'SELECT * FROM tradefx.quarterly_cashflow a limit 1'
qdf = pd.read_sql(query, cnxn)
qcols = list(qdf.columns)

query = "USE tradefx"
cursor.execute(query)
cnxn.commit()

for sym in symbols:
    #print(sym)
    try:
        acf = tickers.tickers[sym].cashflow
        acf_T = acf.T.reset_index()
        cols = list(acf_T.columns)
        cols[0] = 'report_date'
        cols = ['_'.join(c.lower().split(' ')) for c in cols]
        cols = [c for c in cols if c in acols]
        acf_T.columns = cols
        acf_T['symbol'] = sym
        # acf_T.to_sql(con=cnxn, name='annual_cashflow')
        ins_cols = '`,`'.join([str(i) for i in acf_T.columns.tolist()])
        acf_T = acf_T.replace({np.nan: None})
        # Insert DataFrame records one by one. 
        for i,row in acf_T.iterrows():
            sql = "INSERT INTO `annual_cashflow` (`" +ins_cols + "`) VALUES (" + "%s,"*(len(row)-1) + "%s)"
            cursor.execute(sql, tuple(row)) 
            # the connection is not autocommitted by default, so we must commit to save our # changes 
            cnxn.commit()
    except:
        print(sym)

for sym in symbols:
    qcf = tickers.tickers[sym].quarterly_cashflow
    qcf_T = qcf.T.reset_index()
    cols = list(qcf_T.columns)
    cols[0] = 'report_date'
    cols = ['_'.join(c.lower().split(' ')) for c in cols]
    cols = [c for c in cols if c in qcols]
    qcf_T.columns = cols
    qcf_T['symbol'] = sym
    ins_cols = '`,`'.join([str(i) for i in qcf_T.columns.tolist()])
    qcf_T = qcf_T.replace({np.nan: None})
    # Insert DataFrame records one by one. 
    for i,row in qcf_T.iterrows():
        sql = "INSERT INTO `quarterly_cashflow` (`" +ins_cols + "`) VALUES (" + "%s,"*(len(row)-1) + "%s)"
        cursor.execute(sql, tuple(row)) 
        # the connection is not autocommitted by default, so we must commit to save our # changes 
        cnxn.commit()


# for col in quarterly_db_cols[3:]:
#     query = "ALTER TABLE tradefx.quarterly_cashflow "
#     query += f"ADD COLUMN {col} FLOAT "
#     cursor.execute(query)
#     cnxn.commit()

# for col in annual_db_cols[2:]:
#     query = "ALTER TABLE tradefx.annual_cashflow "
#     query += f"ADD COLUMN {col} FLOAT "
#     cursor.execute(query)
#     cnxn.commit()



nvda.recommendations
nvda.recommendations_summary
test = nvda.upgrades_downgrades.reset_index()
test['year'] = test['GradeDate'].dt.year
test['month'] = test['GradeDate'].dt.month
test['day'] = test['GradeDate'].dt.day

test.loc[(test.year == 2024) & (test.month == 5)]

data = yf.download("SPY AAPL MSFT NVDA", period="2y")
offset = data.iloc[0].Close

# data.loc[:, (['Adj Close','Close'],['AAPL', 'MSFT'])]

idxs = np.arange(0, len(data), 50)
idx_labels = [d.strftime("%Y-%m-%d") for d in data.iloc[idxs].index]

fig,ax = plt.subplots(figsize=(10,6))
ax.plot(range(len(data)), data['Close']['SPY'] - offset['SPY'], label='SPY')
ax.plot(range(len(data)), data['Close']['AAPL'] - offset['AAPL'], label='AAPL')
ax.plot(range(len(data)), data['Close']['MSFT'] - offset['MSFT'], label='MSFT')
ax.plot(range(len(data)), data['Close']['NVDA'] - offset['NVDA'], label='NVDA')
ax.set_xticks(idxs)
ax.set_xticklabels(idx_labels, rotation=60)
ax.legend()
fig.tight_layout()
plt.show()

query = "SELECT * FROM tradefx.tickers"
df = pd.read_sql(query, cnxn)

nvda.recommendations_summary

rec_dict = {
    '0m': {'strongBuy': [], 'buy': [], 'hold': [], 'sell': [], 'strongSell': []},
    '-1m': {'strongBuy': [], 'buy': [], 'hold': [], 'sell': [], 'strongSell': []},
    '-2m': {'strongBuy': [], 'buy': [], 'hold': [], 'sell': [], 'strongSell': []},
    '-3m': {'strongBuy': [], 'buy': [], 'hold': [], 'sell': [], 'strongSell': []},
}

failed = []

for ticker in df.symbol:
    try:
        data = yf.Ticker(ticker)
        recs = data.recommendations
        for p in recs['period']:
            rec_dict[p]['strongBuy'].append(recs.loc[recs.period == p].strongBuy.iloc[0])
            rec_dict[p]['buy'].append(recs.loc[recs.period == p].buy.iloc[0])
            rec_dict[p]['hold'].append(recs.loc[recs.period == p].hold.iloc[0])
            rec_dict[p]['sell'].append(recs.loc[recs.period == p].sell.iloc[0])
            rec_dict[p]['strongSell'].append(recs.loc[recs.period == p].strongSell.iloc[0])
    except:
        failed.append(ticker)


emr = yf.Ticker('EMR')
emr.info['longBusinessSummary']
emr.recommendations