import pandas as pd
import numpy as np
import yfinance as yf
import time
import mysql.connector
from datetime import datetime
import os
import matplotlib.pyplot as plt
import seaborn as sns
import statsmodels.formula.api as smf
from dateutil.relativedelta import relativedelta
import random
from scipy.ndimage import gaussian_filter1d

cnxn = mysql.connector.connect(host=os.environ['PIDB_HOST'], user=os.environ['PIDB_USER'], password=os.environ['PIDB_USER_PASSWORD'])
cursor = cnxn.cursor()

def PlotMarketRegression(symbol):
    today = datetime.today().date()
    one_year_ago = today - relativedelta(years=1)
    today_str = today.strftime("%Y-%m-%d")
    one_year_str = one_year_ago.strftime("%Y-%m-%d")

    spyticker = yf.Ticker("SPY")
    spydf = spyticker.history(period="max", interval="1d", start=one_year_str, end=today_str , auto_adjust=True, rounding=True)
    spydf['returns'] = spydf.Close.pct_change() 

    ticker = yf.Ticker(symbol)
    tickerdf = ticker.history(period="max", interval="1d", start=one_year_str, end=today_str , auto_adjust=True, rounding=True)
    tickerdf['returns'] = tickerdf.Close.pct_change()

    returndf = pd.concat([spydf.returns, tickerdf.returns], axis=1).iloc[1:]
    returndf.columns = ['spy', symbol]

    model = smf.ols(formula = f'{symbol} ~ spy', data=returndf).fit()

    fig,ax = plt.subplots(figsize=(10,8))
    sns.regplot(x='spy', y=symbol, data=returndf, label=f"r2 = {np.round(model.rsquared,3)}")
    # ax.set_ylim(np.min(returndf.spy), np.max(returndf.spy))
    ax.set_title(f"Alpha = {np.round(model.params.Intercept, 3)}, Beta = {np.round(model.params.spy, 2)}")
    ax.grid(alpha=0.5, ls='--')
    ax.legend()
    plt.show()
    

##########################################################################################

# p_tickers = ['AAPL','AMZN','GOOG','INTC','META','MSFT','NKE','SPY']
# p_shares = [1,23,3,7,3,2,10,0]

query = "SELECT t.symbol FROM tradefx.tickers2 t WHERE t.marketCap > 100000000 ORDER BY t.marketCap DESC "
df = pd.read_sql(query, cnxn)

p_tickers = random.sample(list(df.symbol), 200)
p_tickers.append("SPY")
p_shares = [10] * len(p_tickers)
#p_mark = [223.7,189.88,173.88,30.93,508.85,419.16,74.86,550.81]
pdf = pd.DataFrame({'symbol': p_tickers, 'shares': p_shares})

today = datetime.today().date()
one_year_ago = today - relativedelta(years=1)
today_str = today.strftime("%Y-%m-%d")
one_year_str = one_year_ago.strftime("%Y-%m-%d")

spyticker = yf.Ticker("SPY")
spydf = spyticker.history(period="max", interval="1d", start=one_year_str, end=today_str , auto_adjust=True, rounding=True)
spydf['returns'] = spydf.Close.pct_change()

betas = []
idio_vols = []
market_vols = []
marks = []
alphas = []
for tick in p_tickers:
    print(tick)
    ticker = yf.Ticker(tick)
    tickerdf = ticker.history(period="max", interval="1d", start=one_year_str, end=today_str , auto_adjust=True, rounding=True)
    marks.append(tickerdf.iloc[-1].Close)
    tickerdf['returns'] = tickerdf.Close.pct_change()
    tempdf = pd.concat([spydf.returns, tickerdf.returns], axis=1)
    tempdf.columns = ['spy', tick]
    tempdf = tempdf[1:]
    model = smf.ols(formula = f'{tick} ~ spy', data=tempdf).fit()

    beta = model.params.spy
    alpha = model.params.Intercept
    market_vol = np.std(tempdf.spy)
    idio_vol = np.std(model.resid)
    #return_vol = np.sqrt((beta * market_vol)**2 + (idio_vol)**2)

    betas.append(np.round(beta,3))
    alphas.append(np.round(alpha,5))
    idio_vols.append(np.round(idio_vol,3))
    market_vols.append(np.round(market_vol,3))

pdf['mark'] = marks
pdf['alpha'] = alphas
pdf['beta'] = betas
pdf['idio_vol'] = idio_vols
pdf['market_vol'] = market_vols
pdf['nmv'] = pdf['shares'] * pdf['mark']
pdf['dollar_beta'] = pdf['beta'] * pdf['nmv']

p_dollar_beta = np.sum(pdf.dollar_beta)
p_market_vol = p_dollar_beta * (pdf.loc[pdf.symbol == 'SPY'].iloc[0].market_vol)
p_pct_beta = p_dollar_beta / np.sum(pdf.nmv)

pdf['idio_var'] = (pdf['nmv'] * pdf['idio_vol'])**2
p_idio_var = np.sum(pdf['idio_var'])
p_idio_vol = np.sqrt(p_idio_var)

p_tot_vol = np.sqrt(p_market_vol**2 + p_idio_vol**2)

# negative sign because if dollar beta is negative then we want to buy shares of SPY
# but if it's positive we want to short shares of SPY
market_hedge = -p_dollar_beta / pdf.loc[pdf.symbol == 'SPY'].mark.iloc[0]

PlotMarketRegression("UBXG")

mean = pdf[['idio_vol','beta']].mean()
std = pdf[['idio_vol','beta']].std()

filtdf = pdf.loc[(pdf.idio_vol <= mean.idio_vol + (2.5*std.idio_vol)) & (pdf.idio_vol >= mean.idio_vol - (2.5*std.idio_vol)) & (pdf.beta <= mean.beta + (2.5*std.beta)) & (pdf.beta >= mean.beta - (2.5*std.beta))]

fig,ax = plt.subplots(figsize=(10,6))
model = smf.ols(formula="beta ~ idio_vol", data=filtdf).fit()
sns.regplot(x="idio_vol", y="beta", data=filtdf, label=f"r2 = {np.round(model.rsquared,3)}")
ax.legend()
plt.show()


################################################################################################################################################

ticker = yf.Ticker("AMZN")
today = datetime.today().date()
one_year_ago = today - relativedelta(years=1)
today_str = today.strftime("%Y-%m-%d")
one_year_str = one_year_ago.strftime("%Y-%m-%d")

N = 50

tickdf = ticker.history(period="max", interval="1d", start=one_year_str, end=today_str , auto_adjust=True, rounding=True)
tickdf['ema'] = tickdf.Close.ewm(span=N, adjust=False).mean()
tickdf['sma'] = tickdf.Close.rolling(window=N).mean()
tickdf['ema_10'] = tickdf.Close.ewm(span=10, adjust=False).mean()
tickdf['sma_10'] = tickdf.Close.rolling(window=10).mean()
tickdf['ema_10_smooth'] = gaussian_filter1d(tickdf.ema_10, sigma=2)
tickdf['ema_smooth'] = gaussian_filter1d(tickdf.ema, sigma=2)
tickdf['velocity'] = tickdf.ema_smooth.diff()
tickdf['momentum'] = tickdf.velocity * tickdf.Volume
tickdf['acceleration'] = tickdf.velocity.diff()
tickdf['force'] = tickdf.momentum.diff()

spydf['ema'] = spydf.Close.ewm(span=N, adjust=False).mean()
spydf['ema_10'] = spydf.Close.ewm(span=10, adjust=False).mean()
spydf['velocity'] = spydf.ema.diff()
spydf['momentum'] = spydf.velocity * spydf.Volume
spydf['acceleration'] = spydf.velocity.diff()
spydf['force'] = spydf.momentum.diff()

test = pd.concat([tickdf.force, spydf.force], axis=1)
test.columns = ['AAPL', 'SPY']
test['idio_force'] = test.AAPL - test.SPY
test['idio_pct'] = test.idio_force / test.AAPL

fig,ax = plt.subplots(figsize=(10,6))
# ax.plot(tickdf.index, tickdf.ema, label='ema-50')
# ax.plot(tickdf.index, tickdf.ema_10, label='ema-10')
# ax.plot(tickdf.index, tickdf.ema_10_smooth, label='ema-10-smooth')
# ax.plot(tickdf.index, tickdf.ema_smooth, label='ema-50-smooth')
ax.plot(tickdf.index, tickdf.force, label='velo')
# ax.plot(tickdf.index, tickdf.acceleration, label='acc')
# ax.plot(tickdf.index, tickdf.momentum)
# ax.plot(tickdf.index, tickdf.force, label='AAPL')
# ax.plot(spydf.index, spydf.force, label='SPY')
plt.axhline(y=0, ls='--', color='grey')
ax.legend()
plt.show()