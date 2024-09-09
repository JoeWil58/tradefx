import yfinance as yf
import pandas as pd
import numpy as np
import statsmodels.formula.api as smf
import matplotlib.pyplot as plt
import seaborn as sns

def calc_ewma(tick_returns, market_returns, lam):
  Rt_bar = np.mean(tick_returns)
  Mt_bar = np.mean(market_returns)
  numerator = 0
  denominator = 0
  t = len(tick_returns)
  for i in range(1,len(tick_returns)):
    numerator += (lam**i) * (tick_returns.iloc[t-i] - Rt_bar) * (market_returns.iloc[t-i] - Mt_bar)
    denominator += (lam**i) * ((market_returns.iloc[t-i] - Mt_bar)**2)
  
  return numerator/denominator

def get_returns(symbol, market, period, interval):
  tick = yf.Ticker(symbol)
  tickdf = tick.history(period=period, interval=interval)
  tickdf['returns'] = tickdf.Open.pct_change()

  spy = yf.Ticker(market)
  spydf = spy.history(period=period, interval=interval)
  spydf['returns'] = spydf.Open.pct_change()

  df = pd.concat([spydf.returns, tickdf.returns], axis=1)
  df.columns = [market, symbol]

  return df

def get_historic_beta(returns, symbol, market):
  model = smf.ols(formula=f'{symbol} ~ {market}', data=returns).fit()
  return model.params[market]


rf = 0.0385
rm = 0.047

# symbols = ['nee', 'so', 'duk', 'ceg', 'aep', 'sre', 'pcg', 'd', 'peg']
symbols = ['AMZN', 'AAPL', 'META', 'MSFT', 'NVDA', 'INTC']

exp_returns = []
adj_betas = []

for symbol in symbols:
  market = 'spy'
  returns = get_returns(symbol, market, '5y', '1mo')
  beta_hist = get_historic_beta(returns, symbol, market)
  ewma = calc_ewma(returns[symbol], returns[market], 0.94)
  ewma_adj = (0.67 * ewma) + 0.33
  # beta_adj = (0.67 * beta_hist) + 0.33
  R_exp = rf + (ewma_adj * (rm - rf))
  # R_exp = rf + (beta_adj * (rm - rf))

  exp_returns.append(R_exp)
  adj_betas.append(ewma_adj)

  print(f"{symbol} | Historic Beta: {beta_hist} | Adj Beta: {ewma_adj} | Exp Return: {R_exp}")


# fig,ax = plt.subplots(figsize=(10,6))
# sns.regplot(x='SPY', y=symbol, data=df)
# plt.show()
