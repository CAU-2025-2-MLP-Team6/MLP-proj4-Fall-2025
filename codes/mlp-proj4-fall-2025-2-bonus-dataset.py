import yfinance as yf
import FinanceDataReader as fdr
import pandas as pd

START_DATE = "2010-01-01"
END_DATE = "2023-12-01"


# 1. KOSPI 지수
kospi = yf.download("^KS11", start=START_DATE, end=END_DATE, auto_adjust=False)
kospi.columns = [col[0] if isinstance(col, tuple) else col for col in kospi.columns]

# 2. 국내 주요 종목 수익률
samsung = fdr.DataReader('005930', start=START_DATE, end=END_DATE)
sk_hynix = fdr.DataReader('000660', start=START_DATE, end=END_DATE)

samsung['Samsung_Return'] = samsung['Close'].pct_change()
sk_hynix['SKHynix_Return'] = sk_hynix['Close'].pct_change()

# 3. VKOSPI (국내 변동성 지수 대체)
v_kospi_proxy = fdr.DataReader('252670', start=START_DATE, end=END_DATE)
v_kospi_proxy.rename(columns={'Close': 'VKOSPI_Index'}, inplace=True)

# 4. 미국 지수 (S&P 500, NASDAQ)
sp500 = yf.download("^GSPC", start=START_DATE, end=END_DATE, auto_adjust=False)
nasdaq = yf.download("^IXIC", start=START_DATE, end=END_DATE, auto_adjust=False)

sp500.columns = [col[0] if isinstance(col, tuple) else col for col in sp500.columns]
nasdaq.columns = [col[0] if isinstance(col, tuple) else col for col in nasdaq.columns]

df = kospi[['Close', 'Open', 'High', 'Low', 'Volume']].rename(columns={
    'Close': 'KOSPI_Close',
    'Open': 'KOSPI_Open',
    'High': 'KOSPI_High',
    'Low': 'KOSPI_Low',
    'Volume': 'KOSPI_Volume'
}).copy()

# 종목 수익률 Merge
df = df.merge(samsung[['Samsung_Return']], left_index=True, right_index=True, how='left')
df = df.merge(sk_hynix[['SKHynix_Return']], left_index=True, right_index=True, how='left')

# VKOSPI Proxy Merge
df = df.merge(v_kospi_proxy[['VKOSPI_Index']], left_index=True, right_index=True, how='left')

# 미국 지수 Merge
sp500_close = sp500[['Close']].rename(columns={'Close': 'SP500_Close'})
nasdaq_close = nasdaq[['Close']].rename(columns={'Close': 'NASDAQ_Close'})

df = df.merge(sp500_close, left_index=True, right_index=True, how='left')
df = df.merge(nasdaq_close, left_index=True, right_index=True, how='left')

# 최종 결측치 처리 (NaN)
df = df.fillna(method='ffill')
df.dropna(inplace=True)

df.to_csv("kospi_dataset.csv")

print(df.head())