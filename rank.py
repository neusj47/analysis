import pandas as pd
import requests
import FinanceDataReader as fdr
from pandas.io.json import json_normalize
import json
import datetime
import matplotlib.pyplot as plt

start_date = '20201030'
end_date ='20211130'

df_krx = fdr.StockListing('KRX')

tgt_n = 5
lookback_m = 1

url = 'https://finance.naver.com/api/sise/etfItemList.nhn'
json_data = json.loads(requests.get(url).text)
df = json_normalize(json_data['result']['etfItemList'])[['itemcode','itemname','nav','marketSum']]

bm = pd.DataFrame(fdr.DataReader('148020', start_date, end_date)['Close'])

target_list = ['400580','300640','401170','381170','368590','387270','390390','354350','394670','371460','394660','385600','305540','387280']


def get_top_pick(start_date, end_date, df, tgt_n) :
    etf = pd.DataFrame()
    for i in range(0, len(target_list)):
        etf_temp = pd.DataFrame(fdr.DataReader(target_list[i], start_date, end_date)['Close'])
        etf_temp.columns = df[df['itemcode'] == target_list[i]]['itemname']
        etf = pd.concat([etf, etf_temp], axis=1)
    month_list = etf.index.map(lambda x: datetime.datetime.strftime(x, '%Y-%m')).unique()
    rebal_date = pd.DataFrame()
    for m in month_list:
        rebal_date = rebal_date.append(
            etf[etf.index.map(lambda x: datetime.datetime.strftime(x, '%Y-%m')) == m].iloc[-1])
    rebal_date = rebal_date / rebal_date.shift(1) - 1
    rebal_date = rebal_date.fillna(-1)[1:len(rebal_date)]
    signal = pd.DataFrame((rebal_date.rank(axis=1, ascending=False) <= tgt_n).applymap(lambda x: '1' if x == True else '0'))
    df_etf = pd.DataFrame(index=signal.index, columns=list(range(1, tgt_n + 1)))
    df_rtn = pd.DataFrame(index=signal.index, columns=list(range(1, tgt_n + 1)))
    for s in range(0, len(signal)):
        if len(signal.columns[signal.iloc[s] == '1'].tolist()) != tgt_n:
            df_etf.iloc[s] = signal.columns[signal.iloc[s] == '1'].tolist() + ['Nan'] * (tgt_n - len(signal.columns[signal.iloc[s] == '1'].tolist()))
            df_rtn.iloc[s] = rebal_date[signal.columns[signal.iloc[s] == '1']].iloc[s].tolist() + [-1] * (tgt_n - len(signal.columns[signal.iloc[s] == '1'].tolist()))
        else:
            df_etf.iloc[s] = signal.columns[signal.iloc[s] == '1']
            df_rtn.iloc[s] = rebal_date[signal.columns[signal.iloc[s] == '1']].iloc[s]
    df_rtn_t = pd.DataFrame(columns=signal.index)
    df_etf_t = pd.DataFrame(columns=signal.index)
    for i in range(0, len(df_rtn)):
        df_rtn_t.iloc[:, i] = df_rtn.T.sort_values(by=df_rtn.T.columns[i], ascending=False).iloc[:, i].reset_index(drop=True)
        df_etf_t.iloc[:, i] = df_etf.iloc[i][df_rtn.T.sort_values(by=df_rtn.T.columns[i], ascending=False).iloc[:, i].index].reset_index(drop=True)
    df_rtn_t.columns = df_rtn_t.columns.strftime('%Y%m%d')
    df_etf_t.columns = df_etf_t.columns.strftime('%Y%m%d')
    df_all = pd.concat([df_etf_t, df_rtn_t])
    return df_all, df_etf_t, df_rtn_t

df_all, df_etf_t, df_rtn_t =  get_top_pick(start_date, end_date, df, tgt_n)


df_pf = df[df.itemcode.isin(target_list)]

def get_data(df_pf, target_list, start_date, end_date) :
    prc = pd.DataFrame()
    for i in range(0, len(df_pf)):
        prc_temp = pd.DataFrame(fdr.DataReader(target_list[i], start_date, end_date)['Close'])
        prc_temp.columns = df_pf[df_pf.itemcode == target_list[i]].itemname
        prc = pd.concat([prc, prc_temp], axis=1)
    return prc

df = get_data(df_pf, target_list, start_date, end_date)

def get_signal(df, lookback_m, tgt_n) :
    month_list = df.index.map(lambda x: datetime.datetime.strftime(x, '%Y-%m')).unique()
    rebal_date = pd.DataFrame()
    for m in month_list:
        try:
            rebal_date = rebal_date.append(
                df[df.index.map(lambda x: datetime.datetime.strftime(x, '%Y-%m')) == m].iloc[-1])
        except Exception as e:
            print("Error : ", str(e))
        pass
    rebal_date = rebal_date[sorted(df.columns)]
    rebal_date = rebal_date / rebal_date.shift(1)
    recent_returns = df.pct_change(lookback_m * 20)
    rebal_date.iloc[len(rebal_date) - 1] = recent_returns.iloc[len(recent_returns) - 1][sorted(recent_returns.columns)]
    signal = (rebal_date.rank(axis=1, ascending=False) <= tgt_n).applymap(lambda x: '1' if x else '0')
    signal = pd.DataFrame(signal)
    signal = signal.shift(1).fillna(0)
    signal = signal.astype(float)
    return signal

signal = get_signal(df, lookback_m, tgt_n)

def get_return(df,signal,tgt_n) :
    df = df.rename_axis('Date').reset_index()
    df['Date'] = pd.to_datetime(df['Date'])
    df['YYYY-MM'] = df['Date'].map(lambda x: datetime.datetime.strftime(x, '%Y-%m'))
    signal['YYYY-MM'] = signal.index.map(lambda x: datetime.datetime.strftime(x, '%Y-%m'))
    book = pd.merge(df[['Date', 'YYYY-MM']], signal, on='YYYY-MM', how='left')
    book.set_index(['Date'], inplace=True)
    signal = book[sorted(df_pf.itemname)].astype(float)
    df.set_index(['Date'], inplace=True)
    df = df[sorted(df_pf.itemname)]
    df = df.pct_change().fillna(0)
    result = pd.DataFrame(((signal * df) * 1 / tgt_n).sum(axis=1))
    return result, signal

rtn = get_return(df,signal,tgt_n)[0]


plt.figure(figsize=(17,7))
plt.title('theme rotation return')
plt.ylabel('cumulative_return(100%p)')
plt.plot((1 + rtn).cumprod() - 1, label = 'theme')
plt.plot((1 + bm['Close'].pct_change().fillna(0)).cumprod() - 1, label = 'BenchMark')
plt.legend()
plt.show()
