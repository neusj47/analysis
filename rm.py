import matplotlib.pyplot as plt
from etf import *


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