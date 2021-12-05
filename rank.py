import pandas as pd
import requests
import FinanceDataReader as fdr
from pandas.io.json import json_normalize
import json
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from pykrx import stock


start_date = '20201030'
end_date ='20211130'
tgt_n = 5
lookback_m = 1

# ETF 코드 가져오기
url = 'https://finance.naver.com/api/sise/etfItemList.nhn'
json_data = json.loads(requests.get(url).text)
df = json_normalize(json_data['result']['etfItemList'])[['itemcode','itemname','nav','marketSum']]
# df_krx = fdr.StockListing('KRX')
bm = pd.DataFrame(fdr.DataReader('148020', start_date, end_date)['Close'])


# ETF 정보 가져오기 (상세)
etf_info = pd.read_excel('C:/Users/ysj/Desktop/etf_info.xlsx')


# ETF 일별 시가총액 가져오기 (엔진변경)
def get_etf_siga(stddate, etf_info):
    siga = pd.DataFrame()
    for i in range(0,len(etf_info.종목코드)) :
        etf_info.종목코드.iloc[i] = str(etf_info.종목코드[i]).zfill(6)
        siga_temp = stock.get_etf_ohlcv_by_date(stddate,stddate,etf_info.종목코드[i])
        siga_temp['종목코드'] = etf_info.종목코드[i]
        siga = pd.concat([siga, siga_temp])
    siga = pd.merge(etf_info, siga, how='inner', on='종목코드')
    return siga
siga = get_etf_siga(end_date, etf_info)
siga.to_excel(excel_writer = 'C:/Users/ysj/Desktop/siga.xlsx')


# ETF 기간 수익률 가져오기
def get_etf_rtn(etf_info, stddate) :
    date = [stddate
        , stock.get_nearest_business_day_in_a_week(datetime.strftime(datetime.strptime(stddate, "%Y%m%d") - timedelta(days=8),"%Y%m%d"))
        , stock.get_nearest_business_day_in_a_week(datetime.strftime(datetime.strptime(stddate, "%Y%m%d") - relativedelta(months=1),"%Y%m%d"))
        , stock.get_nearest_business_day_in_a_week(datetime.strftime(datetime.strptime(stddate, "%Y%m%d") - relativedelta(months=3),"%Y%m%d"))
        , stock.get_nearest_business_day_in_a_week(datetime.strftime(datetime.strptime(stddate, "%Y%m%d") - relativedelta(years=1),"%Y%m%d"))]
    etf_rtn = pd.DataFrame()
    for n in range(0, len(etf_info.종목코드)):
#         etf_info.종목코드.iloc[n] = str(etf_info.종목코드[n]).zfill(6)
        df = pd.DataFrame()
        for i in range(0,len(date)) :
            df_temp = stock.get_etf_ohlcv_by_date(date[i],date[i],etf_info.종목코드.iloc[n])
            df = pd.concat([df,df_temp])
        df['종목코드'] = etf_info.종목코드.iloc[n]
        df['1W'] = df.종가 / df.shift(-1).종가 - 1
        df['1M'] = df.종가 / df.shift(-2).종가 - 1
        df['3M'] = df.종가 / df.shift(-3).종가 - 1
        df['1Y'] = df.종가 / df.shift(-4).종가 - 1
        etf_rtn = pd.concat([etf_rtn,df[['종목코드', '종가','NAV','1W','1M','3M','1Y']].iloc[0:1]]).fillna(0)
    etf_rtn = pd.merge(etf_rtn, etf_info[['종목코드','ETF약명','기초시장','기초자산','기초자산상세']], how = 'inner', on ='종목코드').sort_values(by= '1W', ascending = False)
    etf_rtn = etf_rtn[['기초시장','기초자산','기초자산상세','종목코드','ETF약명','종가','NAV','1W','1M','3M','1Y']]
    return etf_rtn
rtn = get_etf_rtn(etf_info, end_date)
rtn.to_excel(excel_writer = 'C:/Users/ysj/Desktop/rtn.xlsx')


# ETF Top5 수익률 정렬
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