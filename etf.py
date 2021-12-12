import FinanceDataReader as fdr
from pandas.io.json import json_normalize
import json
from ticker import *
import warnings
warnings.filterwarnings("ignore")

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


# ETF 정보 가져오기
etf_info = pd.read_excel('C:/Users/ysj/Desktop/etf_info.xlsx', sheet_name = '12월')


# ETF 일별 시가총액 가져오기
def get_etf_data(stddate, etf_info):
    etf = stock.get_etf_ohlcv_by_ticker(stddate).reset_index()
    etf = etf.rename(columns={'티커':'종목코드'})
    for i in range(0,len(etf_info.종목코드)) :
        etf_info.종목코드.iloc[i] = str(etf_info.종목코드[i]).zfill(6)
    etf = pd.merge(etf_info, etf, how = 'inner', on = '종목코드')
    return etf
etf_data = get_etf_data(end_date, etf_info)
# etf_data.to_excel(excel_writer = 'C:/Users/ysj/Desktop/etf_data.xlsx')


# ETF 기간 수익률 가져오기
def get_etf_rtn(etf_info, stddate) :
    date = [stddate
        , stock.get_nearest_business_day_in_a_week(datetime.strftime(datetime.strptime(stddate, "%Y%m%d") - timedelta(days=8),"%Y%m%d"))
        , stock.get_nearest_business_day_in_a_week(datetime.strftime(datetime.strptime(stddate, "%Y%m%d") - relativedelta(months=1),"%Y%m%d"))
        , stock.get_nearest_business_day_in_a_week(datetime.strftime(datetime.strptime(stddate, "%Y%m%d") - relativedelta(months=3),"%Y%m%d"))
        , stock.get_nearest_business_day_in_a_week(datetime.strftime(datetime.strptime(stddate, "%Y%m%d") - relativedelta(months=6),"%Y%m%d"))
        , stock.get_nearest_business_day_in_a_week(datetime.strftime(datetime.strptime(stddate, "%Y%m%d") - relativedelta(years=1),"%Y%m%d"))]
    etf = stock.get_etf_ohlcv_by_ticker(date[0]).reset_index()
    etf_w1 = stock.get_etf_ohlcv_by_ticker(date[1]).reset_index()[['티커','종가']].rename(columns={'종가':'w1_종가'})
    etf_m1 = stock.get_etf_ohlcv_by_ticker(date[2]).reset_index()[['티커','종가']].rename(columns={'종가':'m1_종가'})
    etf_m3 = stock.get_etf_ohlcv_by_ticker(date[3]).reset_index()[['티커','종가']].rename(columns={'종가':'m3_종가'})
    etf_m6 = stock.get_etf_ohlcv_by_ticker(date[4]).reset_index()[['티커','종가']].rename(columns={'종가':'m6_종가'})
    etf_y1 = stock.get_etf_ohlcv_by_ticker(date[5]).reset_index()[['티커','종가']].rename(columns={'종가':'y1_종가'})
    etf = pd.merge(etf, etf_w1, how= 'outer', on = '티커')
    etf = pd.merge(etf, etf_m1, how= 'outer', on = '티커')
    etf = pd.merge(etf, etf_m3, how= 'outer', on = '티커')
    etf = pd.merge(etf, etf_m6, how= 'outer', on = '티커')
    etf = pd.merge(etf, etf_y1, how= 'outer', on = '티커').dropna()
    etf['1W'] = etf['종가'] / etf['w1_종가'] - 1
    etf['1M'] = etf['종가'] / etf['m1_종가'] - 1
    etf['3M'] = etf['종가'] / etf['m3_종가'] - 1
    etf['6M'] = etf['종가'] / etf['m6_종가'] - 1
    etf['1Y'] = etf['종가'] / etf['y1_종가'] - 1
    etf = etf[['티커', '종가','NAV','1W','1M','3M','6M','1Y']].rename(columns={'티커':'종목코드'})
    etf_rtn = pd.merge(etf_info[['기초시장','기초자산','기초자산상세','종목코드','ETF명']], etf, how = 'inner', on ='종목코드').sort_values(by= '1W', ascending = False)
    etf_rtn = etf_rtn[['기초시장','기초자산','기초자산상세','종목코드','ETF명','종가','NAV','1W','1M','3M','6M','1Y']]
    return etf_rtn
etf_rtn = get_etf_rtn(etf_data, end_date)

# ETF Loading
def get_pdf_data(df_theme, ticker_df, stddate) :
    pdf = pd.DataFrame()
    for i in range(0,len(df_theme.종목코드)) :
        df_theme.종목코드.iloc[i] = str(df_theme.종목코드.iloc[i]).zfill(6)
        pdf_temp = stock.get_etf_portfolio_deposit_file(str(df_theme.종목코드.iloc[i]), stddate).reset_index()
        try :
            pdf_temp = pdf_temp.rename(columns={'티커':'code'})
            pdf_temp = pdf_temp[['code','시가총액']].dropna(axis=1)
            pdf_temp['etf_code'] = df_theme.종목코드.iloc[i]
            pdf = pd.concat([pdf, pdf_temp])
        except Exception as e:
            print(i, ' 번 째 오류 발생 : ', df_theme.종목코드.iloc[i], ' 오류:', str(e))
    pdf = pd.merge(pdf, ticker_df[['code','name','sector_l']], how = 'outer', on ='code')
    pdf = pdf.rename(columns={'etf_code':'종목코드','sector_l':'섹터'})
    etf_pdf = df_theme[['자산','기초시장','기초자산','기초자산상세', '종목코드','ETF명','기초지수명', '키워드','CU','상장좌수']].drop_duplicates()
    etf_pdf = pd.merge(pdf, etf_pdf, how = 'outer', on ='종목코드').dropna()
    etf_pdf['시가총액_adj'] = etf_pdf['시가총액'] * etf_pdf['상장좌수'] / etf_pdf['CU']
    return etf_pdf

df_theme = etf_data[(etf_data['기초시장'] == '국내') & (etf_data['기초자산상세'] == '업종/테마')]
etf_pdf = get_pdf_data(df_theme, ticker_df, end_date)

# ETF Top N 수익률 정렬
# target_list = ['400580','300640','401170','381170','368590','387270','390390','354350','394670','371460','394660','385600','305540','387280']
target_list = list(etf_info.종목코드.unique())
def get_top_pick(start_date, end_date, target_list, df, tgt_n) :
    etf = pd.DataFrame()
    for i in range(0, len(target_list)):
        etf_temp = pd.DataFrame(fdr.DataReader(target_list[i], start_date, end_date)['Close'])
        etf_temp.columns = df[df['종목코드'] == target_list[i]]['기초지수명']
        etf = pd.concat([etf, etf_temp], axis=1)
    month_list = etf.index.map(lambda x: datetime.strftime(x, '%Y-%m')).unique()
    rebal_date = pd.DataFrame()
    for m in month_list:
        rebal_date = rebal_date.append(
            etf[etf.index.map(lambda x: datetime.strftime(x, '%Y-%m')) == m].iloc[-1])
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
df_all, df_etf_t, df_rtn_t =  get_top_pick(start_date, end_date, target_list, etf_data, tgt_n)


# ETF 구성 보기
etf_pdf = pd.merge(etf_pdf, ticker_rtn[['code','6M']], on ='code', how  = 'inner')

import plotly.express as px
fig = px.treemap(etf_pdf,
                 path=['키워드', '섹터'],
                 values='시가총액_adj',
                 color='6M', color_continuous_scale='reds', maxdepth = 20
                )
fig.update_layout(
    margin = {'t':0, 'l':0, 'r':0, 'b':0}
)
fig.show()


fig = px.treemap(etf_pdf,
                 path=['키워드', 'name'],
                 values='시가총액_adj',
                 color='6M', color_continuous_scale='Greens', maxdepth = 20
                )
fig.update_layout(
    margin = {'t':0, 'l':0, 'r':0, 'b':0}
)
fig.show()