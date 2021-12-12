import pandas as pd
import requests
from datetime import datetime, timedelta
from pykrx import stock
from dateutil.relativedelta import relativedelta

end_date = '20211210'

# ticker 정보가져오기
def get_ticker(stddate):
    sector = {1010: '에너지',
              1510: '소재',
              2010: '자본재',
              2020: '상업서비스와공급품',
              2030: '운송',
              2510: '자동차와부품',
              2520: '내구소비재와의류',
              2530: '호텔,레스토랑,레저 등',
              2550: '소매(유통)',
              2560: '교육서비스',
              3010: '식품과기본식료품소매',
              3020: '식품,음료,담배',
              3030: '가정용품과개인용품',
              3510: '건강관리장비와서비스',
              3520: '제약과생물공학',
              4010: '은행',
              4020: '증권',
              4030: '다각화된금융',
              4040: '보험',
              4050: '부동산',
              4510: '소프트웨어와서비스',
              4520: '기술하드웨어와장비',
              4530: '반도체와반도체장비',
              4535: '전자와 전기제품',
              4540: '디스플레이',
              5010: '전기통신서비스',
              5020: '미디어와엔터테인먼트',
              5510: '유틸리티'}
    df = pd.DataFrame(columns=['code', 'name', 'sector_l', 'sector_m', 'mktval', 'wgt'])
    for i, sec_code in enumerate(sector.keys()):
        response = requests.get('http://www.wiseindex.com/Index/GetIndexComponets?ceil_yn=0&''dt=' + stddate + '&sec_cd=G' + str(sec_code))
        if (response.status_code == 200):
            json_list = response.json()
            for json in json_list['list']:
                code = json['CMP_CD']
                name = json['CMP_KOR']
                sector_l = json['SEC_NM_KOR']
                sector_m = json['IDX_NM_KOR'][5:]
                mktval = json['MKT_VAL']
                wgt = json['WGT']
                df = df.append(
                    {'code': code, 'name': name, 'sector_l': sector_l, 'sector_m': sector_m, 'mktval': mktval,'wgt': wgt}, ignore_index=True)
    return df

# ticker 시총 데이터 가져오기
def get_ticker_data(stddate) :
    ticker = get_ticker(stddate)
    ticker_df = stock.get_market_cap_by_ticker(stddate).reset_index()
    ticker_df = ticker_df.rename(columns={'티커':'code','종가':'price','시가총액':'mktcap', '상장주식수':'shares','거래량':'amt','거래대금':'mktvol'})
    ticker_df = pd.merge(ticker, ticker_df, how= 'inner', on ='code')
    ticker_df = ticker_df[['code','name','sector_l','sector_m','price','shares','mktcap','amt','mktvol']]
    return ticker_df
ticker_df = get_ticker_data(end_date)

# ticker 기간 수익률 가져오기
def get_ticker_rtn(stddate) :
    ticker_df = get_ticker_data(end_date)
    date = [stddate
        , stock.get_nearest_business_day_in_a_week(datetime.strftime(datetime.strptime(stddate, "%Y%m%d") - timedelta(days=8),"%Y%m%d"))
        , stock.get_nearest_business_day_in_a_week(datetime.strftime(datetime.strptime(stddate, "%Y%m%d") - relativedelta(months=1),"%Y%m%d"))
        , stock.get_nearest_business_day_in_a_week(datetime.strftime(datetime.strptime(stddate, "%Y%m%d") - relativedelta(months=3),"%Y%m%d"))
        , stock.get_nearest_business_day_in_a_week(datetime.strftime(datetime.strptime(stddate, "%Y%m%d") - relativedelta(months=6),"%Y%m%d"))
        , stock.get_nearest_business_day_in_a_week(datetime.strftime(datetime.strptime(stddate, "%Y%m%d") - relativedelta(years=1),"%Y%m%d"))]
    ticker = stock.get_market_cap_by_ticker(date[0]).reset_index()
    ticker_w1 = stock.get_market_cap_by_ticker(date[1]).reset_index()[['티커','종가']].rename(columns={'종가':'w1_종가'})
    ticker_m1 = stock.get_market_cap_by_ticker(date[2]).reset_index()[['티커','종가']].rename(columns={'종가':'m1_종가'})
    ticker_m3 = stock.get_market_cap_by_ticker(date[3]).reset_index()[['티커','종가']].rename(columns={'종가':'m3_종가'})
    ticker_m6 = stock.get_market_cap_by_ticker(date[4]).reset_index()[['티커','종가']].rename(columns={'종가':'m6_종가'})
    ticker_y1 = stock.get_market_cap_by_ticker(date[5]).reset_index()[['티커','종가']].rename(columns={'종가':'y1_종가'})
    ticker = pd.merge(ticker, ticker_w1, how= 'outer', on = '티커')
    ticker = pd.merge(ticker, ticker_m1, how= 'outer', on = '티커')
    ticker = pd.merge(ticker, ticker_m3, how= 'outer', on = '티커')
    ticker = pd.merge(ticker, ticker_m6, how= 'outer', on = '티커')
    ticker = pd.merge(ticker, ticker_y1, how= 'outer', on = '티커').dropna()
    ticker['1W'] = ticker['종가'] / ticker['w1_종가'] - 1
    ticker['1M'] = ticker['종가'] / ticker['m1_종가'] - 1
    ticker['3M'] = ticker['종가'] / ticker['m3_종가'] - 1
    ticker['6M'] = ticker['종가'] / ticker['m6_종가'] - 1
    ticker['1Y'] = ticker['종가'] / ticker['y1_종가'] - 1
    ticker = ticker[['티커', '종가','시가총액','1W','1M','3M','6M','1Y']].rename(columns={'티커':'code'})
    ticker_rtn = pd.merge(ticker_df[['code','name','sector_l','sector_m']], ticker, how = 'inner', on ='code').sort_values(by= '1W', ascending = False)
    ticker_rtn = ticker_rtn[['code','name','sector_l','sector_m','종가','시가총액','1W','1M','3M','6M','1Y']]
    return ticker_rtn
ticker_rtn = get_ticker_rtn(end_date)