import datetime
import pandas as pd

from .models import (
    Date,
    Ticker,
    StockInfo,
    Index,
    ETF,
    OHLCV,
    BuySell,
    MarketCapital,
    Factor,
)

from utils.cache import RedisClient

from keystone.settings import RAVEN_CONFIG
from raven import Client
client = Client(RAVEN_CONFIG['dsn'])

class Reducers:

    def __init__(self, action_type, env_type):
        # action_type should be a str
        self.ACTION = action_type.lower()

        self.ENV = env_type.lower() # this is so you can run Node crawler app (Gobble) from local env
        # set ENV to local if you with to run on a local machine

        # define cache
        self.redis = RedisClient()

    def has_reducer(self):
        # return True if reducer is defined, else False
        if hasattr(self, self.ACTION):
            return True
        else:
            return False

    def reduce(self):
        try:
            reducer = getattr(self, self.ACTION)
            # run reducer
            reducer() # pass in ENV to run locally if told to
        except:
            client.captureException()
            return False # return False on error

        # return True when function ran properly
        return True

    ##### 공통 태스크 #####
    def save_mass_date(self):
        all_dates = self.redis.get_list('mass_date') # 리스트값이다
        print('FnGuide 데이터: {}'.format(len(all_dates)))

        dates_qs = Date.objects.all().order_by('date').distinct('date').values_list('date')
        dates_in_db = [date[0] for date in dates_qs]
        if len(dates_in_db) > 0:
            print('DB 데이터({}개): {} ~ {}'.format(len(dates_in_db), dates_in_db[0], dates_in_db[-1]))

        to_save_dates = list(set(all_dates) - set(dates_in_db))

        dates = []
        for date_data in to_save_dates:
            date_inst = Date(date=date_data)
            dates.append(date_inst)
        Date.objects.bulk_create(dates)
        print('Date 데이터 저장: {}개'.format(len(dates)))
        if len(dates) != 0:
            print('데이터 첫번째: {}, 마지막: {}'.format(dates[0], dates[-1]))
        ### 태스크 분배 시스템이 날짜가 업데이트됐음을 알 수 있게, 레디스에 그 정보를 저장한다
        self.redis.set_key('DATE_JUST_UPDATED_TO_DB', 'True')

    # 날짜 데이터가 있어야하기 때문에 SAVE_MASS_DATE 태스크를 먼저 실행시킨 후에 실행시킨다
    def set_update_tasks(self):
        today_date = datetime.datetime.now().strftime('%Y-%m-%d')
        all_dates = self.redis.get_list('mass_date') # 리스트값이다

        # 업데이트한 새로운 날짜 데이터를 데이터베이스의 데이터들과 비교하여, 추가해야할 날짜가 언제인지 캐시에 저장해두기
        # 확인해야할 모델들: Ticker, StockInfo, Index, ETF, OHLCV, MarketCapital, BuySell, Factor

        for model in ['Ticker', 'StockInfo', 'Index', 'ETF', 'OHLCV', 'MarketCapital', 'BuySell', 'Factor']:
            if model == 'Ticker':
                db_model = Ticker
            elif model == 'StockInfo':
                db_model = StockInfo
            elif model == 'Index':
                db_model = Index
            elif model == 'ETF':
                db_model = ETF
            elif model == 'OHLCV':
                db_model = OHLCV
            elif model == 'MarketCapital':
                db_model = MarketCapital
            elif model == 'BuySell':
                db_model = BuySell

            print('{} 모델 데이터베이스 상태 확인'.format(model))
            model_qs = db_model.objects.order_by('date').distinct('date').values('date')
            dates = pd.DataFrame(list(model_qs))

            if len(dates) == 0:
                dates = all_dates
            else:
                dates = list(dates['date'])
                dates = list(set(all_dates) - set(dates))

            dates = list(pd.DataFrame(dates).sort_values(by=[0])[0])

            if all_dates[-1] != dates[-1]:
                # all_dates의 -1 인덱스가 최근 날짜이다
                self.redis.set_key('UPDATE_{}'.format(model.upper()), 'True')
            else:
                self.redis.set_key('UPDATE_{}'.format(model.upper()), 'False')

            if model == 'Ticker' or model == 'StockInfo':
                redis_data = ['to_update_{}_list'.format(model.lower())] + [dates[-1]]
            else:
                redis_data = ['to_update_{}_list'.format(model.lower())] + dates

            res = self.redis.set_list(redis_data)
            print(res)

    def save_kospi_tickers(self):
        all_tickers = self.redis.get_list('kospi_tickers') # 리스트값이다
        print('FnGuide 데이터: {}'.format(len(all_tickers)))

        today_date = datetime.datetime.now().strftime('%Y%m%d')
        to_update_date = self.redis.get_key('to_update_ticker_list')[0]
        if today_date == to_update_date:
            bulk_tickers_list = []
            for ticker_info in all_tickers:
                ticker = ticker_info.split('|')[0]
                name = ticker_info.split('|')[1]
                ticker_inst = Ticker(date=today_date,
                                     code=ticker,
                                     name=name,
                                     market_type='KOSPI',
                                     state=1)
                bulk_tickers_list.append(ticker_inst)
            Ticker.objects.bulk_create(bulk_tickers_list)
            print('코드 업데이트: {}개'.format(len(all_tickers)))
        else:
            print('코드 업데이트: 0개')

    def save_kosdaq_tickers(self):
        all_tickers = self.redis.get_list('kosdaq_tickers') # 리스트값이다
        print('FnGuide 데이터: {}'.format(len(all_tickers)))

        today_date = datetime.datetime.now().strftime('%Y%m%d')
        to_update_date = self.redis.get_key('to_update_ticker_list')[0]
        if today_date == to_update_date:
            bulk_tickers_list = []
            for ticker_info in all_tickers:
                ticker = ticker_info.split('|')[0]
                name = ticker_info.split('|')[1]
                ticker_inst = Ticker(date=today_date,
                                     code=ticker,
                                     name=name,
                                     market_type='KOSDAQ',
                                     state=1)
                bulk_tickers_list.append(ticker_inst)
            Ticker.objects.bulk_create(bulk_tickers_list)
            print('코드 업데이트: {}개'.format(len(all_tickers)))
        else:
            print('코드 업데이트: 0개')

    def save_stock_info(self):
        stock_info = self.redis.get_list('stock_info')
        print('FnGuide 데이터: {}'.format(len(stock_info)))

        today_date = datetime.datetime.now().strftime('%Y%m%d')
        to_update_date = self.redis.get_key('to_update_stockinfo_list')[0]
        if today_date == to_update_date:
            bulk_stockinfo_list = []
            for info in stock_info:
                date = info.split('|')[0]
                code = info.split('|')[1]
                name = info.split('|')[2]
                stk_kind = info.split('|')[3]
                mkt_gb = info.split('|')[4]
                mkt_cap = info.split('|')[5]
                mkt_cap_size = info.split('|')[6]
                frg_hld = info.split('|')[7]
                mgt_gb = info.split('|')[8]
                stockinfo_inst = StockInfo(date=date,
                                           code=code,
                                           name=name,
                                           stk_kind=stk_kind,
                                           mkt_gb=mkt_gb,
                                           mkt_cap=mkt_cap,
                                           mkt_cap_size=mkt_cap_size,
                                           frg_hld=frg_hld,
                                           mgt_gb=mgt_gb)
                bulk_stockinfo_list.append(stockinfo_inst)
            StockInfo.objects.bulk_create(bulk_stockinfo_list)
            print('정보 업데이트: {}개'.format(len(bulk_stockinfo_list)))
        else:
            print('정보 업데이트: 0개')
