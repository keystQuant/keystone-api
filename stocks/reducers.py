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
        print('DATE 데이터 저장:')
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
        print('업데이트할 데이터 지정하기:')
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

            db_not_updated = False
            first_time_saving = False

            if len(dates) == 0:
                # DB에 데이터가 없다면, 업데이트 시작하기
                dates = all_dates
                first_time_saving = True
            else:
                dates = list(dates['date'])
                if all_dates[-1] != dates[-1]:
                    # DB에 저장된 데이터가 최근 데이터와 다르다
                    db_not_updated = True
                dates = list(set(all_dates) - set(dates))

            dates = list(pd.DataFrame(dates).sort_values(by=[0])[0])

            if db_not_updated or first_time_saving:
                # all_dates의 -1 인덱스가 최근 날짜이다
                self.redis.set_key('UPDATE_{}'.format(model.upper()), 'True')
            else:
                self.redis.set_key('UPDATE_{}'.format(model.upper()), 'False')

            to_update_list_key = 'to_update_{}_list'.format(model.lower())
            if model == 'Ticker' or model == 'StockInfo':
                redis_data = [to_update_list_key] + [dates[-1]]
            else:
                redis_data = [to_update_list_key] + dates

            if self.redis.key_exists(to_update_list_key):
                self.redis.del_key(to_update_list_key)
            res = self.redis.set_list(redis_data)
            print(res)

    def save_kospi_tickers(self):
        print('KOSPI 티커 데이터 저장:')
        all_tickers = self.redis.get_list('kospi_tickers') # 리스트값이다
        print('FnGuide 데이터: {}'.format(len(all_tickers)))

        today_date = datetime.datetime.now().strftime('%Y%m%d')
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
        self.redis.set_key('KOSPI_TICKERS_JUST_UPDATED_TO_DB', 'True')

    def save_kosdaq_tickers(self):
        print('KOSDAQ 티커 데이터 저장:')
        all_tickers = self.redis.get_list('kosdaq_tickers') # 리스트값이다
        print('FnGuide 데이터: {}'.format(len(all_tickers)))

        today_date = datetime.datetime.now().strftime('%Y%m%d')
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
        self.redis.set_key('KOSDAQ_TICKERS_JUST_UPDATED_TO_DB', 'True')

    def save_etf_tickers(self):
        print('ETF 티커 데이터 저장:')
        all_tickers = self.redis.get_list('etf_tickers') # 리스트값이다
        print('FnGuide 데이터: {}'.format(len(all_tickers)))

        today_date = datetime.datetime.now().strftime('%Y%m%d')
        bulk_tickers_list = []
        for ticker_info in all_tickers:
            ticker = ticker_info.split('|')[0]
            name = ticker_info.split('|')[1]
            ticker_inst = Ticker(date=today_date,
                                 code=ticker,
                                 name=name,
                                 market_type='ETF',
                                 state=1)
            bulk_tickers_list.append(ticker_inst)
        Ticker.objects.bulk_create(bulk_tickers_list)
        print('코드 업데이트: {}개'.format(len(all_tickers)))
        self.redis.set_key('ETF_TICKERS_JUST_UPDATED_TO_DB', 'True')

    def save_stock_info(self):
        print('STOCK INFO 데이터 저장:')
        cached_data = self.redis.get_list('stock_info')
        print('FnGuide 데이터: {}'.format(len(cached_data)))

        bulk_data_list = []
        for data in cached_data:
            date = data.split('|')[0]
            code = data.split('|')[1]
            name = data.split('|')[2]
            stk_kind = data.split('|')[3]
            mkt_gb = data.split('|')[4]
            mkt_cap = int(data.split('|')[5].strip().replace(',', ''))
            mkt_cap_size = data.split('|')[6]
            frg_hlg = float(data.split('|')[7].strip().replace(',',''))
            mgt_gb = data.split('|')[8]
            db_inst = StockInfo(date=date,
                                code=code,
                                name=name,
                                stk_kind=stk_kind,
                                mkt_gb=mkt_gb,
                                mkt_cap=mkt_cap,
                                mkt_cap_size=mkt_cap_size,
                                frg_hlg=frg_hlg,
                                mgt_gb=mgt_gb)
            bulk_data_list.append(db_inst)
        StockInfo.objects.bulk_create(bulk_data_list)
        print('정보 업데이트: {}개'.format(len(bulk_data_list)))
        self.redis.set_key('STOCKINFO_JUST_UPDATED_TO_DB', 'True')

    def save_mass_index(self):
        print('INDEX 데이터 저장:')
        cached_data = self.redis.get_list('mass_index')
        print('FnGuide 데이터: {}'.format(len(cached_data)))

        bulk_data_list = []
        for data in cached_data:
            date = data.split('|')[0]
            code = data.split('|')[1]
            name = data.split('|')[2]
            strt_prc = float(data.split('|')[3].strip().replace(',', ''))
            high_prc = float(data.split('|')[4].strip().replace(',', ''))
            low_prc = float(data.split('|')[5].strip().replace(',', ''))
            cls_prc = float(data.split('|')[6].strip().replace(',', ''))
            trd_qty = float(data.split('|')[7].strip().replace(',', ''))
            trd_amt = float(data.split('|')[8].strip().replace(',', ''))
            db_inst = Index(date=date,
                            code=code,
                            name=name,
                            strt_prc=strt_prc,
                            high_prc=high_prc,
                            low_prc=low_prc,
                            cls_prc=cls_prc,
                            trd_qty=trd_qty,
                            trd_amt=trd_amt)
            bulk_data_list.append(db_inst)
        Index.objects.bulk_create(bulk_data_list)
        print('정보 업데이트: {}개'.format(len(bulk_data_list)))
        self.redis.set_key('INDEX_JUST_UPDATED_TO_DB', 'True')

    def save_mass_etf(self):
        print('ETF 데이터 저장:')
        cached_data = self.redis.get_list('mass_etf')
        print('FnGuide 데이터: {}'.format(len(cached_data)))

        bulk_data_list = []
        for data in cached_data:
            date = data.split('|')[0]
            code = data.split('|')[1]
            name = data.split('|')[2]
            cls_prc = float(data.split('|')[3].strip().replace(',', ''))
            trd_qty = int(data.split('|')[4].strip().replace(',', ''))
            trd_amt = int(data.split('|')[5].strip().replace(',', ''))
            etf_nav = float(data.split('|')[6].strip().replace(',', ''))
            spread = float(data.split('|')[7].strip().replace(',', ''))
            db_inst = ETF(date=date,
                          code=code,
                          name=name,
                          cls_prc=cls_prc,
                          trd_qty=trd_qty,
                          trd_amt=trd_amt,
                          etf_nav=etf_nav,
                          spread=spread)
            bulk_data_list.append(db_inst)
        ETF.objects.bulk_create(bulk_data_list)
        print('정보 업데이트: {}개'.format(len(bulk_data_list)))
        self.redis.set_key('ETF_JUST_UPDATED_TO_DB', 'True')

    def save_mass_ohlcv(self):
        print('OHLCV 데이터 저장:')
        cached_data = self.redis.get_list('mass_ohlcv')
        print('FnGuide 데이터: {}'.format(len(cached_data)))

        bulk_data_list = []
        for data in cached_data:
            date = data.split('|')[0]
            code = data.split('|')[1]
            name = data.split('|')[2]
            strt_prc = int(data.split('|')[3].strip().replace(',', ''))
            high_prc = int(data.split('|')[4].strip().replace(',', ''))
            low_prc = int(data.split('|')[5].strip().replace(',', ''))
            cls_prc = int(data.split('|')[6].strip().replace(',', ''))
            adj_prc = int(data.split('|')[7].strip().replace(',', ''))
            trd_qty = float(data.split('|')[8].strip().replace(',', ''))
            trd_amt = float(data.split('|')[9].strip().replace(',', ''))
            shtsale_trd_qty = float(data.split('|')[10].strip().replace(',', ''))
            db_inst = OHLCV(date=date,
                            code=code,
                            name=name,
                            strt_prc=strt_prc,
                            high_prc=high_prc,
                            low_prc=low_prc,
                            cls_prc=cls_prc,
                            adj_prc=adj_prc,
                            trd_qty=trd_qty,
                            trd_amt=trd_amt,
                            shtsale_trd_qty=shtsale_trd_qty)
            bulk_data_list.append(db_inst)
        OHLCV.objects.bulk_create(bulk_data_list)
        print('정보 업데이트: {}개'.format(len(bulk_data_list)))
        self.redis.set_key('OHLCV_JUST_UPDATED_TO_DB', 'True')

    def save_mass_marketcapital(self):
        print('MARKET CAPITAL 데이터 저장:')
        cached_data = self.redis.get_list('mass_marketcapital')
        print('FnGuide 데이터: {}'.format(len(cached_data)))

        bulk_data_list = []
        for data in cached_data:
            date = data.split('|')[0]
            code = data.split('|')[1]
            name = data.split('|')[2]
            comm_stk_qty = int(data.split('|')[3].strip().replace(',', ''))
            pref_stk_qty = int(data.split('|')[4].strip().replace(',', ''))
            db_inst = MarketCapital(date=date,
                                    code=code,
                                    name=name,
                                    comm_stk_qty=comm_stk_qty,
                                    pref_stk_qty=pref_stk_qty)
            bulk_data_list.append(db_inst)
        MarketCapital.objects.bulk_create(bulk_data_list)
        print('정보 업데이트: {}개'.format(len(bulk_data_list)))
        self.redis.set_key('MARKETCAPITAL_JUST_UPDATED_TO_DB', 'True')

    def save_mass_buysell(self):
        print('BUYSELL 데이터 저장:')
        cached_data = self.redis.get_list('mass_buysell')
        print('FnGuide 데이터: {}'.format(len(cached_data)))

        bulk_data_list = []
        for data in cached_data:
            date = data.split('|')[0]
            code = data.split('|')[1]
            name = data.split('|')[2]
            forgn_b = int(data.split('|')[3].strip().replace(',', ''))
            forgn_s = int(data.split('|')[4].strip().replace(',', ''))
            forgn_n = int(data.split('|')[5].strip().replace(',', ''))
            private_b = int(data.split('|')[6].strip().replace(',', ''))
            private_s = int(data.split('|')[7].strip().replace(',', ''))
            private_n = int(data.split('|')[8].strip().replace(',', ''))
            inst_sum_b = int(data.split('|')[9].strip().replace(',', ''))
            inst_sum_s = int(data.split('|')[10].strip().replace(',', ''))
            inst_sum_n = int(data.split('|')[11].strip().replace(',', ''))
            trust_b = int(data.split('|')[12].strip().replace(',', ''))
            trust_s = int(data.split('|')[13].strip().replace(',', ''))
            trust_n = int(data.split('|')[14].strip().replace(',', ''))
            pension_b = int(data.split('|')[15].strip().replace(',', ''))
            pension_s = int(data.split('|')[16].strip().replace(',', ''))
            pension_n = int(data.split('|')[17].strip().replace(',', ''))
            etc_inst_b = int(data.split('|')[18].strip().replace(',', ''))
            etc_inst_s = int(data.split('|')[19].strip().replace(',', ''))
            etc_inst_n = int(data.split('|')[20].strip().replace(',', ''))
            db_inst = BuySell(date=date,
                              code=code,
                              name=name,
                              forgn_b=forgn_b,
                              forgn_s=forgn_s,
                              forgn_n=forgn_n,
                              private_b=private_b,
                              private_s=private_s,
                              private_n=private_n,
                              inst_sum_b=inst_sum_b,
                              inst_sum_s=inst_sum_s,
                              inst_sum_n=inst_sum_n,
                              trust_b=trust_b,
                              trust_s=trust_s,
                              trust_n=trust_n,
                              pension_b=pension_b,
                              pension_s=pension_s,
                              pension_n=pension_n,
                              etc_inst_b=etc_inst_b,
                              etc_inst_s=etc_inst_s,
                              etc_inst_n=etc_inst_n)
            bulk_data_list.append(db_inst)
        BuySell.objects.bulk_create(bulk_data_list)
        print('정보 업데이트: {}개'.format(len(bulk_data_list)))
        self.redis.set_key('BUYSELL_JUST_UPDATED_TO_DB', 'True')

    def save_mass_factor(self):
        print('FACTOR 데이터 저장:')
        cached_data = self.redis.get_list('mass_factor')
        print('FnGuide 데이터: {}'.format(len(cached_data)))

        bulk_data_list = []
        for data in cached_data:
            date = data.split('|')[0]
            code = data.split('|')[1]
            name = data.split('|')[2]
            per = float(data.split('|')[3].strip().replace(',', ''))
            pbr = float(data.split('|')[3].strip().replace(',', ''))
            pcr = float(data.split('|')[3].strip().replace(',', ''))
            psr = float(data.split('|')[3].strip().replace(',', ''))
            divid_yield = float(data.split('|')[3].strip().replace(',', ''))
            db_inst = Factor(date=date,
                             code=code,
                             name=name,
                             per=per,
                             pbr=pbr,
                             pcr=pcr,
                             psr=psr,
                             divid_yield=divid_yield)
            bulk_data_list.append(db_inst)
        Factor.objects.bulk_create(bulk_data_list)
        print('정보 업데이트: {}개'.format(len(bulk_data_list)))
        self.redis.set_key('FACTOR_JUST_UPDATED_TO_DB', 'True')
