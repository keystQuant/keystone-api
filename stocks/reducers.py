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

MARKET_CODES = {
    # 시장 인덱스
    '코스피': 'I.001',
    '코스닥': 'I.201',

    # 사이즈 인덱스
    '코스피 대형주': 'I.002',
    '코스피 중형주': 'I.003',
    '코스피 소형주': 'I.004',
    '코스닥 대형주': 'I.202',
    '코스닥 중형주': 'I.203',
    '코스닥 소형주': 'I.204',

    # 스타일 인덱스
    '성장주': 'I.431', # KRX 스마트 모멘텀
    '가치주': 'I.432', # KRX 스마트 밸류
    '배당주': 'I.192', # KRX 고배당 50
    '퀄리티주': 'I.433', # KRX 스마트 퀄리티
    '사회책임경영주': 'I.426', # KRX 사회책임경영

    # 산업 인덱스
    '코스피 음식료품': 'I.005',
    '코스피 섬유,의복': 'I.006',
    '코스피 종이,목재': 'I.007',
    '코스피 화학': 'I.008',
    '코스피 의약품': 'I.009',
    '코스피 비금속광물': 'I.010',
    '코스피 철강및금속': 'I.011',
    '코스피 기계': 'I.012',
    '코스피 전기,전자': 'I.013',
    '코스피 의료정밀': 'I.014',
    '코스피 운수장비': 'I.015',
    '코스피 유통업': 'I.016',
    '코스피 전기가스업': 'I.017',
    '코스피 건설업': 'I.018',
    '코스피 운수창고': 'I.019',
    '코스피 통신업': 'I.020',
    '코스피 금융업': 'I.021',
    '코스피 은행': 'I.022',
    '코스피 증권': 'I.024',
    '코스피 보험': 'I.025',
    '코스피 서비스업': 'I.026',
    '코스피 제조업': 'I.027',
    '코스닥 기타서비스': 'I.212',
    '코스닥 IT종합': 'I.215',
    '코스닥 제조': 'I.224',
    '코스닥 건설': 'I.226',
    '코스닥 유통': 'I.227',
    '코스닥 운송': 'I.229',
    '코스닥 금융': 'I.231',
    '코스닥 오락, 문화': 'I.237',
    '코스닥 통신방송서비스': 'I.241',
    '코스닥 IT S/W & SVC': 'I.242',
    '코스닥 IT H/W': 'I.243',
    '코스닥 음식료,담배': 'I.256',
    '코스닥 섬유,의류': 'I.258',
    '코스닥 종이,목재': 'I.262',
    '코스닥 출판,매체복제': 'I.263',
    '코스닥 화학': 'I.265',
    '코스닥 제약': 'I.266',
    '코스닥 비금속': 'I.267',
    '코스닥 금속': 'I.268',
    '코스닥 기계,장비': 'I.270',
    '코스닥 일반전기,전자': 'I.272',
    '코스닥 의료,정밀기기': 'I.274',
    '코스닥 운송장비,부품': 'I.275',
    '코스닥 기타 제조': 'I.277',
    '코스닥 통신서비스': 'I.351',
    '코스닥 방송서비스': 'I.352',
    '코스닥 인터넷': 'I.353',
    '코스닥 디지탈컨텐츠': 'I.354',
    '코스닥 소프트웨어': 'I.355',
    '코스닥 컴퓨터서비스': 'I.356',
    '코스닥 통신장비': 'I.357',
    '코스닥 정보기기': 'I.358',
    '코스닥 반도체': 'I.359',
    '코스닥 IT부품': 'I.360'
}

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
            elif model == 'Factor':
                db_model = Factor

            print('{} 모델 데이터베이스 상태 확인'.format(model))
            model_qs = db_model.objects.order_by('date').distinct('date').values('date')
            dates = pd.DataFrame(list(model_qs))

            db_not_updated = False
            first_time_saving = False
            db_has_missing_data = False

            if len(dates) == 0:
                dates = all_dates
                first_time_saving = True # DB에 데이터가 하나도 없다면, 업데이트 시작하기
            else:
                dates = list(dates['date'])
                if all_dates[-1] != dates[-1]:
                    dates = list(set(all_dates) - set(dates))
                    db_not_updated = True # DB에 저장된 데이터가 최근 데이터와 다르다면 업데이트 시작
                else:
                    dates = list(set(all_dates) - set(dates))
                    if len(dates) != 0:
                        dates = list(pd.DataFrame(dates).sort_values(by=[0])[0])
                        if model != 'Ticker' or model != 'StockInfo':
                            # Ticker나 StockInfo는 당일 데이터만 업데이트되도 넘어간다
                            dates = [all_dates[-1]]
                        else:
                            db_has_missing_data = True # 최근 데이터가 일치하지만, DB에 빠진 데이터가 있다면 업데이트하기
                    else:
                        # 이미 모든 데이터가 업데이트되었고, DB에 빠진 데이터도 없다면 최근 데이터만 넣기
                        dates = [all_dates[-1]]

            if db_not_updated or first_time_saving or db_has_missing_data:
                need_update = 'True'
                self.redis.set_key('UPDATE_{}'.format(model.upper()), need_update)
            else:
                need_update = 'False'
                self.redis.set_key('UPDATE_{}'.format(model.upper()), 'False')

            to_update_list_key = 'to_update_{}_list'.format(model.lower())
            if model == 'Ticker' or model == 'StockInfo':
                redis_data = [to_update_list_key] + [dates[-1]]
            else:
                redis_data = [to_update_list_key] + dates

            if self.redis.key_exists(to_update_list_key):
                self.redis.del_key(to_update_list_key)
            res = self.redis.set_list(redis_data)
            print('업데이트할 필요있다: {}, 날짜: {}개'.format(need_update, res))

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

        date = ''
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
        print('정보 업데이트: {}개, 날짜: {}'.format(len(bulk_data_list), date))
        self.redis.set_key('STOCKINFO_JUST_UPDATED_TO_DB', 'True')

    def save_mass_index(self):
        print('INDEX 데이터 저장:')
        cached_data = self.redis.get_list('mass_index')
        print('FnGuide 데이터: {}'.format(len(cached_data)))

        date = ''
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
        print('정보 업데이트: {}개, 날짜: {}'.format(len(bulk_data_list), date))
        self.redis.set_key('INDEX_JUST_UPDATED_TO_DB', 'True')

    def save_mass_etf(self):
        print('ETF 데이터 저장:')
        cached_data = self.redis.get_list('mass_etf')
        print('FnGuide 데이터: {}'.format(len(cached_data)))

        date = ''
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
        print('정보 업데이트: {}개, 날짜: {}'.format(len(bulk_data_list), date))
        self.redis.set_key('ETF_JUST_UPDATED_TO_DB', 'True')

    def save_mass_ohlcv(self):
        print('OHLCV 데이터 저장:')
        cached_data = self.redis.get_list('mass_ohlcv')
        print('FnGuide 데이터: {}'.format(len(cached_data)))

        date = ''
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
        print('정보 업데이트: {}개, 날짜: {}'.format(len(bulk_data_list), date))
        self.redis.set_key('OHLCV_JUST_UPDATED_TO_DB', 'True')

    def save_mass_marketcapital(self):
        print('MARKET CAPITAL 데이터 저장:')
        cached_data = self.redis.get_list('mass_marketcapital')
        print('FnGuide 데이터: {}'.format(len(cached_data)))

        date = ''
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
        print('정보 업데이트: {}개, 날짜: {}'.format(len(bulk_data_list), date))
        self.redis.set_key('MARKETCAPITAL_JUST_UPDATED_TO_DB', 'True')

    def save_mass_buysell(self):
        print('BUYSELL 데이터 저장:')
        cached_data = self.redis.get_list('mass_buysell')
        print('FnGuide 데이터: {}'.format(len(cached_data)))

        date = ''
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
        print('정보 업데이트: {}개, 날짜: {}'.format(len(bulk_data_list), date))
        self.redis.set_key('BUYSELL_JUST_UPDATED_TO_DB', 'True')

    def save_mass_factor(self):
        print('FACTOR 데이터 저장:')
        cached_data = self.redis.get_list('mass_factor')
        print('FnGuide 데이터: {}'.format(len(cached_data)))

        date = ''
        bulk_data_list = []
        for data in cached_data:
            data_list = data.split('|')
            date = data_list[0]
            code = data_list[1]
            name = data_list[2]
            per = data_list[3].strip().replace(',', '')
            pbr = data_list[4].strip().replace(',', '')
            pcr = data_list[5].strip().replace(',', '')
            psr = data_list[6].strip().replace(',', '')
            divid_yield = data_list[7].strip().replace(',', '')

            # additional processing for factors, give None is non-existent
            if per == '':
                per = None
            else:
                per = float(per)

            if pbr == '':
                pbr = None
            else:
                pbr = float(pbr)

            if pcr == '':
                pcr = None
            else:
                pcr = float(pcr)

            if psr == '':
                psr = None
            else:
                psr = float(psr)

            if divid_yield == '':
                divid_yield = None
            else:
                divid_yield = float(divid_yield)

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
        print('정보 업데이트: {}개, 날짜: {}'.format(len(bulk_data_list), date))
        self.redis.set_key('FACTOR_JUST_UPDATED_TO_DB', 'True')

    ###################
    ##### 캐시 작업 #####
    ###################
    def cache_tickers(self):
        print('CACHE TICKERS')
        # 코스피, 코스닥 소속 모든 종목의 코드값을 리스트 형식으로 저장한다
        kospi_tickers_key = 'KOSPI_TICKERS'
        kosdaq_tickers_key = 'KOSDAQ_TICKERS'
        etf_tickers_key = 'ETF_TICKERS'

        key_exists = self.redis.key_exists(kospi_tickers_key)
        if key_exists == False:
            print('키값이 존재하지 않습니다.')
        else:
            # 이미 키값이 존재한다면, 키값들을 지우고 다시 캐싱한다
            self.redis.del_key(kospi_tickers_key)
            self.redis.del_key(kosdaq_tickers_key)
            self.redis.del_key(etf_tickers_key)
        kospi_tickers = self.redis.get_list(kospi_tickers_key.lower())
        kospi_data = [kospi_tickers_key] + kospi_tickers
        self.redis.set_list(kospi_data)

        kosdaq_tickers = self.redis.get_list(kosdaq_tickers_key.lower())
        kosdaq_data = [kosdaq_tickers_key] + kosdaq_tickers
        self.redis.set_list(kosdaq_data)

        etf_tickers = self.redis.get_list(etf_tickers_key.lower())
        etf_data = [etf_tickers_key] + etf_tickers
        self.redis.set_list(etf_data)
        print('KOSPI_TICKERS, KOSDAQ_TICKERS, ETF_TICKERS 새팅 완료')

    def cache_index_data(self):
        print('CACHE INDEX DATA')
        # 우선 인덱스 티커값들을 리스트로 캐싱한다
        index_tickers_key = 'INDEX_TICKERS'
        index_codes = [index_tickers_key]

        for key, val in MARKET_CODES.items():
            index_codes.append(val)

        key_exists = self.redis.key_exists(index_tickers_key)
        if key_exists == False:
            print('키값이 존재하지 않습니다.')
        else:
            self.redis.del_key(index_tickers_key)
            self.redis.set_list(index_codes)

        for index in index_codes:
            if index == 'INDEX_TICKERS':
                continue
            index_data = Index.objects.filter(code=index).order_by('date')
            index_data = list(index_data.values('date', 'code', 'name', 'cls_prc', 'trd_qty'))
            index_df = pd.DataFrame(index_data)
            key = '{}_INDEX'.format(index)
            key_exists = self.redis.key_exists(key)
            if key_exists != False:
                self.redis.del_key(key)
                print('{} 이미 있음, 삭제하는 중...'.format(key))
            response = self.redis.set_df(key, index_df)
            if response == True:
                print('{} 캐싱 성공'.format(key))
                print('Data count: {}'.format(len(index_data)))
            else:
                print('{} 캐싱 FAILED'.format(key))

    def _get_tickers(self):
        kospi_tickers = self.redis.get_list('KOSPI_TICKERS')
        kosdaq_tickers = self.redis.get_list('KOSDAQ_TICKERS')
        tickers = kospi_tickers + kosdaq_tickers
        return tickers

    def cache_ohlcv_data(self):
        print('CACHE OHLCV DATA')
        tickers = self._get_tickers()
        print('Total ticker count: {}개'.format(len(tickers)))
        ticker_count = 0
        for ticker in tickers:
            ticker_count += 1
            ohlcv = OHLCV.objects.filter(code=ticker).order_by('date')
            ohlcv_data = list(ohlcv.values('date', 'code', 'cls_prc', 'adj_prc', 'trd_qty', 'shtsale_trd_qty'))
            ohlcv_df = pd.DataFrame(ohlcv_data)
            key = '{}_OHLCV'.format(ticker)
            key_exists = self.redis.key_exists(key)
            if key_exists != False:
                self.redis.del_key(key)
                print('{} 이미 있음, 삭제하는 중...'.format(key))
            response = self.redis.set_df(key, ohlcv_df)
            if response == True:
                print('{} / {} - {} 캐싱 성공'.format(ticker_count, len(tickers), key))
                print('Data count: {}'.format(len(ohlcv_data)))
            else:
                print('{} / {} - {} 캐싱 FAILED'.format(ticker_count, len(tickers), key))

    def cache_full_ohlcv_data(self):
        print('CACHE FULL OHLCV DATA')
        tickers = self._get_tickers()
        print('Total ticker count: {}개'.format(len(tickers)))
        ticker_count = 0
        for ticker in tickers:
            ticker_count += 1
            ohlcv = OHLCV.objects.filter(code=ticker).order_by('date')
            ohlcv_data = list(ohlcv.values('date',
                                           'code',
                                           'strt_prc',
                                           'high_prc',
                                           'low_prc',
                                           'cls_prc',
                                           'adj_prc',
                                           'trd_qty',
                                           'shtsale_trd_qty'))
            ohlcv_df = pd.DataFrame(ohlcv_data)
            key = '{}_FULL_OHLCV'.format(ticker)
            key_exists = self.redis.key_exists(key)
            if key_exists != False:
                self.redis.del_key(key)
                print('{} 이미 있음, 삭제하는 중...'.format(key))
            response = self.redis.set_df(key, ohlcv_df)
            if response == True:
                print('{} / {} - {} 캐싱 성공'.format(ticker_count, len(tickers), key))
                print('Data count: {}'.format(len(ohlcv_data)))
            else:
                print('{} / {} - {} 캐싱 FAILED'.format(ticker_count, len(tickers), key))

    def cache_buysell_data(self):
        print('CACHE BUYSELL DATA')
        # 모든 종목의 BuySell 모델 데이터를 캐싱한다
        tickers = self._get_tickers()
        print('Total ticker count: {}개'.format(len(tickers)))
        ticker_count = 0
        for ticker in tickers:
            ticker_count += 1
            buysell = BuySell.objects.filter(code=ticker).order_by('date')
            buysell_data = list(buysell.values('date',
                                               'code',
                                               'forgn_b',
                                               'forgn_s',
                                               'forgn_n',
                                               'private_b',
                                               'private_s',
                                               'private_n',
                                               'inst_sum_b',
                                               'inst_sum_s',
                                               'inst_sum_n',
                                               'trust_b',
                                               'trust_s',
                                               'trust_n',
                                               'pension_b',
                                               'pension_s',
                                               'pension_n',
                                               'etc_inst_b',
                                               'etc_inst_s',
                                               'etc_inst_n'))
            buysell_df = pd.DataFrame(buysell_data)
            key = '{}_BUYSELL'.format(ticker)
            key_exists = self.redis.key_exists(key)
            if key_exists != False:
                self.redis.del_key(key)
                print('{} 이미 있음, 삭제하는 중...'.format(key))
            response = self.redis.set_df(key, buysell_df)
            if response == True:
                print('{} / {} - {} 캐싱 성공'.format(ticker_count, len(tickers), key))
                print('Data count: {}'.format(len(buysell_df)))
            else:
                print('{} / {} - {} 캐싱 FAILED'.format(ticker_count, len(tickers), key))
