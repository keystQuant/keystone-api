import datetime

from .models import (
    Date,
    Ticker,
    Index,
    ETF,
    OHLCV,
    BuySell,
    MarketCapital,
    Factor,
)

from utils.cache import RedisClient

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

        dates_qs = Date.objects.all().distinct('date').values_list('date')
        dates_in_db = [date[0] for date in dates_qs]
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

    def save_kospi_tickers(self):
        all_tickers = self.redis.get_list('kospi_tickers') # 리스트값이다
        print('FnGuide 데이터: {}'.format(len(all_tickers)))

        tickers_qs = Ticker.objects.filter(market_type='KOSPI')
        tickers_in_db = ['{}|{}'.format(ticker.code, ticker.name) for ticker in tickers_qs]
        print('DB 데이터: {}'.format(len(tickers_in_db)))

        # 거래 중지 종목: 데이터 state 0으로 업데이트하기
        stopped_tickers = list(set(tickers_in_db) - set(all_tickers))
        # 새로 저장해야할 종목들
        to_save_tickers = list(set(all_tickers) - set(tickers_in_db))
        # 날짜만 업데이트하면 되는 종목들
        update_date_tickers = list(set(tickers_in_db) - set(stopped_tickers))

        stopped_tickers_qs = Ticker.objects.filter(code__in=[ticker[0] for ticker in stopped_tickers]).delete()
        bulk_tickers_list = []
        for ticker_info in stopped_tickers:
            ticker = ticker_info.split('|')[0]
            name = ticker_info.split('|')[1]
            ticker_inst = Ticker(date=today_date,
                                 code=ticker,
                                 name=name,
                                 market_type='KOSPI',
                                 state=0)
            bulk_tickers_list.append(ticker_inst)
        print('거래정지 종목 업데이트: {}개'.format(len(stopped_tickers)))

        today_date = datetime.datetime.now().strftime('%Y%M%d')

        for ticker_info in to_save_tickers:
            ticker = ticker_info.split('|')[0]
            name = ticker_info.split('|')[1]
            ticker_inst = Ticker(date=today_date,
                                 code=ticker,
                                 name=name,
                                 market_type='KOSPI',
                                 state=1)
            bulk_tickers_list.append(ticker_inst)
        print('새로 종목 업데이트: {}개'.format(len(to_save_tickers)))

        update_date_tickers_qs = Ticker.objects.filter(code__in=[ticker[0] for ticker in update_date_tickers]).delete()
        for ticker_inst in update_date_tickers:
            ticker = ticker_info.split('|')[0]
            name = ticker_info.split('|')[1]
            ticker_inst = Ticker(date=today_date,
                                 code=ticker,
                                 name=name,
                                 market_type='KOSPI',
                                 state=1)
            bulk_tickers_list.append(ticker_inst)
        print('종목 날짜 업데이트: {}개'.format(len(update_date_tickers)))
        Ticker.objects.bulk_create(bulk_tickers_list)

    def save_kosdaq_tickers(self):
        all_tickers = self.redis.get_list('kosdaq_tickers') # 리스트값이다
        print('FnGuide 데이터: {}'.format(len(all_tickers)))

        tickers_qs = Ticker.objects.filter(market_type='KOSDAQ')
        tickers_in_db = [ticker.code for ticker in tickers_qs]
        print('DB 데이터: {}'.format(len(tickers_in_db)))

        # 거래 중지 종목: 데이터 state 0으로 업데이트하기
        stopped_tickers = list(set(tickers_in_db) - set(all_tickers))
        # 새로 저장해야할 종목들
        to_save_tickers = list(set(all_tickers) - set(tickers_in_db))
        # 날짜만 업데이트하면 되는 종목들
        update_date_tickers = list(set(tickers_in_db) - set(stopped_tickers))

        stopped_tickers_qs = Ticker.objects.filter(code__in=stopped_tickers)
        for ticker_inst in stopped_tickers_qs:
            ticker_inst.state = 0
            ticker_inst.save()
        print('거래정지 종목 업데이트: {}개'.format(len(stopped_tickers)))

        today_date = datetime.datetime.now().strftime('%Y%M%d')

        for ticker in to_save_tickers:
            ticker_inst = Ticker(date=today_date,
                                 code=ticker,
                                 name='',
                                 market_type='KOSDAQ',
                                 state=1)
            ticker_inst.save()
        print('새로 종목 업데이트: {}개'.format(len(stopped_tickers)))

        update_date_tickers_qs = Ticker.objects.filter(code__in=update_date_tickers)
        for ticker_inst in update_date_tickers_qs:
            ticker_inst.date = today_date
            ticker_inst.save()
        print('종목 날짜 업데이트: {}개'.format(len(update_date_tickers)))

    # def get_dates_in_db(self, task_name):
    #     # 크롤링 마무리하여 데이터베이스에 저장된 날짜들을 리턴한다
    #     if 'INDEX' in task_name:
    #         qs = Index.objects.all()
    #     elif 'OHLCV' in task_name:
    #         qs = OHLCV.objects.all()
    #     elif 'BUYSELL' in task_name:
    #         qs = BuySell.objects.all()
    #     elif 'FACTOR' in task_name:
    #         qs = Factor.objects.all()
    #     dates_in_db = list(qs.distinct('date').values_list('date'))
    #     dates_in_db = [date[0] for date in dates_in_db]
    #     return dates_in_db
