"""
데이터베이스를 따로 분리해서 migration이 안 될 경우:
1. python manage.py sqlmigrate stocks 0001 --> stocks앱의 모델들의 SQL문을 프린트한다.
2. migrate하고 싶은 모델의 SQL문을 복사하여 keystone-db-server로 접속한다.
3. 서버에서 docker ps를 친 후 PostgreSQL 컨테이너가 실행중인지 본다.
4. docker exec -it keystone-db /bin/bash를 쳐서 도커 컨테이너 내부로 접속한다.
5. psql --> \connect keystone --> \dt를 하여 현재 migrate가 됐는지 본다.
6. migrate가 안 되었다면, 위에서 복사한 SQL문을 실행시킨다.
7. ALTER TABLE <tablename> OWNER TO <username>; 를 실행시켜서 권한을 바꿔준다.
"""
from django.db import models


class Date(models.Model):
    date = models.CharField(max_length=10)

    def __str__(self):
        return self.date


class Ticker(models.Model):
    date = models.CharField(max_length=10)
    code = models.CharField(max_length=20)
    name = models.CharField(max_length=50)
    market_type = models.CharField(max_length=10)
    state = models.BooleanField(default=True) # 거래되고 있는 종목이면 1, 아니면 0

    def __str__(self):
        return '{} {}'.format(self.code, self.name)


class StockInfo(models.Model):
    date = models.CharField(max_length=10)
    code = models.CharField(max_length=20)
    name = models.CharField(max_length=50)
    stk_kind = models.CharField(max_length=50) # 산업 구분
    mkt_gb = models.CharField(max_length=10) # 코스피, 코스닥 시장
    mkt_cap = models.IntegerField() # 시가총액
    mkt_cap_size = models.CharField(max_length=10) # 대형주, 중형주, 소형주, 제외
    frg_hlg = models.FloatField() # 외인 지분 (foreign holding)
    mgt_gb = models.CharField(max_length=20) # 정상, 정지, 관리 등

    def __str__(self):
        return '{} {}'.format(self.date, self.name)


class Index(models.Model):
    ### U_CD, U_NM, STRT_PRC, HIGH_PRC, LOW_PRC, CLS_PRC, TRD_QTY, TRD_AMT
    date = models.CharField(max_length=10)
    code = models.CharField(max_length=20)
    name = models.CharField(max_length=50)
    strt_prc = models.FloatField() # 시가
    high_prc = models.FloatField() # 고가
    low_prc = models.FloatField() # 저가
    cls_prc = models.FloatField() # 종가
    trd_qty = models.FloatField() # 거래량 (x1000)
    trd_amt = models.FloatField() # 거래대금 (x1000000)

    def __str__(self):
        return '{} {}'.format(self.date, self.name)


class ETF(models.Model):
    ### GICODE, ITEMABBRNM, CLS_PRC, TRD_QTY, TRD_AMT, ETF_NAV, SPREAD
    date = models.CharField(max_length=10)
    code = models.CharField(max_length=20)
    name = models.CharField(max_length=50)
    cls_prc = models.FloatField()
    trd_qty = models.IntegerField()
    trd_amt = models.IntegerField() # (x1000)
    etf_nav = models.FloatField()
    spread = models.FloatField()

    def __str__(self):
        return '{} {}'.format(self.date, self.name)


class OHLCV(models.Model):
    ### GICODE, ITEMABBRNM, STRT_PRC, HIGH_PRC, LOW_PRC, CLS_PRC, ADJ_PRC, TRD_QTY, TRD_AMT, SHTSALE_TRD_QTY
    date = models.CharField(max_length=10)
    code = models.CharField(max_length=20)
    name = models.CharField(max_length=50)
    strt_prc = models.IntegerField()
    high_prc = models.IntegerField()
    low_prc = models.IntegerField()
    cls_prc = models.IntegerField()
    adj_prc = models.IntegerField(blank=True, null=True)
    trd_qty = models.FloatField() # (x1000)
    trd_amt = models.FloatField(blank=True, null=True) # (x1000000)
    shtsale_trd_qty = models.FloatField() # (x1000)

    def __str__(self):
        return '{} {}'.format(self.date, self.name)


class MarketCapital(models.Model):
    ### GICODE, ITEMABBRNM, COMM_STK_QTY, PREF_STK_QTY
    date = models.CharField(max_length=10)
    code = models.CharField(max_length=20)
    name = models.CharField(max_length=50)
    comm_stk_qty = models.IntegerField() # (x1000)
    pref_stk_qty = models.IntegerField() # (x1000)

    def __str__(self):
        return '{} {}'.format(self.date, self.name)


class BuySell(models.Model):
    ### GICODE, GINAME, FORGN_B/S/N, PRIVATE_B/S/N, INST_SUM_B/S/N, TRUST_B/S/N, PENSTION_B/S/N, ETC_INST_B/S/B (x1000)
    date = models.CharField(max_length=10)
    code = models.CharField(max_length=20)
    name = models.CharField(max_length=50)
    forgn_b = models.IntegerField()
    forgn_s = models.IntegerField()
    forgn_n = models.IntegerField()
    private_b = models.IntegerField()
    private_s = models.IntegerField()
    private_n = models.IntegerField()
    inst_sum_b = models.IntegerField()
    inst_sum_s = models.IntegerField()
    inst_sum_n = models.IntegerField()
    trust_b = models.IntegerField() # 투자신탁
    trust_s = models.IntegerField()
    trust_n = models.IntegerField()
    pension_b = models.IntegerField() # 연기금
    pension_s = models.IntegerField()
    pension_n = models.IntegerField()
    etc_inst_b = models.IntegerField()
    etc_inst_s = models.IntegerField()
    etc_inst_n = models.IntegerField()

    def __str__(self):
        return '{} {}'.format(self.date, self.name)


class Factor(models.Model):
    ### GICODE, ITEMABBRNM, PER, PBR, PCR, PSR, DIVID_YIELD
    date = models.CharField(max_length=10)
    code = models.CharField(max_length=20)
    name = models.CharField(max_length=50)
    per = models.FloatField(blank=True, null=True)
    pbr = models.FloatField(blank=True, null=True)
    pcr = models.FloatField(blank=True, null=True)
    psr = models.FloatField(blank=True, null=True)
    divid_yield = models.FloatField(blank=True, null=True)

    def __str__(self):
        return '{} {}'.format(self.date, self.name)
