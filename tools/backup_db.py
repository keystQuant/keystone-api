import os
import pandas as pd

from stocks.models import (
    BuySell,
    Factor,
    Index,
    OHLCV,
)


# 디비를 없애면서 데이터를 backup 폴더 안에 넣어뒀다
# 백업 데이터를 새로 만든 데이터베이스 서버로 넣어준다
class BackupDB:

    def __init__(self):
        print('백업 시작합니다')

        pwd = os.getcwd()
        print('현재 working directory: {} 입니다'.format(pwd))

        dir = os.listdir()
        print('현재 디렉터리의 폴더/파일 리스트입니다: {}'.format(dir))

        app_directory_name = input('프로젝트 디렉터리 이름을 입력하세요: ')
        os.chdir(app_directory_name)

        pwd = os.getcwd()
        print('현재 working directory: {} 입니다'.format(pwd))

    def backup(self):
        # self._backup_buysell()
        # self._backup_factor()
        # self._backup_index()
        self._backup_ohlcv()

    def _backup_buysell(self):
        print('BuySell 데이터 백업 시작')
        df = pd.read_csv('./backup/buysell.csv', low_memory=False, header=None)
        total_data_num = len(df)
        for i in range(total_data_num):
            if i == 0:
                to_save_data_list = []
            if i % 10000 == 0:
                BuySell.objects.bulk_create(to_save_data_list)
                print('BuySell 데이터 {} / {} 저장 완료'.format(i, total_data_num))
                to_save_data_list = []
            row = df.iloc[i]
            date = row[0]
            code = row[1]
            name = row[2]
            forgn_b = row[3]
            forgn_s = row[4]
            forgn_n = row[5]
            private_b = row[6]
            private_s = row[7]
            private_n = row[8]
            inst_sum_b = row[9]
            inst_sum_s = row[10]
            inst_sum_n = row[11]
            trust_b = row[12]
            trust_s = row[13]
            trust_n = row[14]
            pension_b = row[15]
            pension_s = row[16]
            pension_n = row[17]
            etc_inst_b = row[18]
            etc_inst_s = row[19]
            etc_inst_n = row[20]
            data_inst = BuySell(date=date,
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
            to_save_data_list.append(data_inst)
            # data_inst.save()
        if len(to_save_data_list) > 0:
            BuySell.objects.bulk_create(to_save_data_list)
        print('BuySell 데이터 저장 완료')

    def _backup_factor(self):
        print('Factor 데이터 백업 시작')
        df = pd.read_csv('./backup/factor.csv', low_memory=False, header=None)
        total_data_num = len(df)
        for i in range(total_data_num):
            # if i < 1190000:
            #     to_save_data_list = []
            #     continue
            if i == 0:
                to_save_data_list = []
            if i % 10000 == 0:
                Factor.objects.bulk_create(to_save_data_list)
                print('Factor 데이터 {} / {} 저장 완료'.format(i, total_data_num))
                to_save_data_list = []
            row = df.iloc[i]
            date = row[0]
            code = row[1]
            name = row[2]
            per = None if row[3] == '\\N' else row[3]
            pbr = None if row[4] == '\\N' else row[4]
            pcr = None if row[5] == '\\N' else row[5]
            psr = None if row[6] == '\\N' else row[6]
            divid_yield = None if row[7] == '\\N' else row[7]
            data_inst = Factor(date=date,
                               code=code,
                               name=name,
                               per=per,
                               pbr=pbr,
                               pcr=pcr,
                               psr=psr,
                               divid_yield=divid_yield)
            to_save_data_list.append(data_inst)
            # data_inst.save()
        if len(to_save_data_list) > 0:
            Factor.objects.bulk_create(to_save_data_list)
        print('Factor 데이터 저장 완료')

    def _backup_index(self):
        print('Index 데이터 백업 시작')
        """
        CSV 파일 인식이 제대로 되지 않아 한 번 전처리를 한 후에 다시 시도:
        문제: \,가 있어서 delimiter가 제대로 인식되지 않음
        해결 코드:

        import os
        os.chdir('backup')
        with open('index.csv', 'r') as file:
            filedata = file.read()
        filedata = filedata.replace('\,', '!')
        filedata = filedata.replace(',', '|')
        filedata = filedata.replace('!', ',')
        with open('index_2.csv', 'w') as file:
            file.write(filedata)

        --> \,를 !로 대체하고, 실제 delimiter인 ,를 |로 바꾸고 !를 다시 ,로 수정
        """
        df = pd.read_csv('./backup/index_2.csv', sep='|', engine='python', header=None)
        total_data_num = len(df)
        for i in range(total_data_num):
            if i == 0:
                to_save_data_list = []
            if i % 10000 == 0:
                Index.objects.bulk_create(to_save_data_list)
                print('Index 데이터 {} / {} 저장 완료'.format(i, total_data_num))
                to_save_data_list = []
            row = df.iloc[i]
            date = row[0]
            code = row[1]
            name = row[2]
            strt_prc = row[3]
            high_prc = row[4]
            low_prc = row[5]
            cls_prc = row[6]
            trd_qty = row[7]
            trd_amt = row[8]
            data_inst = Index(date=date,
                              code=code,
                              name=name,
                              strt_prc=strt_prc,
                              high_prc=high_prc,
                              low_prc=low_prc,
                              cls_prc=cls_prc,
                              trd_qty=trd_qty,
                              trd_amt=trd_amt)
            to_save_data_list.append(data_inst)
            # data_inst.save()
        if len(to_save_data_list) > 0:
            Index.objects.bulk_create(to_save_data_list)
        print('Index 데이터 저장 완료')

    def _backup_ohlcv(self):
        print('OHLCV 데이터 백업 시작')
        df = pd.read_csv('./backup/ohlcv.csv', low_memory=False, header=None)
        total_data_num = len(df)
        for i in range(total_data_num):
            if i < 1590000:
                to_save_data_list = []
                continue
            if i == 0:
                to_save_data_list = []
            if i % 10000 == 0:
                OHLCV.objects.bulk_create(to_save_data_list)
                print('OHLCV 데이터 {} / {} 저장 완료'.format(i, total_data_num))
                to_save_data_list = []
            row = df.iloc[i]
            date = row[0]
            code = row[1]
            name = row[2]
            strt_prc = row[3]
            high_prc = row[4]
            low_prc = row[5]
            cls_prc = row[6]
            adj_prc = row[7]
            trd_qty = row[8]
            trd_amt = row[9]
            shtsale_trd_qty = row[10]
            data_inst = OHLCV(date=date,
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
            to_save_data_list.append(data_inst)
            # data_inst.save()
        if len(to_save_data_list) > 0:
            OHLCV.objects.bulk_create(to_save_data_list)
        print('OHLCV 데이터 저장 완료')
