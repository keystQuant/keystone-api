'''
Date : 7/23/2018
author : robby
sprint : 3rd
description : ppt 데이터 데이터 베이스로 전송
'''

import pandas as pd
import os, sys, glob


start_path = os.getcwd()
proj_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "keystone.settings")
sys.path.append(proj_path)
os.chdir(proj_path)

from django.core.wsgi import get_wsgi_application
application = get_wsgi_application()

from stocks.models import MkfGro, MkfVal

data_list = os.listdir('../data')
print(os.getcwd())

csv_data = pd.read_excel('../data/' + data_list[0])
print(csv_data.columns)
print(len(csv_data.columns))
print(csv_data.ix[1])

def data_import():
    gro_list = []
    val_list = []
    for excel_file in data_list:
        print(excel_file)
        mkf_data = pd.read_excel('../data/' + excel_file)
        for i in range(mkf_data.shape[0]):
            print(mkf_data.iloc[i]['name'])
            date = mkf_data.iloc[i]['date']
            code = mkf_data.iloc[i]['code']
            name = mkf_data.iloc[i]['name']
            type = mkf_data.iloc[i]['type']
            sector = mkf_data.iloc[i]['sector']
            if excel_file == 'mkfgro.csv':
                mkf_gro_obj = MkfGro(date=date, code=code, name=name, type=type, sector=sector)
                gro_list.append(mkf_gro_obj)
            elif excel_file == 'mkfval.csv':
                mkf_val_obj = MkfVal(date=date, code=code, name=name, type=type, sector=sector)
                val_list.append(mkf_val_obj)
            else:
                print("파일이 없습니다")
        if excel_file == 'mkfgro.csv':
            MkfGro.objects.bulk_create(gro_list)
        elif excel_file == 'mkfval.csv':
            MkfVal.objects.bulk_create(val_list)
        else:
            print("파일이 없습니다")
