#! /bin/bash

# Backing up Django DB

su keystone -c "psql -c \"\copy stockapi_index (date, code, name, strt_prc, high_prc, low_prc, cls_prc, trd_qty, trd_amt) to '~/backup/index.csv' delimiter ',';\""

su keystone -c "psql -c \"\copy stockapi_buysell (date, code, name, forgn_b, forgn_s, forgn_n, private_b, private_s, private_n, inst_sum_b, inst_sum_s, inst_sum_n, trust_b, trust_s, trust_n, pension_b, pension_s, pension_n, etc_inst_b, etc_inst_s, etc_inst_n) to '~/backup/buysell.csv' delimiter ',';\""

su keystone -c "psql -c \"\copy stockapi_ohlcv (date, code, name, strt_prc, high_prc, low_prc, cls_prc, adj_prc, trd_qty, trd_amt, shtsale_trd_qty) to '~/backup/ohlcv.csv' delimiter ',';\""

su keystone -c "psql -c \"\copy stockapi_factor (date, code, name, per, pbr, pcr, psr, divid_yield) to '~/backup/factor.csv' delimiter ',';\""
