#! /bin/bash

# Backing up Django DB

##### Date backup #####
su keystone -c "psql -c \"\copy stocks_date (date) to '~/backup/date.csv' delimiter ',';\""

##### Ticker backup #####
su keystone -c "psql -c \"\copy stocks_ticker (date, code, name, market_type, state) to '~/backup/ticker.csv' delimiter ',';\""

##### StockInfo backup #####
su keystone -c "psql -c \"\copy stocks_stockinfo (date, code, name, stk_kind, mkt_gb, mkt_cap, mkt_cap_size, frg_hlg, mgt_gb) to '~/backup/stockinfo.csv' delimiter ',';\""

##### Index backup #####
su keystone -c "psql -c \"\copy stocks_index (date, code, name, strt_prc, high_prc, low_prc, cls_prc, trd_qty, trd_amt) to '~/backup/index.csv' delimiter ',';\""

##### ETF backup #####
su keystone -c "psql -c \"\copy stocks_etf (date, code, name, cls_prc, trd_qty, trd_amt, etf_nav, spread) to '~/backup/etf.csv' delimiter ',';\""

##### OHLCV backup #####
su keystone -c "psql -c \"\copy stocks_ohlcv (date, code, name, strt_prc, high_prc, low_prc, cls_prc, adj_prc, trd_qty, trd_amt, shtsale_trd_qty) to '~/backup/ohlcv.csv' delimiter ',';\""

##### MarketCapital backup #####
su keystone -c "psql -c \"\copy stocks_marketcapital (date, code, name, comm_stk_qty, pret_stk_qty) to '~/backup/marketcapital.csv' delimiter ',';\""

##### BuySell backup #####
su keystone -c "psql -c \"\copy stocks_buysell (date, code, name, forgn_b, forgn_s, forgn_n, private_b, private_s, private_n, inst_sum_b, inst_sum_s, inst_sum_n, trust_b, trust_s, trust_n, pension_b, pension_s, pension_n, etc_inst_b, etc_inst_s, etc_inst_n) to '~/backup/buysell.csv' delimiter ',';\""

##### Factor backup #####
su keystone -c "psql -c \"\copy stocks_factor (date, code, name, per, pbr, pcr, psr, divid_yield) to '~/backup/factor.csv' delimiter ',';\""
