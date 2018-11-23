from app.fetcher.common import Interface
from conf.myapp import ts_token
from app.saver.tables import fields_map
from app.saver.logic import DB

import tushare as ts
import time


class Ts(Interface):

    def __init__(self, start_date='', end_date=''):
        self.pro = ts.pro_api(ts_token)
        self.start_date = start_date
        self.end_date = end_date
        self.code_list = []
        self.trade_dates = []

    def set_trade_dates(self):
        api = 'trade_cal'
        trade_cal = DB.get_open_cal_date(self.start_date, self.end_date)
        if trade_cal.empty:
            now_rows = self.pro.query(api, fields=fields_map[api], start_date=self.start_date, end_date=self.end_date)
            now_rows = now_rows[fields_map[api]]
            now_rows.to_sql(api, DB.engine, index=False, if_exists='append', chunksize=1000)
            if not now_rows.empty:
                trade_cal = DB.get_open_cal_date(self.start_date, self.end_date)
        self.trade_dates = trade_cal

    def update_trade_cal(self):
        api = 'trade_cal'
        existed_cal_date = DB.get_cal_date(start_date='', end_date=self.end_date)
        new_rows = self.pro.query(api, fields=fields_map[api], start_date=existed_cal_date.iloc[-1]['cal_date'], end_date=self.end_date)
        if not existed_cal_date.empty:
            new_rows = new_rows[~new_rows['cal_date'].isin(existed_cal_date['cal_date'])]
        if not new_rows.empty:
            new_rows = new_rows[fields_map[api]]
            new_rows.sort_values(by='cal_date', inplace=True)
            new_rows.to_sql(api, DB.engine, index=False, if_exists='append', chunksize=1000)

    def set_code_list(self):
        code_list = DB.get_code_list(list_status='L')
        self.code_list = code_list

    def update_stock_basic(self):
        api = 'stock_basic'
        existed_code_list = DB.get_code_list()
        new_rows = self.pro.query(api, list_status='L', fields=fields_map[api])
        disappear_rows = self.pro.query(api, list_status='D', fields=fields_map[api])
        if not existed_code_list.empty:
            new_rows = new_rows[~new_rows['ts_code'].isin(existed_code_list['ts_code'])]
        else:
            new_rows = new_rows.append(disappear_rows)

        if not new_rows.empty:
            avail_recorders = new_rows[fields_map[api]]
            avail_recorders.to_sql(api, DB.engine, index=False, if_exists='append', chunksize=1000)

        if not existed_code_list.empty and not disappear_rows.empty:
            new_disappear_rows = existed_code_list[existed_code_list['ts_code'].isin(disappear_rows['ts_code'])]
            new_disappear_rows = new_disappear_rows[new_disappear_rows['list_status'] == 'L']
            if not new_disappear_rows.empty:
                avail_disappear_recorders = disappear_rows[['delist_date', 'ts_code']]
                avail_disappear_recorders = avail_disappear_recorders[avail_disappear_recorders['ts_code'].isin(new_disappear_rows['ts_code'])]
                for delist_date, ts_code in avail_disappear_recorders.values:
                    DB.update_delist_date(delist_date, ts_code)

    def query(self, api):
        # 按trade_date依次拉取所有股票信息
        for date_id, cal_date in self.trade_dates[['date_id', 'cal_date']].values:
            flag = True
            while flag:
                try:
                    self.update_by_trade_date(api, date_id, cal_date)
                    flag = False
                except BaseException as e:
                    # print(e)
                    time.sleep(10)
                    self.update_by_trade_date(api, date_id, cal_date)

    def update_by_trade_date(self, api, date_id, cal_date):
        new_rows = self.pro.query(api, trade_date=cal_date)
        if not new_rows.empty:
            existed_codes = DB.get_existed_codes(table_name=api, date_id=date_id)
            if not existed_codes.empty:
                new_rows = new_rows[~new_rows['ts_code'].isin(existed_codes['ts_code'])]
            new_rows = new_rows.merge(self.trade_dates, left_on='trade_date', right_on='cal_date')
            new_rows = self.code_list.merge(new_rows, on='ts_code')
            avail_recorders = new_rows[fields_map[api]]
            avail_recorders.to_sql(api, DB.engine, index=False, if_exists='append', chunksize=1000)

