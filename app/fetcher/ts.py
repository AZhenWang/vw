from app.fetcher.common import Interface
from conf.myapp import ts_token, init_date
from app.saver.tables import fields_map
from app.saver.logic import DB
from app.saver.service.fina import Fina

import tushare as ts
import time


class Ts(Interface):

    def __init__(self, start_date='', end_date=''):
        self.pro = ts.pro_api(ts_token)
        self.start_date = start_date
        self.end_date = end_date
        self.code_list = []
        self.index_list = []
        self.trade_dates = []
        self.all_dates = []
        self.fut_list = []

    def set_trade_dates(self):
        api = 'trade_cal'
        all_dates = DB.get_cal_date(self.start_date, self.end_date)
        if all_dates.empty:
            now_rows = self.pro.query(api, fields=fields_map[api], start_date=self.start_date, end_date=self.end_date)
            now_rows = now_rows[fields_map[api]]
            now_rows.to_sql(api, DB.engine, index=False, if_exists='append', chunksize=1000)
            if not now_rows.empty:
                all_dates = DB.get_cal_date(self.start_date, self.end_date)
        self.trade_dates = all_dates[all_dates['is_open'] == 1]
        self.all_dates = all_dates

    def update_trade_cal(self):
        api = 'trade_cal'
        existed_cal_date = DB.get_cal_date(start_date=init_date, end_date=self.end_date)
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
            avail_recorders = new_rows[~new_rows['ts_code'].isin(existed_code_list['ts_code'])]
        else:
            avail_recorders = new_rows.append(disappear_rows)

        if not avail_recorders.empty:
            avail_recorders = avail_recorders[fields_map[api]]
            avail_recorders.to_sql(api, DB.engine, index=False, if_exists='append', chunksize=1000)

        # 更新退市股信息
        if not existed_code_list.empty and not disappear_rows.empty:
            new_disappear_rows = existed_code_list[existed_code_list['ts_code'].isin(disappear_rows['ts_code'])]
            new_disappear_rows = new_disappear_rows[new_disappear_rows['list_status'] == 'L']
            if not new_disappear_rows.empty:
                avail_disappear_recorders = disappear_rows[['delist_date', 'ts_code']]
                avail_disappear_recorders = avail_disappear_recorders[avail_disappear_recorders['ts_code'].isin(new_disappear_rows['ts_code'])]
                for delist_date, ts_code in avail_disappear_recorders.values:
                    DB.update_delist_date(delist_date, ts_code)

        # 更新name信息
        if not existed_code_list.empty and not new_rows.empty:
            update_rows = new_rows[new_rows['ts_code'].isin(existed_code_list['ts_code'])][~new_rows['name'].isin(existed_code_list['stock_name'])]
            for i in range(len(update_rows)):
                ts_code = update_rows.iloc[i]['ts_code']
                stock_name = update_rows.iloc[i]['name']
                DB.update_stock_name(stock_name, ts_code)

    def update_fut_basic(self):
        """
        期货基本信息
        :return:
        """
        api = 'fut_basic'
        exchange_list = {
            'CFFEX': '中金所',
            'DCE': '大商所',
            'CZCE': '郑商所',
            'SHFE': '上期所',
            'INE': '上海国际能源交易中心'
        }
        existed_index_list = DB.get_fut_list()
        for exchange in exchange_list.keys():
            new_rows = self.pro.query(api, exchange=exchange, fields=fields_map[api])
            if not new_rows.empty:
                # existed_index = existed_index_list[existed_index_list['exchange'] == exchange]
                new_rows = new_rows[~new_rows['ts_code'].isin(existed_index_list['ts_code'])]
            if not new_rows.empty:
                avail_recorders = new_rows[fields_map[api]]
                avail_recorders.to_sql(api, DB.engine, index=False, if_exists='append', chunksize=1000)

    def set_fut_list(self):
        fut_list = DB.get_fut_list()
        self.fut_list = fut_list

    def query_fut(self, api):
        # 按trade_date依次拉取所有股票信息
        for date_id, cal_date in self.trade_dates[['date_id', 'cal_date']].values:
            flag = True
            while flag:
                try:
                    self.update_fut_by_trade_date(api, date_id, cal_date)
                    flag = False
                except BaseException as e:
                    # print(e)
                    time.sleep(10)
                    self.update_fut_by_trade_date(api, date_id, cal_date)

    def update_fut_by_trade_date(self, api, date_id, cal_date):
        new_rows = self.pro.query(api, trade_date=cal_date)
        if not new_rows.empty:
            existed_codes = DB.get_existed_fut(table_name=api, date_id=date_id)
            if not existed_codes.empty:
                new_rows = new_rows[~new_rows['ts_code'].isin(existed_codes['ts_code'])]
            new_rows = new_rows.merge(self.trade_dates, left_on='trade_date', right_on='cal_date')
            new_rows = self.fut_list.merge(new_rows, on='ts_code')
            avail_recorders = new_rows[fields_map[api]]
            avail_recorders.to_sql(api, DB.engine, index=False, if_exists='append', chunksize=1000)

    def update_index_basic(self):
        """
        指数基本信息
        :return:
        """
        api = 'index_basic'
        market_list = {
            'CSI':  '中证指数',
            'SSE': '上交所指数',
            'SZSE': '深交所指数',
            'SW': '申万指数',
        }
        existed_index_list = DB.get_index_list()
        for market in market_list.keys():
            new_rows = self.pro.query(api, market=market, fields=fields_map[api])
            if not new_rows.empty:
                existed_index = existed_index_list[existed_index_list['market'] == market]
                new_rows = new_rows[~new_rows['ts_code'].isin(existed_index['ts_code'])]
            if not new_rows.empty:
                avail_recorders = new_rows[fields_map[api]]
                avail_recorders.to_sql(api, DB.engine, index=False, if_exists='append', chunksize=1000)

    def set_index_list(self):
        index_list = DB.get_index_list()
        self.index_list = index_list

    def query_index(self, api):
        start_date = self.trade_dates.iloc[0]['cal_date']
        end_date = self.trade_dates.iloc[-1]['cal_date']
        index_ts_code = {
            '2054': '000001.SH',
            '2068': '000016.SH',
            '2660': '399001.SZ',
            '2664': '399005.SZ',
            '2665': '399006.SZ',
            '2252': '000905.SH',
            '2199': '000300.SH'
        }

        for index_id in index_ts_code:
            ts_code = index_ts_code[index_id]
            flag = True
            while flag:
                try:
                    self.update_index_by_trade_date(api, index_id, ts_code, start_date, end_date)
                    flag = False
                except BaseException as e:
                    time.sleep(10)
                    self.update_index_by_trade_date(api, index_id, ts_code, start_date, end_date)

    def update_index_by_trade_date(self, api, index_id, ts_code, start_date, end_date):
        new_rows = self.pro.query(api, ts_code=ts_code, start_date=start_date, end_date=end_date)
        if not new_rows.empty:
            existed_dates = DB.get_existed_index(table_name=api, index_id=index_id, start_date=start_date, end_date=end_date)
            if not existed_dates.empty:
                new_rows = new_rows[~new_rows['trade_date'].isin(existed_dates['cal_date'])]
            new_rows = new_rows.merge(self.trade_dates, left_on='trade_date', right_on='cal_date')
            new_rows = self.index_list.merge(new_rows, on='ts_code')
            avail_recorders = new_rows[fields_map[api]]

            avail_recorders.to_sql(api, DB.engine, index=False, if_exists='append', chunksize=1000)

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

    def query_by_ann_date(self, api):
        # 按trade_date依次拉取所有股票信息
        for date_id, cal_date in self.all_dates[['date_id', 'cal_date']].values:
            flag = True
            while flag:
                try:
                    self.update_by_ann_date(api, date_id, cal_date)
                    flag = False
                except BaseException as e:
                    time.sleep(10)
                    self.update_by_ann_date(api, date_id, cal_date)

    def update_by_ann_date(self, api, date_id, cal_date):
        new_rows = self.pro.query(api, anna_date=cal_date)
        if not new_rows.empty:
            existed_codes = DB.get_existed_codes(table_name=api, date_id=date_id)
            if not existed_codes.empty:
                new_rows = new_rows[~new_rows['ts_code'].isin(existed_codes['ts_code'])]
            new_rows = new_rows.merge(self.all_dates, left_on='ann_date', right_on='cal_date')
            new_rows = self.code_list.merge(new_rows, on='ts_code')
            avail_recorders = new_rows[fields_map[api]]
            avail_recorders.to_sql(api, DB.engine, index=False, if_exists='append', chunksize=1000)

    def query_fina_mainbz(self, api):
        codes = self.code_list['ts_code']
        # codes = ['000002.SZ']
        for ts_code in codes:
            flag = True
            while flag:
                try:
                    self.update_fina_mainbz(api, ts_code, self.start_date, self.end_date)
                    flag = False
                    time.sleep(1)
                except BaseException as e:
                    # print(e)
                    time.sleep(5)
                    self.update_fina_mainbz(api, ts_code, self.start_date, self.end_date)

    def update_fina_mainbz(self, api, ts_code, start_date, end_date):
        new_rows = self.pro.query(api, ts_code=ts_code, start_date=start_date, end_date=end_date)
        if not new_rows.empty:
            existed_finas = Fina.get_existed_fina_by_end_date(table_name=api, ts_code=ts_code, start_date=start_date, end_date=end_date)
            if not existed_finas.empty:
                new_rows = new_rows[~new_rows['end_date'].isin(existed_finas['end_date'])]
                new_rows.drop_duplicates('end_date', inplace=True)
            new_rows = self.code_list.merge(new_rows, on='ts_code')
            avail_recorders = new_rows[fields_map[api]]
            avail_recorders.to_sql(api, DB.engine, index=False, if_exists='append', chunksize=3000)

    def query_fina_indicator(self, api):
        codes = self.code_list['ts_code']
        # codes = ['000002.SZ']
        fields = fields_map[api].copy()
        fields.remove('code_id')
        fields.remove('date_id')
        fields.append('ts_code')
        fields.append('ann_date')
        for ts_code in codes:
            flag = True
            while flag:
                try:
                    self.update_fina_indicator(api, fields, ts_code, self.start_date, self.end_date)
                    flag = False
                    time.sleep(1)
                except BaseException as e:
                    # print(e)
                    time.sleep(5)
                    self.update_fina_indicator(api, fields, ts_code, self.start_date, self.end_date)

    def update_fina_indicator(self, api, fields, ts_code, start_date, end_date):
        new_rows = self.pro.query(api, fields=fields, ts_code=ts_code, start_date=start_date, end_date=end_date)
        if not new_rows.empty:
            existed_finas = Fina.get_existed_fina_by_end_date(table_name=api, ts_code=ts_code, start_date=start_date, end_date=end_date)
            if not existed_finas.empty:
                new_rows = new_rows[~new_rows['end_date'].isin(existed_finas['end_date'])]
                new_rows.drop_duplicates('end_date', inplace=True)
            new_rows = new_rows.merge(self.all_dates, left_on='ann_date', right_on='cal_date')
            new_rows = self.code_list.merge(new_rows, on='ts_code')
            avail_recorders = new_rows[fields_map[api]]
            avail_recorders.to_sql(api, DB.engine, index=False, if_exists='append', chunksize=3000)

    def query_finance(self, api, report_type=''):
        # 按trade_date依次拉取所有股票信息
        codes = self.code_list['ts_code']
        for ts_code in codes:
            flag = True
            while flag:
                try:
                    self.update_finance_by_code(api, ts_code, self.start_date, self.end_date, report_type=report_type)
                    flag = False
                    time.sleep(1)
                except BaseException as e:
                    # print(e)
                    time.sleep(5)
                    self.update_finance_by_code(api, ts_code, self.start_date, self.end_date, report_type=report_type)

    def update_finance_by_code(self, api, ts_code, start_date, end_date, report_type):
        new_rows = self.pro.query(api, ts_code=ts_code,  start_date=start_date, end_date=end_date, report_type=report_type)
        if not new_rows.empty:
            existed_reports = Fina.get_existed_reports(table_name=api, ts_code=ts_code, report_type=report_type, start_date=start_date, end_date=end_date)

            if not existed_reports.empty:
                new_rows = new_rows[~new_rows['end_date'].isin(existed_reports['end_date'])]
                new_rows.drop_duplicates('end_date', inplace=True)
            new_rows = new_rows.merge(self.all_dates, left_on='ann_date', right_on='cal_date')
            new_rows = self.code_list.merge(new_rows, on='ts_code')
            avail_recorders = new_rows[fields_map[api]]
            avail_recorders.to_sql(api, DB.engine, index=False, if_exists='append', chunksize=3000)

