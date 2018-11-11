from app.fetcher.common import Interface
from conf.myapp import ts_token
from app.saver.tables import fields_map
from globalvar import GL

from datetime import datetime, timedelta
import pandas as pd
import tushare as ts
import sqlalchemy as sa
import time


class Ts(Interface):

    def __init__(self, start_date='', end_date=''):
        self.pro = ts.pro_api(ts_token)
        self.start_date = start_date
        self.end_date = end_date
        self.engine = GL.get_value('db_engine')
        self.code_list = []
        self.trade_dates = self.get_trade_dates()

    def get_trade_dates(self):
        trade_cal = self.pro.trade_cal(start_date=self.start_date, end_date=self.end_date)
        trade_cal = trade_cal[trade_cal['is_open'] == 1]
        trade_cal.sort_values(by=['cal_date'], inplace=True)
        return trade_cal['cal_date']

    def set_code_list(self):
        api = 'stock_basic'
        code_list = pd.read_sql(
            sa.text('SELECT ts_code FROM ' + api + ' where list_status=:ls'),
            self.engine,
            params={'ls': 'L'}
        )
        self.code_list = code_list

    def update_stock_basic(self):
        api = 'stock_basic'
        existed_code_list = pd.read_sql(
            sa.text('SELECT ts_code FROM ' + api),
            self.engine
        )
        new_rows = self.pro.stock_basic(fields=fields_map[api])
        if not existed_code_list.empty:
            new_rows = new_rows[~new_rows['ts_code'].isin(existed_code_list['ts_code'])]
        if not new_rows.empty:
            avail_recorders = new_rows[fields_map[api]]
            avail_recorders.to_sql(api, self.engine, index=False, if_exists='append', chunksize=1000)

    def update_update_date(self, ts_code='', update_date=''):
        if not update_date:
            update_date = self.end_date
        if not ts_code:
            # 全部股票更新一遍
            pd.io.sql.execute('update stock_basic set update_date=%s', self.engine, params=[update_date])
        else:
            # pd.io.sql.execute('update stock_basic set update_date=' + update_date + ' where ts_code=' + ts_code,
            #               self.engine, params=[update_date, ts_code])
            pd.io.sql.execute('update stock_basic set update_date=%s where ts_code=%s',
                              self.engine, params=[update_date, ts_code])

    def query(self, api):
        if not self.start_date:
            # 如果start_date为空，就按ts_code依次拉取所有股票的迄今为止的信息
            for ts_code in self.code_list.values:
                flag = True
                while flag:
                    try:
                        self.update_by_ts_code(api, ts_code)
                        flag = False

                    except BaseException as e:
                        # print(e)
                        time.sleep(10)
                        self.update_by_ts_code(api, ts_code)

        else:
            # 按trade_date依次拉取所有股票信息
            for trade_date in self.trade_dates:
                flag = True
                while flag:
                    try:
                        self.update_by_trade_date(api, trade_date)
                        flag = False
                    except BaseException as e:
                        # print(e)
                        time.sleep(10)
                        self.update_by_trade_date(api, trade_date)

    def update_by_ts_code(self, api, ts_code):
        existed_dates = pd.read_sql(
            sa.text('SELECT trade_date FROM ' + api + ' where ts_code=:tc'),
            self.engine,
            params={'tc': ts_code}
        )

        if existed_dates.empty or (~self.trade_dates.isin(existed_dates['trade_date'])).any():
            # print('打进来了')
            new_rows = self.pro.query(api, ts_code=ts_code, start_date=self.start_date, end_date=self.end_date)
            if not new_rows.empty:
                if not existed_dates.empty:
                    new_rows = new_rows[~new_rows['trade_date'].isin(existed_dates['trade_date'])]
                if not new_rows.empty:
                    # print('新增')
                    avail_recorders = new_rows[fields_map[api]]
                    avail_recorders.sort_values(by=['trade_date'], inplace=True)
                    avail_recorders.to_sql(api, self.engine, index=False, if_exists='append', chunksize=1000)

    def update_by_trade_date(self, api, trade_date):
        new_rows = self.pro.query(api, trade_date=trade_date)
        if not new_rows.empty:
            existed_codes = pd.read_sql(
                sa.text('SELECT ts_code FROM ' + api + ' where trade_date=:td'),
                self.engine,
                params={'td': trade_date}
            )
            if not existed_codes.empty:
                new_rows = new_rows[~new_rows['ts_code'].isin(existed_codes['ts_code'])]
            if not new_rows.empty:
                avail_recorders = new_rows[fields_map[api]]
                avail_recorders.to_sql(api, self.engine, index=False, if_exists='append', chunksize=1000)

