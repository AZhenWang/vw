from conf.myapp import db_config
import sqlalchemy as sa
from sqlalchemy import create_engine
import pandas as pd

class DB(object):

    engine = create_engine(
        'mysql+mysqlconnector://{user}:{password}@{host}:{port}/{database}'.format(**db_config), echo=True)

    @classmethod
    def get_cal_date(cls, start_date, end_date):
        existed_cal_date = pd.read_sql(
            sa.text('SELECT cal_date FROM trade_cal where cal_date >= :sd and cal_date <= :ed'),
            cls.engine,
            params={'sd': start_date, 'ed':end_date}
        )
        return existed_cal_date

    @classmethod
    def get_open_cal_date(cls, start_date, end_date, period=1):
        if start_date != '':
            trade_cal = pd.read_sql(
                sa.text(
                    'SELECT id as date_id, cal_date FROM trade_cal where is_open = 1 and cal_date >= :sd and cal_date <= :ed'),
                cls.engine,
                params={'sd': start_date, 'ed': end_date}
            )
        else:
            trade_cal = pd.read_sql(
                sa.text(
                    'SELECT id as date_id, cal_date FROM trade_cal where is_open = 1 and cal_date <= :ed order by cal_date desc limit :period'),
                cls.engine,
                params={'sd': start_date, 'ed': end_date, 'period': period}
            )
        return trade_cal

    @classmethod
    def get_code_list(cls, list_status=''):
        if list_status != '':
            code_list = pd.read_sql(
                sa.text('SELECT id as code_id, ts_code FROM stock_basic where list_status=:ls'),
                cls.engine,
                params={'ls': list_status}
            )
        else:
            code_list = pd.read_sql(sa.text('SELECT id as code_id, ts_code, list_status FROM stock_basic'),cls.engine)
        return code_list

    @classmethod
    def get_existed_codes(cls, table_name, date_id):
        existed_codes = pd.read_sql(
            sa.text(
                'SELECT sb.ts_code FROM ' + table_name + ' as api left join stock_basic as sb on sb.id = api.code_id where api.date_id=:date_id'),
            cls.engine,
            params={'date_id': date_id}
        )
        return existed_codes

    @classmethod
    def update_delist_date(cls, ts_code, delist_date):
        pd.io.sql.execute('update stock_basic set list_status=%s, delist_date=%s where ts_code=%s',
                          cls.engine, params=['D', delist_date, ts_code])

    @staticmethod
    def validate_field(columns):
        valid_fields = list((set(columns).union(set(self.fields))) ^ (set(columns) ^ set(self.fields)))
        return valid_fields


