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

    @classmethod
    def get_code_info(cls, code_id, start_date, end_date):
        code_info = pd.read_sql(
            sa.text(
                ' SELECT d.open, d.close, d.high, d.low, d.vol, db.turnover_rate_f, af.adj_factor FROM daily d '
                ' left join daily_basic db on db.date_id = d.date_id and db.code_id = d.code_id'
                ' left join adj_factor af on af.date_id = d.date_id and af.code_id = d.code_id'
                ' left join trade_cal tc on tc.id = d.date_id'
                ' where d.code_id = :code_id and tc.cal_date >= :sd and tc.cal_date <= :ed'),
            cls.engine,
            params={'code_id': code_id, 'sd': start_date, 'ed': end_date}
        )
        return code_info

    @classmethod
    def insert_features(cls, name, remark):
        pd.io.sql.execute('insert into features (name, remark) values (%s, %s)',
                      cls.engine, params=[name, remark])

    @classmethod
    def get_features(cls):
        features = pd.read_sql(sa.text('SELECT id, name from features'), cls.engine)
        return features

    @classmethod
    def truncate_features(cls):
        pd.io.sql.execute('truncate features', cls.engine)

    @classmethod
    def truncate_feature_groups(cls):
        pd.io.sql.execute('truncate features_groups', cls.engine)

    @classmethod
    def insert_feature_groups(cls, feature_id, group_number):
        pd.io.sql.execute('insert into features_groups (feature_id, group_number) values (%s, %s)', cls.engine, params=[feature_id, group_number])

    # @staticmethod
    # def validate_field(columns, fields):
    #     valid_fields = list((set(columns).union(set(fields))) ^ (set(columns) ^ set(fields)))
    #     return valid_fields


