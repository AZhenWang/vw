from sqlalchemy import create_engine
from conf.myapp import db_config, environment
import pandas as pd
import sqlalchemy as sa


class Base:
    if environment == 'official':
        log = False
    else:
        log = True

    engine = create_engine(
        'mysql+mysqlconnector://{user}:{password}@{host}:{port}/{database}'.format(**db_config), echo=log)

    @classmethod
    def get_date_id(cls, cal_date):
        data = pd.read_sql(
            sa.text('SELECT id as date_id FROM trade_cal where cal_date = :cal_date'),
            cls.engine,
            params={'cal_date': cal_date}
        )
        date_id = data.iloc[0]['date_id']
        return date_id

    @classmethod
    def get_cal_date(cls, start_date='', end_date='', limit=''):
        if start_date =='':
            trade_cal = pd.read_sql(
                sa.text('SELECT id as date_id, cal_date, is_open FROM trade_cal where cal_date <= :ed'
                        ' order by id desc'
                        ' limit :limit'),
                cls.engine,
                params={'ed': end_date, 'limit': limit}
            )
        elif end_date =='':
            trade_cal = pd.read_sql(
                sa.text('SELECT id as date_id, cal_date, is_open FROM trade_cal where cal_date >= :sd'
                        ' order by id asc'
                        ' limit :limit'),
                cls.engine,
                params={'sd': start_date, 'limit': limit}
            )
        else:
            trade_cal = pd.read_sql(
                sa.text('SELECT id as date_id,  cal_date, is_open FROM trade_cal where cal_date >= :sd and cal_date <= :ed'),
                cls.engine,
                params={'sd': start_date, 'ed': end_date}
            )

        trade_cal.sort_values(by='cal_date', inplace=True)
        return trade_cal

    @classmethod
    def get_open_cal_date(cls, start_date='', end_date='', period=''):
        if start_date == '':
            trade_cal = pd.read_sql(
                sa.text(
                    'SELECT id as date_id, cal_date FROM trade_cal '
                    ' where is_open = 1 and cal_date <= :ed'
                    ' order by cal_date desc limit :period'),
                cls.engine,
                params={'ed': str(end_date), 'period': period}
            )

        elif end_date == '':
            trade_cal = pd.read_sql(
                sa.text(
                    'SELECT id as date_id, cal_date FROM trade_cal '
                    ' where is_open = 1 and cal_date >= :sd'
                    ' order by cal_date asc limit :period'),
                cls.engine,
                params={'sd': str(start_date), 'period': period}
            )
        else:
            trade_cal = pd.read_sql(
                sa.text(
                    'SELECT id as date_id, cal_date FROM trade_cal '
                    ' where is_open = 1 and cal_date between :sd and :ed'
                    ' order by cal_date desc'),
                cls.engine,
                params={'sd': str(start_date), 'ed': str(end_date)}
            )
        trade_cal.sort_values(by='cal_date', inplace=True)
        return trade_cal

    @classmethod
    def get_open_cal_date_by_id(cls, start_date_id='', end_date_id='', period=''):
        if start_date_id == '':
            trade_cal = pd.read_sql(
                sa.text(
                    'SELECT id as date_id, cal_date FROM trade_cal '
                    ' where is_open = 1 and id <= :edi'
                    ' order by cal_date desc limit :period'),
                cls.engine,
                params={'edi': str(end_date_id), 'period': period}
            )

        elif end_date_id == '':
            trade_cal = pd.read_sql(
                sa.text(
                    'SELECT id as date_id, cal_date FROM trade_cal '
                    ' where is_open = 1 and id >= :sdi'
                    ' order by cal_date asc limit :period'),
                cls.engine,
                params={'sdi': str(start_date_id), 'period': period}
            )
        else:
            trade_cal = pd.read_sql(
                sa.text(
                    'SELECT id as date_id, cal_date FROM trade_cal '
                    ' where is_open = 1 and id between :sdi and :edi'
                    ' order by cal_date desc'),
                cls.engine,
                params={'sdi': str(start_date_id), 'edi': str(end_date_id), 'period': period}
            )
        trade_cal.sort_values(by='cal_date', inplace=True)
        return trade_cal

    @classmethod
    def get_table_logs(cls, code_id, start_date_id, end_date_id, table_name):
        logs = pd.read_sql(
            sa.text(
                'SELECT tc.cal_date, api.* FROM ' + table_name + ' as api '
                                                    ' left join trade_cal tc on tc.id = api.date_id'
                                                    ' where api.code_id = :code_id and api.date_id between :sdi and :edi'
                                                    ' order by api.date_id asc'),
            cls.engine,
            params={'code_id': str(code_id), 'sdi': str(start_date_id), 'edi': str(end_date_id)}
        )
        logs.sort_values(by='date_id', inplace=True)
        logs.set_index('date_id', inplace=True)
        return logs

    @classmethod
    def get_all_recommend_data(cls, table_name=''):
        logs = pd.read_sql(
            sa.text(
                'SELECT tc.cal_date, api.* FROM ' + table_name + ' as api '),
            cls.engine,
        )
        return logs
