from conf.myapp import db_config, environment
import sqlalchemy as sa
from sqlalchemy import create_engine
import pandas as pd


class DB(object):

    if environment == 'official':
        log = False
    else:
        log = True

    engine = create_engine(
        'mysql+mysqlconnector://{user}:{password}@{host}:{port}/{database}'.format(**db_config), echo=log)

    @classmethod
    def get_cal_date(cls, start_date, end_date):
        existed_cal_date = pd.read_sql(
            sa.text('SELECT cal_date FROM trade_cal where cal_date >= :sd and cal_date <= :ed'),
            cls.engine,
            params={'sd': start_date, 'ed':end_date}
        )
        return existed_cal_date

    @classmethod
    def get_open_cal_date(cls, start_date='', end_date='', period=''):
        if period != '':
            trade_cal = pd.read_sql(
                sa.text(
                    'SELECT id as date_id, cal_date FROM trade_cal where is_open = 1 and cal_date >= :sd and cal_date <= :ed order by cal_date desc limit :period'),
                cls.engine,
                params={'sd': start_date, 'ed': end_date, 'period': period}
            )

        else:
            trade_cal = pd.read_sql(
                sa.text(
                    'SELECT id as date_id, cal_date FROM trade_cal where is_open = 1 and cal_date >= :sd and cal_date <= :ed'),
                cls.engine,
                params={'sd': start_date, 'ed': end_date}
            )
        trade_cal.sort_values(by='cal_date', inplace=True)
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
    def get_latestopendays_code_list(cls, latest_open_days=''):
        code_list = pd.read_sql(
            sa.text(' select d.code_id FROM daily d '
                    ' left join stock_basic sb on sb.id = d.code_id'
                    ' where sb.list_status=:ls'
                    ' group by d.code_id'
                    ' having count(d.code_id) >= :latest_open_days'
                    ),
            cls.engine,
            params={'ls': 'L', 'latest_open_days': latest_open_days}
        )

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
    def get_trade_codes(cls, date_id):
        trade_codes = pd.read_sql(
            sa.text(
                'SELECT d.code_id from daily d where d.date_id=:date_id'),
            cls.engine,
            params={'date_id': date_id}
        )
        return trade_codes

    @classmethod
    def update_delist_date(cls, ts_code, delist_date):
        pd.io.sql.execute('update stock_basic set list_status=%s, delist_date=%s where ts_code=%s',
                          cls.engine, params=['D', delist_date, ts_code])

    @classmethod
    def get_classifiers(cls, classifier_type):
        classifiers = pd.read_sql(
            sa.text('SELECT id, params from classifiers where type = :t'), cls.engine, params={'t':classifier_type})
        return classifiers

    @classmethod
    def get_code_info(cls, code_id, start_date='', end_date='', period=''):
        if period == '':
            code_info = pd.read_sql(
                sa.text(
                    ' SELECT tc.cal_date, d.date_id, d.open, d.close, d.high, d.low, d.vol, db.turnover_rate_f, af.adj_factor FROM  daily d '
                    ' left join daily_basic db on db.date_id = d.date_id and db.code_id = d.code_id'
                    ' left join adj_factor af on af.date_id = d.date_id and af.code_id = d.code_id'
                    ' left join trade_cal tc on tc.id = d.date_id'
                    ' where d.code_id = :code_id and tc.cal_date >= :sd and tc.cal_date <= :ed'
                    ' order by tc.cal_date desc '),
                cls.engine,
                params={'code_id': code_id, 'sd': start_date, 'ed': end_date}
            )
        else:
            code_info = pd.read_sql(
                sa.text(
                    ' SELECT tc.cal_date, d.date_id, d.open, d.close, d.high, d.low, d.vol, db.turnover_rate_f, af.adj_factor FROM daily d '
                    ' left join daily_basic db on db.date_id = d.date_id and db.code_id = d.code_id'
                    ' left join adj_factor af on af.date_id = d.date_id and af.code_id = d.code_id'
                    ' left join trade_cal tc on tc.id = d.date_id'
                    ' where d.code_id = :code_id and tc.cal_date >= :sd and tc.cal_date <= :ed '
                    ' order by tc.cal_date desc '
                    ' limit :period'),
                cls.engine,
                params={'code_id': code_id, 'sd': start_date, 'ed': end_date, 'period': period}
            )

        code_info.sort_values(by='date_id', inplace=True)
        code_info.set_index('date_id', inplace=True)

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
    def truncate_features_groups(cls):
        pd.io.sql.execute('truncate features_groups', cls.engine)

    @classmethod
    def insert_features_groups(cls, feature_id, group_number):
        pd.io.sql.execute('insert into features_groups (feature_id, group_number) values (%s, %s)', cls.engine, params=[feature_id, group_number])

    @classmethod
    def get_features_groups(cls, group_number=''):
        if group_number != '':
            features_groups = pd.read_sql(
                sa.text(' select f.name, fg.group_number from features_groups fg '
                                                ' left join features f on f.id = fg.feature_id'
                                                ' where fg.group_number = :gn'), cls.engine,
                              params={'gn':group_number})
        else:
            features_groups = pd.read_sql(
                sa.text(' select f.name, fg.group_number from features_groups fg '
                        ' left join features f on f.id = fg.feature_id'), cls.engine)
        return features_groups

    @classmethod
    def truncate_thresholds(cls):
        pd.io.sql.execute('truncate thresholds', cls.engine)

    @classmethod
    def insert_threshold(cls, code_id, date_id, SMS_month, SMS_year, simple_threshold_v):
        existed = pd.read_sql(
            sa.text(' select 1 from thresholds'
                    ' where date_id = :date_id and code_id = :code_id'), cls.engine,
            params={'date_id': str(date_id), 'code_id': str(code_id)})
        if existed.empty:
            print('进来起')
            pd.io.sql.execute('insert into thresholds (code_id, date_id, SMS_month, SMS_year, simple_threshold_v) values (%s, %s, %s, %s, %s)',
                          cls.engine, params=[str(code_id), str(date_id), str(SMS_month), str(SMS_year), str(simple_threshold_v)])

    @classmethod
    def get_thresholds(cls, code_id, start_date_id='', end_date_id=''):
        existed_codes = pd.read_sql(
            sa.text(' select t.code_id, t.date_id, t.SMS_month, t.SMS_year, t.simple_threshold_v from thresholds t'
                    ' where t.date_id >= :sdi and t.date_id <= :edi and t.code_id = :code_id'), cls.engine,
            params={'code_id': code_id, 'sdi': str(start_date_id), 'edi': str(end_date_id)})

        existed_codes.sort_values(by='date_id', inplace=True)
        existed_codes.set_index('date_id', inplace=True, drop=False)
        return existed_codes

    @classmethod
    def get_classified_v(cls, code_id, group_number, classifier_id):
        existed_classified_v = pd.read_sql(
            sa.text(' select cv.id, cv.date_id, cv.code_id, cv.classifier_id, cv.classifier_v, cv.feature_group_number '
                    ' from classified_v cv'
                    ' left join trade_cal tc on tc.id = cv.date_id'
                    ' where cv.code_id = :ci and cv.feature_group_number = :gn and cv.classifier_id = :classifier_id'), cls.engine,
            params={'ci': code_id, 'gn': group_number, 'classifier_id': classifier_id})
        existed_classified_v.set_index('date_id', inplace=True, drop=False)
        return existed_classified_v

    @classmethod
    def delete_classified_v(cls, classified_v_id):
        pd.io.sql.execute('delete from classified_v where id=%s',
                          cls.engine,
                          params=[classified_v_id])

    @classmethod
    def batch_delete_classified_v(cls, classified_v_ids):
        if len(classified_v_ids) >= 1:
            pd.io.sql.execute(
                'delete from classified_v where id in (' + ('%s,' * len(classified_v_ids)).strip(',') + ')',
                cls.engine,
                params=[classified_v_ids])

    @classmethod
    def recommend_stock_by_classifier(cls, classifier_id, current_date_id, last_date_id):
        stocks = pd.read_sql(
            sa.text(' select cv.id, cv.code_id, cv.r2_score, cv.classifier_v'
                    ' from classified_v cv'
                    ' left join trade_cal tc on tc.id = cv.date_id'
                    ' where cv.date_id = :date_id'
                    ' and cv.classifier_id = :classifier_id'
                    ' and cv.code_id in '
                    ' (select cv2.code_id from classified_v cv2 where cv2.date_id =:last_date_id and cv2.classifier_v > 0.04)'
                    ' and cv.classifier_v > 0.05 and cv.r2_score > 0.01 and cv.classifier_v > 0.05'
                    ' order by cv.r2_score desc'
                    ),
            cls.engine,
            params={'date_id': str(current_date_id), 'last_date_id': str(last_date_id), 'classifier_id': classifier_id})
        return stocks

    @classmethod
    def get_max_r2_score(cls, code_id, date_id, limit=1):
        data = pd.read_sql(
            sa.text(' select cv.id, cv.feature_group_number'
                    ' from classified_v cv'
                    ' where cv.date_id = :date_id'
                    ' and cv.code_id = :code_id '
                    ' order by r2_score desc '
                    ' limit :limit'
                    ),
            cls.engine,
            params={'code_id': str(code_id), 'date_id': str(date_id), 'limit': limit})
        return data

    @classmethod
    def delete_recommend_stock_logs(cls, date_id):
        pd.io.sql.execute('delete from recommend_stocks where date_id=%s',
                          cls.engine,
                          params=[str(date_id)])

    @classmethod
    def count_threshold_group_by_date_id(cls, start_date_id, end_date_id):
        data = pd.read_sql(
            sa.text(' select tc.cal_date, count(t.code_id) as up_stock_number from thresholds  t'
                    ' left join trade_cal tc on tc.id = t.date_id'
                    ' where t.simple_threshold_v < -0.03 and t.date_id between :start_date_id and :end_date_id'
                    ' group by t.date_id'
                    ' order by date_id desc'
                    ),
            cls.engine,
            params={'start_date_id': str(start_date_id), 'end_date_id': str(end_date_id)})
        return data

    # @staticmethod
    # def validate_field(columns, fields):
    #     valid_fields = list((set(columns).union(set(fields))) ^ (set(columns) ^ set(fields)))
    #     return valid_fields


