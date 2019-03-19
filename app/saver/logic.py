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
    def test_select(cls, code_id):
        daily = pd.read_sql(
            sa.text('SELECT d.* FROM daily d'
                    ' left join trade_cal tc on tc.id = d.date_id'
                    '  where d.code_id = :code_id'
                    ' order by tc.cal_date asc'),
            cls.engine,
            params={'code_id': code_id}
        )
        daily.set_index('date_id', inplace=True)
        return daily

    @classmethod
    def test_update(cls, code_id, date_id, pct_chg):
        pd.io.sql.execute('update daily set pct_chg=%s where date_id =%s and code_id =%s',
                          cls.engine, params=[str(pct_chg), str(date_id), str(code_id)])

    @classmethod
    def get_cal_date(cls, start_date, end_date):
        existed_cal_date = pd.read_sql(
            sa.text('SELECT cal_date FROM trade_cal where cal_date >= :sd and cal_date <= :ed'),
            cls.engine,
            params={'sd': start_date, 'ed': end_date}
        )
        return existed_cal_date

    @classmethod
    def get_open_cal_date(cls, start_date='', end_date='', period=''):
        if period != '':
            trade_cal = pd.read_sql(
                sa.text(
                    'SELECT id as date_id, cal_date FROM trade_cal '
                    ' where is_open = 1 and cal_date >= :sd and cal_date <= :ed '
                    ' order by cal_date desc limit :period'),
                cls.engine,
                params={'sd': start_date, 'ed': end_date, 'period': period}
            )

        else:
            trade_cal = pd.read_sql(
                sa.text(
                    'SELECT id as date_id, cal_date FROM trade_cal where is_open = 1 and cal_date between :sd and :ed'
                    ' order by cal_date desc'),
                cls.engine,
                params={'sd': start_date, 'ed': end_date}
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
                    ' order by cal_date desc limit :period'),
                cls.engine,
                params={'sdi': str(start_date_id), 'edi': str(end_date_id), 'period': period}
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
    def get_index_list(cls, market=''):
        if market != '':
            index_list = pd.read_sql(
                sa.text('SELECT id as index_id, ts_code FROM index_basic where market=:market'),
                cls.engine,
                params={'market': market}
            )
        else:
            index_list = pd.read_sql(sa.text('SELECT id as index_id, ts_code, market FROM index_basic'), cls.engine)
        return index_list

    @classmethod
    def get_index_daily(cls, ts_code='', start_date_id='', end_date_id=''):
        if ts_code != '':
            index_list = pd.read_sql(
                sa.text(' SELECT tc.cal_date, ib.ts_code, ib.name, id.close, id.vol FROM index_daily id '
                        ' left join index_basic ib on ib.id = id.index_id'
                        ' left join trade_cal tc on tc.id = id.date_id'
                        ' where ib.ts_code=:ts_code'
                        ' and id.date_id between :sdi and :edi'),
                cls.engine,
                params={'ts_code': ts_code, 'sdi': str(start_date_id), 'edi': str(end_date_id)}
            )
        else:
            index_list = pd.read_sql(
                sa.text(' SELECT tc.cal_date, ib.ts_code, ib.name, id.close, id.vol FROM index_daily id '
                        ' left join index_basic ib on ib.id = id.index_id'
                        ' left join trade_cal tc on tc.id = id.date_id'
                        ' where id.date_id between :sdi and :edi'),
                cls.engine,
                params={'sdi': str(start_date_id), 'edi': str(end_date_id)}
            )
        return index_list

    @classmethod
    def get_latestopendays_code_list(cls, latest_open_days='', date_id=''):
        if date_id == '':
            code_list = pd.read_sql(
                sa.text(' select d.code_id FROM daily d '
                        ' left join stock_basic sb on sb.id = d.code_id'
                        ' where sb.list_status=:ls'
                        ' group by d.code_id'
                        ' having count(d.code_id) >= :latest_open_days'
                        ),
                cls.engine,
                params={'ls': 'L', 'latest_open_days': str(latest_open_days)}
            )
        else:
            code_list = pd.read_sql(
                sa.text(' select d.code_id FROM daily d '
                        ' left join stock_basic sb on sb.id = d.code_id'
                        ' where d.date_id <= :di and sb.list_status=:ls'
                        ' group by d.code_id'
                        ' having count(d.code_id) >= :latest_open_days'
                        ),
                cls.engine,
                params={'ls': 'L', 'di': str(date_id), 'latest_open_days': str(latest_open_days)}
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
    def get_existed_index(cls, table_name, index_id, start_date, end_date):
        existed_codes = pd.read_sql(
            sa.text(
                ' SELECT tc.cal_date FROM ' + table_name + ' as api '
                ' left join trade_cal tc on tc.id = api.date_id'
                ' where api.index_id = :index_id and tc.cal_date between :start_date and :end_date'),
            cls.engine,
            params={'index_id': index_id, 'start_date': start_date, 'end_date': end_date}
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
    def get_code_daily(cls, code_id='', date_id=''):
        daily = pd.read_sql(
            sa.text('select tc.cal_date, d.*, af.adj_factor from daily d'
                    ' left join adj_factor af on af.date_id = d.date_id and af.code_id = d.code_id'
                    ' left join trade_cal tc on tc.id = d.date_id'
                    ' where d.date_id = :date_id and d.code_id = :code_id'), cls.engine,
            params={'code_id': str(code_id), 'date_id': str(date_id)})
        return daily

    @classmethod
    def get_code_daily_later(cls, code_id='', date_id='', period=1):
        daily = pd.read_sql(
            sa.text('select af.adj_factor, d.* from daily d'
                    ' left join adj_factor af on af.date_id = d.date_id and af.code_id = d.code_id'
                    ' left join trade_cal tc on tc.id = d.date_id'
                    ' where d.date_id > :date_id and d.code_id = :code_id '
                    ' order by tc.cal_date asc limit :period'), cls.engine,
            params={'code_id': str(code_id), 'date_id': str(date_id), 'period': period})
        return daily

    @classmethod
    def get_code_info(cls, code_id, start_date='', end_date='', period=''):
        if period == '':
            code_info = pd.read_sql(
                sa.text(
                    ' SELECT tc.cal_date, d.*, db.turnover_rate_f, af.adj_factor FROM  daily d '
                    ' left join daily_basic db on db.date_id = d.date_id and db.code_id = d.code_id'
                    ' left join adj_factor af on af.date_id = d.date_id and af.code_id = d.code_id'
                    ' left join trade_cal tc on tc.id = d.date_id'
                    ' where d.code_id = :code_id and tc.cal_date >= :sd and tc.cal_date <= :ed'
                    ' order by tc.cal_date desc '),
                cls.engine,
                params={'code_id': str(code_id), 'sd': str(start_date), 'ed': str(end_date)}
            )
        else:
            code_info = pd.read_sql(
                sa.text(
                    ' SELECT tc.cal_date, d.*, db.turnover_rate_f, af.adj_factor FROM daily d '
                    ' left join daily_basic db on db.date_id = d.date_id and db.code_id = d.code_id'
                    ' left join adj_factor af on af.date_id = d.date_id and af.code_id = d.code_id'
                    ' left join trade_cal tc on tc.id = d.date_id'
                    ' where d.code_id = :code_id and tc.cal_date >= :sd and tc.cal_date <= :ed '
                    ' order by tc.cal_date desc '
                    ' limit :period'),
                cls.engine,
                params={'code_id': str(code_id), 'sd': str(start_date), 'ed': str(end_date), 'period': period}
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
            pd.io.sql.execute('insert into thresholds (code_id, date_id, SMS_month, SMS_year, simple_threshold_v) values (%s, %s, %s, %s, %s)',
                          cls.engine, params=[str(code_id), str(date_id), str(SMS_month), str(SMS_year), str(simple_threshold_v)])

    @classmethod
    def get_thresholds(cls, code_id, start_date_id='', end_date_id=''):
        existed_codes = pd.read_sql(
            sa.text(' select t.code_id, t.date_id, t.SMS_month, t.SMS_year, t.simple_threshold_v from thresholds t'
                    ' where t.date_id >= :sdi and t.date_id <= :edi and t.code_id = :code_id'), cls.engine,
            params={'code_id': str(code_id), 'sdi': str(start_date_id), 'edi': str(end_date_id)})

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
    def get_recommended_stocks(cls, start_date_id='',  end_date_id='', recommend_type=''):
        stocks = pd.read_sql(
            sa.text(' select tc.cal_date, sb.ts_code, sb.name, rs.* from recommend_stocks rs '
                    ' left join stock_basic sb on sb.id = rs.code_id'
                    ' left join trade_cal tc on tc.id = rs.date_id'
                    ' where rs.date_id between :sdi and :edi and rs.recommend_type = :rt'
                    ' order by rs.date_id asc'
                    ),
            cls.engine,
            params={'sdi': str(start_date_id), 'edi': str(end_date_id), 'rt': recommend_type}
        )
        return stocks

    @classmethod
    def get_all_recommended_stocks(cls, recommend_type=''):
        stocks = pd.read_sql(
            sa.text(' select rs.* from recommend_stocks rs '
                    ' where rs.recommend_type = :rt'
                    ' order by rs.date_id asc, rs.star_idx desc, rs.moods desc'),
            cls.engine,
            params={'rt': recommend_type}
        )
        return stocks

    @classmethod
    def sum_pct_chg(cls, code_id='', end_date_id='', period=4):
        result = pd.read_sql(
            sa.text(' select sum(sub.pct_chg) as sum_pct_chg from '
                    '(select pct_chg from daily where code_id = :code_id and date_id < :edi'
                    ' order by date_id desc limit :period) as sub'
                    ),
            cls.engine,
            params={'code_id': str(code_id), 'edi': str(end_date_id), 'period': period}
        )
        return result.at[0, 'sum_pct_chg']

    @classmethod
    def get_latestrecommend_logs(cls, code_id, end_date_id, start_date_id, recommend_type=''):
        logs = pd.read_sql(
            sa.text(' select tc.cal_date, rs.* from recommend_stocks rs '
                    ' left join trade_cal tc on tc.id = rs.date_id'
                    ' where rs.code_id = :code_id and rs.date_id >= :sdi and rs.date_id < :edi'
                    ' and rs.recommend_type = :recommend_type'
                    ' order by tc.cal_date asc'),
            cls.engine,
            params={'code_id': str(code_id), 'sdi': str(start_date_id), 'edi': str(end_date_id), 'recommend_type': recommend_type}
        )
        return logs

    @classmethod
    def get_recommend_log(cls, code_id, date_id, recommend_type=''):
        logs = pd.read_sql(
            sa.text(' select rs.* from recommend_stocks rs '
                    ' where rs.code_id = :code_id and rs.date_id = :date_id'
                    ' and rs.recommend_type = :recommend_type'),
            cls.engine,
            params={'code_id': str(code_id), 'date_id': str(date_id), 'recommend_type': recommend_type}
        )
        return logs

    @classmethod
    def get_code_recommend_logs(cls, code_id, start_date_id, end_date_id, star_idx, recommend_type=''):
        logs = pd.read_sql(
            sa.text(' select rs.* from recommend_stocks rs '
                    ' where rs.code_id = :code_id and rs.date_id >=:sdi and rs.date_id <=:edi'
                    ' and rs.star_idx =:star_idx'
                    ' and rs.recommend_type = :recommend_type'
                    ' order by rs.date_id asc'),
            cls.engine,
            params={'code_id': str(code_id), 'sdi': str(start_date_id), 'edi':str(end_date_id), 'star_idx':star_idx, 'recommend_type': recommend_type}
        )
        return logs

    @classmethod
    def get_flag_recommend_logs(cls, code_id, start_date_id, end_date_id, flag, recommend_type=''):
        logs = pd.read_sql(
            sa.text(' select rs.* from recommend_stocks rs '
                    ' where rs.code_id = :code_id and rs.date_id >=:sdi and rs.date_id <=:edi'
                    ' and rs.flag =:flag'
                    ' and rs.recommend_type = :recommend_type'
                    ' order by rs.date_id asc'),
            cls.engine,
            params={'code_id': str(code_id), 'sdi': str(start_date_id), 'edi': str(end_date_id), 'flag': flag,
                    'recommend_type': recommend_type}
        )
        return logs

    @classmethod
    def get_pre_flag_logs(cls, code_id, date_id, period='', recommend_type=''):
        logs = pd.read_sql(
            sa.text(' select rs.* from recommend_stocks rs '
                    ' where rs.code_id = :code_id and rs.date_id <=:edi'
                    ' and rs.recommend_type = :recommend_type and rs.flag != 0'
                    ' order by rs.date_id desc'
                    ' limit :period'),
            cls.engine,
            params={'code_id': str(code_id), 'edi': str(date_id), 'period': period, 'recommend_type': recommend_type}
        )
        return logs

    @classmethod
    def count_recommend_star(cls, code_id, start_date_id, end_date_id, star_idx='', recommend_type=''):
        result = pd.read_sql(
            sa.text(' select count(*) as count_star from '
                    ' (select * from recommend_stocks rs '
                    ' where rs.code_id = :code_id and rs.date_id >= :sdi and rs.date_id <= :edi and rs.star_idx = :si'
                    ' and rs.recommend_type = :recommend_type) as sub'
                    ),
            cls.engine,
            params={'code_id': str(code_id), 'sdi': str(start_date_id), 'edi': str(end_date_id), 'si': str(star_idx),
                    'recommend_type': recommend_type}
        )
        return result.at[0, 'count_star']

    @classmethod
    def insert_focus_stocks(cls, code_id, star_idx, predict_rose, recommend_type, recommended_date_id):
        pd.io.sql.execute('insert into focus_stocks (code_id, star_idx, predict_rose, '
                          'recommend_type, recommended_date_id) values (%s,%s,%s,%s,%s)', cls.engine,
                          params=[str(code_id), str(star_idx), str(predict_rose), str(recommend_type), str(recommended_date_id)])
    @classmethod
    def update_focus_stock_log(cls, code_id, recommended_date_id , holding_date_id='', closed_date_id='', star_count=''):
        if holding_date_id != '':
            pd.io.sql.execute('update focus_stocks set holding_date_id = %s where code_id = %s and recommended_date_id = %s',
                              cls.engine,
                              params=[str(holding_date_id), str(code_id), str(recommended_date_id)])
        if closed_date_id != '':
            pd.io.sql.execute('update focus_stocks set closed_date_id = %s where code_id = %s and recommended_date_id = %s',
                              cls.engine,
                              params=[str(closed_date_id), str(code_id), str(recommended_date_id)])

        if star_count != '':
            pd.io.sql.execute('update focus_stocks set star_count = %s where code_id = %s and recommended_date_id = %s',
                              cls.engine,
                              params=[str(star_count), str(code_id), str(recommended_date_id)])
    @classmethod
    def get_focus_stocks(cls):
        stocks = pd.read_sql(
            sa.text(' select fs.* from focus_stocks fs where fs.closed_date_id is null order by fs.recommended_date_id desc'),
            cls.engine)
        return stocks

    @classmethod
    def get_stock_focus_logs(cls, code_id, start_date_id='', end_date_id='', star_idx='', recommend_type=''):
        stocks = pd.read_sql(
            sa.text(
                ' select fs.* from focus_stocks fs '
                ' where fs.code_id = :ci '
                ' and fs.recommended_date_id >=:sdi and fs.recommended_date_id <=:edi '
                ' and fs.recommend_type =:rt'
                ' and fs.star_idx =:star_idx'
                ' and fs.closed_date_id is null '
                'order by fs.recommended_date_id desc'),
            cls.engine,
            params={'ci': str(code_id), 'sdi': str(start_date_id), 'edi': str(end_date_id), 'star_idx': str(star_idx), 'rt': recommend_type}
        )
        return stocks

    @classmethod
    def get_focus_stock_log(cls, code_id, recommended_date_id, recommend_type='pca'):
        stocks = pd.read_sql(
            sa.text(
                ' select fs.* from focus_stocks fs '
                ' where fs.recommended_date_id = :di and fs.code_id = :ci and fs.recommend_type=:rt'),
            cls.engine,
            params={'ci': str(code_id), 'di': str(recommended_date_id), 'rt': recommend_type})
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
    def delete_recommend_stock_logs(cls, date_id, recommend_type=''):
        pd.io.sql.execute('delete from recommend_stocks where date_id=%s and recommend_type=%s',
                          cls.engine,
                          params=[str(date_id), recommend_type])

    @classmethod
    def delete_recommend_log(cls, date_id, code_id, recommend_type=''):
        pd.io.sql.execute('delete from recommend_stocks where date_id=%s and code_id = %s and recommend_type=%s',
                          cls.engine,
                          params=[str(date_id), str(code_id), recommend_type])
    @classmethod
    def delete_focus_stocks(cls):
        pd.io.sql.execute('delete from focus_stocks',
                          cls.engine)

    @classmethod
    def get_up_stocks_by_threshold(cls, date_id='', cal_date=''):
        if date_id != '':
            data = pd.read_sql(
                sa.text(' select t.code_id'
                        ' from thresholds  t'
                        ' where t.simple_threshold_v < -0.03 and t.date_id = :date_id'
                        ),
                cls.engine,
                params={'date_id': str(date_id)})
        else:
            data = pd.read_sql(
                sa.text(' select t.code_id'
                        ' from thresholds  t'
                        ' left join trade_cal tc on tc.id = t.date_id'
                        ' where t.simple_threshold_v < -0.03 and tc.cal_date = :cd'
                        ),
                cls.engine,
                params={'cd': str(cal_date)})
        return data

    @classmethod
    def count_threshold_group_by_date_id(cls, start_date_id, end_date_id):
        data = pd.read_sql(
            sa.text(' select tc.cal_date, count(t.code_id) as up_stock_number, '
                    ' (select count(*) from stock_basic sb where sb.list_status = "L" '
                    ' and sb.list_date < tc.cal_date) as list_stock_number'
                    ' from thresholds  t'
                    ' left join trade_cal tc on tc.id = t.date_id'
                    ' where t.simple_threshold_v < -0.03 and t.date_id between :start_date_id and :end_date_id'
                    ' group by t.date_id'
                    ' order by date_id desc'
                    ),
            cls.engine,
            params={'start_date_id': str(start_date_id), 'end_date_id': str(end_date_id)})
        return data

    @classmethod
    def get_all_recommend_logs(cls, current_date_id, next_date_id, recommend_type=''):
        logs = pd.read_sql(
            sa.text(' select rs.*, d.pct_chg from recommend_stocks rs '
                    ' left join daily d on d.code_id = rs.code_id and d.date_id = :ndi'
                    ' where rs.date_id = :cdi and rs.recommend_type = :recommend_type'
                    ' order by rs.code_id asc'),
            cls.engine,
            params={'cdi': str(current_date_id), 'ndi': str(next_date_id), 'recommend_type': recommend_type}
        )
        return logs


    # @staticmethod
    # def validate_field(columns, fields):
    #     valid_fields = list((set(columns).union(set(fields))) ^ (set(columns) ^ set(fields)))
    #     return valid_fields


