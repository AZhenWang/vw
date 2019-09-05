from app.saver.common import Base
import pandas as pd
import sqlalchemy as sa


class Fina(Base):

    @classmethod
    def get_existed_fina_by_end_date(cls, table_name, ts_code, start_date, end_date):
        sql = ' SELECT end_date FROM ' + table_name + ' as api ' \
                                                      ' left join stock_basic sb on sb.id = api.code_id' \
                                                      ' where sb.ts_code = :ts_code ' \
                                                      ' and api.end_date >= :start_date and api.end_date <= :end_date' \
                                                      ' order by api.end_date desc'

        params = {'ts_code': ts_code, 'start_date': start_date, 'end_date': end_date}

        existed_reports = pd.read_sql(
            sa.text(sql),
            cls.engine,
            params=params
        )

        return existed_reports

    @classmethod
    def get_existed_reports(cls, table_name, ts_code, start_date, end_date, report_type=''):
        sql = ' SELECT end_date FROM ' + table_name + ' as api ' \
                                                      ' left join stock_basic sb on sb.id = api.code_id' \
                                                      ' left join trade_cal tc on tc.id = api.date_id ' \
                                                      ' where sb.ts_code = :ts_code ' \
                                                      ' and tc.cal_date >= :start_date and tc.cal_date <= :end_date'

        params = {'ts_code': ts_code, 'start_date': start_date, 'end_date': end_date}

        if report_type != '':
            sql += ' and api.report_type = :report_type '
            params['report_type'] = report_type

        sql += ' order by api.date_id desc'

        existed_reports = pd.read_sql(
            sa.text(sql),
            cls.engine,
            params=params
        )

        return existed_reports

    @classmethod
    def get_report_info(cls, code_id, start_date='', end_date='', TTB='', report_type='', end_date_type=''):
        """

        :param code_id:
        :param start_date:
        :param end_date: 报告结束日期
        :param TTB: 表名，income: 利润表， balancesheet: 资产负债表， cashflow：现金流量表
        :param report_type: 1： 合并报告
        :param end_date_type: 0331：一季报， 0630：半年报， 0930：三季报， %1231: 年报，
        :return:
        """
        sql_str = ' SELECT r.*' \
                  ' FROM ' + TTB + ' r ' \
                                   ' where r.code_id = :code_id ' \
                                   ' and r.end_date >= :sdi and r.end_date <= :edi'
        params = {'code_id': str(code_id), 'sdi': str(start_date), 'edi': str(end_date)}

        if report_type != '':
            sql_str += ' and r.report_type =:report_type'
            params['report_type'] = report_type

        if end_date_type != '':
            sql_str += ' and r.end_date like ' + ':end_date_type'
            params['end_date_type'] = end_date_type

        sql_str += ' order by r.end_date asc '

        report_info = pd.read_sql(sa.text(sql_str), cls.engine, params=params)
        return report_info

    @classmethod
    def get_fina_sys_logs(cls, code_id, start_date='', end_date='', TTB=''):
        """
        获取财务分析日志
        :param code_id:
        :param start_date:
        :param end_date: 报告结束日期
        :param TTB: 表名，income: 利润表， balancesheet: 资产负债表， cashflow：现金流量表
        :return:
        """
        sql_str = ' SELECT r.* from fina_sys where r.code_id = :code_id ' \
                                   ' and r.end_date >= :sdi and r.end_date <= :edi'
        params = {'code_id': str(code_id), 'sdi': str(start_date), 'edi': str(end_date)}

        if report_type != '':
            sql_str += ' and r.report_type =:report_type'
            params['report_type'] = report_type

        if end_date_type != '':
            sql_str += ' and r.end_date like ' + ':end_date_type'
            params['end_date_type'] = end_date_type

        sql_str += ' order by r.end_date asc '

        report_info = pd.read_sql(sa.text(sql_str), cls.engine, params=params)
        return report_info

    @classmethod
    def delete_logs_by_end_date(cls, code_id='', start_date='', end_date='', tablename=''):
        pd.io.sql.execute('delete from ' + tablename + ' where code_id = %s and end_date >= %s and end_date <= %s',
                          cls.engine, params=[str(code_id), str(start_date), str(end_date)])

    @classmethod
    def delete_fina_super_logs(cls, code_id='', start_date='', end_date=''):
        pd.io.sql.execute('delete from fina_super where code_id = %s and cal_date >= %s and cal_date <= %s',
                          cls.engine, params=[str(code_id), str(start_date), str(end_date)])

    @classmethod
    def get_divdends(cls, code_id, start_date, end_date):
        sql = ' SELECT api.*, b.total_share  FROM dividend as api ' \
              ' left join trade_cal tc on tc.cal_date = api.end_date ' \
              ' left join daily_basic b on b.code_id = api.code_id and b.date_id = tc.id' \
              ' where api.code_id = :ci ' \
              ' and tc.cal_date >= :start_date and tc.cal_date <= :end_date' \
              ' order by api.end_date asc'

        params = {'ci': code_id, 'start_date': start_date, 'end_date': end_date}

        existed_reports = pd.read_sql(
            sa.text(sql),
            cls.engine,
            params=params
        )

        return existed_reports
