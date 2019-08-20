from app.saver.logic import DB
from app.saver.service.fina import Fina
import numpy as np
import pandas as pd


def execute(start_date='', end_date=''):
    """
    企业分析统计
    :param start_date: 公告开始日期
    :param end_date: 公告结束日期
    :return:
    """
    new_rows = pd.DataFrame(columns=['code_id', 'end_date', 'adj_close', 'total_mv',
        'income_rate', 'roe',  'eps_mul',  'V', 'glem_V', 'dpd_V', 'dyr',
        'roe_inc', 'RR', 'glem_RR', 'dpd_RR',
        'pe', 'pb', 'i_debt', 'capital_turn', 'oper_pressure', 'OPM',
        'X1', 'X2', 'X3', 'X4', 'X5', 'Z',
        'free_cash_mv', 'lib_cash', 'receiv_pct', 'money_cap', 'years', 'result'])

    codes = DB.get_code_list(list_status='')
    code_ids = codes['code_id']
    # code_ids = [2]
    for code_id in code_ids:
        DB.delete_code_logs(code_id, tablename='fina_recom_logs')
        logs = Fina.get_report_info(code_id=code_id, start_date=start_date, end_date=end_date, TTB='fina_sys',
                                    end_date_type='%1231%')
        logs.dropna(inplace=True)
        logs.reset_index(inplace=True, drop=True)
        Y = logs['adj_close']
        point_args = np.diff(np.where(np.diff(Y) > 0, 0, 1))
        logs = logs.join(pd.Series(point_args, name='point').shift())
        for j in range(len(logs)-1):
            index = logs.index[j]
            today = logs.iloc[j]['end_date']
            later_logs = logs[logs.end_date > today]
            corner = later_logs[later_logs['point'] != 0]
            if not corner.empty:
                next_act = corner.iloc[0]
                next_close = next_act['adj_close']
                next_date = next_act['end_date']
                today_close = logs.iloc[j]['adj_close']
                result = round((next_close - today_close) / min(next_close, today_close), 2)
                logs.at[index, 'result'] = result
                logs.at[index, 'years'] = (int(next_date) - int(today)) / 10000

        new_rows = pd.concat([new_rows, logs], sort=False)

    if not new_rows.empty:
        new_rows.to_sql('fina_recom_logs', DB.engine, index=False, if_exists='append', chunksize=1000)

