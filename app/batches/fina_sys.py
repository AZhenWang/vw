from app.saver.logic import DB
from app.saver.service.fina import Fina
from app.saver.tables import fields_map
import numpy as np
import pandas as pd
from app.models.finance import get_reports, fina_kpi

def execute(start_date='', end_date=''):
    """
    企业分析
    :param start_date: 公告开始日期
    :param end_date: 公告结束日期
    :return:
    """
    # date_id = DB.get_date_id(end_date)
    # codes = DB.get_latestopendays_code_list(
    #     latest_open_days=244*2, date_id=date_id)
    # code_ids = codes['code_id']
    # new_rows = pd.DataFrame(columns=fields_map['fina_sys'])
    # code_ids = [2, 161]
    # code_ids = range(2000, 3670)
    # code_ids = range(1, 500)
    code_ids = range(500, 2000)
    for ci in code_ids:
        print('code_id=', ci)
        Fina.delete_logs_by_end_date(ci, start_date=start_date, end_date=end_date, tablename='fina_sys')
        incomes, balancesheets, cashflows, fina_indicators, holdernum, code_info, cash_divs = get_reports(ci)
        if incomes.empty or incomes.iloc[0]['comp_type'] != 1:
            continue

        data = fina_kpi(incomes, balancesheets, cashflows, fina_indicators, holdernum, code_info, cash_divs)
        if data.empty:
            continue
        data['comp_type'] = incomes.iloc[0]['comp_type']
        data['code_id'] = ci
        data['end_date'] = data.index
        data[data.isin([np.inf, -np.inf])] = np.nan
        # new_rows = pd.concat([new_rows, data.reset_index(drop=True)], sort=False)
        if not data.empty:
            data.to_sql('fina_sys', DB.engine, index=False, if_exists='append', chunksize=1000)

