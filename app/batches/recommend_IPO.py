from app.saver.service.fina import Fina
from app.models.finance import get_reports, fina_kpi
import numpy as np
from app.saver.logic import DB
import pandas as pd

from app.common.function import send_email
from email.mime.text import MIMEText


def execute(start_date='', end_date=''):
    """
    更新新股IPO财务数据
    :param start_date:
    :param end_date:
    :return:
    """
    code_list = DB.get_ipoing()

    recommend_stocks = pd.DataFrame(columns=['ipo_date', 'code_id', 'cname', 'end_date', 'roe', 'roe_sale', 'roe_mv', 'pp', 'pp_sale', 'dpd_RR',
                                     'pe', 'pb', 'IER', 'oth_receiv_rate', 'Z', 'rev_pct', 'odds',  'odds2', 'odds_pp'])
    # code_ids = [3820, 3819]
    for i in range(len(code_list)):
        ci = code_list.iloc[i]['code_id']
        code_name = code_list.iloc[i]['name']
        Fina.delete_logs_by_end_date(code_id=ci, start_date=start_date, end_date=end_date, tablename='fina_sys')
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
        if not data.empty:
            data.to_sql('fina_sys', DB.engine, index=False, if_exists='append', chunksize=1000)

        data['cname'] = code_name
        recommend_stocks = recommend_stocks.append(data[recommend_stocks.columns])
    if not recommend_stocks.empty:
        msgs = []
        recommend_stocks.sort_values(by=['ipo_date', 'end_date'],
                                     ascending=[False, False], inplace=True)
        recommend_stocks.reset_index(drop=True, inplace=True)
        recommend_text = recommend_stocks.to_string(index=False)

        msgs.append(MIMEText(recommend_text, 'plain', 'utf-8'))
        send_email(subject=end_date + ' IPO', msgs=msgs)


