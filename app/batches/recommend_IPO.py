from app.saver.service.fina import Fina
from app.models.finance import get_reports, fina_kpi
import numpy as np
from app.saver.logic import DB
import pandas as pd

from app.common.function import send_email
from email.mime.text import MIMEText

from conf.myapp import init_date
from datetime import datetime


def execute(start_date='', end_date=''):
    """
    更新新股IPO财务数据
    :param start_date:
    :param end_date:
    :return:
    """
    code_list = DB.get_ipoing(start_date=start_date, end_date=end_date)

    now = datetime.now()
    today = now.strftime('%Y%m%d')
    start_date_id = DB.get_date_id(init_date)
    end_date_id = DB.get_date_id(today)

    recommend_stocks = pd.DataFrame(columns=['ipo_date', 'issue_date', 'code_id', 'cname', 'flag', 'nice',  'end_date', 'roe', 'roe_rd', 'roe_mv',
                                    'freecash_mv', 'receiv_pct', 'receiv_rate', 'oth_receiv_rate', 'Z', 'rev_pct', 'odds',  'odds2'])
    for i in range(len(code_list)):
        ci = code_list.iloc[i]['code_id']
        code_name = code_list.iloc[i]['name']
        Fina.delete_logs_by_date_id(code_id=ci, start_date_id=start_date_id, end_date_id=end_date_id,
                                    tablename='fina_sys')

        incomes, balancesheets, cashflows, fina_indicators, holdernum, code_info, cash_divs = get_reports(ci, start_date_id=start_date_id, end_date_id=end_date_id)
        if incomes.empty or balancesheets.iloc[0]['comp_type'] != 1:
            continue

        data = fina_kpi(incomes, balancesheets, cashflows, fina_indicators, holdernum, code_info, cash_divs)
        if data.empty:
            continue
        data['comp_type'] = balancesheets.iloc[0]['comp_type']
        data['code_id'] = ci
        data['end_date'] = data.index

        data[data.isin([np.inf, -np.inf])] = np.nan
        if not data.empty:
            data.to_sql('fina_sys', DB.engine, index=False, if_exists='append', chunksize=1000)

        data['cname'] = code_name
        data['ipo_date'] = code_list.iloc[i]['ipo_date']
        data['issue_date'] = code_list.iloc[i]['issue_date']
        flags = pd.Series(index=data.index)
        nices = pd.Series(index=data.index)

        for j in range(len(data)):
            log = data.iloc[j]
            # 先判断这个企业是不是历史表现良好
            flag = 0

            if log['liab_pctmv'] < log['rev_pctmv'] * 2 \
                    and log['equity_pctmv'] > 10 and log['fix_asset_pctmv'] > -10 and log['total_assets_pctmv'] > -0 and log['tax_payable_pct'] > 20 \
                    and log['receiv_pct'] < 25 and log['Z'] > 1 \
                    and log['cash_act_in'] > 8 \
                    and log['cash_act_out'] > 0 \
                    and log['i_debt'] < 50 \
                    and log['roe_mv'] > 10 \
                    and log['roe_sale_mv'] > 10\
                    and log['IER'] > 8\
                    and log['oth_receiv_rate'] < 10:
                flag = 1

            elif log['receiv_pct'] > 25 or log['Z'] < 1 \
                    or log['cash_act_in'] < 0 \
                    or log['cash_act_out'] < 0 \
                    or (log['i_debt'] > 40 and log['IER'] < 8) \
                    or log['i_debt'] > 60 \
                    or log['roe_mv'] < 10 \
                    or log['IER'] < 3\
                    or log['oth_receiv_rate'] > 20 \
                    or log['st_borr_rate'] > 40 \
                    or (log['st_borr_pct'] > 50 and log['money_cap_pct'] > 50):
                flag = -1
            # 再看这个企业的业务是不是在突飞猛进
            step = 0

            # 三看年报性价比
            nice = 0
            if (log['pp'] + log['pp_rd']) > 3 \
                    and log['dpd_RR'] > 1.8:
                nice = 1
            elif (log['pp'] + log['pp_rd']) < 1 \
                or log['dpd_RR'] < 1:
                nice = -1

            flags.iloc[j] = flag
            nices.iloc[j] = nice

        data['flag'] = flags
        data['nice'] = nices

        recommend_stocks = recommend_stocks.append(data[recommend_stocks.columns])
    if not recommend_stocks.empty:
        msgs = []
        recommend_stocks.sort_values(by=['ipo_date','code_id', 'end_date'],
                                     ascending=[False, False, False], inplace=True)
        recommend_stocks.reset_index(drop=True, inplace=True)
        recommend_text = recommend_stocks.to_string(index=False)

        msgs.append(MIMEText(recommend_text, 'plain', 'utf-8'))
        send_email(subject=end_date + ' IPO', msgs=msgs)


