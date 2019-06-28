from app.saver.logic import DB
import pandas as pd
import numpy as np
from app.models.pca import Pca
from app.common.function import send_email
from email.mime.text import MIMEText

pre_predict_interval = 5
n_components = 2
recommend_type = 'moneyflow'


def execute(start_date='', end_date=''):
    trade_cal = DB.get_open_cal_date(start_date=start_date, end_date=end_date)
    end_date_id = trade_cal.iloc[-1]['date_id']
    start_date_id = trade_cal.iloc[0]['date_id']
    logs = DB.get_mv_moneyflow(start_date_id=start_date_id, end_date_id=end_date_id)
    msgs = []
    recommend_stocks = pd.DataFrame(columns=['ts_code', 'code_name',  'market',
                                             'predict_rose', 'pct_chg', 'moods', 'qqb',
                                             'code_id', 'type', 'recommend_at', 'holding_at', 'holding_pct_chg',
                                             ])
    for i in range(len(logs)):
        code_id = logs.iloc[i]['code_id']
        recommended_date_id = logs.iloc[i]['date_id']
        log = logs.iloc[i]
        if log['net_elg'] > 0.1 and log['turnover_rate_f2'] < 25 and log['mv_turnover_rate_f2'] < 20 and \
            log['mv_elg_base_diff5'] > log['mv_elg_base_diff10'] and log['mv_elg_base_diff5'] > 0.1 and \
            log['mv_mv_tr_f2_pct_chg'] > 0.3 and log['mv_mv_tr_f2'] > 0:
            predict_rose = 0
            DB.insert_focus_stocks(code_id=code_id,
                                   star_idx=1,
                                   predict_rose=predict_rose,
                                   recommend_type=recommend_type,
                                   recommended_date_id=recommended_date_id,
                                   )

            content = {
                'ts_code': log['ts_code'],
                'code_name': log['name'],
                'code_id': code_id,
                'date': log['cal_date'],
                'mv_mv_tr_f2_pct_chg': log['mv_mv_tr_f2_pct_chg']
            }
            recommend_stocks.loc[code_id] = content

    if not recommend_stocks.empty:
        recommend_stocks.sort_values(by=['date', 'mv_mv_tr_f2_pct_chg'],
                                     ascending=[False, False], inplace=True)
        recommend_stocks.reset_index(drop=True, inplace=True)
        recommend_text = recommend_stocks.to_string(index=True)

        msgs.append(MIMEText(recommend_text, 'plain', 'utf-8'))
        send_email(subject=end_date + '换手率预测', msgs=msgs)

