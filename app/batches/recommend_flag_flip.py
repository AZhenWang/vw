from app.saver.logic import DB
import pandas as pd
import numpy as np
from app.models.pca import Pca
from app.common.function import send_email
from email.mime.text import MIMEText

pre_predict_interval = 5
n_components = 2
recommend_type = 'rff'

def execute(start_date='', end_date=''):
    trade_cal = DB.get_open_cal_date(start_date=start_date, end_date=end_date)
    today_date_id = trade_cal.iloc[-1]['date_id']
    end_date_id = trade_cal.iloc[-1]['date_id']
    start_date_id = trade_cal.iloc[0]['date_id']
    logs = DB.get_recommended_stocks(start_date_id=start_date_id, end_date_id=end_date_id, recommend_type=recommend_type)
    logs = logs[logs['flag']!=0]
    msgs = []
    recommend_stocks = pd.DataFrame(columns=['code_id', 'ts_code', 'name',
                                             'recommend_at', 'average', 'moods', 'flag', 'qqb', 'pre4_sum', 'rose'
                                             ])
    for i in range(len(logs)):
        code_id = logs.iloc[i]['code_id']
        print('code_id=', code_id)
        log = logs.iloc[i]
        recommended_date_id = log.date_id
        focus_log = DB.get_focus_stock_log(code_id=code_id, recommended_date_id=recommended_date_id)
        pre_flag_log = DB.get_pre_flag_logs(code_id=code_id, date_id=recommended_date_id-1, period=1, recommend_type='pca')
        holding = 0
        if focus_log.empty and not pre_flag_log.empty:
            if pre_flag_log.at[0, 'flag'] == 1 and log.flag == -1 \
                and pre_flag_log.at[0, 'average'] < pre_flag_log.at[0, 'moods'] \
                    and log.average > log.moods:
                    holding = 1

            if holding != 0:
                daily = DB.get_code_daily(code_id=code_id, date_id=recommended_date_id)
                rose = int(np.floor((daily.at[0, 'close'] - daily.at[0, 'open']) * 100 / daily.at[0, 'open']))
                DB.insert_focus_stocks(code_id=code_id,
                                       star_idx=logs.iloc[i]['star_idx'],
                                       predict_rose=rose,
                                       recommend_type=recommend_type,
                                       recommended_date_id=recommended_date_id,
                                       )
                if holding > 0:
                    DB.update_focus_stock_log(code_id=code_id, recommended_date_id=recommended_date_id,
                                              holding_date_id=recommended_date_id)
                elif holding < 0:
                    DB.update_focus_stock_log(code_id=code_id, recommended_date_id=recommended_date_id,
                                              closed_date_id=recommended_date_id)
                content = {
                    'flag': int(log.flag),
                    'code_id': code_id,
                    'ts_code': log.ts_code,
                    'name': log['name'],
                    'recommend_at': log.cal_date,
                    'average': log.average,
                    'moods': log.moods,
                    'qqb': log.qqb,
                    'pre4_sum': log.pre4_sum,
                    'rose': rose
                }
                recommend_stocks.loc[i] = content
    if not recommend_stocks.empty:
        recommend_stocks.sort_values(by=['recommend_at', 'flag', 'moods', 'rose'],
                                     ascending=[False, False, False, False], inplace=True)
        recommend_stocks.reset_index(drop=True, inplace=True)
        recommend_text = recommend_stocks.to_string(index=False)

        msgs.append(MIMEText(recommend_text, 'plain', 'utf-8'))
        send_email(subject=end_date + '短期预测', msgs=msgs)

