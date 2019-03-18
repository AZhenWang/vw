from app.saver.logic import DB
import pandas as pd
import numpy as np
from app.models.pca import Pca
from app.common.function import send_email
from email.mime.text import MIMEText

pre_predict_interval = 5
sample_interval = 60
n_components = 2


def execute(start_date='', end_date=''):
    trade_cal = DB.get_open_cal_date(end_date=end_date, period=30)
    today_date_id = trade_cal.iloc[-1]['date_id']
    end_date_id = trade_cal.iloc[-1]['date_id']
    start_date_id = trade_cal.iloc[0]['date_id']
    logs = DB.get_recommended_stocks(start_date_id=start_date_id, end_date_id=end_date_id, recommend_type='pca')
    logs = logs[logs['flag']!=0]
    msgs = []
    recommend_stocks = pd.DataFrame(columns=['flag', 'code_id', 'ts_code', 'name',
                                             'recommend_at', 'average', 'moods', 'rose'
                                             ])
    for i in range(len(logs)):
        code_id = logs.iloc[i]['code_id']
        print('code_id=', code_id)
        log = logs.iloc[i]
        recommended_date_id = log.date_id
        pre_trade_cal = DB.get_open_cal_date_by_id(end_date_id=recommended_date_id, period=2)
        pre_date_id = pre_trade_cal.iloc[0]['date_id']
        focus_log = DB.get_focus_stock_log(code_id=code_id, recommended_date_id=recommended_date_id)
        pre_flag_log = DB.get_flag_recommend_logs(code_id=code_id, start_date_id=pre_date_id,
                                                          end_date_id=pre_date_id,
                                                          flag='1', recommend_type='pca')
        holding = 0
        if focus_log.empty and not pre_flag_log.empty:
            print(log.moods, pre_flag_log.at[0, 'moods'])

            if log.flag == 1 and log.moods >= pre_flag_log.at[0, 'moods'] and log.moods >= 0.2:
                holding = 1

            elif log.flag == -1 and 0.3 <= pre_flag_log.at[0, 'average'] < log.average:
                holding = -1

            if holding != 0:
                pre_pct_chg_sum = DB.sum_pct_chg(code_id=code_id, end_date_id=recommended_date_id, period=4)
                DB.insert_focus_stocks(code_id=code_id,
                                       star_idx=logs.iloc[i]['star_idx'],
                                       predict_rose=0,
                                       recommend_type='pca',
                                       recommended_date_id=recommended_date_id,
                                       pre_pct_chg_sum=round(pre_pct_chg_sum),
                                       )
                if holding > 0:
                    DB.update_focus_stock_log(code_id=code_id, recommended_date_id=recommended_date_id,
                                              holding_date_id=recommended_date_id)
                elif holding < 0:
                    DB.update_focus_stock_log(code_id=code_id, recommended_date_id=recommended_date_id,
                                              closed_date_id=recommended_date_id)
                daily = DB.get_code_daily(code_id=code_id, date_id=recommended_date_id)
                rose = int(np.floor((daily.at[0, 'close'] - daily.at[0, 'open']) * 100 / daily.at[0, 'open']))
                content = {
                    'flag': int(log.flag),
                    'code_id': code_id,
                    'ts_code': log.ts_code,
                    'name': log.name,
                    'recommend_at': log.cal_date,
                    'average': log.average,
                    'moods': log.moods,
                    'rose': rose
                }
                recommend_stocks.loc[i] = content
    if not recommend_stocks.empty:
        recommend_stocks.sort_values(by=['flag', 'recommend_at', 'moods', 'rose'],
                                     ascending=[False, False, False, False], inplace=True)
        recommend_stocks.reset_index(drop=True, inplace=True)
        recommend_text = recommend_stocks.to_string(index=False)

        msgs.append(MIMEText(recommend_text, 'plain', 'utf-8'))
        send_email(subject=end_date + '短期预测', msgs=msgs)

