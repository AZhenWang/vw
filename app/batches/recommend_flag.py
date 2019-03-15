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
    trade_cal = DB.get_open_cal_date(end_date=end_date, period=5)
    today_date_id = trade_cal.iloc[-1]['date_id']
    end_date_id = trade_cal.iloc[-2]['date_id']
    start_date_id = trade_cal.iloc[0]['date_id']
    logs = DB.get_recommended_stocks(start_date_id=start_date_id, end_date_id=end_date_id, recommend_type='pca')
    logs = logs[logs['star_idx']==1]
    print(logs)
    msgs = []
    recommend_stocks = pd.DataFrame(columns=['flag', 'code_id', 'ts_code', 'code_name',
                                             'recommend_at', 'holding_at', 'pct_chg', 'predict_rose'
                                             ])
    for i in range(len(logs)):
        code_id = logs.iloc[i]['code_id']
        print('code_id=', code_id)
        recommended_date_id = logs.iloc[i]['date_id']

        focus_log = DB.get_focus_stock_log(code_id=code_id, recommended_date_id=recommended_date_id)
        next_dailys = DB.get_code_daily_later(code_id=code_id, date_id=recommended_date_id, period=5)
        predict_rose = 0
        if focus_log.empty and not next_dailys.empty:
            for j in range(len(next_dailys)):
                next_daily = next_dailys.iloc[j]
                rose = (next_daily['close'] - next_daily['open']) * 100 / next_daily['open']
                if rose > 3:
                    predict_rose = np.floor(rose) * 10
                elif next_daily['pct_chg'] > 3:
                    predict_rose = np.floor(next_daily['pct_chg'])
                if predict_rose > 0:
                    pre_pct_chg_sum = DB.sum_pct_chg(code_id=code_id, end_date_id=recommended_date_id, period=4)
                    DB.insert_focus_stocks(code_id=code_id,
                                           star_idx=logs.iloc[i]['star_idx'],
                                           predict_rose=predict_rose,
                                           recommend_type='pca',
                                           recommended_date_id=recommended_date_id,
                                           pre_pct_chg_sum=pre_pct_chg_sum,
                                           )

                    DB.update_focus_stock_log(code_id=code_id, recommended_date_id=recommended_date_id,
                                      holding_date_id=next_daily['date_id'])
                    focus_daily = DB.get_code_daily(code_id=code_id, date_id=next_daily['date_id'])
                    content = {
                        'flag': int(logs.iloc[i]['flag']),
                        'predict_rose': int(predict_rose),
                        'code_id': code_id,
                        'ts_code': logs.iloc[i]['ts_code'],
                        'code_name': logs.iloc[i]['name'],
                        'recommend_at': logs.iloc[i]['cal_date'],
                        'holding_at': focus_daily.at[0, 'cal_date'],
                        'pct_chg': np.round(focus_daily.at[0, 'pct_chg'], 1)
                    }
                    recommend_stocks.loc[code_id] = content
                    break

    if not recommend_stocks.empty:
        recommend_stocks.sort_values(by=['predict_rose', 'pct_chg'],
                                     ascending=[False, False], inplace=True)
        recommend_stocks.reset_index(drop=True, inplace=True)
        recommend_text = recommend_stocks.to_string(index=True)

        msgs.append(MIMEText(recommend_text, 'plain', 'utf-8'))
        send_email(subject=end_date + '短期预测', msgs=msgs)

