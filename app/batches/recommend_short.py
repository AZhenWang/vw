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
    trade_cal = DB.get_open_cal_date(end_date=end_date, period=22)
    today_date_id = trade_cal.iloc[-1]['date_id']
    end_date_id = trade_cal.iloc[-2]['date_id']
    start_date_id = trade_cal.iloc[0]['date_id']
    logs = DB.get_recommended_stocks(start_date_id=start_date_id, end_date_id=end_date_id, recommend_type='pca')
    logs = logs[logs['star_idx'] == 3]
    print('logs=', logs)
    msgs = []
    recommend_stocks = pd.DataFrame(columns=['n', 'ts_code', 'code_name',  'market',
                                             'predict_rose', 'pct_chg', 'moods', 'amplitude',
                                             'code_id', 'recommend_at', 'holding_at', 'holding_pct_chg',
                                             ])
    n = 1
    for i in range(len(logs)):
        code_id = logs.iloc[i]['code_id']
        print('code_id=', code_id)
        recommended_date_id = logs.iloc[i]['date_id']

        after_trade_cal = DB.get_open_cal_date_by_id(start_date_id=recommended_date_id, period=3)
        next_date_id = after_trade_cal.iloc[1]['date_id']
        big_next_date_id = after_trade_cal.iloc[-1]['date_id']
        data = DB.count_threshold_group_by_date_id(start_date_id=grand_pre_date_id, end_date_id=next_date_id)
        data.eval('up_stock_ratio=up_stock_number/list_stock_number*100', inplace=True)
        data['up_stock_ratio'] = data['up_stock_ratio'].apply(np.round, decimals=2)
        if len(data) != 4:
            continue
        market = np.where(np.diff(data['up_stock_ratio']) < 0, 1, 0)
        recommended_daily = DB.get_code_daily(code_id=code_id, date_id=recommended_date_id)
        focus_log = DB.get_focus_stock_log(code_id=code_id, recommended_date_id=recommended_date_id)
        if not focus_log.empty and (focus_log.at[0, 'closed_date_id'] or focus_log.at[0, 'holding_date_id']):
            continue
        if focus_log.empty:
            next_daily = DB.get_code_daily_later(code_id=code_id, date_id=recommended_date_id, period=1)
            second_recommend_log = DB.get_code_recommend_logs(code_id=code_id, start_date_id=next_date_id,
                                                             end_date_id=big_next_date_id,
                                                             star_idx='4', recommend_type='pca')
            print(not second_recommend_log.empty,
                     recommended_daily.at[0, 'close'] >= recommended_daily.at[0, 'open'],
                     recommended_daily.at[0, 'pct_chg'] > 3,
                     next_daily.iloc[0]['pct_chg'] >= 0,
                     next_daily.iloc[0]['close'] >= next_daily.iloc[0]['open'],
                     next_daily.iloc[0]['close'] > recommended_daily.at[0, 'close'] * 1.01)

            if not second_recommend_log.empty \
                    and recommended_daily.at[0, 'close'] >= recommended_daily.at[0, 'open'] \
                    and recommended_daily.at[0, 'pct_chg'] > 3 \
                    and next_daily.iloc[0]['pct_chg'] >= 0 \
                    and next_daily.iloc[0]['close'] >= next_daily.iloc[0]['open'] \
                    and next_daily.iloc[0]['close'] > recommended_daily.at[0, 'close'] * 1.01:
                predict_rose = (np.floor(recommended_daily.at[0, 'pct_chg'] + next_daily.iloc[0]['pct_chg'])) * 10

                pre_pct_chg_sum = DB.sum_pct_chg(code_id=code_id, end_date_id=recommended_date_id, period=4)
                DB.insert_focus_stocks(code_id=code_id,
                                       star_idx=logs.iloc[i]['star_idx'],
                                       predict_rose=predict_rose,
                                       recommend_type='pca',
                                       recommended_date_id=recommended_date_id,
                                       pre_pct_chg_sum=pre_pct_chg_sum,
                                       )
                DB.update_focus_stock_log(code_id=code_id, recommended_date_id=recommended_date_id,
                                          holding_date_id=second_recommend_log.at[0, 'date_id'])
                focus_daily = DB.get_code_daily(code_id=code_id, date_id=second_recommend_log.at[0, 'date_id'])
                pca = Pca(cal_date=focus_daily.at[0, 'cal_date'])
                pca_features, prices, Y = pca.run(code_id=code_id, pre_predict_interval=pre_predict_interval,
                                                  n_components=n_components, return_y=True)
                bottom_dis = 20
                Y0 = pca_features.col_0[-bottom_dis:]
                point_args = np.diff(np.where(np.diff(Y0) > 0, 0, 1))
                peaks = Y0[1:-1][point_args == 1]
                bottoms = np.floor((Y0[1:-1][point_args == -1]) * 100) / 100
                amplitude = 0
                if len(bottoms) >= 2 and len(peaks) >= 2:
                    if Y0.iloc[-2] < Y0.iloc[-1] and (
                            Y0.iloc[-1] > peaks.iloc[-1] or peaks.iloc[-1] > peaks.iloc[-2]) and bottoms.iloc[-1] >= \
                            bottoms.iloc[-2]:
                        # 底上升
                        amplitude = 1
                    elif Y0.iloc[-2] > Y0.iloc[-1] and (
                            Y0.iloc[-1] < bottoms.iloc[-1] or bottoms.iloc[-1] <= bottoms.iloc[-2]) and peaks.iloc[-1] < \
                            peaks.iloc[-2]:
                        # 底下降
                        amplitude = -1

                content = {
                    'n': n,
                    'ts_code': logs.iloc[i]['ts_code'],
                    'code_name': logs.iloc[i]['name'],
                    'amplitude': amplitude,
                    'predict_rose': int(predict_rose),
                    'pct_chg': int(np.floor(recommended_daily.at[0, 'pct_chg'])),
                    'market': market,
                    'moods': logs.iloc[i]['moods'],
                    'code_id': code_id,
                    'recommend_at': logs.iloc[i]['cal_date'],
                    'holding_at': focus_daily.at[0, 'cal_date'],
                    'holding_pct_chg': focus_daily.at[0, 'pct_chg']
                }
                recommend_stocks.loc[code_id] = content
                n += 1
    if not recommend_stocks.empty:
        recommend_stocks.sort_values(by=['holding_pct_chg', 'predict_rose'],
                                     ascending=[False, False], inplace=True)
        recommend_text = recommend_stocks.to_string(index=False)

        msgs.append(MIMEText(recommend_text, 'plain', 'utf-8'))
        send_email(subject=end_date + '预测', msgs=msgs)

