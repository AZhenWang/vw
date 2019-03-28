from app.saver.logic import DB
import pandas as pd
import numpy as np
from app.models.pca import Pca
from app.common.function import send_email
from email.mime.text import MIMEText

pre_predict_interval = 5
n_components = 2
recommend_type = 'rss'


def execute(start_date='', end_date=''):
    trade_cal = DB.get_open_cal_date(start_date=start_date, end_date=end_date)
    today_date_id = trade_cal.iloc[-1]['date_id']
    end_date_id = trade_cal.iloc[-2]['date_id']
    start_date_id = trade_cal.iloc[0]['date_id']
    logs = DB.get_recommended_stocks(start_date_id=start_date_id, end_date_id=end_date_id, recommend_type='pca')
    logs = logs[logs['star_idx'] == 3]
    msgs = []
    recommend_stocks = pd.DataFrame(columns=['ts_code', 'code_name',  'market',
                                             'predict_rose', 'pct_chg', 'moods', 'qqb',
                                             'code_id', 'type', 'recommend_at', 'holding_at', 'holding_pct_chg',
                                             ])
    for i in range(len(logs)):
        code_id = logs.iloc[i]['code_id']
        print('code_id=', code_id)
        print('log=', logs.iloc[i])
        recommended_date_id = logs.iloc[i]['date_id']

        pre_trade_cal = DB.get_open_cal_date_by_id(end_date_id=recommended_date_id, period=3)
        after_trade_cal = DB.get_open_cal_date_by_id(start_date_id=recommended_date_id, period=3)
        grand_pre_date_id = pre_trade_cal.iloc[0]['date_id']
        next_date_id = after_trade_cal.iloc[1]['date_id']
        big_next_date_id = after_trade_cal.iloc[-1]['date_id']
        data = DB.count_threshold_group_by_date_id(start_date_id=grand_pre_date_id, end_date_id=next_date_id)
        data.eval('up_stock_ratio=up_stock_number/list_stock_number*100', inplace=True)
        data['up_stock_ratio'] = data['up_stock_ratio'].apply(np.round, decimals=2)
        if len(data) != 4:
            continue
        market = np.where(np.diff(data['up_stock_ratio']) < 0, 1, 0)
        # if market[-2] < 1:
        #     continue
        # print(market)
        recommended_daily = DB.get_code_daily(code_id=code_id, date_id=recommended_date_id)
        focus_log = DB.get_focus_stock_log(code_id=code_id, recommended_date_id=recommended_date_id)
        if not focus_log.empty and (focus_log.at[0, 'closed_date_id'] or focus_log.at[0, 'holding_date_id']):
            continue
        next_daily = DB.get_code_daily_later(code_id=code_id, date_id=recommended_date_id, period=1)
        if focus_log.empty and not next_daily.empty:
            second_recommend_log = DB.get_code_recommend_logs(code_id=code_id, start_date_id=next_date_id,
                                                             end_date_id=big_next_date_id,
                                                             star_idx='3', recommend_type='pca')

            print(not second_recommend_log.empty)
            print(recommended_daily.at[0, 'close'] >= recommended_daily.at[0, 'open'])
            print(recommended_daily.at[0, 'pct_chg'] > 3)
            print(recommended_daily.at[0, 'pct_chg'] > next_daily.iloc[0]['pct_chg'] > 0)
            print(next_daily.iloc[0]['close'] >= next_daily.iloc[0]['open'])
            print(next_daily.iloc[0]['open'] < recommended_daily.at[0, 'close'])
            print(next_daily.iloc[0]['close'] > recommended_daily.at[0, 'high'] * 1.01)
            if not second_recommend_log.empty \
                    and recommended_daily.at[0, 'close'] >= recommended_daily.at[0, 'open'] \
                    and recommended_daily.at[0, 'pct_chg'] > 3 \
                    and recommended_daily.at[0, 'pct_chg'] > next_daily.iloc[0]['pct_chg'] > 0 \
                    and next_daily.iloc[0]['close'] >= next_daily.iloc[0]['open'] \
                    and next_daily.iloc[0]['open'] < recommended_daily.at[0, 'close'] \
                    and next_daily.iloc[0]['close'] > recommended_daily.at[0, 'high'] * 1.01:
                predict_rose = (np.floor(recommended_daily.at[0, 'pct_chg'] + next_daily.iloc[0]['pct_chg'])) * 10

                DB.insert_focus_stocks(code_id=code_id,
                                       star_idx=logs.iloc[i]['star_idx'],
                                       predict_rose=predict_rose,
                                       recommend_type=recommend_type,
                                       recommended_date_id=recommended_date_id,
                                       )
                DB.update_focus_stock_log(code_id=code_id, recommended_date_id=recommended_date_id,
                                          holding_date_id=second_recommend_log.at[0, 'date_id'])
                focus_daily = DB.get_code_daily(code_id=code_id, date_id=second_recommend_log.at[0, 'date_id'])

                content = {
                    'ts_code': logs.iloc[i]['ts_code'],
                    'code_name': logs.iloc[i]['name'],
                    'qqb': logs.iloc[i]['qqb'],
                    'predict_rose': int(predict_rose),
                    'pct_chg': int(np.floor(recommended_daily.at[0, 'pct_chg'])),
                    'market': market,
                    'moods': logs.iloc[i]['moods'],
                    'code_id': code_id,
                    'type': recommend_type,
                    'recommend_at': logs.iloc[i]['cal_date'],
                    'holding_at': focus_daily.at[0, 'cal_date'],
                    'holding_pct_chg': focus_daily.at[0, 'pct_chg']
                }
                recommend_stocks.loc[code_id] = content

    if not recommend_stocks.empty:
        recommend_stocks.sort_values(by=['pct_chg', 'holding_pct_chg'],
                                     ascending=[False, False], inplace=True)
        recommend_stocks.reset_index(drop=True, inplace=True)
        recommend_text = recommend_stocks.to_string(index=True)

        msgs.append(MIMEText(recommend_text, 'plain', 'utf-8'))
        send_email(subject=end_date + '短期预测', msgs=msgs)

