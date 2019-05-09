from app.saver.logic import DB
import pandas as pd
import numpy as np
from app.models.pca import Pca
from app.common.function import send_email
from email.mime.text import MIMEText

pre_predict_interval = 5
n_components = 2
recommend_type = 'rs'


def execute(start_date='', end_date=''):
    trade_cal = DB.get_open_cal_date(start_date=start_date, end_date=end_date)
    today_date_id = trade_cal.iloc[-1]['date_id']
    end_date_id = trade_cal.iloc[-4]['date_id']
    start_date_id = trade_cal.iloc[0]['date_id']
    logs = DB.get_recommended_stocks(start_date_id=start_date_id, end_date_id=end_date_id, recommend_type='pca')
    logs = logs[logs['star_idx'] >= 1]
    msgs = []
    recommend_stocks = pd.DataFrame(columns=['star', 'ts_code', 'code_name', 'type', 'recommend_at', 'market', 'star_count',
                                             'predict_rose', 'pct_chg',  'average', 'moods', 'qqb', 'pre4_sum',
                                             'code_id', 'holding_at', 'holding_pct_chg',
                                             ])
    for i in range(len(logs)):
        code_id = logs.iloc[i]['code_id']
        print('code_id=', code_id)
        if code_id in recommend_stocks.index:
            continue
        recommended_date_id = logs.iloc[i]['date_id']

        pre_trade_cal = DB.get_open_cal_date_by_id(end_date_id=recommended_date_id, period=3)
        after_trade_cal = DB.get_open_cal_date_by_id(start_date_id=recommended_date_id, period=5)
        grand_pre_date_id = pre_trade_cal.iloc[0]['date_id']
        next_date_id = after_trade_cal.iloc[1]['date_id']
        big_next_date = after_trade_cal.iloc[-1]['cal_date']
        market = []
        data = DB.count_threshold_group_by_date_id(start_date_id=grand_pre_date_id, end_date_id=next_date_id)
        data.eval('up_stock_ratio=up_stock_number/list_stock_number*100', inplace=True)
        data['up_stock_ratio'] = data['up_stock_ratio'].apply(np.round, decimals=2)
        if len(data) != 4:
            continue
        market = np.where(np.diff(data['up_stock_ratio']) < 0, 1, 0)
        if market[-2] < 1:
            continue

        recommended_daily = DB.get_code_daily(code_id=code_id, date_id=recommended_date_id)
        focus_log = DB.get_focus_stock_log(code_id=code_id, recommended_date_id=recommended_date_id)
        if not focus_log.empty and (focus_log.at[0, 'closed_date_id'] or focus_log.at[0, 'holding_date_id']):
            continue
        predict_rose = 0  # 预测涨幅
        pre_pct_chg_sum = 0
        if focus_log.empty:
            if logs.iloc[i]['star_idx'] == 1 or logs.iloc[i]['star_idx'] == 3:
                if recommended_daily.at[0, 'close'] > recommended_daily.at[0, 'open']:
                    next_daily = DB.get_code_daily_later(code_id=code_id, date_id=recommended_date_id, period=3)
                    if ((next_daily.iloc[0]['pct_chg'] >= 0 and next_daily.iloc[0]['close'] >= next_daily.iloc[0]['open']) or \
                            (next_daily.iloc[1]['pct_chg'] >= 0 and next_daily.iloc[1]['close'] >= next_daily.iloc[1]['open'])) \
                        and np.min(next_daily['close']) >= recommended_daily.at[0, 'open'] \
                        and not (next_daily.iloc[1]['pct_chg'] < 0 and next_daily.iloc[2]['pct_chg'] < 0):

                        if next_daily.iloc[0]['pct_chg'] > 0:
                            predict_rose = (np.floor(recommended_daily.at[0, 'pct_chg'] + next_daily.iloc[0]['pct_chg'])) * 10
                        else:
                            predict_rose = (np.floor(recommended_daily.at[0, 'pct_chg'])) * 10

            if predict_rose > 0:
                # # 20天内已经有提示的，此时就不再提示
                # pre_trade_cal = DB.get_open_cal_date_by_id(end_date_id=recommended_date_id, period=20)
                #
                # latestfocus_logs = DB.get_stock_focus_logs(code_id=code_id, start_date_id=pre_trade_cal.iloc[0]['date_id'],
                #                                            end_date_id=pre_trade_cal.iloc[-2]['date_id'],
                #                                            star_idx=logs.iloc[i]['star_idx'], recommend_type='pca')
                # if latestfocus_logs.holding_date_id.any():
                #     predict_rose = 0
                # else:
                DB.insert_focus_stocks(code_id=code_id,
                                       star_idx=logs.iloc[i]['star_idx'],
                                       predict_rose=predict_rose,
                                       recommend_type=recommend_type,
                                       recommended_date_id=recommended_date_id,
                                       )
        else:
            predict_rose = focus_log.at[0, 'predict_rose']
            pre_pct_chg_sum = focus_log.at[0, 'pre_pct_chg_sum']

        if predict_rose > 0:
            big_later_dailys = DB.get_code_info(code_id=code_id, start_date=big_next_date, end_date=end_date)
            second_daily = DB.get_code_daily_later(code_id=code_id, date_id=recommended_date_id, period=3)

            holding_at = None
            holding_pct_chg = None
            send = True
            for j in range(len(big_later_dailys)):
                later_daily = big_later_dailys.iloc[j]
                date_id = big_later_dailys.index[j]
                later_recommend_log = DB.get_recommend_log(code_id=code_id, date_id=date_id, recommend_type='pca')
                if later_daily['close'] < recommended_daily.at[0, 'close']:
                    closed_date_id = date_id
                    send = False
                    DB.update_focus_stock_log(code_id=code_id, recommended_date_id=recommended_date_id,
                                              closed_date_id=closed_date_id)
                    break

                elif (later_recommend_log.empty or later_recommend_log.at[0, 'star_idx'] != logs.iloc[i]['star_idx']) and ((later_daily['close'] - later_daily['open']) / later_daily['open']) > 0.01 \
                        and later_daily['pct_chg'] > 2 \
                        and later_daily['close'] > (np.max([recommended_daily.at[0, 'high'], second_daily.at[0, 'high'], second_daily.at[1, 'high'], second_daily.at[2, 'high']]))*1.01:
                    holding_date_id = date_id
                    holding_at = later_daily['cal_date']
                    holding_pct_chg = later_daily['pct_chg']
                    DB.update_focus_stock_log(code_id=code_id, recommended_date_id=recommended_date_id,
                                              holding_date_id=holding_date_id)
                    break

            if send:
                star_count = DB.count_recommend_star(code_id=code_id, start_date_id=recommended_date_id,
                                                     end_date_id=date_id, star_idx=logs.iloc[i]['star_idx'],
                                                     recommend_type='pca')
                DB.update_focus_stock_log(code_id=code_id, recommended_date_id=recommended_date_id,
                                          star_count=star_count)

                content = {
                    'ts_code': logs.iloc[i]['ts_code'],
                    'code_name': logs.iloc[i]['name'],
                    'type': recommend_type,
                    'recommend_at': logs.iloc[i]['cal_date'],
                    'star': logs.iloc[i]['star_idx'],
                    'star_count': star_count,
                    'qqb': logs.iloc[i]['qqb'],
                    'predict_rose': int(predict_rose),
                    'pct_chg': int(np.floor(recommended_daily.at[0, 'pct_chg'])),
                    'market': market,
                    'moods': logs.iloc[i]['moods'],
                    'pre4_sum': logs.iloc[i]['pre4_sum'],
                    'average': logs.iloc[i]['average'],
                    'code_id': code_id,
                    'holding_at': holding_at,
                    'holding_pct_chg': holding_pct_chg
                }
                recommend_stocks.loc[code_id] = content
    if not recommend_stocks.empty:
        recommend_stocks.sort_values(by=['holding_at', 'holding_pct_chg', 'star_count', 'predict_rose'], ascending=[False, False, False, False], inplace=True)
        recommend_text = recommend_stocks.to_string(index=False)

        msgs.append(MIMEText(recommend_text, 'plain', 'utf-8'))
        send_email(subject=end_date + 'rs-中长期预测', msgs=msgs)

