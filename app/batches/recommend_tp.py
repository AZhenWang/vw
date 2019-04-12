from app.saver.logic import DB
import pandas as pd
import numpy as np
from app.models.pca import Pca
from app.common.function import send_email
from email.mime.text import MIMEText

pre_predict_interval = 5
n_components = 2
recommend_type = 'tp'

def execute(start_date='', end_date=''):
    msgs = []
    recommend_stocks = pd.DataFrame(columns=['recommend_at', 'code_id', 'ts_code', 'name', 'up_pdm_sum',
                                             'up_ratio', 'up_pct_sum', 'down_pct_sum', 'pca_chg',
                                             'hold_at',
                                             ])

    i = 1
    trade_cal = DB.get_open_cal_date(start_date=start_date, end_date=end_date)
    for cal_date in trade_cal['cal_date'].values:
        trade_cal = DB.get_cal_date(limit=30, end_date=cal_date)
        end_date_id = trade_cal.iloc[-1]['date_id']
        start_date_id = trade_cal.iloc[0]['date_id']
        logs = DB.get_tp_logs(start_date_id=start_date_id, end_date_id=end_date_id)
        gp = logs.groupby('code_id')
        for code_id, group_data in gp:
            if group_data.empty:
                continue
            recommended_date_id = group_data.iloc[-1]['date_id']
            hold_at = ''
            log = group_data.iloc[-1]
            focus_log = DB.get_focus_stock_log(code_id=code_id, recommended_date_id=recommended_date_id, recommend_type=recommend_type)
            if not focus_log.empty:
                continue
            down_pca = group_data[group_data['pca_diff_mean'] > 0]
            up_pca = group_data[group_data['pca_diff_mean'] <= 0]
            up_pca_len = len(up_pca)
            if up_pca_len <= 1:
                continue
            down_pca_len = len(down_pca)
            up_ratio = up_pca_len / (up_pca_len + down_pca_len)

            up_pdm_sum = up_pca['pca_diff_mean'].sum()

            down_pct = group_data[group_data['pct_chg'] < 0]
            up_pct = group_data[group_data['pct_chg'] >= 0]
            down_pct_sum = down_pct['pct_chg'].sum()
            up_pct_sum = up_pct['pct_chg'].sum()
            print('up_ratio=', up_ratio, down_pct_sum, up_pct_sum)
            holding = 0
            # 一、升多跌少，标准门槛：4天上升，1天下降
            # 二、稳升急跌，升的总值不多，但是次数多。跌的次数虽然少，但跌的总和最少12个点
            if up_ratio > 0.75 and down_pct_sum < -11 and up_pct_sum < 35:
                DB.insert_focus_stocks(code_id=code_id,
                                       star_idx=0,
                                       predict_rose=log.pca_mean,
                                       recommend_type=recommend_type,
                                       recommended_date_id=recommended_date_id,
                                       )
                if (group_data.iloc[-1]['pca_mean'] < group_data.iloc[-2]['pca_mean'] \
                    and group_data.iloc[-1]['pca_diff_mean'] > group_data.iloc[-1]['pca_diff_std'] * 0.4) \
                        or (group_data.iloc[-1]['pca_mean'] > group_data.iloc[-2]['pca_mean'] and group_data.iloc[-1]['pca_diff_mean'] < 0 ):
                    holding = 1
                    hold_at = group_data.iloc[-1]['cal_date']
                if holding > 0:
                    DB.update_focus_stock_log(code_id=code_id, recommended_date_id=recommended_date_id,
                                              holding_date_id=recommended_date_id)
                content = {
                        'recommend_at': log.cal_date,
                        'code_id': code_id,
                        'ts_code': log.ts_code,
                        'name': log.ts_name,
                        'up_pdm_sum': int(round(up_pdm_sum)),
                        'up_ratio': round(up_ratio, 2),
                        'up_pct_sum': int(round(up_pct_sum)),
                        'down_pct_sum': int(round(down_pct_sum)),
                        'pca_chg': round(log.pct_chg, 1),
                        'hold_at': hold_at,


                    }
                recommend_stocks.loc[i] = content
                i += 1
    if not recommend_stocks.empty:
        recommend_stocks.sort_values(by=['hold_at', 'recommend_at', 'up_pdm_sum', 'up_ratio', 'up_pct_sum', 'down_pct_sum'],
                                     ascending=[False, False, False, False, True, False], inplace=True)
        recommend_stocks.reset_index(drop=True, inplace=True)
        recommend_text = recommend_stocks.to_string(index=False)

        msgs.append(MIMEText(recommend_text, 'plain', 'utf-8'))
        send_email(subject=end_date + '--tp进', msgs=msgs)

