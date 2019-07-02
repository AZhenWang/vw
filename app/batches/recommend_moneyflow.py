from app.saver.logic import DB
import pandas as pd
from app.common.function import send_email
from email.mime.text import MIMEText

recommend_type = 'moneyflow'


def execute(start_date='', end_date=''):
    trade_cal = DB.get_open_cal_date(start_date=start_date, end_date=end_date)
    end_date_id = trade_cal.iloc[-1]['date_id']
    start_date_id = trade_cal.iloc[0]['date_id']
    logs = DB.get_mv_moneyflows(start_date_id=start_date_id, end_date_id=end_date_id)
    msgs = []
    recommend_stocks = pd.DataFrame(columns=['ts_code', 'code_name', 'code_id', 'date', 'pct_chg',
                                             ])
    for i in range(len(logs)):
        log = logs.iloc[i]
        code_id = logs.iloc[i]['code_id']
        recommended_date_id = logs.index[i]

        # if log['net_elg'] > 0.1 and 7 < abs(log['turnover_rate_f2']) < 25 and log['mv_turnover_rate_f2'] < 20 and \
        #     log['mv_elg_base_diff5'] > log['mv_elg_base_diff10'] and log['mv_elg_base_diff5'] > 0.1 and \
        #     log['mv_mv_tr_f2_pct_chg'] > 0.3 and log['mv_mv_tr_f2'] > 0:

        if log['net_elg'] > 0.1\
            and log['mv_elg_base_diff5'] > 0 and log['mv_elg_base_diff5'] > log['mv_elg_base_diff10'] \
            and log['mv_mv_tr_f2_pct_chg'] > 0.3 \
            and log['turnover_rate_f3'] > log['mv_turnover_rate_f3'] > 0.3 \
            and log['turnover_rate_f2'] > log['mv_turnover_rate_f2'] and log['turnover_rate_f2'] > 7 and log['mv_turnover_rate_f2'] < 3 \
            and log['weight'] > 7:

            predict_rose = 0
            DB.insert_focus_stocks(code_id=code_id,
                                   star_idx=0,
                                   predict_rose=predict_rose,
                                   recommend_type=recommend_type,
                                   recommended_date_id=recommended_date_id,
                                   )

            content = {
                'ts_code': log['ts_code'],
                'code_name': log['name'],
                'code_id': code_id,
                'date': log['cal_date'],
                'pct_chg': log['pct_chg']
            }
            recommend_stocks.loc[code_id] = content

    if not recommend_stocks.empty:
        recommend_stocks.sort_values(by=['date', 'mv_mv_tr_f2_pct_chg'],
                                     ascending=[False, False], inplace=True)
        recommend_stocks.reset_index(drop=True, inplace=True)
        recommend_text = recommend_stocks.to_string(index=True)

        msgs.append(MIMEText(recommend_text, 'plain', 'utf-8'))
        send_email(subject=end_date + '换手率预测', msgs=msgs)

