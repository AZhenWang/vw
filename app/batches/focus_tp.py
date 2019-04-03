from app.models.tp import Tp
from app.saver.logic import DB
from conf.myapp import init_date
import numpy as np
import pandas as pd
from app.saver.tables import fields_map

# 向前预测时间长度
predict_len = 60


def execute(start_date='', end_date=''):
    """
    筛选处于大的上升趋势的股票作为关注股票，推荐的股票就在这些关注股票里选
    :param start_date:
    :param end_date:
    :return:
    """
    period = 365*18
    trade_cal = DB.get_open_cal_date(start_date=start_date, end_date=end_date)
    pre_cal = DB.get_cal_date(end_date=start_date, limit=period)
    first_date = pre_cal.iloc[0]['cal_date']

    cal_length = len(trade_cal)
    codes = DB.get_latestopendays_code_list(
        latest_open_days=365*10, date_id=trade_cal.iloc[0]['date_id'])
    code_ids = codes['code_id']
    # code_ids = [2772]
    for code_id in code_ids:
        print('code_id=', code_id)
        new_rows = pd.DataFrame(columns=fields_map['tp_logs'])
        dailys_data = DB.get_code_info(code_id=code_id, start_date=first_date, end_date=end_date)
        dailys = dailys_data['close'] * dailys_data['adj_factor']
        dailys.name = 'close'

        tp_model = Tp()
        data_len = len(dailys)
        k = 0
        for i in range(data_len-cal_length, data_len):
            date_id = dailys.index[i]

            cal_date = dailys_data.iloc[i]['cal_date']
            DB.delete_tp_log(code_id=code_id, date_id=date_id)

            Y = dailys[k:i+1]
            predict_Y = tp_model.run(y=Y, fs=0.3, predict_len=predict_len)
            today_v = predict_Y[0]
            tomorrow_v = predict_Y[1]
            diffs = (predict_Y - today_v) * 100/abs(today_v)
            mean = np.mean(diffs)
            std = np.std(diffs)
            diff = (tomorrow_v - today_v) * 100/abs(today_v)
            new_rows.loc[i] = {
                'cal_date': cal_date,
                'date_id': date_id,
                'code_id': code_id,
                'today_v': round(today_v, 2),
                'tomorrow_v': round(tomorrow_v, 2),
                'diff': round(diff, 2),
                'mean': round(mean, 2),
                'std': round(std, 2),
            }
            k += 1
        if not new_rows.empty:
            new_rows.to_sql('tp_logs', DB.engine, index=False, if_exists='append', chunksize=1000)
