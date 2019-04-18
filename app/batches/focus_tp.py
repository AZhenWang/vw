from app.models.tp import Tp
from app.saver.logic import DB
from app.models.pca import Pca
import numpy as np
import pandas as pd
from app.saver.tables import fields_map

# 向前预测时间长度
predict_len = 6
n_components = 2
pre_predict_interval = 5


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
    codes = DB.get_code_list_before_date(min_list_date=first_date,)
    code_ids = codes['code_id']
    # code_ids = [563]
    # code_ids = [1475,  2756]
    # 238: 东方电子，462：豫能控股， 2756：红阳能源， 2274：莲花健康， 2308：天津松江
    for code_id in code_ids:
        print('code_id=', code_id)
        new_rows = pd.DataFrame(columns=fields_map['tp_logs'])
        dailys_data = DB.get_code_info(code_id=code_id, start_date=first_date, end_date=end_date)
        dailys = dailys_data['close'] * dailys_data['adj_factor']
        dailys.name = 'close'

        tp_model = Tp()
        data_len = len(dailys)

        pca = Pca(cal_date=end_date)
        pca_features, prices = pca.run(code_id=code_id, pre_predict_interval=pre_predict_interval,
                                       n_components=n_components)

        k = 0
        for i in range(data_len-cal_length, data_len):
            date_id = dailys.index[i]

            cal_date = dailys_data.iloc[i]['cal_date']
            DB.delete_tp_log(code_id=code_id, date_id=date_id)

            Y = dailys[k:i+1]

            predict_Y = tp_model.run(y=Y, fs=0.32, predict_len=predict_len)
            if np.sum(predict_Y) == 0:
                break
            today_v = predict_Y[0]
            tomorrow_v = predict_Y[1]
            diffs = (predict_Y - today_v) * 100/abs(today_v)
            mean = np.mean(diffs)
            std = np.std(diffs)
            diff = (tomorrow_v - today_v) * 100/abs(today_v)

            # print('pca_features.col_0=', pca_features.col_0)
            # print('prices.index=', prices.index)
            # print('date_id=', date_id)
            pca_0 = pca_features.col_0[prices.index <= date_id][k:]
            # print('pca_0=', pca_0)
            predict_pca_0 = tp_model.run(y=pca_0, predict_len=predict_len)
            today_pca = predict_pca_0[0]
            tomorrow_pca = predict_pca_0[1]
            pca_diffs = (predict_pca_0 - today_pca) * 100 / abs(today_pca)
            pca_mean = np.mean(predict_pca_0)
            pca_std = np.std(predict_pca_0)
            pca_diff_mean = np.mean(pca_diffs)
            pca_diff_std = np.std(pca_diffs)
            pca_diff = tomorrow_pca - today_pca
            pca_max = np.max(predict_pca_0[1:])
            # print('predict_pca_0=', predict_pca_0)
            # os.exit

            new_rows.loc[i] = {
                'cal_date': cal_date,
                'date_id': date_id,
                'code_id': code_id,
                'today_v': round(today_v, 2),
                'tomorrow_v': round(tomorrow_v, 2),
                'diff': round(diff, 2),
                'mean': round(mean, 2),
                'std': round(std, 2),
                'pca_diff': round(pca_diff, 3),
                'pca_mean': round(pca_mean, 3),
                'pca_max': round(pca_max, 2),
                # 'pca_std': round(pca_std, 2),
                'pca_diff_mean': round(pca_diff_mean, 3),
                'pca_diff_std': round(pca_diff_std, 2),
            }
            k += 1
        if not new_rows.empty:
            new_rows.to_sql('tp_logs', DB.engine, index=False, if_exists='append', chunksize=1000)
