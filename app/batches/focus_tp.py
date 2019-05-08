from app.models.tp import Tp
from app.saver.logic import DB
from app.models.pca import Pca
import numpy as np
import pandas as pd
from app.saver.tables import fields_map
import matplotlib.pyplot as plt

# 向前预测时间长度
predict_len = 20
n_components = 2
pre_predict_interval = 5


def execute(start_date='', end_date=''):
    """
    筛选处于大的上升趋势的股票作为关注股票，推荐的股票就在这些关注股票里选
    :param start_date:
    :param end_date:
    :return:
    """
    # period = 365*18
    period = 20
    trade_cal = DB.get_open_cal_date(start_date=start_date, end_date=end_date)
    pre_cal = DB.get_cal_date(end_date=start_date, limit=period)
    first_date = pre_cal.iloc[0]['cal_date']
    full_cal = DB.get_cal_date(start_date=first_date, end_date=end_date)

    cal_length = len(trade_cal)
    codes = DB.get_code_list_before_date(min_list_date=first_date,)
    code_ids = codes['code_id']
    # code_ids = [213]
    # code_ids = [1475,  2756]
    # 238: 东方电子，462：豫能控股， 2756：红阳能源， 2274：莲花健康， 2308：天津松江
    for code_id in code_ids:
        print('code_id=', code_id)
        new_rows = pd.DataFrame(columns=fields_map['tp_logs'])
        dailys_data = DB.get_code_info(code_id=code_id, start_date=first_date, end_date=end_date)
        dailys = dailys_data['close'] * dailys_data['adj_factor']
        dailys.name = 'close'
        # data = pd.DataFrame(full_cal['date_id'], columns=['date_id'])
        # dailys = data.join(dailys, on='date_id')
        # dailys.fillna(method='ffill', inplace=True)
        # dailys.fillna(method='backfill', inplace=True)
        # dailys.dropna(inplace=True)
        # print('dailys=', dailys)

        tp_model = Tp()
        data_len = len(dailys)

        pca = Pca(cal_date=end_date)
        pca_features, prices = pca.run(code_id=code_id, pre_predict_interval=pre_predict_interval,
                                       n_components=n_components)
        # print('pca_features=', pca_features)
        # pca_features = data.join(pca_features, on='date_id')
        # pca_features.fillna(method='ffill', inplace=True)
        # pca_features.fillna(method='backfill', inplace=True)
        # pca_features.dropna(inplace=True)
        # print('pca_features1=', pca_features)
        # os.exit

        k = -1
        for i in range(data_len-cal_length, data_len):
            k += 1
            # date_id = dailys.at[i, 'date_id']
            date_id = dailys.index[i]
            # if date_id not in trade_cal['date_id'].values:
            #     continue

            DB.delete_tp_log(code_id=code_id, date_id=date_id)

            Y = dailys[k:i+1]
            # Y = dailys[dailys['date_id'] <= date_id]['close'][k:]

            predict_Y = tp_model.run(y=Y, fs=0.32, predict_len=predict_len)
            # plt.plot(range(len(predict_Y)), predict_Y)
            # plt.show()
            # return

            if np.sum(predict_Y) == 0:
                break
            today_v = predict_Y[0]
            tomorrow_v = predict_Y[1]
            diffs = (predict_Y - today_v) * 100/abs(today_v)
            mean = np.mean(diffs)
            # std = np.std(diffs)
            diff = (tomorrow_v - today_v) * 100/abs(today_v)

            # print('pca_features.col_0=', pca_features.col_0)
            # print('prices.index=', prices.index)
            # print('date_id=', date_id)
            pca_0 = pca_features.col_0[prices.index <= date_id][k:]
            # print('pca_0=', pca_0)
            predict_pca_0 = tp_model.run(y=pca_0, predict_len=predict_len)
            # plt.plot(range(len(predict_pca_0)), predict_pca_0)
            # plt.show()
            # return
            today_pca = predict_pca_0[0]
            tomorrow_pca = predict_pca_0[1]
            pca_diffs = (predict_pca_0 - today_pca) * 100 / abs(today_pca)
            pca_mean = np.mean(predict_pca_0[1:])
            # pca_std = np.std(predict_pca_0)
            std = np.max(predict_pca_0[1:]) - today_pca
            pca_diff_mean = np.mean(pca_diffs)
            pca_diff_std = np.std(pca_diffs)
            pca_diff = tomorrow_pca - today_pca
            pca_min = np.min(predict_pca_0[1:])
            # print('predict_pca_0=', predict_pca_0)
            # os.exit

            new_rows.loc[i] = {
                'date_id': date_id,
                'code_id': code_id,
                'today_v': round(today_v, 2),
                'tomorrow_v': round(tomorrow_v, 2),
                'diff': round(diff, 2),
                'mean': round(mean, 2),
                'std': round(std, 2),
                'pca_diff': round(pca_diff, 3),
                'pca_mean': round(pca_mean, 3),
                'pca_min': round(pca_min, 3),
                # 'pca_std': round(pca_std, 2),
                'pca_diff_mean': round(pca_diff_mean, 3),
                'pca_diff_std': round(pca_diff_std, 2),
            }

        if not new_rows.empty:
            new_rows.to_sql('tp_logs', DB.engine, index=False, if_exists='append', chunksize=1000)
