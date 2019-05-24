from app.models.pca import Pca
from app.saver.logic import DB
import numpy as np
import pandas as pd
import time
from datetime import timedelta
from app.saver.tables import fields_map

n_components = 2
pre_predict_interval = 5
TTB = 'monthly'

def execute(start_date='', end_date=''):
    """
    筛选处于大的上升趋势的股票作为关注股票，推荐的股票就在这些关注股票里选
    :param start_date:
    :param end_date:
    :return:
    """
    trade_cal = DB.get_open_cal_date(start_date=start_date, end_date=end_date)
    end_date = trade_cal.iloc[-1].cal_date
    start_time = time.strptime(start_date, "%Y%m%d")
    end_time = time.strptime(end_date, "%Y%m%d")
    period = (end_time.tm_year - start_time.tm_year)*12 + (end_time.tm_mon - start_time.tm_mon) + 1
    codes = DB.get_latestopendays_code_list(
        latest_open_days=244, date_id=trade_cal.iloc[0]['date_id'])
    code_ids = codes['code_id']
    # code_ids = [1501]
    pca = Pca(cal_date=end_date)
    for code_id in code_ids:
        print('code_id=', code_id)

        new_rows = pd.DataFrame(columns=fields_map['macro_pca'])
        pca_features, prices, Y, _ = pca.run(code_id=code_id, pre_predict_interval=pre_predict_interval,
                                          n_components=n_components, return_y=True, TTB=TTB)

        sample_pca = pca_features[-period-2:].reset_index(drop=True)
        sample_prices = prices[-period-2:].reset_index(drop=True)
        sample_Y = Y[-period-2:]

        Y0 = sample_pca.col_0
        Y1 = sample_pca.col_1

        DB.delete_macro_logs(code_id=code_id, start_date_id=sample_Y.index[2], end_date_id=sample_Y.index[-1], TTB=TTB)

        # 大趋势买卖点
        len_y = len(Y0)
        for i in range(2, len_y):
            date_id = sample_Y.index[i]

            std0 = np.std(pca_features[:-len_y+i].col_0)
            std1 = np.std(pca_features[:-len_y + i].col_1)
            mean0 = 0
            mean1 = 0

            Y0_chg = Y0.iloc[i] - Y0.iloc[i - 1]
            Y1_chg = Y1.iloc[i] - Y1.iloc[i - 1]
            Y0_line = get_line(Y=Y0.iloc[i], mean=mean0, std=std0)
            Y0_pre_line = get_line(Y=Y0.iloc[i-1], mean=mean0, std=std0)
            Y1_line = get_line(Y=Y1.iloc[i], mean=mean1, std=std1)
            Y1_pre_line = get_line(Y=Y1.iloc[i-1], mean=mean1, std=std1)

            flag = 0
            if (Y0.iloc[i] > mean0 + 2 * std0 or Y0.iloc[i - 1] > mean0 + 2 * std0) \
                    and Y1.iloc[i] < Y1.iloc[i - 1] \
                    and Y1.iloc[i] <= 0.2:
                flag = -2
            elif Y0.iloc[i] > Y0.iloc[i - 1] and Y1.iloc[i] < Y1.iloc[i - 1] and Y1.iloc[i] - Y1.iloc[i - 1] <= -0.1:
                flag = -1
            elif Y0.iloc[i] < mean0 - 1.328 * std0 and Y1.iloc[i] > Y1.iloc[i - 1] and Y1.iloc[i] >= 0 \
                    and Y0.iloc[i] <= Y0.iloc[i - 1]:
                flag = 3
            elif Y0.iloc[i] > Y0.iloc[i - 1] and Y0.iloc[i - 1] < Y0.iloc[i - 2] \
                    and mean0 + 2 * std0 > Y0.iloc[i] > 0.25 \
                    and Y0.iloc[i] > Y1.iloc[i] and Y0.iloc[i - 1] < Y1.iloc[i - 1] \
                    and Y0.iloc[i - 2] > Y1.iloc[i - 2] \
                    and Y1.iloc[i] > Y1.iloc[i - 1] \
                    and Y0.iloc[i - 1] < -0 \
                    and sample_prices.iloc[i] > sample_prices.iloc[i - 1]:
                flag = 2
            elif Y0.iloc[i] > Y1.iloc[i] and Y1.iloc[i] < -1.5 * std1 and mean0 < Y0.iloc[i] < mean0 + 1.328 * std0:
                flag = 1
            new_rows.loc[i] = {
                'date_id': date_id,
                'code_id': code_id,
                'TTB': TTB,
                'flag': flag,
                'std0': round(std0, 3),
                'std1': round(std1, 3),
                'Y0': round(Y0.iloc[i], 3),
                'Y1': round(Y1.iloc[i], 3),
                'Y0_chg': round(Y0_chg, 3),
                'Y1_chg': round(Y1_chg, 3),
                'Y0_line': Y0_line,
                'Y0_pre_line': Y0_pre_line,
                'Y1_line': Y1_line,
                'Y1_pre_line': Y1_pre_line,
            }
        if not new_rows.empty:
            new_rows.to_sql('macro_pca', DB.engine, index=False, if_exists='append', chunksize=1000)


# 所属车道
def get_line(Y, mean, std):
    l0 = mean
    l1 = mean + std
    l2 = mean + 1.328 * std
    l3 = mean + 1.98 * std
    l_1 = mean - std
    l_2 = mean - 1.328 * std
    l_3 = mean - 1.98 * std

    if Y >= l3:
        line = 4
    elif Y >= l2:
        line = 3
    elif Y >= l1:
        line = 2
    elif Y >= l0:
        line = 1
    elif Y >= l_1:
        line = -1
    elif Y >= l_2:
        line = -2
    elif Y >= l_3:
        line = -3
    else:
        line = -4

    return line
