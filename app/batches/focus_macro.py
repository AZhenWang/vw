from app.models.pca import Pca
from app.saver.logic import DB
import matplotlib.pylab as plt
import numpy as np
import pandas as pd
import time
from app.common.exception import MyError
from app.saver.tables import fields_map
import app.common.function as CF

n_components = 2
pre_predict_interval = 5
# TTB = 'daily'
# TTB = 'weekly'
# TTB = 'monthly'

def execute(start_date='', end_date=''):
    """
    筛选处于大的上升趋势的股票作为关注股票，推荐的股票就在这些关注股票里选
    :param start_date:
    :param end_date:
    :return:
    """
    trade_cal = DB.get_open_cal_date(start_date=start_date, end_date=end_date)
    codes = DB.get_latestopendays_code_list(
        latest_open_days=244, date_id=trade_cal.iloc[0]['date_id'])
    code_ids = codes['code_id']
    # code_ids = [583, 1436, 1551, 1591, 1605, 1711, 2423, 2551, 2597]
    # code_ids = [1750, 1680, 751, 270, 2822, 648]
    # code_ids = [213, 583, 432, 1605, 1711]
    # code_ids = [2597, 2551,  344]
    # code_ids = [1605, 1711, 633]
    # code_ids = [2772]
    # code_ids = [1988, 2422, 1979, 2020, 1423, 1949, 1895, 376]
    # 2433, 万向德农， 2418：敦煌种业
    # 583:康强电子,  1551：天晟新材， 1436:欧比特, 1591:森远股份, 1605:正海磁材, 1711:华虹计通, 2291：有研新材， 2261：上海贝岭
    # 2423:华微电子, 2551:交大昂立, 2597:长电科技, 216:*ST金岭， 633：智光电气
    # TTBS = ['daily', 'weekly', 'monthly']
    # TTBS = ['monthly', 'weekly']
    TTBS = ['weekly']
    # TTBS = ['daily']
    for i in range(len(trade_cal)):
        end_date = trade_cal.iloc[i].cal_date
        date_id = trade_cal.iloc[i].date_id
        pca = Pca(cal_date=end_date)
        for TTB in TTBS:
            for code_id in code_ids:
                print('code_id=', code_id)
                new_rows = pd.DataFrame(columns=fields_map['macro_pca'])
                try:
                    pca_features, prices, Y, _ = pca.run(code_id=code_id, pre_predict_interval=pre_predict_interval,
                                                      n_components=n_components, return_y=True, TTB=TTB)
                except MyError as e:
                    continue
                if Y.index[Y.index >= date_id].empty:
                    continue
                bottom_dis = 30
                start_loc = Y.index.get_loc(Y.index[Y.index >= date_id][0])

                sample_pca = pca_features[start_loc-bottom_dis:].reset_index(drop=True)
                sample_prices = prices[start_loc-bottom_dis:].reset_index(drop=True)
                sample_Y = Y[start_loc-bottom_dis:]
                Y0 = round(sample_pca.col_0, 3)
                Y1 = round(sample_pca.col_1, 3)

                DB.delete_macro_logs(code_id=code_id, start_date_id=date_id, end_date_id=date_id, TTB=TTB)

                # 大趋势买卖点
                len_y = len(Y0)
                for i in range(bottom_dis, len_y):
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

                    # flag
                    flag = 0
                    if (Y0.iloc[i] > mean0 + 2 * std0 or Y0.iloc[i - 1] > mean0 + 2 * std0) \
                            and Y1.iloc[i] < Y1.iloc[i - 1] \
                            and Y1.iloc[i] <= 0.2:
                        flag = -2
                    elif Y0.iloc[i] > Y0.iloc[i - 1] and Y1.iloc[i] < Y1.iloc[i - 1] and Y1.iloc[i] - Y1.iloc[i - 1] <= -0.1:
                        flag = -1
                    elif Y0.iloc[i] < mean0 - 1 * std0 and Y1.iloc[i] > Y1.iloc[i - 1] \
                            and Y1.iloc[i] - Y1.iloc[i - 1] > Y1.iloc[i - 1] - Y1.iloc[i - 2] \
                            and Y0.iloc[i - 1] - Y0.iloc[i] < 0.2 \
                            and ((Y0.iloc[i - 2] - Y0.iloc[i - 1] > Y0.iloc[i - 1] - Y0.iloc[i] + 0.1) or (
                            Y0.iloc[i - 1] > Y0.iloc[i - 2])) \
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

                    qqb = CF.get_wave_segment(Y=Y0[-bottom_dis+i:i+1])

                    # print('qqb=', qqb, 'Y0=', Y0.iloc[i-3: i+1],  Y0.iloc[i], Y0.iloc[i-1] , Y0.iloc[i-2], 'Y0_chg=', Y0_chg, 'Y1_chg=', Y1_chg)
                    # plt.plot(Y0)
                    # plt.title(code_id)
                    # plt.show()
                    # return

                    new_rows.loc[i] = {
                        'date_id': date_id,
                        'code_id': code_id,
                        'TTB': TTB,
                        'flag': flag,
                        'qqb': qqb,
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
