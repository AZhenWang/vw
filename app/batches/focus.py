from app.models.pca import Pca
from app.saver.logic import DB
import matplotlib.pylab as plt
import numpy as np
import pandas as pd
from app.saver.tables import fields_map
from app.common.function import get_cum_return_rate, get_buy_sell_points


def execute(start_date='', end_date=''):
    """
    筛选处于大的上升趋势的股票作为关注股票，推荐的股票就在这些关注股票里选
    :param start_date:
    :param end_date:
    :return:
    """
    # 取半年样本区间
    sample_len = 122
    trade_cal = DB.get_open_cal_date(end_date=end_date, period=1)
    date_id = trade_cal.iloc[-1]['date_id']

    DB.delete_recommend_stock_logs(date_id=date_id, recommend_type='pca')
    # DB.delete_focus_stocks()
    codes = DB.get_latestopendays_code_list(
        latest_open_days=sample_len+25)
    pca = Pca(cal_date=end_date)
    i = 0
    code_ids = codes['code_id']
    new_rows = pd.DataFrame(columns=fields_map['recommend_stocks'])
    for code_id in code_ids:
        sample_pca, sample_prices = pca.run(code_id, sample_len)
        # holdings = get_holdings(sample_pca, sample_prices, primary_key=False)
        # 标示holding状态码
        plan_number = 60
        holdings = get_holdings(sample_pca, sample_prices, plan_number=plan_number)
        daily = DB.get_code_daily(code_id=code_id, date_id=date_id)
        if holdings[-1] != 0 and (holdings[-1] == 1 or holdings[-1] == -1 or daily.at[0, 'pct_chg'] > 9.99):
            new_rows.loc[i] = {
                'date_id': date_id,
                'code_id': code_id,
                'recommend_type': 'pca',
                'star_idx': holdings[-1],
                'average': daily.at[0, 'pct_chg']
            }
            # DB.insert_focus_stocks(code_id, holdings[-1])
        i += 1
    if not new_rows.empty:
        new_rows.to_sql('recommend_stocks', DB.engine, index=False, if_exists='append', chunksize=1000)


# 原来的最初的选择
# def get_holdings(sample_pca, sample_prices, primary_key=True):
#     dis = 20
#     holdings = [0] * dis
#     holding = 0
#     Y = sample_pca['col_0']
#     Y1 = sample_pca['col_1']
#     sample_mean = sample_pca.mean()
#     sample_std = sample_pca.std()
#     column_loc = 0
#     mean = sample_mean[column_loc]
#     std = sample_std[column_loc]
#     for i in range(dis, len(Y)):
#         if Y[i-dis:i-1].min() < Y[i-1] < Y[i] and Y[i-1] < mean-2*std:
#             # 大双底部买
#             holding = 1
#
#         elif Y[i-1] > Y[i] and Y[i-1] > mean+2*std:
#             # 大顶部卖
#             holding = 0
#         #
#         # elif not primary_key and Y[i] > mean > Y[i-1] and Y1[i] < Y[i-1]:
#         #     holding = 1
#
#         holdings.append(holding)
#
#     return holdings

def get_holdings(sample_pca, sample_prices, plan_number=3):
    Y = sample_pca.col_0
    sample_mean = sample_pca.mean()
    sample_std = sample_pca.std()
    column_loc = 0
    mean = sample_mean[column_loc]
    std = sample_std[column_loc]
    correlation = sample_pca.col_0.corr(sample_prices.reset_index(drop=True))
    if np.abs(correlation) < 0.1:
        holdings = [0] * len(Y)
        return holdings

    start_loc = len(Y) - 5
    holdings = [0] * start_loc
    bottom_dis = 20
    peak_dis = 40

    if np.abs(correlation) < 0.2:
        holdings = [0] * len(Y)
        return holdings

    for i in range(start_loc, len(Y)):

        if correlation > 0:
            # 正相关

            if plan_number & 1 and Y[i - 1] > mean + 1.5 * std > Y[i]:
                # 大顶部
                holding = -1
                print('大顶部')

            elif plan_number & 2 and Y[i - bottom_dis:i - 2].min() < Y[i - 2] < Y[i - 1] < Y[i] \
                    and Y[i - bottom_dis:i - 2].min() < mean - 2 * std and Y[i - 2] < mean - 1.5 * std:
                print(Y[i - 2], mean, std, mean - 1.5 * std)
                # 大双底部
                holding = 1
                print('大双底部')

            elif plan_number & 4 and (Y[i] - Y[i-2:i].min()) > 2*std and Y[i-2:i].min() < mean and Y[i] > mean + std:
                # 疯牛的多个板的底部的冲锋形态，至少还有2个板，可能连接着 多个板的顶部形态
                holding = 2
                print('疯牛的多个板的底部的冲锋i=', i)

            elif plan_number & 8 and Y[i] > mean + 2 * std and Y[i] > Y[i - 1] > mean + 1.5 * std and Y[i - 2] > Y[
                i - 1]:
                # 疯牛的多个板的顶部形态， 最少还有3-5个板
                holding = 3
                print('疯牛的4-6个板的顶部形态i=', i)

            elif plan_number & 16 and Y[i - peak_dis:i - 2].max() > mean + 1.5 * std and Y[i] > mean + std and Y[
                    i - 1] < mean < Y[i-3:i-1].max():
                # 中间3个板的反弹，最少还有2个板。
                # 优化备注：要求当前的涨幅>5个点，才有2天5个点以上的收益。当天涨幅10%，才有可能连续3个板
                print('中间3个板的反弹i=', i)
                holding = 4

            else:
                holding = 0
                print('哟西')

        else:
            # 负相关
            if plan_number & 1 and Y[i - 1] < mean - 1.5 * std < Y[i]:
                # 大底部
                holding = -1
                print('大底部')

            elif plan_number & 2 and Y[i - bottom_dis: i - 2].max() > Y[i - 2] > Y[i - 1] > Y[i] \
                    and Y[i - bottom_dis: i - 2].max() > mean + 2 * std and Y[i - 2] > mean + 1.5 * std:
                # 大双顶部
                holding = 1
                print('大双顶部')

            elif plan_number & 4 and (Y[i] - Y[i-2:i].max()) < -2*std and Y[i-2:i].max() > mean and Y[i] < mean - std:
                # 疯牛的多个板的底部的冲锋形态，最少还有2个板，可能连接着 多个板的顶部形态
                holding = 2
                print('疯牛的多个板的底部的冲锋i=', i)

            elif plan_number & 8 and Y[i] < mean - 2 * std and Y[i] < Y[i - 1] < mean - 1.5 * std and Y[i - 2] < Y[
                i - 1]:
                # 疯牛的多个板的顶部形态， 最少还有2个板
                holding = 3
                print('疯牛的多个板的顶部形态i=', i)

            elif plan_number & 16 and Y[i - peak_dis:i - 2].min() < mean - 1.5 * std \
                    and Y[i] < mean - std and Y[i - 1] > mean > Y[i - 3:i - 1].min():
                # 中间3个板的反弹，最少还有2个板
                holding = 4
                print('中间3个板的反弹i=', i)

            else:
                holding = 0
                print('哟西')

        holdings.append(holding)

    return holdings

