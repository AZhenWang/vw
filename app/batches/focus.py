from app.models.pca import Pca
from app.saver.logic import DB
import numpy as np
import pandas as pd
from app.saver.tables import fields_map
from app.common.function import knn_predict

# 取半年样本区间
sample_len = 60
n_components = 2
pre_predict_interval = 5


def execute(start_date='', end_date=''):
    """
    筛选处于大的上升趋势的股票作为关注股票，推荐的股票就在这些关注股票里选
    :param start_date:
    :param end_date:
    :return:
    """
    trade_cal = DB.get_open_cal_date(start_date=start_date, end_date=end_date)
    cal_length = len(trade_cal)
    codes = DB.get_latestopendays_code_list(
        latest_open_days=244, date_id=trade_cal.iloc[0]['date_id'])
    code_ids = codes['code_id']
    # code_ids = [1501]
    pca = Pca(cal_date=trade_cal.iloc[-1]['cal_date'])
    for code_id in code_ids:
        print('code_id=', code_id)
        new_rows = pd.DataFrame(columns=fields_map['recommend_stocks'])
        pca_features, prices, Y, _ = pca.run(code_id=code_id, pre_predict_interval=pre_predict_interval,
                                          n_components=n_components, return_y=True)
        pca_length = len(pca_features)
        for i in range(pca_length-cal_length, pca_length):
            date_id = Y.index[i]
            DB.delete_recommend_log(code_id=code_id, date_id=date_id, recommend_type='pca')
            if sample_len != 0:
                sample_pca = pca_features[:i+1][-sample_len:].reset_index(drop=True)
                sample_prices = prices[:i+1][-sample_len:].reset_index(drop=True)
                sample_Y = Y[:i+1][-sample_len:]
            else:
                sample_pca = pca_features
                sample_prices = prices
                sample_Y = Y

            diff_Y0 = np.where(np.diff(pca_features.col_0) > 0, 1, -1)
            diff_Y1 = np.where(np.diff(pca_features.col_1) > 0, 1, -1)
            diff_price = np.where(np.diff(prices) > 0, 1, -1)
            dot_price_Y0 = np.dot(diff_Y0, diff_price)
            dot_price_Y1 = np.dot(diff_Y1, diff_price)
            if dot_price_Y0 < 0:
                print('转Y0')
                sample_pca.col_0 = (-1) * sample_pca.col_0
            if dot_price_Y1 < 0:
                print('转Y1')
                sample_pca.col_1 = (-1) * sample_pca.col_1

            Y0 = sample_pca.col_0
            Y1 = sample_pca.col_1

            mean = 0
            std = np.std(Y0)

            # 大趋势买卖点
            flag = 0
            if Y0.iloc[-1] > Y0.iloc[-2] and Y0.iloc[-2] < Y0.iloc[-3] \
                    and Y0.iloc[-1] > Y1.iloc[-1] and Y0.iloc[-2] < Y1.iloc[-2] \
                    and Y0.iloc[-3] > Y1.iloc[-3] \
                    and Y1.iloc[-1] > Y1.iloc[-2] \
                    and Y0[-20:-1].max() > 0.5 and Y0.iloc[-2] < -0\
                    and sample_prices.iloc[-1] > sample_prices.iloc[-2]:
                flag = 2
            elif Y0.iloc[-1] > Y0.iloc[-2] and Y1.iloc[-1] > Y1.iloc[-2] and Y1.iloc[-1] >= 0 and sample_prices.iloc[-1] < sample_prices.iloc[-2]:
                flag = 1
            elif Y0.iloc[-1] < Y0.iloc[-2] and Y1.iloc[-1] < Y1.iloc[-2] and Y1.iloc[-1] <= 0.2 and sample_prices.iloc[-1] > sample_prices.iloc[-2]:
                flag = -1

            holdings = get_holdings(Y=Y0, Y1=Y1, sample_prices=sample_prices)
            daily = DB.get_code_daily(code_id=code_id, date_id=date_id)
            if daily.empty or (holdings[-1] == 0 and flag == 0):
                continue
            y1_y1 = Y1[-3:-1].max() - Y1.iloc[-1]

            # y_hat = knn_predict(pca_features, Y, k=2, sample_interval=244*2,
            #                     pre_predict_interval=pre_predict_interval, predict_idx=sample_Y.index[-1])

            bottom_dis = 30
            point_args = np.diff(np.where(np.diff(Y0[-bottom_dis:]) > 0, 0, 1))
            peaks = Y0[-bottom_dis + 1:-1][point_args == 1]
            bottoms = np.floor((Y0[-bottom_dis + 1:-1][point_args == -1])*100)/100
            amplitude = 0
            # if len(bottoms) >= 2 and len(peaks) >= 2:
            #     if Y0.iloc[-2] < Y0.iloc[-1] and (Y0.iloc[-1] > peaks.iloc[-1] or peaks.iloc[-1] > peaks.iloc[-2]) and bottoms.iloc[-1] >= bottoms.iloc[-2]:
            #         # 底上升
            #         amplitude = 1
            #     elif Y0.iloc[-2] > Y0.iloc[-1] and (Y0.iloc[-1] < bottoms.iloc[-1] or bottoms.iloc[-1] <= bottoms.iloc[-2]) and peaks.iloc[-1] < peaks.iloc[-2]:
            #         # 底下降
            #         amplitude = -1
            qqb = 0
            point_args_price = np.diff(np.where(np.diff(sample_prices[-bottom_dis:]) > 0, 0, 1))
            peaks_price = sample_prices[-bottom_dis + 1:-1][point_args_price == 1]
            bottoms_price = np.floor((sample_prices[-bottom_dis + 1:-1][point_args_price == -1]) * 100) / 100
            if len(bottoms_price) >= 1 and len(peaks_price) >= 1:
                if Y0.iloc[-1] < peaks.iloc[-1] and sample_prices.iloc[-1] > peaks_price.iloc[-1]:
                    # 顶背离
                    qqb = -1
                elif Y0.iloc[-1] > bottoms.iloc[-1] and sample_prices.iloc[-1] < bottoms_price.iloc[-1]:
                    # 底背离
                    qqb = 1
            pre4_sum = DB.sum_pct_chg(code_id=code_id, end_date_id=date_id, period=4)
            pre40_sum = round(
                (sample_prices.iloc[-40] - sample_prices.iloc[-1]) * 100 / sample_prices.iloc[-1], 2)
            # positive_mean = round((Y0 - Y1)[Y0 > Y1].mean(), 2)
            positive_mean = round((Y0[:-3] - Y1[:-3])[Y0[:-3] > Y1[:-3]].mean(), 2)
            negative_mean = round((Y1[:-3] - Y0[:-3])[Y0[:-3] < Y1[:-3]].mean(), 2)
            new_rows.loc[i] = {
                'date_id': date_id,
                'code_id': code_id,
                'recommend_type': 'pca',
                'star_idx': holdings[-1],
                'average': round(Y0.iloc[-1], 2),
                'pre4_sum': round(pre4_sum),
                'pre40_sum': pre40_sum,
                'pre40_positive_mean': positive_mean,
                'pre40_negative_mean': negative_mean,
                'qqb': qqb,
                'moods': round(Y1.iloc[-1], 2),
                'flag': flag,
            }
        if not new_rows.empty:
            new_rows.to_sql('recommend_stocks', DB.engine, index=False, if_exists='append', chunksize=1000)


def get_holdings(Y, Y1, sample_prices):
    mean = 0
    std = np.std(Y)

    start_loc = len(Y) - 1
    holdings = [0] * start_loc
    bottom_dis = 20

    point_args = np.diff(np.where(np.diff(Y[-bottom_dis:]) > 0, 0, 1))
    peaks = Y[-bottom_dis + 1:-1][point_args == 1]
    bottoms = Y[-bottom_dis + 1:-1][point_args == -1]
    bottoms_length = len(bottoms)

    for i in range(start_loc, len(Y)):

        # 正相关
        if bottoms_length >= 2 and 2*std > Y[i] > Y[i-1] and Y[i] > Y1[i] and Y1[-3:-1].max() - Y1.iloc[-1] > 0 \
                and Y.iloc[i] > peaks.iloc[-1] and peaks.iloc[-1] < mean + std and mean > bottoms.iloc[-1] >= bottoms.iloc[-2] and (bottoms.iloc[-2] < mean - 1 * std):
            # 大双底部
            # 大底部反转之前的数据都有大的价格波动，会增加std和mean，为了反转的灵敏度，std限制可以打个折扣，2std=>1.94, 1.5std=>1.328
            holding = 1
            print('大双底部')

        elif (Y[i - 20:i - 2].sort_values()[-2:] > (mean + 1.5 * std)).all(axis=None) \
             and bottoms.iloc[-1] < mean + std \
             and (Y[i] - Y[i - 5:i].min()) > 1.328 * std \
             and 2*std > Y[i] > mean and Y[i] > Y[i - 1]:
                # 强势震荡之后的反弹,一般反弹到原来的一半，原来涨幅1倍，此次就反弹50%
            print('强势之后的深度震荡后的强烈反弹i=', i)
            holding = 3

        elif bottoms_length >= 2 and 2*std > Y[i] > Y[i-1] and Y.iloc[i] > peaks.iloc[-1] and std > bottoms.iloc[-1] > mean > bottoms.iloc[-2]:
            # 大双底部
            # 大底部反转之前的数据都有大的价格波动，会增加std和mean，为了反转的灵敏度，std限制可以打个折扣，2std=>1.94, 1.5std=>1.328
            holding = 11
            print('大双底部')

        elif Y[i-10: i-2].max() - Y[i-7:i].min() > (2*std) and (Y[i] - Y[i - 2:i].min()) > 1.5 * std and (mean - 1.5 * std) < Y[
                                                                                                                  i - 2:i].min() and \
                Y[i] > mean + std:
            # 疯牛的多个板的底部的冲锋形态，至少还有2个板，可能连接着 多个板的顶部形态
            holding = 2
            print('疯牛的多个板的底部的冲锋i=', i)
            # 实测备注：此讯号可靠性高，此讯号包含3连板的形态，如果前几天出现1的信号，则加大此信号的可靠性，一般会有大于3个板的涨幅。
            # 实测备注：两种形态：
            #   一、发出此信号的当天的开盘价在5、10、20均线以下，一条红柱贯穿5、10、20三条均线时，为典型的牛头底部冲锋形态。
            #   二、5、10、20均线均已向上发展
            #   三、前一天的涨幅<2个点，负的更好，如果前一天已经涨的多了，这个信号就失效了
            # 20190122: -0.01 < mean < 0.01, 此时2的信号更可靠

        elif (Y[i - 5:i].sort_values()[-2:] > (mean + 1.5 * std)).all(axis=None) and Y[i] < mean + 1.5 * std < Y[i - 1]:
            # 大顶部
            # 实测备注：如果是连续大涨幅，则第二天还会有反弹，第二天收盘价卖
            holding = -2
            print('双顶')

        elif Y[i - 1] > mean + 1.5 * std > Y[i]:
            # 大顶部
            # 实测备注：如果是连续大涨幅，则第二天还会有反弹，第二天收盘价卖
            holding = -1
            print('大顶部')

        elif (Y[i - 10:i - 2].sort_values()[-2:] > (mean + std)).all(axis=None) \
                and (Y[i] - Y[i - 4:i].min()) > 1.328 * std \
                and mean < Y[i - 5:i - 2].max():
            # 3个板的反弹。
            # 优化备注：当天涨幅10%，才有可能连续3个板。此讯号预测连续大涨过后，下跌几日过后继续强势涨幅
            print('双顶后的强势反抽3个板i=', i)
            holding = 4

        else:
            holding = 0
            print('哟西')

        holdings.append(holding)

    return holdings