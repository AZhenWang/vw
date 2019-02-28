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
    for cal_date, date_id in trade_cal[['cal_date', 'date_id']].values:
        DB.delete_recommend_stock_logs(date_id=date_id, recommend_type='pca')
        codes = DB.get_latestopendays_code_list(
            latest_open_days=sample_len*2+25, date_id=date_id)
        code_ids = codes['code_id']
        # code_ids = [2852]
        pca = Pca(cal_date=cal_date)
        i = 0
        new_rows = pd.DataFrame(columns=fields_map['recommend_stocks'])
        for code_id in code_ids:
            pca_features, prices, Y = pca.run(code_id=code_id, pre_predict_interval=pre_predict_interval, n_components=n_components, return_y=True)
            if sample_len != 0:
                sample_pca = pca_features[-sample_len:].reset_index(drop=True)
                sample_prices = prices[-sample_len:].reset_index(drop=True)
                sample_Y = Y[-sample_len:]
            else:
                sample_pca = pca_features
                sample_prices = prices
                sample_Y = Y

            Y0 = sample_pca.col_0
            Y1 = sample_pca.col_1
            correlation0 = Y0.corr(sample_prices)
            # correlation1 = Y1.corr(sample_prices)
            if correlation0 < 0:
                # 负相关的先反过来
                Y0 = (-1) * Y0
            # if correlation1 < 0 and abs(correlation1) >= 0.1:
            #     # 负相关的先反过来
            #     Y1 = (-1) * Y1

            mean = np.mean(Y0)
            mean = mean * sample_len / (sample_len - 1)

            flag = 0
            if Y0.iloc[-1] > Y0.iloc[-2] and sample_prices.iloc[-1] < sample_prices.iloc[-2]:
                flag += 1
            if Y0.iloc[-2] > Y0.iloc[-3] and sample_prices.iloc[-2] < sample_prices.iloc[-3]:
                flag += 1
            if Y0.iloc[-3] > Y0.iloc[-4] and sample_prices.iloc[-3] < sample_prices.iloc[-4]:
                flag += 1
            if Y0.iloc[-1] < Y0.iloc[-2] and sample_prices.iloc[-1] > sample_prices.iloc[-2]:
                flag -= 1
            if Y0.iloc[-2] < Y0.iloc[-3] and sample_prices.iloc[-2] > sample_prices.iloc[-3]:
                flag -= 1
            if Y0.iloc[-3] < Y0.iloc[-4] and sample_prices.iloc[-3] > sample_prices.iloc[-4]:
                flag -= 1

            holdings = get_holdings(sample_pca,sample_prices)
            daily = DB.get_code_daily(code_id=code_id, date_id=date_id)

            if daily.empty or (holdings[-1] == 0 and flag == 0):
                continue

            y1_y1 = Y1[-3:-2].max() - Y1.iloc[-1]

            y_hat = knn_predict(pca_features, Y, k=2, sample_interval=244*2,
                                pre_predict_interval=pre_predict_interval, predict_idx=sample_Y.index[-1])
            new_rows.loc[i] = {
                'date_id': date_id,
                'code_id': code_id,
                'recommend_type': 'pca',
                'star_idx': holdings[-1],
                'average': round(np.mean(Y[-10:]), 2),
                'amplitude': round(y_hat, 1),
                'moods': round(y1_y1, 1),
                'flag': flag
            }
            i += 1
        if not new_rows.empty:
            new_rows.to_sql('recommend_stocks', DB.engine, index=False, if_exists='append', chunksize=1000)


def get_holdings(sample_pca, sample_prices):
    Y = sample_pca.col_0.reset_index(drop=True)
    correlation = Y.corr(sample_prices.reset_index(drop=True))
    if correlation < 0:
        Y = (-1) * Y
        correlation = (-1) * correlation

    mean = np.mean(Y)
    mean = mean * sample_len / (sample_len - 1)
    std = np.std(Y)

    # if abs(correlation) < 0.1:
    #     holdings = [0] * len(Y)
    #     return holdings
    start_loc = len(Y) - 1
    holdings = [0] * start_loc
    bottom_dis = 20

    for i in range(start_loc, len(Y)):
        # 正相关
        if Y[i - 10: i - 2].max() > (mean + std) and (Y[i] - Y[i - 2:i].min()) > 1.5 * std and (mean - 1.5 * std) < Y[
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

        elif (Y[i - 30:i - 2].sort_values()[-2:] > (mean + 1.5*std)).all(axis=None) \
                and Y[i - 5:i].min() < (mean - 1.5*std) \
                and (Y[i] - Y[i - 5:i].min()) > 1.5 * std \
                and Y[i-1] < mean \
                and Y[i] > Y[i-1] > Y[i-2]:
                # 强势震荡之后的反弹,一般反弹到原来的一半，原来涨幅1倍，此次就反弹50%
            print('强势之后的深度震荡后的强烈反弹i=', i)
            holding = 3

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

        elif Y[i - bottom_dis:i - 5].min() < Y[i - 5:i].min() and Y[i - 2] < Y[i - 1] < Y[i] \
                and Y[i - bottom_dis:i - 5].min() < mean - 1.5 * std and Y[i - 2] < mean - 1 * std:
            print(Y[i - 2], mean, std, mean - 1.5 * std)
            # 大双底部
            # 大底部反转之前的数据都有大的价格波动，会增加std和mean，为了反转的灵敏度，std限制可以打个折扣，2std=>1.94, 1.5std=>1.328
            holding = 1
            print('大双底部')

        else:
            holding = 0
            print('哟西')

        holdings.append(holding)

    return holdings