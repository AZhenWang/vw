from app.models.pca import Pca
from app.saver.logic import DB
import matplotlib.pylab as plt
from app.common.function import get_cum_return_rate
import numpy as np
import pandas as pd
from app.saver.tables import fields_map
from app.common.function import knn_predict


pre_predict_interval=5
sample_len = 30

def execute(start_date='', end_date=''):
    end_date = '20190212'
    trade_cal = DB.get_open_cal_date(end_date=end_date, period=1)
    date_id = trade_cal.iloc[-1]['date_id']


    pca = Pca(cal_date=end_date)

    # recommended_codes = DB.get_recommended_stocks(cal_date=end_date)
    # focus_codes = DB.get_focus_stocks()
    # up_stocks = DB.get_up_stocks_by_threshold(cal_date=end_date)

    # codes = focus_codes['code_id']
    # new_rows = pd.DataFrame(columns=fields_map['rate_yearly'])
    # draw = False
    draw = True
    codes = [2187]
    # 687:鱼跃医疗，2187：东方金钰, 2876:秋林集团
    # 1241:银宝山新，3368: 薄天环境， 1836：赢合科技， 1442：东方财富, 3179:汇嘉时代, 3476:柯利达
    # 20190122预测：2179:4, 346:2，3067:2, 996:4-2, 2750:2

    # correlation： 涨：2179:0.46/0.16/0.07(f), 3067:0.48/0.21/-0.019(f), 2553:0.397/0.121/0.002(f);
    #               跌：2750:0.18/0.049/0.15(f), 996:0.21/0.16/0.042(f), 1174:0.418/0.258/-0.035(f)

    # Y-Y1： 涨：2179: 0.969, 3067: 0.965 , 2553: 0.825,       922: 0.91, 2772: 0.751,  2772: 0.725, 2772: 0.914
    #        跌：2750: 0.89,   996: 0.979,   1174: 0.96

    # Y1-Y1： 涨：2179: 0.401,  3067: 0.165,   2553: 0.443,    922: 0.141,  2772: 0.625,  2772: 0.578,  2772: 0.376
    #        跌：2750: 0.233,   996: -0.022,   1174: 0.358

    # Y-Y：  涨：2179: -0.468,   3067: -0.916,   2553: -0.828, 922: -0.81,  2772: -0.779,  2772: -0.61, 2772: -0.24
    #        跌：2750: -0.826,   996: -0.401,   1174: -0.829

    # Y_mean:涨：2179: -0.0,   3067: 0.139,   2553: -0.075,    922: -0.158,  2772: -0.059,   2772: 0.011,  2772: 0.11
    #        跌：2750: -0.147,   996: -0.091,   1174: 0.013

    # Y1_mean:涨：2179: -0.006,   3067: 0.052,   2553: -0.027, 922: -0.041,  2772: -0.023,  2772: -0.012, 2772: -0.0
    #         跌：2750: -0.026,   996: -0.031,   1174: -0.001

    # 心得： Y1-Y1越大越好，且单只股票的Y_mean与Y1-Y1应该成负相关，Y_mean越小，Y1-Y1应该越大，Y_mean越大，Y1-Y1可以越小，

    # 涨std, 1402:0.367/-0.002, 190:0.344/-0.084, 2553:0.389/-0.075,
    # 跌std, 411:0.384/-0.117, 1174:0.387/0.013, 2597:0.362/-0.216， 2802：0.344/-0.144,
    # codes = [3548]
    # codes = [1174, 2553]
    # codes = [2082, 2496]
    # 此股形态特别备注：像edit一样，50个点的涨幅
    # codes = [2678]
    i = 0
    n_components = 2
    for code_id in codes:
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
        correlation1 = Y1.corr(sample_prices)
        if correlation0 < 0:
            # 负相关的先反过来
            Y0 = (-1) * Y0
        if correlation1 < 0:
            # 负相关的先反过来
            Y1 = (-1) * Y1

        mean = np.mean(Y0)
        # mean = mean * sample_len / (sample_len - 1)
        mean = 0
        std = np.std(Y0)

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

        holdings = get_holdings(sample_pca, sample_prices)
        print('holdings = ', holdings[-5:])
        print('std=', std)
        cum_return_rate_set = get_cum_return_rate(sample_prices, holdings)
        buy, sell = get_buy_sell_points(holdings)
        print('buy=', buy[-5:])
        print('sell=', sell[-5:])
        print('len(holding)=', len(holdings), ', len(buy)=', len(buy))
        print('code_id=', code_id, ' rate_yearly=', cum_return_rate_set[-1],
              ' diff=', (sample_prices.iloc[-1] - sample_prices.iloc[20])/sample_prices.iloc[20],
              ' holding=', np.sum(holdings)
              )
        print('explained_variance_ratio_=', pca.explained_variance_ratio_)
        print(Y0[-10:])
        print('correlation0= ', correlation0)
        print('correlation1= ', correlation1)
        print('correlation2= ', Y0.corr(Y1))
        # Y0-Y0.shift()
        print('Y1-Y1= ', Y1[-3:-2].max() - Y1.iloc[-1])
        print('std=', std)
        print('mean=', mean)
        print('flag=', flag)

        knn_v = ''
        for predict_idx in sample_Y.index[-5:]:
            y_hat = knn_predict(pca_features, Y, k=2, sample_interval=244*2,
                                pre_predict_interval=pre_predict_interval, predict_idx=predict_idx)
            knn_v = knn_v + ' | ' + str(np.floor(y_hat))
        print('knn=', knn_v)
        #  存储比较不同的策略组合的收益率
        # new_rows.loc[i] = {
        #     'code_id': code_id,
        #     'date_id': date_id,
        #     'rate_yearly': cum_return_rate_set[-1],
        #     'min_rate': np.min(cum_return_rate_set),
        #     'max_rate': np.max(cum_return_rate_set),
        #     'mean_rate': np.mean(cum_return_rate_set),
        # }

        i += 1

        if draw:
            fig, ax = plt.subplots(2, 1, figsize=(16, 8))
            x_axis = [i for i in range(sample_len)]
            ax1 = ax[0]

            ax1.plot(x_axis, Y0, label='Y')
            ax1.plot(x_axis, Y1, label='Y1')

            ax1.set_ylabel('pca')
            ax1.axhline(mean - 3 * std, color='b')
            ax1.axhline(mean - 2 * std, color='c')
            ax1.axhline(mean - 1.5 * std, color='r')
            ax1.axhline(mean - 1 * std, color='green')
            ax1.axhline(mean, color='black')
            ax1.axhline(mean + 1 * std, color='green')
            ax1.axhline(mean + 1.5 * std, color='r')
            ax1.axhline(mean + 2 * std, color='c')
            ax1.axhline(mean + 3 * std, color='b')

            # x_axis = mdates.date2num(data['cal_date'].apply(lambda x: dt.strptime(x, '%Y%m%d')))
            ax1_1 = ax1.twinx()
            ax1_1.plot(x_axis, sample_prices, 'bo-', label='price')
            ax1_1.set_ylabel('price')

            ax1_1.plot(x_axis, np.multiply(sample_prices, buy), 'r^', label='buy')
            ax1_1.plot(x_axis, np.multiply(sample_prices, sell), 'g^', label='sell')

            ax2 = ax[1]
            ax2.plot(x_axis, cum_return_rate_set, label=str(code_id) + '=' + str(round(cum_return_rate_set[-1], 2))
                                                        + ', min=' + str(min(cum_return_rate_set)))
            ax2.grid(axis='y')
            ax2.set_title('Cumulative rate of return')
            ax2.legend()
            plt.title(code_id)
            plt.legend(loc=2)

            plt.show()

    # if not draw:
        # new_rows.to_sql('rate_yearly', DB.engine, index=False, if_exists='append', chunksize=1000)


def get_holdings(sample_pca, sample_prices):
    Y = sample_pca.col_0
    correlation = Y.corr(sample_prices)
    if correlation < 0:
        Y = (-1) * Y
        correlation = (-1) * correlation

    mean = np.mean(Y)
    mean = mean * sample_len / (sample_len - 1)
    mean = 0
    std = np.std(Y)

    print('std=', std)
    print('mean+1.328std', mean + 1.328 * std)
    print('mean+1.5std', mean + 1.5*std)
    print('mean+1.94std', mean + 1.94 * std)
    print('mean+2std', mean + 2 * std)
    print('mean+3std', mean + 3 * std)
    print('mean+1std', mean + 1 * std)
    print('mean=', mean)
    print('mean-std', mean - 1 * std)
    print('mean-1.328std', mean - 1.328 * std)
    print('mean-1.5std', mean - 1.5 * std)
    print('mean-1.94std', mean - 1.94 * std)
    print('mean-2std', mean - 2 * std)
    print('mean-3std', mean - 3 * std)
    print('correlation=', correlation)

    # if correlation < 0.01:
    #     holdings = [0] * len(Y)
    #     return holdings

    start_loc = len(Y) - 1
    holdings = [0] * start_loc
    bottom_dis = 20
    point_args = np.diff(np.where(np.diff(Y[-bottom_dis:]) > 0, 0, 1))
    peaks = np.ceil((Y[-bottom_dis + 1:-1][point_args == 1]) * 100) / 100
    bottoms = np.floor((Y[-bottom_dis + 1:-1][point_args == -1]) * 100) / 100
    bottoms_length = len(bottoms)
    print('Y=', Y)
    print('bottoms=', bottoms)
    print('peaks=', peaks)
    print(Y.iloc[-2], Y.iloc[-1])

    amplitude = 0
    if Y.iloc[-2] < Y.iloc[-1] and (Y.iloc[-1] > peaks.iloc[-1] or peaks.iloc[-1] > peaks.iloc[-2]) and bottoms.iloc[-1] > bottoms.iloc[-2]:
        # 底上升
        amplitude = 1
    elif Y.iloc[-2] > Y.iloc[-1] and (Y.iloc[-1] < bottoms.iloc[-1] or bottoms.iloc[-1] < bottoms.iloc[-2]) and peaks.iloc[-1] < peaks.iloc[-2]:
        # 底上升
        amplitude = -1
    print('amplitude=', amplitude)


    for i in range(start_loc, len(Y)):
        # 正相关
        if bottoms_length >= 2 and 2*std > Y[i] > Y[i-1] and Y.iloc[i] > peaks.iloc[-1] and peaks.iloc[-1] < mean + std and mean > bottoms.iloc[-1] >= bottoms.iloc[-2] and (bottoms.iloc[-2] < mean - 1 * std):
            # 大双底部
            # 大底部反转之前的数据都有大的价格波动，会增加std和mean，为了反转的灵敏度，std限制可以打个折扣，2std=>1.94, 1.5std=>1.328
            holding = 1
            print('大双底部')

        elif bottoms_length >= 2 and 2*std > Y[i] > Y[i-1] and Y.iloc[i] > peaks.iloc[-1] and std > bottoms.iloc[-1] > mean > bottoms.iloc[-2]:
            # 大双底部
            # 大底部反转之前的数据都有大的价格波动，会增加std和mean，为了反转的灵敏度，std限制可以打个折扣，2std=>1.94, 1.5std=>1.328
            holding = 11
            print('大双底部11')

        elif Y[i-10: i-2].max() > (mean + std) and (Y[i] - Y[i-2:i].min()) > 1.5*std and (mean - 1.5*std) < Y[i-2:i].min() and Y[i] > mean + std:
            # 疯牛的多个板的底部的冲锋形态，至少还有2个板，可能连接着 多个板的顶部形态
            holding = 2
            print('疯牛的多个板的底部的冲锋i=', i)
            # 实测备注：此讯号可靠性高，此讯号包含3连板的形态，如果前几天出现1的信号，则加大此信号的可靠性，一般会有大于3个板的涨幅。
            # 实测备注：两种形态：
            #   一、发出此信号的当天的开盘价在5、10、20均线以下，一条红柱贯穿5、10、20三条均线时，为典型的牛头底部冲锋形态。
            #   二、5、10、20均线均已向上发展
            #   三、前一天的涨幅<2个点，负的更好，如果前一天已经涨的多了，这个信号就失效了
            # 实测备注: -0.02 < mean < 0.02, 此时2的信号更可靠， Y1-Y1越大越好
        elif (Y[i - 30:i - 2].sort_values()[-2:] > (mean + 1.5*std)).all(axis=None) \
                and Y[i - 5:i].min() < (mean - 1*std) \
                and (Y[i] - Y[i - 5:i].min()) > 1.5 * std \
                and Y[i - 1] < mean \
                and Y[i] > Y[i-1] > Y[i-2]:
                # 强势震荡之后的反弹,一般反弹到原来的一半，原来涨幅1倍，此次就反弹50%
            print('强势之后的深度震荡后的强烈反弹i=', i)
            holding = 3

        elif (Y[i-5:i].sort_values()[-2:] > (mean+1.5*std)).all(axis=None) and Y[i] < mean + 1.5*std < Y[i-1]:
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
                and (Y[i] - Y[i-4:i].min()) > 1.328*std \
                and mean < Y[i-5:i-2].max():
            # 3个板的反弹。
            # 优化备注：当天涨幅10%，才有可能连续3个板。此讯号预测连续大涨过后，下跌几日过后继续强势涨幅
            # 优化备注：Y_mean与Y1-Y1的符号相同时更靠谱，Y_mean>0.05, 绝对值越大越好

            print('双顶后的强势反抽3个板i=', i)
            holding = 4

        else:
            holding = 0
            print('哟西')

        holdings.append(holding)
        print(2*std > Y[i] > Y[i-1], Y.iloc[i] > peaks.iloc[-1], mean > bottoms.iloc[-1] >= bottoms.iloc[-2],  (bottoms.iloc[-2] < mean - 1 * std))
        print(std > bottoms.iloc[-1] > mean > bottoms.iloc[-2])

    return holdings


def get_buy_sell_points(holdings):
    buy, sell = [np.nan], [np.nan]
    for i in range(1, len(holdings)):
        if holdings[i] != holdings[i - 1]:
            if holdings[i] > 0:
                buy.append(1)
                sell.append(np.nan)
            elif holdings[i] < 0:
                sell.append(1)
                buy.append(np.nan)
            else:
                buy.append(np.nan)
                sell.append(np.nan)
        else:
            buy.append(np.nan)
            sell.append(np.nan)

    return buy, sell
