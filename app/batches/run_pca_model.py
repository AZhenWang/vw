from app.models.pca import Pca
from app.saver.logic import DB
import matplotlib.pylab as plt
from app.common.function import get_cum_return_rate
import numpy as np
import pandas as pd
from mpl_finance import candlestick_ohlc
from matplotlib import dates as mdates
from matplotlib import ticker as mticker
from datetime import datetime as dt
import matplotlib
# 提供汉字支持
matplotlib.rcParams['font.sans-serif'] = ['Microsoft YaHei']
from app.saver.tables import fields_map
from app.common.function import knn_predict

pre_predict_interval = 5
sample_len = 60


def execute(start_date='', end_date=''):
    # end_date = '20190326'
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
    # TTB = 'daily'
    TTB = 'weekly'
    # TTB = 'monthly'
    codes = [1481]

    # 213：仁和药业，1501：瑞普生物, 271：美锦能源， 1527：中金环境, 521：中钢天源
    # 622: 汉钟精机， 2373：维维股份， 331：美利云， 944：双塔食品，
    # 2850：哈投股份， 2406：美克家居， 2430：五洲交通， 2198：林海股份， 365：新希望，
    # 79：长虹华意,  1475: 金刚玻璃， 2756：红阳能源， 2702：舍得酒业， 462：豫能控股，1575：福安药业， 1949：兴齐眼药， 250：沈阳化工
    # 2292：安彩高科， 137：中原环保， 236：山推股份， 251：模塑科技
    # 238:东方电子，462：豫能控股， 2756：红阳能源， 2274：莲花健康， 2308：天津松江， 171: 启迪古汉,
    # 1481:精准信息， 211：万方发展, 52:中国长城, 2018:正元智慧
    # 2633:光大嘉宝, 2867:妙可蓝多, 540:雪莱特,  2412:恒力股份， 3547：恒润股份， 677：特尔佳, 432:天保基建
    # 687:鱼跃医疗，2187：东方金钰, 2876:秋林集团, 42:深天马A, 2011:广和通， 782：乐通, 819:赫美集团， 975：达华智能
    # 1241:银宝山新，3368: 薄天环境， 1836：赢合科技， 1442：东方财富, 3179:汇嘉时代, 3476:柯利达,
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
        pca_features, prices, Y, dailys = pca.run(code_id=code_id, pre_predict_interval=pre_predict_interval, n_components=n_components, return_y=True, TTB=TTB)
        if sample_len != 0:
            sample_pca = pca_features[-sample_len:].reset_index(drop=True)
            sample_prices = prices[-sample_len:].reset_index(drop=True)
            sample_Y = Y[-sample_len:]
            sample_dailys = dailys[-sample_len:].reset_index(drop=True)
        else:
            sample_pca = pca_features
            sample_prices = prices
            sample_dailys = dailys
        # diff_Y0 = np.where(np.diff(pca_features.col_0) > 0, 1, -1)
        # diff_Y1 = np.where(np.diff(pca_features.col_1) > 0, 1, -1)
        # diff_price = np.where(np.diff(prices) > 0, 1, -1)
        # dot_price_Y0 = np.dot(diff_Y0, diff_price)
        # dot_price_Y1 = np.dot(diff_Y1, diff_price)
        # print('dot_price_y1=', dot_price_Y1)
        # # diff_Y0 = np.where(np.diff(Y0) > 0, 1, -1)
        # # diff_Y1 = np.where(np.diff(Y1) > 0, 1, -1)
        # # diff_price = np.where(np.diff(sample_prices) > 0, 1, -1)
        # # dot_price_Y0 = np.dot(diff_Y0, diff_price)
        # # dot_price_Y1 = np.dot(diff_Y1, diff_price)
        # # print('dot_price_y1=', dot_price_Y1)
        # if dot_price_Y0 < 0:
        #     print('转Y0')
        #     sample_pca.col_0 = (-1) * sample_pca.col_0
        # if dot_price_Y1 < 0:
        #     print('转Y1')
        #     sample_pca.col_1 = (-1) * sample_pca.col_1

        Y0 = sample_pca.col_0
        Y1 = sample_pca.col_1
        # os.exit()
        # print(Y1[-5:])
        # print(sample_prices[-5:])
        # os.exit()

        mean = np.mean(Y0)
        # mean = mean * sample_len / (sample_len - 1)
        mean = 0
        std = np.std(Y0)

        # flags = []
        # for i in range(sample_len):
        #     flag = 0
        #     if Y0[i] < -0.4 and Y0.iloc[i] > Y0.iloc[i - 1] and Y1.iloc[i] > Y1.iloc[i - 1] and sample_prices.iloc[i] <= \
        #             sample_prices.iloc[i - 1]:
        #         flag = 1
        #     elif Y0[i] > 0.4 and Y0.iloc[i] < Y0.iloc[i - 1] and Y1.iloc[i] < Y1.iloc[i - 1] and sample_prices.iloc[
        #         i] >= sample_prices.iloc[i - 1]:
        #         flag = -1
        #     flags.append(flag)

        # 目前最好的买入点，但不是卖出点
        # flags = []
        # for i in range(sample_len):
        #     flag = 0
        #     if Y1.iloc[i] > Y1.iloc[i - 1] and Y1.iloc[i] > 0 and sample_prices.iloc[i] <= \
        #             sample_prices.iloc[i - 1]:
        #         flag = 1
        #     elif Y0[i] > 0.4 and Y0.iloc[i] < Y0.iloc[i - 1] and Y1.iloc[i] < Y1.iloc[i - 1] and sample_prices.iloc[
        #         i] >= sample_prices.iloc[i - 1]:
        #         flag = -1
        #     flags.append(flag)

        flags = []
        for i in range(sample_len):
            flag = 0
            if Y0.iloc[i] > Y0.iloc[i-1] and Y0.iloc[i-1] < Y0.iloc[i-2] \
                    and Y0.iloc[i] > Y1.iloc[i] and Y0.iloc[i-1] < Y1.iloc[i-1] \
                    and Y0.iloc[i-2] > Y1.iloc[i-2] \
                    and Y1.iloc[i] > Y1.iloc[i-1] \
                    and Y0[i - 20:i].max() > 0.5 and Y0.iloc[i-1] < -0 \
                    and sample_prices.iloc[i] > sample_prices.iloc[i-1]:
                flag = 2
            elif Y0.iloc[i] > Y0.iloc[i - 1] and  Y1.iloc[i] > Y1.iloc[i - 1] and Y1.iloc[i] >= 0 and sample_prices.iloc[i] < \
                    sample_prices.iloc[i - 1]:
                flag = 1
            elif Y0.iloc[i] < Y0.iloc[i - 1] and Y1.iloc[i] < Y1.iloc[i - 1] and Y1.iloc[i] <= 0.2 and sample_prices.iloc[
                i] > sample_prices.iloc[i - 1]:
                flag = -1
            flags.append(flag)

        # flags = [0, 0]
        # for i in range(2, sample_len):
        #     flag = 0
        #     if Y1.iloc[i] > Y1.iloc[i - 1] and Y1.iloc[i] > Y1.iloc[i - 1] and sample_prices.iloc[i] < \
        #             sample_prices.iloc[i - 1]:
        #         flag = 1
        #     elif Y1.iloc[i] < Y1.iloc[i - 1] and sample_prices.iloc[i] >= \
        #             sample_prices.iloc[i - 1]:
        #         flag = -1
        #     flags.append(flag)

        holdings = get_holdings(Y=Y0, Y1=Y1, sample_prices=sample_prices)

        cum_return_rate_set = get_cum_return_rate(sample_prices, holdings)
        cum_return_rate_set_flag = get_cum_return_rate(sample_prices, flags)

        buy, sell = get_buy_sell_points(holdings)
        flag_buy, flag_sell = get_buy_sell_points(flags)
        print('holdings=', holdings[-5:])
        print('buy=', buy[-5:])
        print('sell=', sell[-5:])
        print('len(holding)=', len(holdings), ', len(buy)=', len(buy))
        print('Y1-Y1= ', Y1[-3:-1].max() - Y1.iloc[-1])
        print('std=', std)
        print('mean=', mean)

        print('std=', std)
        do_idx = np.not_equal(0, flags)
        s1 = pd.Series(flags)[do_idx]
        # s2 = pd.Series(sample_Y.index)[np.not_equal(0, flags)]
        s2 = pd.Series(sample_dailys['cal_date'])[do_idx]
        s3 = pd.concat([s1, s2], axis=1)
        s4 = sample_pca[do_idx]
        s5 = pd.concat([s3, s4], axis=1)
        s6 = sample_pca.shift()[do_idx]
        s7 = pd.concat([s5, s6], axis=1)
        s8 = s4-s6
        p = pd.concat([s7, s8], axis=1)
        print('p=', p)
        print('flags=', flags[-3:])
        pre40_sum = round((sample_prices[-40:-1].max() - sample_prices[-40:-1].min()) / sample_prices[-40:-1].min(), 2)
        pre40_y0_mean = Y0[-20:].mean()
        pre40_y1_mean = Y1[-20:].mean()
        positive_mean = (Y0[:-3] - Y1[:-3])[Y0[:-3] > Y1[:-3]].mean()
        negative_mean = (Y1[:-3] - Y0[:-3])[Y0[:-3] < Y1[:-3]].mean()

        positive_std = (Y0[:-3] - Y1[:-3])[Y0[:-3] > Y1[:-3]].std()
        negative_std = (Y1[:-3] - Y0[:-3])[Y0[:-3] < Y1[:-3]].std()

        positive_pct_std = (Y0[:-3] - Y0.shift()[:-3])[Y0[:-3] > Y1[:-3]].std()
        negative_pct_std = (Y1[:-3] - Y1.shift()[:-3])[Y0[:-3] < Y1[:-3]].std()

        pct_std0 = (sample_dailys[:-3]['pct_chg'])[Y0[:-3] > Y1[:-3]].std()
        pct_std1 = (sample_dailys[:-3]['pct_chg'])[Y0[:-3] < Y1[:-3]].std()
        pct_mean0 = (sample_dailys[:-3]['pct_chg'])[Y0[:-3] > Y1[:-3]].mean()
        pct_mean1 = (sample_dailys[:-3]['pct_chg'])[Y0[:-3] < Y1[:-3]].mean()
        print('Y0-mean', ', Y1-mean')
        print(round(pct_mean0, 2), round(pct_mean1, 2))
        print('Y0-std', ', Y1-std')
        print(round(pct_std0, 2), round(pct_std1, 2))
        print('explained_variance_ratio_=', pca.explained_variance_ratio_)

        qqb = 0
        bottom_dis = 20
        point_args_price = np.diff(np.where(np.diff(sample_prices[-bottom_dis:]) > 0, 0, 1))
        peaks_price = sample_prices[-bottom_dis + 1:-1][point_args_price == 1]
        bottoms_price = np.floor((sample_prices[-bottom_dis + 1:-1][point_args_price == -1]) * 100) / 100
        point_args = np.diff(np.where(np.diff(Y[-bottom_dis:]) > 0, 0, 1))
        peaks = np.ceil((Y[-bottom_dis + 1:-1][point_args == 1]) * 100) / 100
        bottoms = np.floor((Y[-bottom_dis + 1:-1][point_args == -1]) * 100) / 100
        if len(bottoms_price) >= 1 and len(peaks_price) >= 1:
            if Y0.iloc[-1] < peaks.iloc[-1] and sample_prices.iloc[-1] > peaks_price.iloc[-1]:
                # 顶背离
                qqb = -1
            elif Y0.iloc[-1] > bottoms.iloc[-1] and sample_prices.iloc[-1] < bottoms_price.iloc[-1]:
                # 底背离
                qqb = 1
        print('log', flags[-5:], 'qqb', qqb)
        print('Y0=', round(Y0.iloc[-1], 4), 'Y1=', round(Y1.iloc[-1], 4), 'Y0>Y1:', Y0.iloc[-1] > Y1.iloc[-1])
        # os.exit
        sample_dailys['dateTime'] = mdates.date2num(
            sample_dailys['cal_date'].apply(lambda x: dt.strptime(x, '%Y%m%d')))
        sample_dailys['close'] = sample_dailys['close'] * sample_dailys['adj_factor']
        sample_dailys['open'] = sample_dailys['open'] * sample_dailys['adj_factor']
        sample_dailys['high'] = sample_dailys['high'] * sample_dailys['adj_factor']
        sample_dailys['low'] = sample_dailys['low'] * sample_dailys['adj_factor']
        i += 1
        if draw:
            fig = plt.figure(figsize=(20, 16))
            x_axis = sample_dailys['dateTime']
            ax0 = fig.add_subplot(211)
            ax0.xaxis.set_major_locator(mticker.MaxNLocator(10))
            ax0.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
            ax0.plot(x_axis, Y0, label='Y')
            ax0.plot(x_axis, Y1, label='Y1')
            ax0.set_ylabel('pca')
            # ax0.axhline(mean - 3 * std, color='b')
            ax0.axhline(mean - 2 * std, color='c')
            ax0.axhline(mean - 1.5 * std, color='r')
            ax0.axhline(mean - 1 * std, color='k')
            ax0.axhline(mean, color='black')
            ax0.axhline(mean + 1 * std, color='k')
            ax0.axhline(mean + 1.5 * std, color='r')
            ax0.axhline(mean + 2 * std, color='c')
            # ax0.axhline(mean + 3 * std, color='b')
            ax0.set_ylabel('pca')

            # plt.legend(loc=3)
            ax0.set_title(str(code_id) + '-图形信号')

            ax1 = ax0.twinx()
            ax1.plot(x_axis, sample_prices, 'b-', label='price', alpha=0.5)
            ax1.plot(x_axis, np.multiply(sample_prices, buy), 'mo', label='buy')
            ax1.plot(x_axis, np.multiply(sample_prices, sell), 'co', label='sell')
            ax1.plot(x_axis, np.multiply(sample_prices, flag_buy), 'rv', label='flag_buy')
            ax1.plot(x_axis, np.multiply(sample_prices, flag_sell), 'gv', label='flag_sell')
            ax1.set_ylabel('price')

            ax2 = fig.add_subplot(212, facecolor='#07000d')
            ax2.xaxis.set_major_locator(mticker.MaxNLocator(10))
            ax2.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
            # ax2.grid()
            # 支撑压力画线
            support_lines = sample_dailys[np.equal(1, flags)]
            press_lines = sample_dailys[np.equal(-1, flags)]
            for i in range(len(support_lines)):
                y1 = support_lines.iloc[i]['close']
                y2 = support_lines.iloc[i]['open']
                ax2.axhline(y1, color='red', alpha=0.23)
                ax2.axhline(y2, color='red', alpha=0.23)
            for i in range(len(press_lines)):
                y1 = press_lines.iloc[i]['close']
                y2 = press_lines.iloc[i]['open']
                ax2.axhline(y1, color='w', alpha=0.23)
                ax2.axhline(y2, color='w', alpha=0.23)
            data_mat = sample_dailys[['dateTime', 'open', 'high', 'low', 'close']]
            candlestick_ohlc(ax2, data_mat.values, width=.7, colorup='y', colordown='c')

            ax2.plot(x_axis, np.multiply(sample_dailys['high']*1.05, flag_buy), 'rv', label='flag_buy')
            ax2.plot(x_axis, np.multiply(sample_dailys['high']*1.05, flag_sell), 'wv', label='flag_sell')

            ax2.set_title(str(code_id) + '-资金信号')
            # ax2.plot(x_axis, cum_return_rate_set, label=str(code_id) + '=' + str(round(cum_return_rate_set[-1], 2))
            #                                             + ', min=' + str(min(cum_return_rate_set)))
            # ax2.plot(x_axis, cum_return_rate_set_flag, label='flag' + str(code_id) + '=' + str(round(cum_return_rate_set_flag[-1], 2))
            #                                             + ', min=' + str(min(cum_return_rate_set_flag)))
            # ax2.grid(axis='y')
            # ax2.set_title('Cumulative rate of return')
            # ax2.legend()

            # plt.legend(loc=3)

            plt.show()

    # if not draw:
        # new_rows.to_sql('rate_yearly', DB.engine, index=False, if_exists='append', chunksize=1000)


def get_holdings(Y, Y1, sample_prices):
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

    # if correlation < 0.01:
    #     holdings = [0] * len(Y)
    #     return holdings

    start_loc = len(Y) - len(Y) + 1
    holdings = [0] * start_loc
    bottom_dis = 20
    point_args = np.diff(np.where(np.diff(Y[-bottom_dis:]) > 0, 0, 1))
    peaks = np.ceil((Y[-bottom_dis + 1:-1][point_args == 1]) * 100) / 100
    bottoms = np.floor((Y[-bottom_dis + 1:-1][point_args == -1]) * 100) / 100
    bottoms_length = len(bottoms)
    print('bottoms=', bottoms)
    print('peaks=', peaks)

    amplitude = 0
    if Y.iloc[-2] < Y.iloc[-1] and (Y.iloc[-1] > peaks.iloc[-1] or peaks.iloc[-1] > peaks.iloc[-2]) and bottoms.iloc[-1] > bottoms.iloc[-2]:
        # 底上升
        amplitude = 1
    elif Y.iloc[-2] > Y.iloc[-1] and (Y.iloc[-1] < bottoms.iloc[-1] or bottoms.iloc[-1] < bottoms.iloc[-2]) and peaks.iloc[-1] < peaks.iloc[-2]:
        # 底上升
        amplitude = -1
    print('amplitude=', amplitude)

    for i in range(start_loc, len(Y)):

        if bottoms_length >= 2 and 2*std > Y[i] > Y[i-1] and Y[i] > Y1[i] and Y1[-3:-1].max() - Y1.iloc[-1] > 0 \
                and Y.iloc[i] > peaks.iloc[-1] and peaks.iloc[-1] < mean + std and mean > bottoms.iloc[-1] >= bottoms.iloc[-2] and (bottoms.iloc[-2] < mean - 1 * std):
            # 大双底部
            # 大底部反转之前的数据都有大的价格波动，会增加std和mean，为了反转的灵敏度，std限制可以打个折扣，2std=>1.94, 1.5std=>1.328
            holding = 1
            # print('大双底部')

        elif (Y[i - bottom_dis:i - 2].sort_values()[-2:] > (mean + 1.5 * std)).all(axis=None) \
                and bottoms.iloc[-1] < mean + std \
                and (Y[i] - Y[i - 5:i].min()) > 1.328 * std \
                and 2 * std > Y[i] > mean and Y[i] > Y[i - 1]:
            # 强势震荡之后的反弹,一般反弹到原来的一半，原来涨幅1倍，此次就反弹50%
            # print('强势之后的深度震荡后的强烈反弹i=', i)
            holding = 3

        elif bottoms_length >= 2 and 2*std > Y[i] > Y[i-1] and Y.iloc[i] > peaks.iloc[-1] and std > bottoms.iloc[-1] > mean > bottoms.iloc[-2]:
            # 大双底部
            # 大底部反转之前的数据都有大的价格波动，会增加std和mean，为了反转的灵敏度，std限制可以打个折扣，2std=>1.94, 1.5std=>1.328
            holding = 11
            # print('大双底部11')

        elif Y[i-10: i-2].max() - Y[i-7:i].min() > (2*std) and (Y[i] - Y[i-2:i].min()) > 1.5*std and (mean - 1.5*std) > Y[i-5:i].min() and Y[i] > mean + std:
            # 疯牛的多个板的底部的冲锋形态，至少还有2个板，可能连接着 多个板的顶部形态
            holding = 2
            # print('疯牛的多个板的底部的冲锋i=', i)
            # 实测备注：此讯号可靠性高，此讯号包含3连板的形态，如果前几天出现1的信号，则加大此信号的可靠性，一般会有大于3个板的涨幅。
            # 实测备注：两种形态：
            #   一、发出此信号的当天的开盘价在5、10、20均线以下，一条红柱贯穿5、10、20三条均线时，为典型的牛头底部冲锋形态。
            #   二、5、10、20均线均已向上发展
            #   三、前一天的涨幅<2个点，负的更好，如果前一天已经涨的多了，这个信号就失效了
            # 实测备注: -0.02 < mean < 0.02, 此时2的信号更可靠， Y1-Y1越大越好

        elif (Y[i-5:i].sort_values()[-2:] > (mean+1.5*std)).all(axis=None) and Y[i] < mean + 1.5*std < Y[i-1]:
            # 大顶部
            # 实测备注：如果是连续大涨幅，则第二天还会有反弹，第二天收盘价卖
            holding = -2
            # print('双顶')

        elif Y[i - 1] > mean + 1.5 * std > Y[i]:
            # 大顶部
            # 实测备注：如果是连续大涨幅，则第二天还会有反弹，第二天收盘价卖
            holding = -1
            # print('大顶部')

        elif (Y[i - 10:i - 2].sort_values()[-2:] > (mean + std)).all(axis=None) \
                and (Y[i] - Y[i-4:i].min()) > 1.328*std \
                and mean < Y[i-5:i-2].max():
            # 3个板的反弹。
            # 优化备注：当天涨幅10%，才有可能连续3个板。此讯号预测连续大涨过后，下跌几日过后继续强势涨幅
            # 优化备注：Y_mean与Y1-Y1的符号相同时更靠谱，Y_mean>0.05, 绝对值越大越好

            # print('双顶后的强势反抽3个板i=', i)
            holding = 4

        else:
            holding = 0
            # print('哟西')

        holdings.append(holding)

    return holdings


def get_buy_sell_points(holdings):
    buy, sell = [np.nan], [np.nan]
    for i in range(1, len(holdings)):
        if holdings[i] > 0:
            buy.append(1)
            sell.append(np.nan)
        elif holdings[i] < 0:
            sell.append(1)
            buy.append(np.nan)
        else:
            buy.append(np.nan)
            sell.append(np.nan)

    return buy, sell


