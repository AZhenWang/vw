from app.models.pca import Pca
from app.saver.logic import DB
import matplotlib.pylab as plt
from app.common.function import get_cum_return_rate
import numpy as np
import pandas as pd
from app.saver.tables import fields_map


sample_len = 122

def execute(start_date='', end_date=''):
    end_date = '20190115'
    trade_cal = DB.get_open_cal_date(end_date=end_date, period=1)
    date_id = trade_cal.iloc[-1]['date_id']

    pca = Pca(cal_date=end_date)

    # recommended_codes = DB.get_recommended_stocks(cal_date=end_date)
    focus_codes = DB.get_focus_stocks()
    # up_stocks = DB.get_up_stocks_by_threshold(cal_date=end_date)

    codes = focus_codes['code_id']
    new_rows = pd.DataFrame(columns=fields_map['rate_yearly'])
    draw = False
    draw = True
    codes = [1027]
    # 涨std, 1402:0.367/-0.002, 190:0.344/-0.084, 2553:0.389/-0.075,
    # 跌std, 411:0.384/-0.117, 1174:0.387/0.013, 2597:0.362/-0.216， 2802：0.344/-0.144,
    # codes = [3548]
    # codes = [1174, 2553]
    # codes = [2082, 2496]
    # 此股形态特别备注：像edit一样，50个点的涨幅
    # codes = [2678]
    i = 0
    plan_number = 31
    # plan_number = 7
    # plan_number = 15
    n_components = 1
    for code_id in codes:
        sample_pca, sample_prices = pca.run(code_id=code_id, sample_len=sample_len, n_components=n_components)
        Y = sample_pca.col_0
        correlation = Y.corr(sample_prices.reset_index(drop=True))
        if correlation < 0:
            # 负相关的先反过来
            Y = (-1) * Y

        mean = np.mean(Y)
        mean = mean * sample_len / (sample_len - 1)
        std = np.std(Y)

        holdings = get_holdings(sample_pca, sample_prices, plan_number=plan_number)
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
        print('col_0= ', sample_pca.col_0.corr(sample_prices.reset_index(drop=True)))
        print('reverse_col_0= ', Y.corr(sample_prices.reset_index(drop=True)))
        # print('col_1= ', sample_pca.col_1.corr(sample_prices.reset_index(drop=True)))
        # print('col_0 corr col_1 = ', sample_pca.col_0.corr(sample_pca.col_1))

        #  存储比较不同的策略组合的收益率
        new_rows.loc[i] = {
            'plan_number': plan_number,
            'code_id': code_id,
            'date_id': date_id,
            'rate_yearly': cum_return_rate_set[-1],
            'min_rate': np.min(cum_return_rate_set),
            'max_rate': np.max(cum_return_rate_set),
            'mean_rate': np.mean(cum_return_rate_set),
        }

        i += 1

        if draw:
            fig, ax = plt.subplots(2, 1, figsize=(16, 8))
            x_axis = [i for i in range(sample_len)]
            ax1 = ax[0]
            ax1.plot(x_axis, Y)
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
            ax1_1.plot(x_axis, sample_prices.iloc[-sample_len:], 'bo-', label='price')
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


def get_holdings(sample_pca, sample_prices, plan_number):
    Y = sample_pca.col_0
    correlation = Y.corr(sample_prices.reset_index(drop=True))
    if correlation < 0:
        Y = (-1) * Y
        correlation = (-1) * correlation

    mean = np.mean(Y)
    mean = mean * sample_len / (sample_len - 1)
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
    print(Y[-7:])

    if correlation < 0.01:
        holdings = [0] * len(Y)
        return holdings

    start_loc = len(Y) - 5
    holdings = [0] * start_loc
    bottom_dis = 20

    for i in range(start_loc, len(Y)):

        # 正相关
        if Y[i-10: i-2].max() > (mean + std) and (Y[i] - Y[i-2:i].min()) > 2*std and (mean - 1.5*std) < Y[i-2:i].min() and Y[i] > mean + std:
            # 疯牛的多个板的底部的冲锋形态，至少还有2个板，可能连接着 多个板的顶部形态
            holding = 2
            print('疯牛的多个板的底部的冲锋i=', i)
            # 实测备注：此讯号可靠性高，此讯号包含3连板的形态，如果前几天出现1的信号，则加大此信号的可靠性，一般会有大于3个板的涨幅。
            # 实测备注：两种形态：
            #   一、发出此信号的当天的开盘价在5、10、20均线以下，一条红柱贯穿5、10、20三条均线时，为典型的牛头底部冲锋形态。
            #   二、5、10、20均线均已向上发展
            #   三、前一天的涨幅<2个点，负的更好，如果前一天已经涨的多了，这个信号就失效了
            # 20190122: -0.1 < mean < 0.1, 此时2的信号更可靠

        elif ((mean > 0.05 and Y[i] > mean + 1.5 * std) or Y[i] > mean + 2 * std) \
                and Y[i] > Y[i - 1] > mean + std and Y[i - 2] > Y[i - 1]:
            # elif plan_number & 8 and Y[i] > 2 * std and Y[i] > Y[i - 1] > 1.5 * std and Y[i - 2] > Y[
            #     i - 1]:
            # 疯牛的多个板的顶部形态， 最少还有3-5个板
            # 实测备注：当接下来的几天出现第二个3时，一般在第二天会出现最高值，第二天必卖，第三天大概率会跌。
            # 当连续几天的涨幅已经很大了，比如100%，这个3的信号都是一个危险预警信号，第二天大概率出现-1的信号。
            # 实测备注：出现此信号时，保险的操作是静待明天的收盘价，如果第二天收盘价走高，就确认此信号延续，否则就是触顶信号
            holding = 3
            print('疯牛的4-6个板的顶部形态i=', i)

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

        elif (Y[i - 10:i - 2].sort_values()[-2:] > (mean+1.5*std)).all(axis=None) \
                and (Y[i] - Y[i-4:i].min()) > 1.328*std \
                and mean < Y[i-5:i-2].max():
            # 3个板的反弹。
            # 优化备注：当天涨幅10%，才有可能连续3个板。此讯号预测连续大涨过后，下跌几日过后继续强势涨幅
            print('双顶后的强势反抽3个板i=', i)
            holding = 4

        elif Y[i - bottom_dis:i - 2].min() < Y[i - 2] < Y[i - 1] < Y[i] \
                and Y[i - bottom_dis:i - 2].min() < mean - 1.5 * std and Y[i - 2] < mean - 1 * std:
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
