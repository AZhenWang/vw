from app.models.pca import Pca
from app.saver.logic import DB
import matplotlib.pylab as plt
from app.common.function import get_cum_return_rate
import numpy as np
import pandas as pd
from app.saver.tables import fields_map


sample_len = 122

def execute(start_date='', end_date=''):
    end_date = '20180301'
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
    codes = [868]
    # codes = [2082, 2496]
    # 此股形态特别备注：像edit一样，50个点的涨幅
    # codes = [2678]
    i = 0
    plan_number = 31
    # plan_number = 7
    # plan_number = 15
    n_components = 2
    for code_id in codes:
        sample_pca, sample_prices = pca.run(code_id=code_id, sample_len=sample_len, n_components=n_components)

        sample_mean = sample_pca.mean()
        sample_std = sample_pca.std()
        column_loc = 0
        mean = sample_mean[column_loc]
        mean = mean * sample_len / (sample_len - 1)
        std = sample_std[column_loc]

        holdings = get_holdings(sample_pca, sample_prices, plan_number=plan_number)
        print('holdings = ', holdings)
        cum_return_rate_set = get_cum_return_rate(sample_prices, holdings)
        buy, sell = get_buy_sell_points(holdings)
        print('buy=', buy)
        print('sell=', sell)
        print('len(holding)=', len(holdings), ', len(buy)=', len(buy))
        print('code_id=', code_id, ' rate_yearly=', cum_return_rate_set[-1],
              ' diff=', (sample_prices.iloc[-1] - sample_prices.iloc[20])/sample_prices.iloc[20],
              ' holding=', np.sum(holdings)
              )
        print('explained_variance_ratio_=', pca.explained_variance_ratio_)
        print('col_0= ', sample_pca.col_0.corr(sample_prices.reset_index(drop=True)))
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

        print(new_rows)
        i += 1

        if draw:
            fig, ax = plt.subplots(2, 1, figsize=(16, 8))
            x_axis = [i for i in range(sample_len)]
            ax1 = ax[0]
            ax1.plot(x_axis, sample_pca)
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

    if not draw:
        new_rows.to_sql('rate_yearly', DB.engine, index=False, if_exists='append', chunksize=1000)


def get_holdings(sample_pca, sample_prices, plan_number):
    Y = sample_pca.col_0
    sample_mean = sample_pca.mean()
    sample_std = sample_pca.std()
    column_loc = 0
    mean = sample_mean[column_loc]
    # mean = 0
    print('old_mean=', mean)
    mean = mean * sample_len / (sample_len-1)
    std = sample_std[column_loc]
    correlation = sample_pca.col_0.corr(sample_prices.reset_index(drop=True))

    start_loc = len(Y) - 5
    holdings = [0] * start_loc
    bottom_dis = 20
    peak_dis = 40

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
    print(Y[-peak_dis:])
    if np.abs(correlation) < 0.2:
        holdings = [0] * len(Y)
        return holdings

    for i in range(start_loc, len(Y)):

        if correlation > 0:
            # 正相关
            if plan_number & 4 and (Y[i] - Y[i-2:i].min()) > 2*std and (mean - 1.5*std) < Y[i-2:i].min() < mean and Y[i] > mean + std:
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

            elif plan_number & 1 and Y[i - 1] > mean + 1.5 * std > Y[i]:
                # 大顶部
                holding = -1
                print('大顶部')

            elif plan_number & 2 and Y[i - bottom_dis:i - 2].min() < Y[i - 2] < Y[i - 1] < Y[i] \
                    and Y[i - bottom_dis:i - 2].min() < mean - 1.5 * std and Y[i - 2] < mean - 1 * std:
                print(Y[i - 2], mean, std, mean - 1.5 * std)
                # 大双底部
                # 大底部反转之前的数据都有大的价格波动，会增加std和mean，为了反转的灵敏度，std限制可以打个折扣，2std=>1.94, 1.5std=>1.328
                holding = 1
                print('大双底部')

            else:
                holding = 0
                print('哟西')

        else:
            # 负相关
            if plan_number & 4 and (Y[i] - Y[i-2:i].max()) < -2*std and (mean + 1.5*std) > Y[i-2:i].max() > mean and Y[i] < mean - std:
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

            elif plan_number & 1 and Y[i - 1] < mean - 1.5 * std < Y[i]:
                # 大底部
                holding = -1
                print('大底部')

            elif plan_number & 2 and Y[i - bottom_dis: i - 2].max() > Y[i - 2] > Y[i - 1] > Y[i] \
                    and Y[i - bottom_dis: i - 2].max() > mean + 1.5 * std and Y[i - 2] > mean + 1 * std:
                # 大双顶部
                holding = 1
                print('大双顶部')

            else:
                holding = 0
                print('哟西')

        holdings.append(holding)

    return holdings


def get_buy_sell_points(holdings):
    print('范德萨发大')
    buy, sell = [np.nan], [np.nan]
    for i in range(1, len(holdings)):
        if holdings[i] != holdings[i - 1]:
            if holdings[i] != -1 and holdings[i] != 0:
                buy.append(1)
                sell.append(np.nan)
            elif holdings[i] == -1:
                sell.append(1)
                buy.append(np.nan)
            else:
                buy.append(np.nan)
                sell.append(np.nan)
        else:
            buy.append(np.nan)
            sell.append(np.nan)

    return buy, sell