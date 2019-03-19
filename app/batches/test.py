from matplotlib import pyplot as plt
from app.saver.logic import DB


def execute(start_date, end_date):
    # 测试recommend效率
    code_id = 1442
    start_date = '20120101'
    end_date = '20150101'
    trade_cal = DB.get_open_cal_date(end_date=end_date, start_date=start_date)
    for cal_date, date_id in trade_cal[['cal_date', 'date_id']].values:
        logs = DB.get_recommended_stocks(code_id=code_id, recommend_type='pca')
        for i in range(len(logs)):


        holdings = get_holdings(sample_pca, sample_prices)
        print('holdings = ', holdings[-5:])
        print('std=', std)
        cum_return_rate_set = get_cum_return_rate(sample_prices, holdings)
        buy, sell = get_buy_sell_points(holdings)


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