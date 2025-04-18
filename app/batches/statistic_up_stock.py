from app.saver.logic import DB
from app.common.function import send_email
from email.mime.text import MIMEText
from email.mime.image import MIMEImage
from matplotlib import pyplot as plt
import numpy as np
import pandas as pd
from app.common.function import get_cum_return_rate, get_buy_sell_points

ratio_lable = {
    6: '6',
    25: '25',

}
#
# ratio_lable = {
#     8: '8',
#     16: '16',
#     25: '25',
#     34: '34',
#     45: '45',
#     55: '55',
#     61.8: '61.8',
#     76: '76',
#     80: '80',
# }


def execute(start_date='', end_date=''):
    trade_cal = DB.get_open_cal_date(end_date=end_date, period=22)
    start_date_id = trade_cal.iloc[0]['date_id']
    end_date_id = trade_cal.iloc[-1]['date_id']
    # index_code = '000905.SH'
    index_code = '000300.SH'
    # index_code = '000001.SH'
    up_data = DB.count_threshold_group_by_date_id(dire='up', start_date_id=start_date_id, end_date_id=end_date_id)
    all_data = DB.count_threshold_group_by_date_id(start_date_id=start_date_id, end_date_id=end_date_id)
    up_ratio = up_data['stock_number'] / all_data['stock_number'] * 100
    up_ratio.name = 'up_ratio'
    up_circ_ratio = up_data['circ_mv'] / all_data['circ_mv'] * 100
    up_circ_ratio.name = 'up_circ_ratio'

    up_ratio = up_ratio.apply(np.round, decimals=2)
    up_circ_ratio = up_circ_ratio.apply(np.round, decimals=2)
    up_pct = round(up_ratio.pct_change(periods=-1) * 100, 1)
    up_pct.name = 'up_pct'
    up_circ_pct = round(up_circ_ratio.pct_change(periods=-1) * 100, 1)
    up_circ_pct.name = 'up_circ_pct'

    data = pd.concat([up_data.cal_date, up_ratio, up_circ_ratio, up_pct, up_circ_pct], axis=1)

    statistic_data = data.sort_values(by='cal_date', ascending=True)
    statistic_data.reset_index(inplace=True)
    # holdings = get_holdings(statistic_data['up_ratio'])
    holdings = get_holdings(statistic_data['up_circ_ratio'])
    buy, sell = get_buy_sell_points(holdings)

    fig, ax = plt.subplots(2, 1, figsize=(16, 16), sharex=True)

    index_daily = DB.get_index_daily(ts_code=index_code, start_date_id=start_date_id, end_date_id=end_date_id)
    index_daily['pct_chg'] = round(index_daily['pct_chg'], 1)
    gp = index_daily.groupby('ts_code')
    for ts_code, group_data in gp:
        group_data = group_data.set_index('cal_date')
        data = data.join(group_data[['pct_chg', 'close']], on='cal_date')
        # group_data = group_data.reindex(data['cal_date'], method='ffill')
        group_data.sort_values(by='cal_date', ascending=True, inplace=True)
        group_data = group_data.reset_index()
        adj_index = group_data['close'] / group_data.iloc[0]['close']
        cum_return_set = get_cum_return_rate(group_data['close'], holdings)

        ax0_0 = ax[0]
        ax0_0.plot(adj_index, label=ts_code)
        ax0_0.plot(np.multiply(adj_index, buy), 'r^', label='buy')
        ax0_0.plot(np.multiply(adj_index, sell), 'g^', label='sell')
        ax1 = ax[1]
        ax1.plot(cum_return_set, label=ts_code + '=' + str(round(cum_return_set[-1], 2)))

    max_loc = len(statistic_data) - 1
    xticks_loc = [round(i / 14 * max_loc) for i in np.arange(0, 15)]
    plt.xticks(xticks_loc, statistic_data.iloc[xticks_loc]['cal_date'], rotation=60)

    ax0_0 = ax[0]
    # ax0_0.legend(loc=2, ncol=4)
    ax0_0.set_ylabel('Index')
    # ratio_lable = {
    #     1: '1',
    #     2: '2',
    #     3: '3',
    #     5: '5',
    #     8: '8',
    #     13: '13',
    #     21: '21',
    #     25: 'litter_bull',
    #     34: '34',
    #     45: '45',
    #     55: 'big_bull',
    #     61.8: '61.8',
    #     76: 'alter',
    #     80: 'danger',
    # }
    color = 'tab:red'
    ax0_1 = ax[0].twinx()
    ax0_1.plot(statistic_data['up_ratio'], 'r-', label='number')
    ax0_1.plot(statistic_data['up_circ_ratio'], 'b-', label='money')
    ax0_1.legend(loc=3)
    ax0_1.yaxis.set_ticks(list(ratio_lable.keys()))
    ax0_1.yaxis.set_ticklabels(list(ratio_lable.values()))
    ax0_1.tick_params(axis='y', labelcolor=color)
    ax0_1.grid()
    # ax0_1.legend(loc=1)
    ax0_1.set_title('Up-stock percentage by threshold')
    ax0_1.set_xlabel('Time')
    ax0_1.set_ylabel('Up-stock ratio')

    ax1 = ax[1]
    ax1.legend(loc=2)
    ax1.set_title('Cum Return', fontsize=12)
    ax1.set_ylabel('Return', fontsize=10)
    ax1.set_xlabel('Time', fontsize=10)
    ax1.grid()

    plt.show()

    text = data.to_string(index=False)
    msgs = []
    msgs.append(MIMEText(text, 'plain', 'utf-8'))

    fig.savefig('threshold_picture.png')
    image_msg = MIMEImage(open('threshold_picture.png', 'rb').read())
    image_msg.add_header('Content-Disposition', 'attachment', filename='threshold_picture.png')
    msgs.append(image_msg)

    send_email(subject=end_date+'的thresholds统计数据', msgs=msgs)


def get_holdings(Y_hat):
    holdings = [0] * 2
    holding = 0
    for i in range(2, len(Y_hat)):
        if (Y_hat[i] > 7) and (Y_hat[i - 1] > 7):
            holding = 1
        elif (Y_hat[i] < Y_hat[i - 1]) and (Y_hat[i] < 7):
            holding = 0

        holdings.append(holding)

    return holdings


