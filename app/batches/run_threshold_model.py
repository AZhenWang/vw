from app.saver.logic import DB
from app.common.function import send_email
from email.mime.text import MIMEText
from email.mime.image import MIMEImage
from matplotlib import pyplot as plt
import numpy as np
from app.common.function import get_cum_return, get_buy_sell_points

ratio_lable = {
    8: '8',
    16: '16',
    25: '25',
    34: '34',
    45: '45',
    55: '55',
    61.8: '61.8',
    76: '76',
    80: '80',
}


def execute(start_date='', end_date=''):
    trade_cal = DB.get_open_cal_date(end_date=end_date, period=40)
    start_date_id = trade_cal.iloc[0]['date_id']
    end_date_id = trade_cal.iloc[-1]['date_id']
    data = DB.count_threshold_group_by_date_id(start_date_id=start_date_id, end_date_id=end_date_id)
    data.eval('up_stock_ratio=up_stock_number/list_stock_number*100', inplace=True)

    text = data[['cal_date', 'up_stock_ratio']].to_string(index=False)
    msgs = []
    msgs.append(MIMEText(text, 'plain', 'utf-8'))

    data.sort_values(by='cal_date', ascending=True, inplace=True)
    data.reset_index(inplace=True)

    holdings = get_holdings(data['up_stock_ratio'])
    buy, sell = get_buy_sell_points(holdings)

    fig, ax = plt.subplots(2, 1, figsize=(16, 16), sharex=True)

    index_daily = DB.get_index_daily(ts_code='000001.SH', start_date_id=start_date_id, end_date_id=end_date_id)
    index_daily.sort_values(by='cal_date', ascending=True, inplace=True)
    gp = index_daily.groupby('ts_code')
    for ts_code, group_data in gp:
        name = group_data.iloc[0]['name']
        group_data = group_data.set_index('cal_date')
        group_data = group_data.reindex(data['cal_date'], method='ffill')
        group_data = group_data.reset_index()
        adj_index = group_data['close'] / group_data.iloc[0]['close']
        cum_return_set = get_cum_return(group_data['close'], holdings)

        ax0_0 = ax[0]
        ax0_0.plot(adj_index, label=ts_code)
        ax0_0.plot(np.multiply(index_daily['close'], buy), 'r^', label='buy')
        ax0_0.plot(np.multiply(index_daily['close'], sell), 'g^', label='sell')
        ax1 = ax[1]
        ax1.plot(cum_return_set, label=ts_code + '=' + str(round(cum_return_set[-1], 2)))

    max_loc = len(data) - 1
    xticks_loc = [round(i / 7 * max_loc) for i in np.arange(0, 8)]
    plt.xticks(xticks_loc, data.iloc[xticks_loc]['cal_date'], rotation=60)

    ax0_0 = ax[0]
    ax0_0.legend(loc=2, ncol=4)
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
    ax0_1.plot(data['up_stock_ratio'], color=color, label='up_stock_ratio', linestyle='-')
    ax0_1.yaxis.set_ticks(list(ratio_lable.keys()))
    ax0_1.yaxis.set_ticklabels(list(ratio_lable.values()))
    ax0_1.tick_params(axis='y', labelcolor=color)
    ax0_1.grid()
    ax0_1.legend(loc=1)
    ax0_1.set_title('Up-stock percentage by threshold')
    ax0_1.set_xlabel('Time')
    ax0_1.set_ylabel('Up-stock ratio')

    ax1 = ax[1]
    ax1.legend(loc=2, ncol=4)
    ax1.set_title('Cum Return', fontsize=12)
    ax1.set_ylabel('Return', fontsize=10)
    ax1.set_xlabel('Time', fontsize=10)
    ax1.grid()

    plt.show()

    fig.savefig('threshold_picture.png')
    image_msg = MIMEImage(open('threshold_picture.png', 'rb').read())
    image_msg.add_header('Content-Disposition', 'attachment', filename='threshold_picture.png')
    msgs.append(image_msg)

    send_email(subject=end_date+'的thresholds统计数据', msgs=msgs)


def get_holdings(Y_hat):
    holdings = [0] * 2
    holding = 0
    for i in range(2, len(Y_hat)):
        if Y_hat[i] <= 8 <= Y_hat[i - 1]:
            holding = 0
        elif (Y_hat[i] > Y_hat[i - 1]) and (Y_hat[i - 1] > 8):
            holding = 1

        holdings.append(holding)

    return holdings


