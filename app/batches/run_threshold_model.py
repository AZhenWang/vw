from app.saver.logic import DB
from app.common.function import send_email
from email.mime.text import MIMEText
from email.mime.image import MIMEImage
from matplotlib import pyplot as plt
import numpy as np
from app.common.function import get_cum_return, get_buy_sell_points


def execute(start_date='', end_date=''):
    trade_cal = DB.get_open_cal_date(end_date=end_date, period=300)
    start_date_id = trade_cal.iloc[0]['date_id']
    end_date_id = trade_cal.iloc[-2]['date_id']
    data = DB.count_threshold_group_by_date_id(start_date_id=start_date_id, end_date_id=end_date_id)
    data.eval('up_stock_ratio=up_stock_number/list_stock_number*100', inplace=True)

    text = data[['cal_date', 'up_stock_ratio']].to_string(index=False)
    msgs = []
    msgs.append(MIMEText(text, 'plain', 'utf-8'))

    data.sort_values(by='cal_date', ascending=True, inplace=True)
    data.reset_index(inplace=True)

    index_daily = DB.get_index_daily(ts_code='000001.SH', start_date_id=start_date_id, end_date_id=end_date_id)
    index_daily.sort_values(by='cal_date', ascending=True, inplace=True)
    index_daily.reset_index(inplace=True)

    holdings = get_holdings(data['up_stock_ratio'])
    buy, sell = get_buy_sell_points(holdings)
    cum_return, cum_return_set = get_cum_return(index_daily['close'], holdings)

    fig, ax = plt.subplots(2, 1, figsize=(16, 16), sharex=True)

    color = 'tab:grey'
    ax0 = ax[0]
    ax0.plot(data['up_stock_ratio'], color=color, label='up_stock_ratio')
    ax0.grid()
    ax0.legend(loc=2, ncol=6)
    plt.title('Up-stock percentage by threshold')
    plt.xlabel('Time')
    plt.ylabel('Up-stock ratio')

    max_loc = len(data)-1
    xticks_loc = [round(i / 7 * max_loc) for i in np.arange(0, 8)]
    ratio_lable = {
        1: '1',
        2: '2',
        3: '3',
        5: '5',
        8: '8',
        13: '13',
        21: '21',
        28: '28',
        34: '34',
        45: '45',
        55: '55',
        61.8: '61.8',
        80: '80',
        89: '89'
    }
    plt.xticks(xticks_loc, data.iloc[xticks_loc]['cal_date'], rotation=60)
    # ratio_lable = {
    #     1: '1',
    #     16.18: '16.18',
    #     32.36: '32.36',
    #     38.2: '38.2',
    #     50: '50',
    #     55: '55',
    #     61.8: '61.8',
    #     75: '75',
    #     80: '80',
    # }

    ax0.yaxis.set_ticks(list(ratio_lable.keys()))
    ax0.yaxis.set_ticklabels(list(ratio_lable.values()))
    ax0.tick_params(axis='y', labelcolor=color)

    color = 'tab:blue'
    ax0_1 = ax0.twinx()
    ax0_1.plot(index_daily['close'], color=color, label='index')
    ax0_1.plot(np.multiply(index_daily['close'], buy), 'r^', label='buy')
    ax0_1.plot(np.multiply(index_daily['close'], sell), 'g^', label='sell')
    ax0_1.tick_params(axis='y', labelcolor=color)

    color = 'tab:red'
    ax1 = ax[1]
    ax1.plot(cum_return_set, color=color, label='cum_return=' + str(cum_return))
    ax1.legend(loc=1, ncol=8)
    ax1.set_title('Cum Return', fontsize=12)
    ax1.set_ylabel('Return', fontsize=10)
    ax1.set_xlabel('Time', fontsize=10)
    ax1.grid()

    plt.legend()

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


