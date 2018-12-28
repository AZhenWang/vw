from app.saver.logic import DB
from app.common.function import send_email
from email.mime.text import MIMEText
from email.mime.image import MIMEImage
from matplotlib import pyplot as plt
import numpy as np


def execute(start_date='', end_date=''):
    trade_cal = DB.get_open_cal_date(end_date=end_date, period=400)
    data = DB.count_threshold_group_by_date_id(start_date_id=trade_cal.iloc[0]['date_id'], end_date_id=trade_cal.iloc[-2]['date_id'])
    data.eval('up_stock_ratio=up_stock_number/list_stock_number*100', inplace=True)

    text = data[['cal_date', 'up_stock_ratio']].to_string(index=False)
    msgs = []
    msgs.append(MIMEText(text, 'plain', 'utf-8'))
    data.sort_values(by='cal_date', ascending=True, inplace=True)
    data.reset_index(inplace=True)
    fig = plt.figure(figsize=(10, 8))
    plt.plot(data['up_stock_ratio'])
    plt.grid()
    plt.title('Up-stock percentage by threshold')
    plt.xlabel('Time')
    plt.ylabel('Up-stock ratio')

    max_loc = len(data)-1
    xticks_loc = [round(i / 7 * max_loc) for i in np.arange(0, 8)]
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

    plt.yticks(list(ratio_lable.keys()), list(ratio_lable.values()))
    pos = plt.ginput(3)
    print(pos)

    fig.savefig('threshold_picture.png')
    image_msg = MIMEImage(open('threshold_picture.png', 'rb').read())
    image_msg.add_header('Content-Disposition', 'attachment', filename='threshold_picture.png')
    msgs.append(image_msg)

    send_email(subject=end_date+'的thresholds统计数据', msgs=msgs)
