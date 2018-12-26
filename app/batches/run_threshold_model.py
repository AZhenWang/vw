from app.saver.logic import DB
from app.common.function import send_email
from email.mime.text import MIMEText
from email.mime.image import MIMEImage
from matplotlib import pyplot as plt
from datetime import date


def execute(start_date='', end_date=''):
    trade_cal = DB.get_open_cal_date(end_date=end_date, period=21)

    data = DB.count_threshold_group_by_date_id(start_date_id=trade_cal.iloc[0]['date_id'], end_date_id=trade_cal.iloc[-1]['date_id'])
    text = data.to_string(index=False)
    msgs = []
    msgs.append(MIMEText(text, 'plain', 'utf-8'))
    fig = plt.figure(figsize=(10, 8))
    plt.plot(data['cal_date'].str[-4:], data['up_counts'])
    plt.grid()
    plt.title('Up-stock statistics by threshold')
    plt.xlabel('Time')
    plt.ylabel('Up-stock Number')
    fig.savefig('threshold_picture.png')
    image_msg = MIMEImage(open('threshold_picture.png', 'rb').read())
    image_msg.add_header('Content-Disposition', 'attachment', filename='threshold_picture.png')
    msgs.append(image_msg)

    send_email(subject='近20个交易日的thresholds统计数据', msgs=msgs)
