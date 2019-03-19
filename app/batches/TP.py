import numpy as np
from app.saver.logic import DB
import matplotlib.pylab as plt


def execute(start_date='', end_date=''):
    trade_cal = DB.get_open_cal_date(end_date=end_date, period=244*10)
    start_date_id = trade_cal.iloc[0]['date_id']
    end_date_id = trade_cal.iloc[-1]['date_id']

    index_daily = DB.get_index_daily(ts_code='000001.SH', start_date_id=start_date_id, end_date_id=end_date_id)
    index_daily.sort_values(by='cal_date', ascending=True, inplace=True)

    print(index_daily)
    y = index_daily['close']
    print(y)
    yf = np.fft.fft(y)/len(y)
    print(yf)
    z = abs(yf)
    print(np.rad2deg(np.angle(yf)))
    plt.plot(z[1:10])
    plt.show()


