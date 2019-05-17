import numpy.fft as nf
import numpy as np
import pandas as pd
from app.saver.logic import DB
import matplotlib.pylab as plt
from app.models.pca import Pca

# 提供汉字支持
plt.rcParams['font.sans-serif'] = ['Microsoft YaHei']

n_components = 2
pre_predict_interval = 5


def execute(start_date='', end_date=''):
    year_dis = 4
    per_year = 10
    period = year_dis * per_year
    trade_cal = DB.get_cal_date(end_date=end_date, limit=period)
    start_date_id = trade_cal.iloc[0]['date_id']
    start_date = trade_cal.iloc[0]['cal_date']
    end_date_id = trade_cal.iloc[-1]['date_id']
    end_date = trade_cal.iloc[-1]['cal_date']
    print('trade_cal=', trade_cal)
    print('start_date=', start_date)
    print('end_date=', end_date)
    # code_id = '2018'
    code_id = ''
    index_code = ''
    index_code = '000001.SH'
    # index_code = '399001.SZ'

    if index_code != '':
        daily = DB.get_index_daily(ts_code=index_code, start_date_id=start_date_id, end_date_id=end_date_id)
        daily_data = daily['close']
        daily_data = pack_daily_return(daily_data)
        daily_data.name = 'close'

    elif code_id != '':
        # daily = DB.get_code_info(code_id=code_id, start_date=start_date, end_date=end_date)
        # daily_data = daily['close'] * daily['adj_factor']
        # daily_data = pack_daily_return(daily_data)
        pca = Pca(cal_date=end_date)
        pca_features, prices, Y, _ = pca.run(code_id=code_id, pre_predict_interval=pre_predict_interval,
                                             n_components=n_components, return_y=True)
        daily_data = pca_features.set_index(Y.index)
    else:
        daily_data = pd.DataFrame()

    if daily_data.empty:
        return

    base = pd.DataFrame(trade_cal['date_id'],  columns=['date_id'])
    data = base.join(daily_data, on='date_id')
    data.fillna(method='ffill', inplace=True)
    data.dropna(inplace=True)
    data.reset_index(inplace=True)
    data = data[['col_0', 'col_1']]

    x_len = len(data)
    times = np.linspace(0, 2*np.pi, x_len)
    x_axis = np.arange(x_len)
    # 频率序列，将数据按照这个序列展开
    unit = times[1] - times[0]
    freqs = nf.fftshift(nf.fftfreq(x_len, unit))
    fy = nf.fft(data)
    ffts = nf.fftshift(fy)
    iffts = nf.ifft(nf.ifftshift(ffts))

    ffts_pows = np.abs(ffts)
    positive_pows = ffts_pows[freqs > 0]

    # 历史曲线
    z = [0] * x_len
    predict_time = 30
    # init_len = 0
    # init_len = x_len - 1
    init_len = 30
    shift_time = init_len + predict_time + 1
    shift_times = times[x_len - 1 - init_len] + np.arange(1, shift_time+1) * unit
    shift_x_axis = np.arange(shift_time)

    fig = plt.figure(figsize=(18, 14))
    ax0 = fig.add_subplot(311)
    ax0.plot(x_axis, iffts, label='iffts', alpha=0.5, linewidth=4)
    data.plot(ax=ax0)

    plt.grid()
    plt.title(str(code_id) + ' - ' + str(end_date))

    ax1 = fig.add_subplot(312)

    ax1.axhline(0, color='k')

    ax2 = fig.add_subplot(313)
    ax2.axvline(init_len-1, color='k', linewidth=1, alpha=0.5)
    ax2.axvline(init_len-1 + 30, color='y', linewidth=1, alpha=0.5)

    pow_bands = positive_pows
    for k in range(data.shape[1]):
        s = [0] * shift_time
        ftt_pow = ffts_pows[:, k]
        pow_band = pow_bands[:, k]
        positive_pow = positive_pows[:, k]
        fft = ffts[:, k]
        for i in range(len(pow_band)):
            fund_freq = np.abs(freqs[freqs>0][positive_pow == pow_band[i]])[0]

            A = pow_band[i] * 2 / x_len
            F = fund_freq
            P = np.angle(fft[freqs>0][positive_pow == pow_band[i]])[0]
            print('A=', A, 'F=', F, 'P=', P)
            shift_fx = A * np.cos(2 * np.pi * F * shift_times + P)
            s += shift_fx
        # s = s + ftt_pow[freqs == 0] / x_len

        ax2.plot(shift_x_axis, s, label='s'+str(k))

    next_trade_cal = DB.get_cal_date(start_date=end_date, limit=predict_time)
    if index_code != '':
        daily = DB.get_index_daily(ts_code=index_code, start_date_id=next_trade_cal.iloc[0]['date_id'],
                                   end_date_id=next_trade_cal.iloc[-1]['date_id'],)
        next_y = daily['close']
        next_y = pack_daily_return(next_y)
        next_y.name = 'close'
    elif code_id != '':
        pca = Pca(cal_date=next_trade_cal.iloc[-1]['cal_date'])
        pca_features, prices, Y, _ = pca.run(code_id=code_id, pre_predict_interval=pre_predict_interval,
                                             n_components=n_components, return_y=True)
        next_y = pca_features.set_index(Y.index)
        next_y = next_y[Y.index > next_trade_cal.iloc[0]['date_id']]
    else:
        return

    if not next_y.empty:
        next_data = pd.DataFrame(next_trade_cal['date_id'], columns=['date_id'])
        next_y = next_data.join(next_y, on='date_id')
        next_y.fillna(method='ffill', inplace=True)
        next_y.fillna(method='backfill', inplace=True)
        next_y = next_y[['col_0', 'col_1']]

        fact_next_y = data[-init_len:]
        fact_next_y = fact_next_y.append(next_y)
        fact_next_y.reset_index(inplace=True, drop=True)
        fact_next_y.plot(ax=ax2)
        ax2.grid()

    ax1.legend(loc=3)
    ax2.legend(loc=3)
    ax1.grid()
    plt.title('FTD')

    plt.show()


def pack_daily_return(close):
    # pre_close = close.shift()
    # diff = close - pre_close
    # fm = pre_close[diff >= 0].add(close[diff < 0], fill_value=0)
    # target = (diff / fm * 100).fillna(value=0)
    # target = np.log(close)
    target = close
    return target



