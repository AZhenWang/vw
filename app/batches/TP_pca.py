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
    year_dis = 5
    per_year = 365
    period = year_dis * per_year
    trade_cal = DB.get_cal_date(end_date=end_date, limit=period)
    start_date_id = trade_cal.iloc[0]['date_id']
    start_date = trade_cal.iloc[0]['cal_date']
    end_date_id = trade_cal.iloc[-1]['date_id']
    end_date = trade_cal.iloc[-1]['cal_date']
    # TTB = 'daily'
    TTB = 'weekly'
    # TTB = 'monthly'
    code_id = '1481'
    # code_id = ''
    index_code = ''
    # index_code = '000001.SH'
    # index_code = '399001.SZ'
    predict_time = 30 * 3
    # init_len = 0
    # init_len = x_len - 1
    init_len = 30*2

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
                                             n_components=n_components, return_y=True, TTB=TTB)
        daily_data = pca_features.set_index(Y.index).col_0

        daily_data.name = 'close'
    else:
        daily_data = pd.DataFrame()

    if daily_data.empty:
        return

    data = pd.DataFrame(trade_cal['date_id'],  columns=['date_id'])
    data = data.join(daily_data, on='date_id')
    data.fillna(method='ffill', inplace=True)
    data.dropna(inplace=True)
    y = data['close']

    x_len = len(y)
    # times = np.linspace(0, 2*np.pi, x_len)
    fact_len = x_len / period * 2 * np.pi
    times = np.linspace(0, fact_len, x_len)
    x_axis = np.arange(x_len)
    # 频率序列，将数据按照这个序列展开
    unit = times[1] - times[0]
    freqs = nf.fftshift(nf.fftfreq(x_len, unit))
    fy = nf.fft(y)
    ffts = nf.fftshift(fy)
    iffts = nf.ifft(nf.ifftshift(ffts))

    fig = plt.figure(figsize=(18, 14))
    ax0 = fig.add_subplot(311)
    ax0.plot(x_axis, y, label='y')
    ax0.plot(x_axis, iffts, label='iffts', alpha=0.5, linewidth=6)
    plt.grid()
    plt.title(str(code_id) + ' - ' + str(end_date))

    ax1 = fig.add_subplot(312)
    ax1.axhline(0, color='k')

    ffts_pows = np.abs(ffts)
    #  选择几个频率
    ts = 4
    pow = np.sort(pd.unique(ffts_pows[freqs > 0]))
    pow = pow[::-1]
    pow_low_band = pow[ts:]
    pow_high_band = pow[:ts]

    # 取幅值占比fs的那些频率
    fs = 0.618
    pow_cumsum = pow.cumsum()
    pow_limit = pow_cumsum[-1] * fs
    high_idx = np.argwhere(pow_cumsum <= pow_limit).flatten()
    pow_high_band = pow[high_idx]
    all_idx = np.arange(len(pow))
    low_idx = [i for i in all_idx if i not in high_idx]
    pow_low_band = pow[low_idx]

    # 过滤噪声
    mixed_fund_freq = np.abs(freqs[ffts_pows < min(pow_high_band)])
    mixed_noised_indices = pd.Series(np.abs(freqs)).isin(mixed_fund_freq)
    mixed_filter_ffts = ffts.copy()
    mixed_filter_ffts[mixed_noised_indices] = 0
    mixed_filter_sigs = nf.ifft(nf.ifftshift(mixed_filter_ffts))
    ax0_1 = ax0.twinx()
    ax0_1.plot(x_axis, mixed_filter_sigs, label='c', color='k', linewidth=1)

    ax2 = fig.add_subplot(313)
    # 历史曲线
    z = [0] * x_len
    pow_band = pow_high_band

    shift_time = init_len + predict_time + 1
    shift_times = times[x_len - 1 - init_len] + np.arange(1, shift_time+1) * unit
    # shift_times = times[init_len - x_len] + np.arange(shift_time) * unit
    shift_x_axis = np.arange(shift_time)
    # 测试曲线
    s = [0] * shift_time

    for i in range(4):
        fund_freq = np.abs(freqs[ffts_pows == pow_band[i]])[0]
        noised_indices = np.where(np.abs(freqs) != fund_freq)
        filter_ffts = ffts.copy()
        filter_ffts[noised_indices] = 0
        filter_sigs = nf.ifft(nf.ifftshift(filter_ffts)).real
        sub_time_period = year_dis / (fund_freq * 2 * np.pi)

        A = pow_band[i] * 2 / x_len
        F = fund_freq
        P = np.angle(ffts[freqs>0][ffts_pows[freqs>0] == pow_band[i]])
        fx0 = A * np.cos(2 * np.pi * F * times + P)
        z += fx0
        shift_fx = A * np.cos(2 * np.pi * F * shift_times + P)
        s += shift_fx
        ax1.plot(x_axis, fx0, label='f'+str(round(sub_time_period, 1)), linewidth=i+1, alpha=0.8)
        ax1.axhline(A, color='g')
        # ax1.plot(x_axis, filter_sigs, label='c'+str(round(sub_time_period, 1)))
    s = s + ffts_pows[freqs == 0] / x_len
    ax0_1.plot(x_axis, z+ffts_pows[freqs == 0]/x_len, label='z', color='r', linewidth=4, alpha=0.5)
    ax1.plot(x_axis, z, label='z')

    ax2.axvline(init_len-1, color='k', linewidth=1, alpha=0.5)
    ax2.axhline(0, color='k')
    ax2.axvline(init_len-1 + 30, color='y', linewidth=1, alpha=0.5)

    pow_band = pow
    s1 = [0] * shift_time

    for i in range(len(pow_band)):
        fund_freq = np.abs(freqs[ffts_pows == pow_band[i]])[0]
        noised_indices = np.where(np.abs(freqs) != fund_freq)
        filter_ffts = ffts.copy()
        filter_ffts[noised_indices] = 0
        filter_sigs = nf.ifft(nf.ifftshift(filter_ffts)).real
        sub_time_period = year_dis / (fund_freq * 2 * np.pi)

        A = pow_band[i] * 2 / x_len
        F = fund_freq
        P = np.angle(ffts[freqs>0][ffts_pows[freqs>0] == pow_band[i]])
        shift_fx = A * np.cos(2 * np.pi * F * shift_times + P)

        s1 += shift_fx
    s1 = s1 + ffts_pows[freqs == 0] / x_len
    ax2.plot(shift_x_axis, s1, linewidth=1, color='r', label='s1', alpha=0.5)

    predict_pca_0 = s1[init_len:init_len+30]

    today_pca = s1[init_len-1]
    tomorrow_pca = s1[init_len]
    pca_diffs = (predict_pca_0 - today_pca) * 100 / abs(today_pca)
    pca_mean = np.mean(predict_pca_0)
    pca_std = np.std(predict_pca_0)
    pca_diff_mean = np.mean(pca_diffs)
    pca_diff_std = np.std(pca_diffs)
    pca_diff = tomorrow_pca - today_pca
    print('pca_mean=', pca_mean, 'pca_std', pca_std)
    print('pca_diff=', pca_diff, 'pca_diff_mean=', pca_diff_mean, 'pca_diff_std=', pca_diff_std)
    print('s1_max=', np.max(predict_pca_0))
    print('s1_median=', np.median(predict_pca_0))

    next_trade_cal = DB.get_cal_date(start_date=end_date, limit=predict_time)
    if index_code != '':
        daily = DB.get_index_daily(ts_code=index_code, start_date_id=next_trade_cal.iloc[0]['date_id'],
                                   end_date_id=next_trade_cal.iloc[-1]['date_id'],)
        next_y = daily['close']
        next_y = pack_daily_return(next_y)
        next_y.name = 'close'
    elif code_id != '':
        # daily = DB.get_code_info(code_id=code_id, start_date=next_trade_cal.iloc[0]['cal_date'],
        #                               end_date=next_trade_cal.iloc[-1]['cal_date'])
        # next_y = daily['close'] * daily['adj_factor']
        # next_y = pack_daily_return(next_y)
        pca = Pca(cal_date=next_trade_cal.iloc[-1]['cal_date'])
        pca_features, prices, Y, _ = pca.run(code_id=code_id, pre_predict_interval=pre_predict_interval,
                                             n_components=n_components, return_y=True, TTB=TTB)
        next_y = pca_features.set_index(Y.index).col_0
        next_y = next_y[Y.index > next_trade_cal.iloc[0]['date_id']]
        next_y.name = 'close'
    else:
        return

    if not next_y.empty:
        next_data = pd.DataFrame(next_trade_cal['date_id'], columns=['date_id'])
        next_y = next_data.join(next_y, on='date_id')
        next_y.fillna(method='ffill', inplace=True)
        next_y.fillna(method='backfill', inplace=True)
        fact_next_y = y[-init_len:]
        fact_next_y = fact_next_y.append(next_y['close'])
        # ax2_1 = ax2.twinx()
        # ax2_1.plot(shift_x_axis, fact_next_y, linewidth=1)
        # ax2_1.plot(range(len(fact_next_y)), fact_next_y, linewidth=1)
        # ax2_1.set_ylabel('Fact price')
        # ax2_1.legend(loc=3)
        ax2.plot(range(len(fact_next_y)), fact_next_y, linewidth=1)
        ax2.grid()
    ax2.plot(shift_x_axis, s, linewidth=2, color='r', label='s', alpha=0.5)
        # print(freqs * shift_time * 2 * np.pi)
        # alpha_filter_ffts = []
        # for i in range(len(filter_ffts)):
        #     shift_alpha = complex(real=0, imag=shift_time * 2 * np.pi / period)
        #     alpha_filter_ffts.append(filter_ffts[i] + shift_alpha)
        # alpha_filter_sigs = nf.ifft(alpha_filter_ffts).real
        # plt.plot(x_axis, alpha_filter_sigs, label='cc' + str(round(sub_time_period, 1)))


    # fund_freq_1 = np.abs(freqs[ffts_pows == b1])[0]
    # print('fund_freq_1=', fund_freq_1)
    # noised_indices_1 = np.where(np.abs(freqs) != fund_freq_1)
    # filter_ffts_1 = ffts.copy()
    # filter_ffts_1[noised_indices_1] = 0
    # filter_ffts_1_pow = np.abs(filter_ffts_1)
    # filter_sigs_1 = nf.ifft(filter_ffts_1).real
    #
    # fund_freq_2 = np.abs(freqs[ffts_pows == b2])[0]
    # print('fund_freq_2=', fund_freq_2)
    # noised_indices_2 = np.where(np.abs(freqs) != fund_freq_2)
    # filter_ffts_2 = ffts.copy()
    # filter_ffts_2[noised_indices_2] = 0
    # filter_ffts_2_pow = np.abs(filter_ffts_2)
    # filter_sigs_2 = nf.ifft(filter_ffts_2).real
    #
    # plt.plot(x_axis, filter_sigs, label='c')
    # plt.plot(x_axis, filter_sigs_1, label='f'+str(round(fund_freq_1, 2)))
    # plt.plot(x_axis, filter_sigs_2, label='f'+str(round(fund_freq_2, 2)))
    # plt.plot(x_axis, filter_sigs_1+filter_sigs_2, label='f和')
    # s1_amp = filter_sigs_1[np.abs(filter_sigs_1).argmax()]
    # s2_amp = filter_sigs_2[np.abs(filter_sigs_2).argmax()]
    # plt.axhline(s1_amp, color='y', alpha=0.5, label='a1')
    # plt.axhline(-s1_amp, color='y', alpha=0.5)
    # plt.axhline(s2_amp, color='g', alpha=0.5, label='a2')
    # plt.axhline(-s2_amp, color='g', alpha=0.5)

    ax1.legend(loc=3)
    ax1.grid()
    # ax0.legend(loc=3)
    ax0_1.legend(loc=3)
    plt.title('FTD')

    # fig.add_subplot(313)
    # shift_time = 60
    # alpha_filter_ffts = ffts.copy()
    # alpha_filter_ffts[mixed_noised_indices] = 0
    # alpha_filter_ffts = alpha_filter_ffts + freqs * shift_time * 2 * np.pi
    # alpha_filter_sigs = nf.ifft(alpha_filter_ffts)
    # plt.plot(x_axis, alpha_filter_sigs, label='cc')


    #
    # fig.add_subplot(313)
    # plt.plot(freqs[freqs >= 0], filter_ffts_1_pow[freqs >= 0], label='f'+str(round(fund_freq_1, 2)), alpha=0.2, linewidth=6)
    # plt.plot(freqs[freqs >= 0], filter_ffts_2_pow[freqs >= 0], label='f'+str(round(fund_freq_2, 2)))
    # plt.legend()
    # plt.title('FFD')

    plt.show()


def pack_daily_return(close):
    # pre_close = close.shift()
    # diff = close - pre_close
    # fm = pre_close[diff >= 0].add(close[diff < 0], fill_value=0)
    # target = (diff / fm * 100).fillna(value=0)
    # target = np.log(close)
    target = close
    return target



