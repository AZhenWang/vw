import numpy.fft as nf
import numpy as np
import pandas as pd
from app.saver.logic import DB
import matplotlib.pylab as plt
# 提供汉字支持
plt.rcParams['font.sans-serif'] = ['Microsoft YaHei']

def execute(start_date='', end_date=''):
    period = 244*10
    trade_cal = DB.get_open_cal_date(end_date=end_date, period=period)
    start_date_id = trade_cal.iloc[0]['date_id']
    end_date_id = trade_cal.iloc[-1]['date_id']

    index_daily = DB.get_index_daily(ts_code='000001.SH', start_date_id=start_date_id, end_date_id=end_date_id)
    index_daily.sort_values(by='cal_date', ascending=True, inplace=True)

    y = index_daily['close']
    times = np.linspace(0, 2 * np.pi, period)
    x_axis = np.arange(period)
    # 频率序列，将数据按照这个序列展开
    freqs = nf.fftfreq(period, times[1]-times[0])

    ffts = nf.fft(y)
    iffts = nf.ifft(ffts)
    pows = np.abs(ffts)

    fig = plt.figure(figsize=(20, 18))
    plt.subplot(211)
    plt.plot(x_axis, y, label='y')
    plt.plot(x_axis, iffts, label='iffts', alpha=0.5, linewidth=6)
    plt.grid()
    plt.title('TD')

    ffts_pows = np.abs(ffts)
    #  选择几个频率
    ts = 5
    pow_band = np.sort(pd.unique(ffts_pows[freqs >= 0][1:]))[-ts:]
    b1 = pow_band[0]
    b2 = pow_band[1]
    print('pow_band', pow_band)
    # 过滤噪声
    fund_freq = np.abs(freqs[ffts_pows < min(pow_band)])
    noised_indices = np.where(pd.Index(pd.unique(np.abs(freqs))).get_indexer(fund_freq) >= 0)
    filter_ffts = ffts.copy()
    filter_ffts[noised_indices] = 0
    filter_pows = np.abs(filter_ffts)
    filter_sigs = nf.ifft(filter_ffts)
    print('filter_pows', filter_pows)
    print('filter_sigs', filter_sigs)
    print('filter_sigs_real', filter_sigs.real)
    print('filter_sigs_pows', np.abs(filter_sigs))
    os.exit()

    fund_freq_1 = np.abs(freqs[ffts_pows == b1])[0]
    print('fund_freq_1=', fund_freq_1)
    noised_indices_1 = np.where(np.abs(freqs) != fund_freq_1)
    filter_ffts_1 = ffts.copy()
    filter_ffts_1[noised_indices_1] = 0
    filter_ffts_1_pow = np.abs(filter_ffts_1)
    filter_sigs_1 = nf.ifft(filter_ffts_1).real

    fund_freq_2 = np.abs(freqs[ffts_pows == b2])[0]
    print('fund_freq_2=', fund_freq_2)
    noised_indices_2 = np.where(np.abs(freqs) != fund_freq_2)
    filter_ffts_2 = ffts.copy()
    filter_ffts_2[noised_indices_2] = 0
    filter_ffts_2_pow = np.abs(filter_ffts_2)
    filter_sigs_2 = nf.ifft(filter_ffts_2).real

    plt.subplot(212)
    plt.plot(x_axis, filter_sigs, label='c')
    plt.plot(x_axis, filter_sigs_1, label='f'+str(round(fund_freq_1, 2)))
    plt.plot(x_axis, filter_sigs_2, label='f'+str(round(fund_freq_2, 2)))
    s1_amp = filter_sigs_1[np.abs(filter_sigs_1).argmax()]
    s2_amp = filter_sigs_2[np.abs(filter_sigs_2).argmax()]
    plt.axhline(s1_amp, color='y', alpha=0.5, label='a1')
    plt.axhline(-s1_amp, color='y', alpha=0.5)
    plt.axhline(s2_amp, color='g', alpha=0.5, label='a2')
    plt.axhline(-s2_amp, color='g', alpha=0.5)
    plt.legend(loc=3)
    plt.grid()
    plt.title('FTD')
    #
    # plt.subplot(313)
    # plt.plot(freqs[freqs >= 0], filter_ffts_1_pow[freqs >= 0], label='f'+str(round(fund_freq_1, 2)), alpha=0.2, linewidth=6)
    # plt.plot(freqs[freqs >= 0], filter_ffts_2_pow[freqs >= 0], label='f'+str(round(fund_freq_2, 2)))
    # plt.legend()
    # plt.title('FFD')

    plt.tight_layout()
    plt.show()


