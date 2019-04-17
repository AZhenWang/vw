import numpy as np
import numpy.fft as nf
import pandas as pd


class Tp(object):
    def __init__(self):
        """ PCA 预测模型
        """

        pass

    def run(self, y, fs=0.618, predict_len=3):
        """
        傅立叶分析周期规律
        :param y: 用来分析周期规律的一维数据
        :param fs: 筛选原周期幅度的多少来预测接下来的规律，0-1之间的值
        :param predict_len: 向后预测天数
        :return: 预测值，一维数组
        """
        x_len = len(y)
        # times = np.linspace(0, 2 * np.pi, x_len)
        period = 18*365
        fact_len = x_len / period * 2 * np.pi
        times = np.linspace(0, fact_len, x_len)
        x_axis = np.arange(x_len)
        # 频率序列，将数据按照这个序列展开
        unit = times[1] - times[0]
        freqs = nf.fftshift(nf.fftfreq(x_len, unit))
        fy = nf.fft(y)
        ffts = nf.fftshift(fy)

        ffts_pows = np.abs(ffts)
        iffts = nf.ifft(nf.ifftshift(ffts))
        pow = np.sort(pd.unique(ffts_pows[freqs > 0]))
        pow = pow[::-1]

        pow_cumsum = pow.cumsum()
        pow_limit = pow_cumsum[-1] * fs
        high_idx = np.argwhere(pow_cumsum <= pow_limit).flatten()
        pow_high_band = pow[high_idx]
        all_idx = np.arange(len(pow))
        low_idx = [i for i in all_idx if i not in high_idx]
        pow_low_band = pow[low_idx]

        shift_times = times[-1] + np.arange(predict_len) * unit
        # shift_times = np.append(times, np.arange(predict_len) * unit)
        s = [0] * len(shift_times)

        # pow_band = pow_low_band
        pow_band = pow
        # pow_band = pow[::-1]
        # print('pow_band=', pow_band)
        # pow_band = pow_high_band
        # if len(pow_band) < 1:
        #     return []
        #
        for i in range(len(pow_band)):
            fund_freq = np.abs(freqs[ffts_pows == pow_band[i]])[0]
            noised_indices = np.where(np.abs(freqs) != fund_freq)
            filter_ffts = ffts.copy()
            filter_ffts[noised_indices] = 0

            A = pow_band[i] * 2 / x_len
            F = fund_freq
            P = np.angle(ffts[freqs > 0][ffts_pows[freqs > 0] == pow_band[i]])
            print(A, F, P)
            shift_fx = A * np.cos(2 * np.pi * F * shift_times + P)
            s += shift_fx

        s = s + ffts_pows[freqs == 0] / x_len

        return s