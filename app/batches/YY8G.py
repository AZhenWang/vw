from app.saver.logic import DB
from app.saver.tables import fields_map
import pandas as pd
import numpy as np
import app.common.function as FC
from app.saver.service.yy8g import YY8GSer
from mpl_finance import candlestick_ohlc
from matplotlib import dates as mdates
from matplotlib import ticker as mticker
from datetime import datetime as dt
import matplotlib.pylab as plt


def execute(start_date='', end_date=''):
    """
    能量子：
        人的信心和偏好可以聚集能量，也会因为恐惧担心等因素导致能量外泄，这种能量的聚集和外泄，导致股票表现上涨和下跌，
        1、能量聚集阶段，能量平稳增加，
        2、能量开始爆发阶段，能量的提升速度快速增加，
        3、能量调整阶段，能量减少速度远小于第2阶段增加的速度
        3、能量持续上升点，能量提升速度增加
        4、能量到顶阶段，能量提升速度变慢变平，
        5、能量外泄，正能量提升不足，原来的储能能量开始向负能量转换
        6、能量加速外泄

    以最近6个月的大中流入金额作为衡量能量的指标
    :param start_date:
    :param end_date:
    :return:
    """

    start_date = '19990101'
    end_date = end_date
    # end_date = '20101119'
    # start_date = '20180101'
    # end_date = end_date

    # code_ids = [2772]
    # code_ids = [378]
    # code_ids = [530]
    code_ids = range(1, 100)
    TTBS = ['monthly', 'weekly']
    # TTB = 'daily'
    # TTB = 'weekly'
    # TTB = 'monthly'
    # picture = True
    picture = False

    trade_cal = DB.get_open_cal_date(end_date=end_date, start_date=start_date)
    start_date_id = trade_cal.iloc[0]['date_id']
    end_date_id = trade_cal.iloc[-1]['date_id']
    #
    # codes = DB.get_latestopendays_code_list(
    #     latest_open_days=244, date_id=trade_cal.iloc[0]['date_id'])
    # code_ids = codes['code_id']
    # code_ids = [1949, 1895, 376]
    # code_ids = [2020, 1423,378, 530, 1895, 2087]
    # code_ids = [1988, 2422, 1979, 2020, 1423, 1949, 1895, 376, 378, 530]
    # code_ids = [6, 7]

    Gnames = {
        0: '坤',
        1: '艮',
        2: '坎',
        3: '巽',
        4: '震',
        5: '离',
        6: '兑',
        7: '乾',
    }
    new_rows = pd.DataFrame(columns=fields_map['YY8G'])
    for code_id in code_ids:
        print(code_id)
        for TTB in TTBS:

            dailys = DB.get_code_info(code_id=code_id, start_date=start_date, end_date=end_date, TTB=TTB)
            if TTB == 'monthly':
                pct_chg = dailys['pct_chg'] * 100
            elif TTB == 'weekly':
                pct_chg = dailys['pct_chg'] * 100
            else:
                pct_chg = dailys['pct_chg']

            mean_cost = 1
            # mean_cost = 0
            # if TTB == 'monthly':
            #     mean_cost = 1
            # elif TTB == 'weekly':
            #     mean_cost = 1/4

            maps = pd.Series(np.where(pct_chg >= mean_cost, 1, 0), index=pct_chg.index, name='maps')

            g_numbers = pd.Series(index=maps.index, name='g_number')
            g_type = '64'
            for i in range(5, len(maps)):
                date_id = maps.index[i]
                g_number = maps.iloc[i] * (2**5) + maps.iloc[i-1] * (2**4) + maps.iloc[i-2] * (2**3) + maps.iloc[i-3] * (2**2) + maps.iloc[i-4] * 2 + maps.iloc[i-5]

                # g_name = Gnames[g_number]
                g_numbers.loc[date_id] = g_number
                # g_names.loc[date_id] = g_name

            diff_g = pd.Series(np.diff(g_numbers), index=g_numbers.index[1:], name='diff_g')

            # 计算g_number峰谷形态
            peaks, bottoms = FC.get_peaks_bottoms(g_numbers)
            if len(peaks) < 2 or len(bottoms) < 2:
                qqbs = pd.Series(index=g_numbers.index, name='qqb')
            else:
                peaks, bottoms = FC.get_section_max(peaks, bottoms)
                qqbs = FC.qqbs(Y=g_numbers, peaks=peaks, bottoms=bottoms)
                qqbs.name = 'qqb'
                peaks.name = 'peak'
                bottoms.name = 'bottom'
            base = pd.DataFrame(index=dailys.index)
            wave = base.join(peaks)
            wave = wave.join(bottoms)
            wave = wave.join(qqbs)
            wave.fillna(method='ffill', inplace=True)

            data = pd.concat([g_numbers, diff_g, wave['qqb'], wave['peak'], wave['bottom']], axis=1).dropna()

            # print(pd.concat([dailys[['cal_date', 'close', 'pct_chg']], g_numbers, qqbs, peaks, bottoms], axis=1))

            if picture:
                fig = plt.figure(figsize=(20, 26))
                ax0 = fig.add_subplot(111, facecolor='#07000d')
                plt.title(str(code_id) + '-' + end_date)
                dailys['dateTime'] = mdates.date2num(
                    dailys['cal_date'].apply(lambda x: dt.strptime(x, '%Y%m%d')))

                dailys['close'] = dailys['close'] * dailys['adj_factor']
                dailys['open'] = dailys['open'] * dailys['adj_factor']
                dailys['high'] = dailys['high'] * dailys['adj_factor']
                dailys['low'] = dailys['low'] * dailys['adj_factor']
                dailys['pct_chg'] = pct_chg
                x_axis = dailys['dateTime']
                data_mat = dailys[['dateTime', 'open', 'high', 'low', 'close', 'pct_chg']]
                ax0.xaxis.set_major_locator(mticker.MaxNLocator(10))
                ax0.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
                candlestick_ohlc(ax0, data_mat.values, width=.7, colorup='y', colordown='c')

                ax1 = ax0.twinx()
                ax1.axhline(5, alpha=0.5)
                ax1.axhline(10, alpha=0.6)
                ax1.axhline(32, alpha=0.8)
                ax1.axhline(40, alpha=0.8)
                ax1.plot(x_axis, g_numbers, '-o')
                ax1.plot(x_axis, qqbs, '-o')

                ax0.legend()
                ax1.legend()
                plt.show()
                return

            data['code_id'] = code_id
            data['g_type'] = g_type
            data['TTB'] = TTB
            data.reset_index(inplace=True)
            new_rows = pd.concat([new_rows, data], sort=False)
            YY8GSer.delete_logs(code_id, start_date_id, end_date_id, TTB)

    if not new_rows.empty:
        new_rows.to_sql('YY8G', DB.engine, index=False, if_exists='append', chunksize=1000)
