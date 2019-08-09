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

    # start_date = '19990101'
    # end_date = end_date
    # start_date = '20180101'
    start_date = '20180801'
    end_date = '20190809'
    # end_date = '20190501'

    # code_ids = [2772]
    # code_ids = [2772, 521]
    # code_ids = [521]
    # code_ids = [8]
    # code_ids = range(100, 200)
    # TTBS = ['monthly', 'weekly']
    # TTBS = ['weekly']
    TTBS = ['monthly']
    # TTBS = ['daily']
    # picture = True
    picture = False

    trade_cal = DB.get_open_cal_date(end_date=end_date, start_date=start_date)
    start_date_id = trade_cal.iloc[0]['date_id']
    end_date_id = trade_cal.iloc[-1]['date_id']
    #
    codes = DB.get_latestopendays_code_list(
        latest_open_days=244, date_id=trade_cal.iloc[0]['date_id'])
    code_ids = codes['code_id']
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
            if dailys.shape[0] < 8:
                print('时间间隔太短！')
                return
            if TTB == 'monthly':
                pct_chg = dailys['pct_chg'] * 100
            elif TTB == 'weekly':
                pct_chg = dailys['pct_chg'] * 100
            else:
                pct_chg = dailys['pct_chg']
            # # mean_cost = 1
            mean_cost = 0
            # if TTB == 'monthly':
            #     mean_cost = 1
            # elif TTB == 'weekly':
            #     mean_cost = 1/4

            maps = pd.Series(np.where(pct_chg > mean_cost, 1, 0), index=pct_chg.index, name='maps')
            m_net1, m_net12, m_net34 = get_net(code_id=code_id, start_date_id=start_date_id, end_date_id=end_date_id, dates=pct_chg.index)
            g_numbers = pd.Series(index=dailys.index, name='g_number')
            g_type = '64'
            maps = m_net1
            g_type = '1'
            for i in range(5, len(maps)):
                date_id = maps.index[i]
                # g_number = maps.iloc[i] * (2**5) + maps.iloc[i-1] * (2**4) + maps.iloc[i-2] * (2**3) + maps.iloc[i-3] * (2**2) + maps.iloc[i-4] * 2 + maps.iloc[i-5]
                g_number = maps.iloc[i] + maps.iloc[i-1] /2 + maps.iloc[i-2] / 4 + maps.iloc[i-3] / 8 + maps.iloc[i-4] / 16 + maps.iloc[i-5] / 32

                g_numbers.loc[date_id] = g_number

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
            section = FC.get_wave_section(Y=g_numbers, peaks=peaks, bottoms=bottoms)
            section.name = 'section'
            base = pd.DataFrame(index=g_numbers.index)
            wave = base.join(peaks)
            wave = wave.join(bottoms)
            wave = wave.join(qqbs)
            wave = wave.join(section)
            wave.fillna(method='ffill', inplace=True)
            data = pd.concat([g_numbers, diff_g, wave['qqb'], wave['peak'], wave['bottom'], wave['section']], axis=1)
            data['g_type'] = g_type
            holdings = get_holdings(pct_chg, maps)
            m_net12 = m_net12*dailys['adj_factor']
            m_net1 = m_net1 * dailys['adj_factor']
            m_net34 = m_net34 * dailys['adj_factor']
            maps_cum = maps.rolling(window=6).sum()
            net12_cum = m_net12.rolling(window=6).sum()
            net1_mv = m_net1.rolling(window=6).sum()
            net12_mv = m_net12.rolling(window=6).sum()
            net2_mv = (m_net12-m_net1).rolling(window=6).sum()
            net34_mv = m_net34.rolling(window=6).sum()
            net1_cum = m_net1.cumsum()
            net2_cum = (m_net12 - m_net1).cumsum()
            net12_cum = m_net12.cumsum()
            net34_cum = m_net34.cumsum()
            g_numbers_cum = g_numbers.cumsum()
            g_numbers_cum.name = 'g_numbers_cum'
            print(pd.concat([dailys['cal_date'], maps_cum, g_numbers_cum, g_numbers, diff_g, wave['qqb'], wave['peak'], wave['bottom'], wave['section']], axis=1).tail(20))
            # os.ex
            if picture:
                fig = plt.figure(figsize=(20, 26))
                ax0 = fig.add_subplot(211, facecolor='#07000d')
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
                # plt.show()
                # return
                # ax1.plot(x_axis, qqbs, '-o')
                #
                # for k in range(len(holdings)):
                #     if holdings[k] > 0:
                #         ax0.text(x_axis.iloc[k], dailys.iloc[k]['low'], holdings[k], ha='center', va='top',
                #                  fontsize=9, color='red')
                #     elif holdings[k] < 0:
                #         ax0.text(x_axis.iloc[k], dailys.iloc[k]['low'], np.abs(holdings[k]), ha='center', va='top',
                #                  fontsize=9, color='m')

                ax2 = fig.add_subplot(212)

                # ax1.plot(x_axis, maps_cum, color='y', label='maps_cum')
                ax1.plot(x_axis, net12_cum, color='c', label='net12_cum')
                ax1.plot(x_axis, net1_cum, color='m', label='net1_cum')
                ax1.axhline(0, alpha=0.5)
                ax3 = ax2.twinx()
                ax2.xaxis.set_major_locator(mticker.MaxNLocator(10))
                ax2.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
                ax2.plot(x_axis, net1_mv, label='net1_mv')
                ax2.plot(x_axis, net12_mv, label='net12_mv')
                ax2.plot(x_axis, net34_mv, label='net34_mv')
                ax2.plot(x_axis, net1_cum, label='net1_cum')
                ax2.plot(x_axis, net12_cum, label='net12_cum')
                ax2.plot(x_axis, net34_cum, label='net34_cum')
                # ax2.plot(x_axis, maps_cum, color='y', label='maps_cum')
                ax3.plot(x_axis, g_numbers_cum, color='k', label='g_numbers_cum')
                ax0.legend(loc=2)
                ax1.legend(loc=3)
                ax2.legend(loc=2)
                ax3.legend(loc=3)
                plt.show()
                return

            data['code_id'] = code_id
            data['g_type'] = g_type
            data['TTB'] = TTB
            data.reset_index(inplace=True)
            new_rows = pd.concat([new_rows, data], sort=False)
            YY8GSer.delete_logs(code_id, start_date_id, end_date_id, TTB, g_type)

    if not new_rows.empty:
        new_rows.to_sql('YY8G', DB.engine, index=False, if_exists='append', chunksize=1000)


def get_holdings(pct_chg, maps):
    holdings = [0]
    for i in range(1, len(pct_chg)):
        if pct_chg.iloc[i] > 0 and pct_chg.iloc[i-1] > 0 and maps.iloc[i] < maps.iloc[i-1] < maps.iloc[i-2]:
            flag = -1
        elif pct_chg.iloc[i] < 0 and pct_chg.iloc[i-1] < 0 and maps.iloc[i] > maps.iloc[i-1] > maps.iloc[i-2]:
            flag = 1
        else:
            flag = 0
        holdings.append(flag)
    return holdings


def get_net(code_id, start_date_id, end_date_id, dates):
    window = 60
    flow = DB.get_moneyflows(code_id=code_id, end_date_id=end_date_id, start_date_id=start_date_id)
    # flow = data[
    #     ['cal_date', 'open', 'close', 'high', 'low', 'pct_chg', 'adj_factor', 'float_share', 'turnover_rate_f']]
    # flow_mean = data[
    #     ['net_mf_vol', 'sell_elg_vol', 'buy_elg_vol', 'sell_lg_vol', 'buy_lg_vol', 'sell_md_vol', 'buy_md_vol',
    #      'sell_sm_vol', 'buy_sm_vol']].rolling(window=window).mean()
    #
    # flow = flow.join(flow_mean)
    # flow.dropna(inplace=True)
    # if flow.empty:
    #     return

    flow['close'] = flow['close'] * flow['adj_factor']
    flow['open'] = flow['open'] * flow['adj_factor']
    flow['high'] = flow['high'] * flow['adj_factor']
    flow['low'] = flow['low'] * flow['adj_factor']

    net_elg = (flow['buy_elg_vol'] - flow['sell_elg_vol']) * 100 / flow['float_share']
    net_elg.name = 'net_elg'
    net_sm = (flow['buy_sm_vol'] - flow['sell_sm_vol']) * 100 / flow['float_share']
    net_sm.name = 'net_sm'
    net_md = (flow['buy_md_vol'] - flow['sell_md_vol']) * 100 / flow['float_share']
    net_md.name = 'net_md'
    net_lg = (flow['buy_lg_vol'] - flow['sell_lg_vol']) * 100 / flow['float_share']
    net_lg.name = 'net_lg'

    net1 = net_elg
    net12 = net_elg + net_lg
    net34 = net_sm + net_md

    m_net1 = pd.Series(index=dates, name='m_net1')
    m_net12 = pd.Series(index=dates, name='m_net12')
    m_net34 = pd.Series(index=dates, name='m_net34')
    for i in range(1, len(dates)):
        date_id = dates[i]
        pre_date_id = dates[i-1]
        m_net1.loc[date_id] = net1.loc[pre_date_id+1:date_id].sum()
        m_net12.loc[date_id] = net12.loc[pre_date_id+1:date_id].sum()
        m_net34.loc[date_id] = net34.loc[pre_date_id+1:date_id].sum()

    # pct_chg = mon_net12.diff()
    # maps = pd.Series(np.where(pct_chg > 0, 1, 0), index=pct_chg.index, name='maps')

    return m_net1, m_net12, m_net34
