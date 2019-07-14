from app.saver.logic import DB
from app.saver.tables import fields_map
import numpy as np
import matplotlib.pylab as plt
import pandas as pd
from mpl_finance import candlestick_ohlc
from matplotlib import dates as mdates
from matplotlib import ticker as mticker
from datetime import datetime as dt
from app.common.function import remove_noise
import matplotlib


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

    period = 20*6
    window = 20*6

    trade_cal = DB.get_open_cal_date(end_date=end_date, period=period+window)
    start_date_id = trade_cal.iloc[0]['date_id']
    end_date_id = trade_cal.iloc[-1]['date_id']
    start_date = trade_cal.iloc[0]['cal_date']

    code_id = 2772
    # code_id = 2020
    # code_id = 1423
    # code_id = 2115
    # code_id = 408
    # code_id = 1988
    # code_id = 1895
    # code_id = 2975
    # code_id = 378
    # code_id = 1501
    #2011:广和通， 1307, 1895:久之洋, 2115:金力永磁， 1949：兴齐眼药， 1389：莱美药业， 2087：华信新材， 530：瑞泰科技， 378：欢瑞世纪
    flow = DB.get_moneyflows(code_id=code_id, end_date_id=end_date_id, start_date_id=start_date_id)

    dailys = flow[['cal_date', 'open', 'close', 'high', 'low', 'pct_chg', 'adj_factor', 'float_share', 'turnover_rate_f']]
    flow_mean = flow[
        ['net_mf_vol', 'sell_elg_vol', 'buy_elg_vol', 'sell_lg_vol', 'buy_lg_vol', 'sell_md_vol', 'buy_md_vol',
         'sell_sm_vol', 'buy_sm_vol']].rolling(window=window).mean()

    dailys = dailys.join(flow_mean)
    dailys.dropna(inplace=True)
    if dailys.empty:
        return

    dailys['dateTime'] = mdates.date2num(
        dailys['cal_date'].apply(lambda x: dt.strptime(x, '%Y%m%d')))

    dailys['close'] = dailys['close'] * dailys['adj_factor']
    dailys['open'] = dailys['open'] * dailys['adj_factor']
    dailys['high'] = dailys['high'] * dailys['adj_factor']
    dailys['low'] = dailys['low'] * dailys['adj_factor']
    fig = plt.figure(figsize=(20, 26))
    ax0 = fig.add_subplot(311, facecolor='#07000d')
    plt.title(str(code_id) + '-' + end_date)
    x_axis = dailys['dateTime']
    data_mat = dailys[['dateTime', 'open', 'high', 'low', 'close', 'pct_chg']]
    ax0.xaxis.set_major_locator(mticker.MaxNLocator(10))
    ax0.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
    candlestick_ohlc(ax0, data_mat.values, width=.7, colorup='y', colordown='c')
    # ax0.axhline(max(dailys['high']) - max(dailys['close']) * 0.2)

    # 固定支撑压力线

    level_1 = dailys['high'].max() - dailys['close'][dailys['high'].argmax] * 0.02
    level_2 = dailys['high'].max() - dailys['close'][dailys['high'].argmax] * 0.21
    level_3 = dailys['high'].max() - dailys['close'][dailys['high'].argmax] * 0.34
    level_4 = dailys['high'].max() - dailys['close'][dailys['high'].argmax] * 0.55
    last_adj_factor = dailys.iloc[-1]['adj_factor']

    ax0.axhline(level_1, alpha=0.5, label=str(round(level_1/last_adj_factor, 2)))
    ax0.axhline(level_2, alpha=0.5, label=str(round(level_2/last_adj_factor, 2)))
    ax0.axhline(level_3, alpha=0.5, label=str(round(level_2 / last_adj_factor, 2)))
    ax0.axhline(level_4, alpha=0.5, label=str(round(level_2 / last_adj_factor, 2)))

    # 移动支撑压力线
    high_max = dailys['high'].rolling(window=20*3).max()
    price_max = high_max
    adj_l1 = high_max - price_max * 0.08
    adj_l2 = high_max - price_max * 0.2

    l1 = adj_l1 / dailys['adj_factor']
    l2 = adj_l2 / dailys['adj_factor']
    l1.name = 'l1'
    l2.name = 'l2'

    from_l1 = round((dailys['close'] - adj_l1) * 100 / adj_l1, 2)
    from_l1.name = 'from_l1'

    from_l2 = round((dailys['close'] - adj_l2) * 100 / adj_l2, 2)
    from_l2.name = 'from_l2'

    base = pd.DataFrame(index=dailys.index)
    support = adj_l1[from_l1 > 0].add(adj_l2[(from_l2 > 0) & (from_l1 < 0)], fill_value=0)
    pressure = adj_l2[from_l2 < 0].add(adj_l1[(from_l2 > 0) & (from_l1 < 0)], fill_value=0)

    pressure.name = 'pressure'
    support.name = 'support'
    line = base.join(support)
    line = line.join(pressure)
    ax0.plot(x_axis, line['pressure'], 'r-')
    ax0.plot(x_axis, line['support'], 'm-')

    real_pressure = pressure / dailys['adj_factor']
    real_support = support / dailys['adj_factor']
    real_pressure.name = 'real_pressure'
    real_support.name = 'real_support'
    real_high = dailys['high']/dailys['adj_factor']
    real_high.name = 'real_high'
    base = base.join(dailys['cal_date'])
    base = base.join(real_high)
    base = base.join(l1)
    re_line = base.join(l2)
    re_line = re_line.join(real_support)
    re_line = re_line.join(real_pressure)

    ax1 = ax0.twinx()
    ax1.axhline(0 , alpha=0.2)
    net_elg = (dailys['buy_elg_vol'] - dailys['sell_elg_vol']) * 100 /dailys['float_share']
    net_elg.name = 'net_elg'
    net_sm = (dailys['buy_sm_vol'] - dailys['sell_sm_vol']) * 100 /dailys['float_share']
    net_md = (dailys['buy_md_vol'] - dailys['sell_md_vol']) * 100/dailys['float_share']
    net_lg = (dailys['buy_lg_vol'] - dailys['sell_lg_vol']) * 100/dailys['float_share']
    net_lg_elg = net_elg + net_lg

    #
    buy_elg = dailys['buy_elg_vol'] / dailys['float_share']
    buy_elg.name = 'buy_elg'
    sell_elg = dailys['sell_elg_vol'] / dailys['float_share']
    sell_elg.name = 'sell_elg'


    ax1.plot(x_axis, buy_elg, color='#8B1A1A')
    ax1.plot(x_axis, sell_elg, color='#6A5ACD')
    #
    ax1.plot(x_axis, (dailys['net_mf_vol'])/dailys['float_share'], color='c', label='net_mf')
    #
    # ax1.plot(x_axis, net_lg_elg, label='net_lg_elg', color='m')

    # ax1.plot(x_axis, net_elg, label='net_elg', color='red')
    # ax1.fill_between(x_axis, net_elg, 0, net_elg > 0, facecolor='red', alpha=0.8)
    # ax1.plot(x_axis, net_lg, label='net_lg', color='pink')
    # # ax1.fill_between(x_axis, net_lg, 0, net_lg > 0, facecolor='pink', alpha=0.6)
    # ax1.plot(x_axis, net_md, label='net_md', color='green')
    # # ax1.fill_between(x_axis, net_md, 0, net_md > 0, facecolor='green', alpha=0.5)
    # ax1.plot(x_axis, net_sm, label='net_sm', color='blue')
    # ax1.fill_between(x_axis, net_sm, 0, net_sm > 0, facecolor='blue', alpha=0.3)


    ax4 = fig.add_subplot(312)
    ax5 = ax4.twinx()
    ax4.xaxis.set_major_locator(mticker.MaxNLocator(10))
    ax4.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
    candlestick_ohlc(ax4, data_mat.values, width=.7, colorup='y', colordown='c')

    # elg_base_diff = (net_elg - net_elg.shift()) * 100
    # mv_elg_base_diff10 = elg_base_diff.rolling(window=10).mean()
    # mv_elg_base_diff10.name = 'mv_elg_base_diff10'
    # mv_elg_base_diff5 = elg_base_diff.rolling(window=5).mean()
    # mv_elg_base_diff5.name = 'mv_elg_base_diff5'
    # ax5.plot(x_axis, elg_base_diff, label='elg_base_diff')
    # ax5.plot(x_axis, mv_elg_base_diff5, label='mv_elg_base_diff5')
    # ax5.plot(x_axis, mv_elg_base_diff10, label='mv_elg_base_diff10')

    # 求 beta_net_elg
    beta_net_1 = pd.Series(index=net_lg.index)
    beta_net_1.name = 'beta_net_1'
    beta_net_2 = pd.Series(index=net_lg.index)
    beta_net_2.name = 'beta_net_2'
    beta = 0.9
    beta_net_1.iloc[0] = net_elg.iloc[0]
    beta_net_2.iloc[0] = net_lg.iloc[0]

    for i in range(1, len(net_elg)):
        beta_net_1.iloc[i] = net_elg.iloc[i] + beta * beta_net_1.iloc[i - 1]
        beta_net_2.iloc[i] = net_lg.iloc[i] + beta * beta_net_2.iloc[i - 1]
    ax5.plot(x_axis, beta_net_1, label='beta_net_1', alpha=0.8)
    ax5.plot(x_axis, beta_net_2, label='beta_net_2', alpha=0.8)
    beta_net_12 = pd.Series(index=net_elg.index)
    beta_net_12.name = 'beta_net_12'
    beta = 0.9
    beta_net_12.iloc[0] = net_elg.iloc[0] + net_lg.iloc[0]
    for i in range(1, len(net_elg)):
        beta_net_12.iloc[i] = net_elg.iloc[i] + net_lg.iloc[i] + beta * beta_net_12.iloc[i - 1]
    ax5.plot(x_axis, beta_net_12, label='beta_net_12', alpha=0.8)

    beta_net_14 = pd.Series(index=net_elg.index)
    beta_net_14.name = 'beta_net_14'
    beta_net_34 = pd.Series(index=net_elg.index)
    beta_net_34.name = 'beta_net_34'
    beta = 0.9
    beta_net_34.iloc[0] = net_md.iloc[0] + net_sm.iloc[0]
    beta_net_14.iloc[0] = net_elg.iloc[0] + net_sm.iloc[0]
    for i in range(1, len(net_elg)):
        beta_net_14.iloc[i] = net_elg.iloc[i] + net_sm.iloc[i] + beta * beta_net_14.iloc[i - 1]
        beta_net_34.iloc[i] = net_md.iloc[i] + net_sm.iloc[i] + beta * beta_net_34.iloc[i - 1]
    ax5.plot(x_axis, beta_net_34, label='beta_net_14', alpha=0.8)
    ax5.plot(x_axis, beta_net_34, label='beta_net_34', alpha=0.8)
    # ax2 = fig.add_subplot(313)
    # pv12 = (beta_net_elg_lg - beta_net_elg_lg.shift(10))
    # ax5.plot(x_axis, pv12, label='pv_lg', alpha=0.8)
    #
    # ax3 = ax2.twinx()
    # candlestick_ohlc(ax3, data_mat.values, width=.7, colorup='y', colordown='c')
    pv2 = (beta_net_2 - beta_net_2.shift(5))
    ax5.plot(x_axis, pv2, label='pv2', alpha=0.8)
    pv12 = (beta_net_12 - beta_net_12.shift(5))
    ax5.plot(x_axis, pv12, label='pv12', alpha=0.8)
    pv14 = (beta_net_14 - beta_net_14.shift(5))
    ax5.plot(x_axis, pv14, label='pv14', alpha=0.8)
    pv34 = (beta_net_34 - beta_net_34.shift(5))
    ax5.plot(x_axis, pv34, label='pv34', alpha=0.8)


    # ax2.plot(x_axis, pv12+pv34, label='pv1234', alpha=0.8)
    #
    # ax5.axhline(0)
    # ax2.axhline(0)
    # ax2.legend()
    # ax5.legend()
    # plt.show()
    # return

    ax2 = fig.add_subplot(313)
    ax3 = ax2.twinx()
    ax2.xaxis.set_major_locator(mticker.MaxNLocator(10))
    ax2.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
    candlestick_ohlc(ax2, data_mat.values, width=.7, colorup='y', colordown='c')

    trf_back = dailys['turnover_rate_f'] * dailys['pct_chg'] / abs(
        dailys['pct_chg'])

    trf = dailys['turnover_rate_f'] * (2 * dailys['close'] - dailys['high'] - dailys['low']) /((dailys['open'] - dailys.shift()['close']) + 2*dailys['high'] - 2*dailys['low'])

    # trf = dailys['turnover_rate_f'] * (2 * dailys['close'] - dailys['high'] - dailys['low']) /((dailys['open'] - dailys.shift()['close']) + 2*dailys['high'] - 2*dailys['low'])
    # trf = dailys['turnover_rate_f'] * (2 * dailys['close'] - dailys['high'] - dailys['low']) /\
    #       (-abs(dailys['close'] - dailys['open']) + 2*dailys['high'] - 2*dailys['low'])
    trf.fillna(value=trf_back, inplace=True)

    beta_trf2 = pd.Series(index=trf.index)
    beta = 0.9
    beta_trf2.iloc[0] = 0
    for i in range(1, len(trf)):
        beta_trf2.iloc[i] = trf.iloc[i] + beta * beta_trf2.iloc[i-1]
    ax3.plot(x_axis, beta_trf2, label='beta_trf2', alpha=0.8)

    beta_trf3 = pd.Series(index=trf.index)
    beta = 0.8
    beta_trf3.iloc[0] = 0
    for i in range(1, len(trf)):
        beta_trf3.iloc[i] = trf.iloc[i] + beta * beta_trf3.iloc[i - 1]
    ax3.plot(x_axis, beta_trf3, label='beta_trf3', alpha=0.8)

    # beta_trf2_c = remove_noise(beta_trf2, unit=6)
    # ax3.plot(x_axis, beta_trf2_c, label='beta_trf2_c', alpha=0.8)


    # trf2_v = beta_trf2 - beta_trf2.shift()
    # trf2_v.name = 'trf2_v'
    # ax3.plot(x_axis, trf2_v, label='trf2_v', alpha=0.8)
    #
    # trf2_a = trf2_v - trf2_v.shift()
    # trf2_a.name = 'trf2_a'
    # ax3.plot(x_axis, trf2_a, label='trf2_a', alpha=0.8)

    # acc_trf = trf.cumsum()
    # acc_trf_back = trf_back.cumsum()
    # acc_va = va.cumsum()
    # ax3.plot(x_axis, acc_trf, label='acc_trf', alpha=0.8)
    # ax3.plot(x_axis, acc_trf_back, label='trf_back', alpha=0.8)
    # ax3.plot(x_axis, acc_va, label='acc_va', alpha=0.8)

    # buy, sell = get_buy_sell_points(trf, acc_trf)
    # d = pd.concat([dailys['cal_date'], elg_base_diff, mv_tr_f2_pct_chg, mv_mv_tr_f2_pct_chg, net_elg, buy, sell, mv_elg_base_diff5, mv_elg_base_diff10], axis=1)
    # ax2.plot(x_axis, np.multiply(dailys['high'], buy), 'm^', label='buy')
    # ax2.plot(x_axis, np.multiply(dailys['high'], sell), 'rv', label='sell')

    ax3.axhline(0, alpha=0.2)

    # ax1.plot(x_axis, dailys['sell_lg_vol'])
    # ax1.plot(x_axis, dailys['buy_lg_vol'])
    #
    # ax1.plot(x_axis, dailys['sell_md_vol'])
    # ax1.plot(x_axis, dailys['buy_md_vol'])
    # ax1.plot(x_axis, dailys['sell_sm_vol'])
    # ax1.plot(x_axis, dailys['buy_sm_vol'])

    ax0.legend(loc=2)
    ax1.legend(loc=3)
    ax2.legend(loc=3)
    ax3.legend(loc=2)
    ax5.legend(loc=3)

    # 计算近2个月大单增加的比例
    diff_12 = pd.Series(np.where(np.diff(beta_net_1) > 0, 1, -1), index=beta_net_1.index[1:], name='diff_12')
    incre2_12 = diff_12.rolling(window=20 * 2).sum()
    incre6_12 = diff_12.rolling(window=20 * 6).sum()
    incre2_12.name = 'incre2_12'
    incre6_12.name = 'incre6_12'

    diff_2 = pd.Series(np.where(np.diff(beta_net_2) > 0, 1, -1), index=beta_net_2.index[1:], name='diff_2')
    incre2_2 = diff_2.rolling(window=20 * 2).sum()
    incre6_2 = diff_2.rolling(window=20 * 6).sum()
    incre2_2.name = 'incre2_2'
    incre6_2.name = 'incre6_2'
    diff_pc = pd.Series(np.where(np.diff(dailys['pct_chg']) > 0, 1, -1), index=beta_net_2.index[1:], name='diff_pc')
    incre2_pc = diff_pc.rolling(window=20 * 2).sum()
    incre6_pc = diff_pc.rolling(window=20 * 6).sum()
    incre2_pc.name = 'incre2_pc'
    incre6_pc.name = 'incre6_pc'
    print(pd.concat(
        [dailys.iloc[1:]['cal_date'], round(dailys.iloc[1:]['close'], 1), round(dailys.iloc[1:]['pct_chg'], 1), beta_net_1, beta_net_2, diff_12, incre2_12, incre6_12, diff_2,  incre2_2,
         incre6_2, ], axis=1).tail(80))
    # os.ex
    plt.show()


def get_buy_sell_points(trf, mtrf):

    buy, sell = [np.nan]*3, [np.nan]*3
    for i in range(3, len(trf)):

        if trf.iloc[i] > trf.iloc[i-1] and trf.iloc[i] > trf.iloc[i-2] and mtrf.iloc[i] > mtrf.iloc[i-2] and mtrf.iloc[i] > 0:
            buy.append(1)
            sell.append(np.nan)
        elif trf.iloc[i] < trf.iloc[i-1] and  trf.iloc[i] < trf.iloc[i-2] and mtrf.iloc[i] < mtrf.iloc[i-2]:
            sell.append(1)
            buy.append(np.nan)
        else:
            buy.append(np.nan)
            sell.append(np.nan)
    buy = pd.Series(buy, index=trf.index)
    buy.name = 'buy'
    sell = pd.Series(sell, index=trf.index)
    sell.name = 'sell'
    return buy, sell
