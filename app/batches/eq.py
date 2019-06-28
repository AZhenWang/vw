from app.saver.logic import DB
from app.saver.tables import fields_map
import numpy as np
import matplotlib.pylab as plt
import pandas as pd
from mpl_finance import candlestick_ohlc
from matplotlib import dates as mdates
from matplotlib import ticker as mticker
from datetime import datetime as dt
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

    period = 20*3
    window = 20*6
    trade_cal = DB.get_open_cal_date(end_date=end_date, period=period+window)
    start_date_id = trade_cal.iloc[0]['date_id']
    end_date_id = trade_cal.iloc[-1]['date_id']
    start_date = trade_cal.iloc[0]['cal_date']

    # code_id = 2772
    code_id = 2433

    flow = DB.get_moneyflows(code_id=code_id, end_date_id=end_date_id, start_date_id=start_date_id)

    dailys = flow[['cal_date', 'open', 'close', 'high', 'low', 'pct_chg', 'adj_factor', 'float_share', 'turnover_rate_f']]
    print('99')
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
    print('77')
    fig = plt.figure(figsize=(20, 26))
    ax0 = fig.add_subplot(211, facecolor='#07000d')
    plt.title(str(code_id) + '-' + end_date)
    x_axis = dailys['dateTime']
    data_mat = dailys[['dateTime', 'open', 'high', 'low', 'close', 'pct_chg']]
    ax0.xaxis.set_major_locator(mticker.MaxNLocator(10))
    ax0.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
    candlestick_ohlc(ax0, data_mat.values, width=.7, colorup='y', colordown='c')
    print('66')
    ax1 = ax0.twinx()
    ax1.axhline(0, color='g')
    net_elg = (dailys['buy_elg_vol'] - dailys['sell_elg_vol'])/dailys['float_share']
    net_elg.name = 'net_elg'
    net_sm = (dailys['buy_sm_vol'] - dailys['sell_sm_vol'])/dailys['float_share']
    net_md = (dailys['buy_md_vol'] - dailys['sell_md_vol'])/dailys['float_share']
    net_lg = (dailys['buy_lg_vol'] - dailys['sell_lg_vol'])/dailys['float_share']
    net_lg_elg = net_elg + net_lg

    #
    buy_elg = dailys['buy_elg_vol'] / dailys['float_share']
    buy_elg.name = 'buy_elg'
    sell_elg = dailys['sell_elg_vol'] / dailys['float_share']
    sell_elg.name = 'sell_elg'
    ax1.plot(x_axis, buy_elg, color='#8B1A1A')
    ax1.plot(x_axis, sell_elg, color='#6A5ACD')
    #
    # ax1.plot(x_axis, (dailys['net_mf_vol'])/dailys['float_share'], color='c')
    #
    # ax1.plot(x_axis, net_lg_elg, label='net_lg_elg', color='m')

    ax1.plot(x_axis, net_elg, label='net_elg', color='red')
    ax1.fill_between(x_axis, net_elg, 0, net_elg > 0, facecolor='red', alpha=0.8)
    # ax1.plot(x_axis, net_lg, label='net_lg', color='pink')
    # ax1.fill_between(x_axis, net_lg, 0, net_lg > 0, facecolor='pink', alpha=0.6)
    # ax1.plot(x_axis, net_md, label='net_md', color='green')
    # ax1.fill_between(x_axis, net_md, 0, net_md > 0, facecolor='green', alpha=0.5)
    # ax1.plot(x_axis, net_sm, label='net_sm', color='blue')
    # ax1.fill_between(x_axis, net_sm, 0, net_sm > 0, facecolor='blue', alpha=0.3)
    print('3')
    elg_base_diff = (net_elg - net_elg.shift()) * 100
    mv_elg_base_diff10 = elg_base_diff.rolling(window=10).mean()
    mv_elg_base_diff10.name = 'mv_elg_base_diff10'
    mv_elg_base_diff5 = elg_base_diff.rolling(window=5).mean()
    mv_elg_base_diff5.name = 'mv_elg_base_diff5'
    ax1.axhline(0.2)
    ax1.plot(x_axis, elg_base_diff, label='elg_base_diff')
    ax1.plot(x_axis, mv_elg_base_diff5, label='mv_elg_base_diff5')
    ax1.plot(x_axis, mv_elg_base_diff10, label='mv_elg_base_diff10')
    print('2')
    ax2 = fig.add_subplot(212)
    ax3 = ax2.twinx()
    ax2.xaxis.set_major_locator(mticker.MaxNLocator(10))
    ax2.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
    candlestick_ohlc(ax2, data_mat.values, width=.7, colorup='y', colordown='c')

    turnover_rate_back = dailys['turnover_rate_f'] * dailys['pct_chg'] / abs(
        dailys['pct_chg'])


    # turnover_rate_f = dailys['turnover_rate_f'] * (dailys['close'] - dailys['open']) / abs(
    #     dailys['close'] - dailys['open'])
    # turnover_rate_f.fillna(value=turnover_rate_back, inplace=True)
    # ax3.plot(x_axis, turnover_rate_f, label='turnover_rate_f')
    #
    # mv_turnover_rate_f = turnover_rate_f.rolling(window=5).mean()
    # ax3.plot(x_axis, mv_turnover_rate_f, label='mv_turnover_rate_f')
    #
    turnover_rate_f2 = dailys['turnover_rate_f'] * (2 * dailys['close'] - dailys['high'] - dailys['low']) / abs(
        dailys['high'] - dailys['low'])
    turnover_rate_f2.fillna(value=turnover_rate_back, inplace=True)
    # ax3.plot(x_axis, turnover_rate_f2, label='turnover_rate_f2', alpha=0.8)
    mv_turnover_rate_f2 = turnover_rate_f2.rolling(window=5).mean()
    # ax3.plot(x_axis, mv_turnover_rate_f2, label='mv_turnover_rate_f2', alpha=0.8)

    mv_tr_f2_pct_chg = mv_turnover_rate_f2 - mv_turnover_rate_f2.shift()
    mv_tr_f2_pct_chg.name = 'mv_tr_f2_pct_chg'
    # ax3.plot(x_axis, mv_tr_f2_pct_chg, label='mv_tr_f2_pct_chg', alpha=0.6)

    mv_mv_tr_f2 = mv_tr_f2_pct_chg.rolling(window=5).mean()
    mv_mv_tr_f2.name = 'mv_mv_tr_f2'
    ax3.plot(x_axis, mv_mv_tr_f2, label='mv_mv_tr_f2', alpha=0.6)

    mv_mv_tr_f2_pct_chg = mv_mv_tr_f2 - mv_mv_tr_f2.shift()
    mv_mv_tr_f2_pct_chg.name = 'mv_mv_tr_f2_pct_chg'
    ax3.plot(x_axis, mv_mv_tr_f2_pct_chg, label='mv_mv_tr_f2_pct_chg', alpha=0.6)
    print('1')
    mv_mv_mv_tr_f2 = mv_mv_tr_f2_pct_chg.rolling(window=5).mean()
    mv_mv_mv_tr_f2_pct_chg = mv_mv_mv_tr_f2 - mv_mv_mv_tr_f2.shift()
    mv_mv_mv_tr_f2_pct_chg.name = 'mv_mv_mv_tr_f2_pct_chg'
    ax3.plot(x_axis, mv_mv_mv_tr_f2_pct_chg, label='mv_mv_mv_tr_f2_pct_chg', alpha=0.6)

    buy, sell = get_buy_sell_points(mv_mv_tr_f2_pct_chg)
    d = pd.concat([dailys['cal_date'], elg_base_diff, mv_mv_tr_f2_pct_chg, net_elg, buy, sell, mv_elg_base_diff5, mv_elg_base_diff10], axis=1)
    print(d)
    # os.ex
    ax2.plot(x_axis, np.multiply(dailys['high'], buy), 'm^', label='buy')
    ax2.plot(x_axis, np.multiply(dailys['high'], sell), 'rv', label='sell')

    ax3.axhline(0)

    # ax1.plot(x_axis, dailys['sell_lg_vol'])
    # ax1.plot(x_axis, dailys['buy_lg_vol'])
    #
    # ax1.plot(x_axis, dailys['sell_md_vol'])
    # ax1.plot(x_axis, dailys['buy_md_vol'])
    # ax1.plot(x_axis, dailys['sell_sm_vol'])
    # ax1.plot(x_axis, dailys['buy_sm_vol'])

    ax1.legend(loc=3)
    ax0.legend(loc=2)
    ax2.legend(loc=3)
    ax3.legend(loc=2)


    plt.show()


def get_buy_sell_points(holdings):

    buy, sell = [np.nan]*3, [np.nan]*3
    for i in range(3, len(holdings)):
        if holdings.iloc[i] > holdings.iloc[i-1] > 0 and holdings.iloc[i-1] > holdings.iloc[i-2]:
            buy.append(1)
            sell.append(np.nan)
        elif holdings.iloc[i] < holdings.iloc[i-1] < 0 and holdings.iloc[i-1] < holdings.iloc[i-2]:
            sell.append(1)
            buy.append(np.nan)
        else:
            buy.append(np.nan)
            sell.append(np.nan)
    buy = pd.Series(buy, index=holdings.index)
    buy.name = 'buy'
    sell = pd.Series(sell, index=holdings.index)
    sell.name = 'sell'
    return buy, sell
