from app.saver.logic import DB
from app.saver.tables import fields_map
import pandas as pd
import numpy as np


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

    max_window = 20*12
    pre_trade_cal = DB.get_open_cal_date(end_date=start_date, period=max_window)
    trade_cal = DB.get_open_cal_date(end_date=end_date, start_date=start_date)
    pre_date = pre_trade_cal.iloc[0]['cal_date']
    start_date_id = trade_cal.iloc[0]['date_id']
    end_date_id = trade_cal.iloc[-1]['date_id']
    #
    codes = DB.get_latestopendays_code_list(
        latest_open_days=244, date_id=trade_cal.iloc[0]['date_id'])
    #
    code_ids = codes['code_id']
    # code_ids = [1895]
    new_rows = pd.DataFrame(columns=fields_map['2line'])
    i = 0
    for code_id in code_ids:
        print(code_id)
        DB.delete_logs(code_id, start_date_id, end_date_id, tablename='2line')
        dailys = DB.get_code_info(code_id=code_id, end_date=end_date, start_date=pre_date)

        close = dailys['close'] * dailys['adj_factor']
        high = dailys['high'] * dailys['adj_factor']

        # 移动支撑压力线: 短中长三个时间
        sm_l1, sm_l2, sm_l3 = l1_l2(high, window=20 * 6)
        lg_l1, lg_l2, lg_l3 = l1_l2(high, window=20 * 12)
        sm_l1.dropna(inplace=True)
        sm_l2.dropna(inplace=True)
        sm_l3.dropna(inplace=True)
        lg_l1.dropna(inplace=True)
        lg_l2.dropna(inplace=True)
        lg_l3.dropna(inplace=True)

        bot = pd.Series(index=dailys.index)
        top = bot.copy()

        for i in range(len(lg_l1)):
            date_id = lg_l1.index[i]
            temp = np.sort([close.loc[date_id], sm_l1.loc[date_id], sm_l2.loc[date_id], sm_l3.loc[date_id],
                            lg_l1.loc[date_id], lg_l2.loc[date_id], lg_l3.loc[date_id]])
            close_loc = np.argwhere(temp == close.loc[date_id])[0]
            if close_loc == 6:
                top.loc[date_id] = np.nan
                bot.loc[date_id] = temp[close_loc - 1]
            elif close_loc == 0:
                bot.loc[date_id] = np.nan
                top.loc[date_id] = temp[close_loc + 1]
            else:
                top.loc[date_id] = temp[close_loc + 1]
                bot.loc[date_id] = temp[close_loc - 1]

        from_top = round((top - close) * 100 / close, 1)
        from_bot = round((close - bot) * 100 / bot, 1)
        from_bot.name = 'from_bot'
        from_top.name = 'from_top'

        top = round(top / dailys['adj_factor'], 2)
        bot = round(bot / dailys['adj_factor'], 2)
        top.name = 'top'
        bot.name = 'bot'

        sm_l1 = round(sm_l1 / dailys['adj_factor'], 2)
        sm_l2 = round(sm_l2 / dailys['adj_factor'], 2)
        lg_l1 = round(lg_l1 / dailys['adj_factor'], 2)
        lg_l2 = round(lg_l2 / dailys['adj_factor'], 2)
        sm_l1.name = 'sm_l1'
        sm_l2.name = 'sm_l2'
        lg_l1.name = 'lg_l1'
        lg_l2.name = 'lg_l2'

        data = pd.concat([sm_l1, sm_l2, lg_l1, lg_l2, top, bot, from_top, from_bot], axis=1)
        data['code_id'] = code_id

        data = data[data.index >= start_date_id]

        data.reset_index(inplace=True)
        new_rows = pd.concat([new_rows, data])

    if not new_rows.empty:
        new_rows.to_sql('2line', DB.engine, index=False, if_exists='append', chunksize=1000)


def l1_l2(high, window):
    high_max = high.rolling(window=window).max()
    price_max = high_max
    l1 = high_max - price_max * 0.026
    l2 = high_max - price_max * 0.21
    l3 = high_max - price_max * 0.55

    return l1, l2, l3

