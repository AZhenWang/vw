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

    window = 20*6
    pre_trade_cal = DB.get_open_cal_date(end_date=start_date, period=window+41)
    trade_cal = DB.get_open_cal_date(end_date=end_date, start_date=start_date)
    pre_date_id = pre_trade_cal.iloc[0]['date_id']
    start_date_id = trade_cal.iloc[0]['date_id']
    end_date_id = trade_cal.iloc[-1]['date_id']
    #
    codes = DB.get_latestopendays_code_list(
        latest_open_days=244, date_id=trade_cal.iloc[0]['date_id'])
    code_ids = codes['code_id']
    # code_ids = [2020, 1423]
    new_rows = pd.DataFrame(columns=fields_map['mv_moneyflow'])
    for code_id in code_ids:
        DB.delete_logs(code_id, start_date_id, end_date_id, tablename='mv_moneyflow')
        flow = DB.get_moneyflows(code_id=code_id, end_date_id=end_date_id, start_date_id=pre_date_id)

        flow['close'] = flow['close'] * flow['adj_factor']
        flow['open'] = flow['open'] * flow['adj_factor']
        flow['high'] = flow['high'] * flow['adj_factor']
        flow['low'] = flow['low'] * flow['adj_factor']

        flow_mean = flow[
            ['net_mf_vol', 'sell_elg_vol', 'buy_elg_vol', 'sell_lg_vol', 'buy_lg_vol', 'sell_md_vol',
             'buy_md_vol', 'sell_sm_vol', 'buy_sm_vol']].rolling(window=window).mean()

        net_mf = (flow_mean['net_mf_vol']) * 100 / flow['float_share']
        net_mf.name = 'net_mf'

        mv_buy_elg = (flow_mean['buy_elg_vol']) * 100 / flow['float_share']
        mv_buy_elg.name = 'mv_buy_elg'

        mv_sell_elg = (flow_mean['sell_elg_vol']) * 100 / flow['float_share']
        mv_sell_elg.name = 'mv_sell_elg'

        net_elg = (flow_mean['buy_elg_vol'] - flow_mean['sell_elg_vol']) * 100 / flow['float_share']
        net_elg.name = 'net_elg'
        net_sm = (flow_mean['buy_sm_vol'] - flow_mean['sell_sm_vol']) * 100 / flow['float_share']
        net_sm.name = 'net_sm'
        net_md = (flow_mean['buy_md_vol'] - flow_mean['sell_md_vol']) * 100 / flow['float_share']
        net_md.name = 'net_md'
        net_lg = (flow_mean['buy_lg_vol'] - flow_mean['sell_lg_vol']) * 100 / flow['float_share']
        net_lg.name = 'net_lg'

        tr_back = flow['turnover_rate_f'] * flow['pct_chg'] / abs(
            flow['pct_chg'])

        trf2 = flow['turnover_rate_f'] * (2 * flow['close'] - flow['high'] - flow['low']) / (
                -abs(flow['close'] - flow['open']) + 2 * flow['high'] - 2 * flow['low'])

        trf2.name = 'trf2'
        trf2.fillna(value=tr_back, inplace=True)

        # 求特大资金资金流入与流出的差
        elg_base_diff = (net_elg - net_elg.shift()) * 100
        mv_elg_base_diff10 = elg_base_diff.rolling(window=10).mean()
        mv_elg_base_diff10.name = 'mv_elg_base_diff10'
        mv_elg_base_diff5 = elg_base_diff.rolling(window=5).mean()
        mv_elg_base_diff5.name = 'mv_elg_base_diff5'

        max_pre_trf2 = round(trf2.shift().rolling(window=40).max(), 1)
        max_pre_trf2.name = 'max_pre_trf2'

        data = pd.concat([net_mf, net_elg, net_lg, net_md, net_sm, mv_buy_elg, mv_sell_elg,
                          trf2, mv_elg_base_diff5, mv_elg_base_diff10, max_pre_trf2], axis=1)

        data = data.dropna()
        data = data[data.index >= start_date_id]

        # 求beta_trf2
        first_logs = DB.get_mv_moneyflows(code_id=code_id, start_date_id=start_date_id, end_date_id=start_date_id)
        if not first_logs.empty:
            init_beta_mv_trf2 = first_logs.iloc[0]['beta_mv_trf2']
        else:
            init_beta_mv_trf2 = data['trf2'].iloc[0]

        beta_trf2 = pd.Series(index=data.index)
        beta = 0.5
        beta_trf2.iloc[0] = init_beta_mv_trf2
        for i in range(1, len(data)):
            beta_trf2.iloc[i] = beta * data['trf2'].iloc[i] + (1 - beta) * beta_trf2.iloc[i - 1]
        data['beta_trf2'] = beta_trf2

        # 求trf2改变的速度v、加速度a
        trf2_v = beta_trf2 - beta_trf2.shift()
        trf2_v.name = 'trf2_v'
        data['trf2_v'] = trf2_v

        trf2_a = trf2_v - trf2_v.shift()
        trf2_a.name = 'trf2_a'
        data['trf2_a'] = trf2_a

        data = data.apply(np.round, decimals=2)

        data['code_id'] = code_id
        data.reset_index(inplace=True)
        new_rows = pd.concat([new_rows, data])

    if not new_rows.empty:
        new_rows.to_sql('mv_moneyflow', DB.engine, index=False, if_exists='append', chunksize=1000)
