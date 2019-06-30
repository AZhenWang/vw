from app.saver.logic import DB
from app.saver.tables import fields_map
import pandas as pd


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
    pre_trade_cal = DB.get_open_cal_date(end_date=start_date, period=window+11)
    trade_cal = DB.get_open_cal_date(end_date=end_date, start_date=start_date)
    start_date_id = pre_trade_cal.iloc[0]['date_id']
    end_date_id = trade_cal.iloc[-1]['date_id']
    #
    codes = DB.get_latestopendays_code_list(
        latest_open_days=244, date_id=trade_cal.iloc[0]['date_id'])
    #
    code_ids = codes['code_id']
    # code_ids = [1,2]
    new_rows = pd.DataFrame(columns=fields_map['mv_moneyflow'])
    i = 0
    for code_id in code_ids:
        DB.delete_mv_moneyflow_by_code(code_id=code_id)
        flow = DB.get_moneyflows(code_id=code_id, end_date_id=end_date_id, start_date_id=start_date_id)
        flow_mean = flow[
            ['net_mf_vol', 'sell_elg_vol', 'buy_elg_vol', 'sell_lg_vol', 'buy_lg_vol', 'sell_md_vol',
             'buy_md_vol', 'sell_sm_vol', 'buy_sm_vol']].rolling(window=window).mean()

        net_mf = (flow_mean['net_mf_vol']) * 100 /flow['float_share']
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

        turnover_rate_f = flow['turnover_rate_f'] * (flow['close'] - flow['open']) / abs(
            flow['close'] - flow['open'])
        turnover_rate_back = flow['turnover_rate_f'] * flow['pct_chg'] / abs(
            flow['pct_chg'])
        turnover_rate_f.fillna(value=turnover_rate_back, inplace=True)
        turnover_rate_f.name = 'turnover_rate_f'
        mv_turnover_rate_f = turnover_rate_f.rolling(window=5).mean()
        mv_turnover_rate_f.name = 'mv_turnover_rate_f'
        turnover_rate_f2 = flow['turnover_rate_f'] * (2 * flow['close'] - flow['high'] - flow['low']) / abs(
            flow['high'] - flow['low'])
        turnover_rate_f2.name = 'turnover_rate_f2'
        mv_turnover_rate_f2 = turnover_rate_f2.rolling(window=5).mean()
        mv_turnover_rate_f2.name = 'mv_turnover_rate_f2'

        mv_tr_f2_pct_chg = mv_turnover_rate_f2 - mv_turnover_rate_f2.shift()
        mv_tr_f2_pct_chg.name = 'mv_tr_f2_pct_chg'

        mv_mv_tr_f2 = mv_tr_f2_pct_chg.rolling(window=5).mean()
        mv_mv_tr_f2.name = 'mv_mv_tr_f2'
        mv_mv_tr_f2_pct_chg = mv_mv_tr_f2 - mv_mv_tr_f2.shift()
        mv_mv_tr_f2_pct_chg.name = 'mv_mv_tr_f2_pct_chg'

        elg_base_diff = (net_elg - net_elg.shift()) * 100
        mv_elg_base_diff10 = elg_base_diff.rolling(window=10).mean()
        mv_elg_base_diff10.name = 'mv_elg_base_diff10'
        mv_elg_base_diff5 = elg_base_diff.rolling(window=5).mean()
        mv_elg_base_diff5.name = 'mv_elg_base_diff5'

        weight = mv_elg_base_diff5 - mv_elg_base_diff5.shift() + mv_elg_base_diff10 - mv_elg_base_diff10.shift()
        weight.name = 'weight'

        data = pd.concat([net_mf, net_elg, net_lg, net_md, net_sm, mv_buy_elg, mv_sell_elg,
                          turnover_rate_f, mv_turnover_rate_f, turnover_rate_f2,
                          mv_turnover_rate_f2, mv_tr_f2_pct_chg, mv_mv_tr_f2, mv_mv_tr_f2_pct_chg,
                          mv_elg_base_diff5, mv_elg_base_diff10, weight], axis=1).dropna()
        for j in range(len(data)):
            new_rows.loc[i] = {
                'code_id': code_id,
                'date_id': data.index[j],
                'net_mf': round(data.iloc[j]['net_mf'], 2),
                'net_elg': round(data.iloc[j]['net_elg'], 2),
                'net_lg': round(data.iloc[j]['net_lg'], 2),
                'net_md': round(data.iloc[j]['net_md'], 2),
                'net_sm': round(data.iloc[j]['net_sm'], 2),
                'mv_buy_elg': round(data.iloc[j]['mv_buy_elg'], 2),
                'mv_sell_elg': round(data.iloc[j]['mv_sell_elg'], 2),
                'turnover_rate_f': round(data.iloc[j]['turnover_rate_f'], 2),
                'turnover_rate_f2': round(data.iloc[j]['turnover_rate_f2'], 2),
                'mv_turnover_rate_f': round(data.iloc[j]['mv_turnover_rate_f'], 2),
                'mv_turnover_rate_f2': round(data.iloc[j]['mv_turnover_rate_f2'], 2),
                'mv_tr_f2_pct_chg': round(data.iloc[j]['mv_tr_f2_pct_chg'], 2),
                'mv_mv_tr_f2': round(data.iloc[j]['mv_mv_tr_f2'], 2),
                'mv_mv_tr_f2_pct_chg': round(data.iloc[j]['mv_mv_tr_f2_pct_chg'], 2),
                'mv_elg_base_diff5': round(data.iloc[j]['mv_elg_base_diff5'], 2),
                'mv_elg_base_diff10': round(data.iloc[j]['mv_elg_base_diff10'], 2),
                'weight': round(data.iloc[j]['weight'], 2),
            }
            i += 1
    if not new_rows.empty:
        new_rows.to_sql('mv_moneyflow', DB.engine, index=False, if_exists='append', chunksize=1000)
