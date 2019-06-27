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
    pre_trade_cal = DB.get_open_cal_date(end_date=start_date, period=window)

    code_id = ''
    flow = DB.get_moneyflows(code_id=code_id, end_date=end_date, start_date=pre_trade_cal.iloc[0]['cal_date'])
    gp = flow.groupby(by='code_id')
    new_rows = pd.DataFrame(columns=fields_map['mv_moneyflow'])
    i = 0
    DB.truncate_mv_moneyflow()
    for code_id, group_data in gp:
        flow_mean = group_data[
            ['net_mf_vol', 'sell_elg_vol', 'buy_elg_vol', 'sell_lg_vol', 'buy_lg_vol', 'sell_md_vol',
             'buy_md_vol', 'sell_sm_vol', 'buy_sm_vol']].rolling(window=window).mean()

        net_mf = (flow_mean['net_mf_vol']) * 100 /group_data['float_share']
        net_mf.name = 'net_mf'

        mv_buy_elg = (flow_mean['buy_elg_vol']) * 100 / group_data['float_share']
        mv_buy_elg.name = 'mv_buy_elg'

        mv_sell_elg = (flow_mean['sell_elg_vol']) * 100 / group_data['float_share']
        mv_sell_elg.name = 'mv_sell_elg'

        net_elg = (flow_mean['buy_elg_vol'] - flow_mean['sell_elg_vol']) * 100 / group_data['float_share']
        net_elg.name = 'net_elg'
        net_sm = (flow_mean['buy_sm_vol'] - flow_mean['sell_sm_vol']) * 100 / group_data['float_share']
        net_sm.name = 'net_sm'
        net_md = (flow_mean['buy_md_vol'] - flow_mean['sell_md_vol']) * 100 / group_data['float_share']
        net_md.name = 'net_md'
        net_lg = (flow_mean['buy_lg_vol'] - flow_mean['sell_lg_vol']) * 100 / group_data['float_share']
        net_lg.name = 'net_lg'

        turnover_rate_f = group_data['turnover_rate_f'] * (group_data['close'] - group_data['open']) / abs(
            group_data['close'] - group_data['open'])
        turnover_rate_back = group_data['turnover_rate_f'] * group_data['pct_chg'] / abs(
            group_data['pct_chg'])
        turnover_rate_f.fillna(value=turnover_rate_back, inplace=True)
        turnover_rate_f.name = 'turnover_rate_f'
        mv_turnover_rate_f = turnover_rate_f.rolling(window=5).mean()
        mv_turnover_rate_f.name = 'mv_turnover_rate_f'
        turnover_rate_f2 = group_data['turnover_rate_f'] * (2 * group_data['close'] - group_data['high'] - group_data['low']) / abs(
            group_data['high'] - group_data['low'])
        turnover_rate_f2.name = 'turnover_rate_f2'
        mv_turnover_rate_f2 = turnover_rate_f2.rolling(window=5).mean()
        mv_turnover_rate_f2.name = 'mv_turnover_rate_f2'

        mv_tr_f2_pct_chg = mv_turnover_rate_f2 - mv_turnover_rate_f2.shift()
        mv_tr_f2_pct_chg.name = 'mv_tr_f2_pct_chg'

        mv_mv_tr_f2 = mv_tr_f2_pct_chg.rolling(window=5).mean()
        mv_mv_tr_f2.name = 'mv_mv_tr_f2'
        mv_mv_tr_f2_pct_chg = mv_mv_tr_f2 - mv_mv_tr_f2.shift()
        mv_mv_tr_f2_pct_chg.name = 'mv_mv_tr_f2_pct_chg'

        data = pd.concat([net_mf, net_elg, net_lg, net_md, net_sm, mv_buy_elg, mv_sell_elg,
                          turnover_rate_f, mv_turnover_rate_f, turnover_rate_f2,
                          mv_turnover_rate_f2, mv_tr_f2_pct_chg, mv_mv_tr_f2, mv_mv_tr_f2_pct_chg], axis=1).dropna()

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

            }
            i += 1
    if not new_rows.empty:
        new_rows.to_sql('mv_moneyflow', DB.engine, index=False, if_exists='append', chunksize=1000)
