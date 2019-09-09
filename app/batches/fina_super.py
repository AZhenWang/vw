from app.saver.logic import DB
from app.saver.service.fina import Fina
from app.saver.tables import fields_map
import numpy as np
import pandas as pd
from datetime import datetime

def execute(start_date='', end_date=''):
    """
    企业分析
    :param start_date: 公告开始日期
    :param end_date: 公告结束日期
    :return:
    """
    init_start_date = str(int(start_date) - 10000)
    trade_cal = DB.get_cal_date(start_date=init_start_date, end_date=end_date)
    start_date_id = trade_cal.iloc[0]['date_id']
    end_date_id = trade_cal.iloc[-1]['date_id']
    # codes = DB.get_latestopendays_code_list(
    #     latest_open_days=244*2, date_id=date_id)
    # code_ids = codes['code_id']
    new_rows = pd.DataFrame(columns=fields_map['fina_super'])
    # code_ids = [132]
    code_ids = range(1, 500)
    str2date = lambda x: datetime.strptime(str(x), '%Y%m%d')
    for code_id in code_ids:
        print('code_id=', code_id)
        Fina.delete_fina_super_logs(code_id, start_date=start_date, end_date=end_date)
        logs = Fina.get_report_info(code_id=code_id, start_date=init_start_date, end_date=end_date, TTB='fina_sys',
                                    end_date_type='%1231%')
        dailys = DB.get_code_info(code_id=code_id, start_date=init_start_date, end_date=end_date, TTB='daily')
        logs['report_adj_factor'] = logs['adj_factor']
        base = trade_cal.join(logs[['f_ann_date', 'end_date', 'LP', 'MP', 'HP', 'pp0', 'roe_mv', 'report_adj_factor']].set_index('f_ann_date', drop=False), on='cal_date')
        base = base.join(dailys[['close', 'high', 'low', 'open', 'adj_factor']], on='date_id')
        base.fillna(method='ffill', inplace=True)
        base.dropna(inplace=True)
        base = base[base['cal_date'] >= start_date]

        d_dates = pd.Series(index=base.index)
        for i in range(len(base)):
            sd = datetime.strptime(base.iloc[i]['f_ann_date'], '%Y%m%d')
            ed = datetime.strptime(base.iloc[i]['cal_date'], '%Y%m%d')
            d_dates.iloc[i] = (ed-sd).days

        had_benefit = (1 + base['roe_mv'] /100 * d_dates / 365)
        adj_close = base['close'] * base['adj_factor']

        adj_LP = round(had_benefit * base['LP'] , 2)
        LP = round(adj_LP / base['adj_factor'], 2)
        adj_MP = round(had_benefit * base['MP'], 2)
        MP = round(adj_MP / base['adj_factor'], 2)
        adj_HP = round(had_benefit * base['HP'], 2)
        HP = round(adj_HP / base['adj_factor'], 2)

        win_base = pd.concat([adj_HP, adj_close], axis=1).min(axis=1)
        lose_base = pd.concat([adj_LP, adj_close], axis=1).min(axis=1)
        win_return = round((adj_HP - adj_close) * 100 / win_base, 2)
        lose_return = round((adj_LP - adj_close) * 100 / lose_base, 2)
        odds = round(win_return + lose_return, 2)

        data = pd.DataFrame()
        data['end_date'] = base['end_date']
        data['f_ann_date'] = base['f_ann_date']
        data['cal_date'] = base['cal_date']
        data['code_id'] = code_id
        data['adj_factor'] = base['adj_factor']
        data['LP'] = LP
        data['MP'] = MP
        data['HP'] = HP
        data['adj_LP'] = adj_LP
        data['adj_MP'] = adj_MP
        data['adj_HP'] = adj_HP
        data['adj_close'] = round(base['close'] * base['adj_factor'], 2)
        data['adj_high'] = round(base['high'] * base['adj_factor'], 2)
        data['adj_low'] = round(base['low'] * base['adj_factor'], 2)
        data['win_return'] = win_return
        data['lose_return'] = lose_return
        data['odds'] = odds
        data['buy_p1'] = round(adj_LP / 0.8, 2)
        data['buy_p2'] = round(adj_LP / 0.9, 2)
        data['sell_p1'] = round(adj_HP / 1.2, 2)
        data['sell_p2'] = round(adj_HP / 1.1, 2)
        position = pd.Series(index=base.index)
        for i in range(len(base)):
            if data.iloc[i]['adj_high'] >= data.iloc[i]['adj_HP']:
                position.iloc[i] = 3
            elif data.iloc[i]['adj_high'] >= data.iloc[i]['sell_p2']:
                position.iloc[i] = 2
            elif data.iloc[i]['adj_high'] >= data.iloc[i]['sell_p1']:
                position.iloc[i] = 1
            elif data.iloc[i]['adj_low'] <= data.iloc[i]['adj_LP']:
                position.iloc[i] = -3
            elif data.iloc[i]['adj_low'] <= data.iloc[i]['buy_p2']:
                position.iloc[i] = -2
            elif data.iloc[i]['adj_low'] <= data.iloc[i]['buy_p1']:
                position.iloc[i] = -1
            else:
                position.iloc[i] = 0

        basis = position.diff()
        basis[basis == 0] = np.nan
        basis.fillna(method='ffill', inplace=True)

        data['position'] = position
        data['basis'] = basis
        data[data.isin([np.inf, -np.inf])] = np.nan
        data.dropna(inplace=True)
        new_rows = pd.concat([new_rows, data.reset_index(drop=True)], sort=False)
    if not new_rows.empty:
        new_rows.to_sql('fina_super', DB.engine, index=False, if_exists='append', chunksize=1000)

