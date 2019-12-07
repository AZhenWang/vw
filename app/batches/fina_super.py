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
    # code_ids = [3664]
    code_ids = [3,56,63,65,78,94,103,104,110,111,115,132,138,156,171,177,198,208,213,220,239,250,256,284,290,297,309,311,313,324,330,333,337,390,396,399,410,422,431,450,453,454,460,465,466,468,471,483,484,486,494,501,502,508,518,525,532,556,560,563,566,571,573,582,589,600,609,626,629,630,637,662,683,687,690,710,714,716,738,750,757,780,789,795,804,812,823,824,828,833,845,849,853,856,862,870,871,872,874,875,885,887,895,896,898,900,905,913,916,918,939,960,725,
1012,1013,1024,1028,1043,1046,1053,1059,1063,1065,1076,1094,1105,1106,1110,1115,1128,1131,1137,1142,1145,1150,1155,1170,1187,1196,1200,1206,1208,1211,1229,1239,1256,1258,1262,1263,1269,1273,1277,1278,1279,1313,1321,1322,1328,1342,1345,1347,1348,1353,1355,1360,1362,1368,1376,1383,1386,1389,1392,1398,1399,1402,1409,1413,1420,1422,1424,1432,1437,1443,1464,1468,1489,1490,1491,1492,1501,1503,1504,1514,1517,1524,1525,1529,1540,1553,1556,1563,1575,1580,1581,1585,1587,1595,1597,1614,1619,1620,1624,1625,1627,1628,1635,1636,1648,1654,1670,1675,1679,1695,1699,1707,1728,1738,1739,1743,1766,1775,1783,1784,1785,1791,1808,1813,1815,1816,1818,1825,1831,1832,1838,1842,1860,1861,1864,1866,1876,1884,1898,1907,1908,1912,1913,1939,1944,1949,1958,1959,1961,1969,1970,1975,1984,
2003,2006,2009,2010,2012,2014,2015,2025,2027,2048,2049,2055,2056,2057,2071,2074,2076,2091,2093,2095,2097,2119,2157,2164,2176,2179,2180,2181,2186,2191,2226,2231,2236,2251,2252,2283,2288,2296,2300,2306,2311,2333,2344,2349,2350,2352,2357,2361,2372,2374,2381,2390,2398,2399,2402,2415,2429,2439,2441,2442,2465,2471,2473,2481,2506,2514,2536,2540,2543,2550,2551,2555,2572,2580,2600,2606,2624,2629,2643,2652,2668,2673,2720,2721,2745,2749,2760,2764,2768,2777,2783,2804,2832,2853,2910,2925,2989,2992,2586,
3834,3828,3827,3826,3050,3114,3121,3135,3149,3161,3164,3165,3186,3192,3199,3209,3215,3220,3230,3234,3236,3242,3243,3255,3257,3262,3291,3295,3304,3315,3334,3340,3346,3347,3352,3353,3355,3360,3390,3394,3395,3403,3404,3409,3422,3430,3432,3466,3485,3496,3497,3499,3508,3515,3519,3526,3531,3537,3539,3542,3543,3549,3557,3664,3682,3684,3692,3699,3702,3704,3718,3731,3734,3753,3759,3780,3793,3794,3810,3822,3835,3839,3842,3843,3844,3849,3860,]

#     code_ids = [2352, 1587, 1348, 2119, 1376, 3304, 1383, 2074, 2057]
#     code_ids = range(1, 3890)
    # code_ids = range(2920, 3670)
    # code_ids = range(1, 3879)
    for code_id in code_ids:
        print('code_id=', code_id)
        Fina.delete_fina_super_logs(code_id, start_date=start_date, end_date=end_date)
        logs = Fina.get_report_info(code_id=code_id, start_date_id=start_date_id, end_date_id=end_date_id, TTB='fina_sys')
        if logs.empty:
            continue
        recom_logs = Fina.get_report_info(code_id=code_id, start_date_id=start_date_id, end_date_id=end_date_id, TTB='fina_recom_logs')
        dailys = DB.get_code_info(code_id=code_id, start_date=init_start_date, end_date=end_date, TTB='daily')
        logs['report_adj_factor'] = logs['adj_factor']
        logs['report_adj_close'] = logs['adj_close']
        base = trade_cal.join(logs[['date_id', 'end_date', 'pb', 'pe', 'LLP', 'LP', 'MP', 'HP', 'HHP', 'MP_pct',
                                    'roe_mv', 'roe_sale_mv', 'report_adj_factor', 'rev_pct_yearly',
                                    'V', 'V_ci0', 'V_ci1', 'V_adj', 'V_rd', 'V_sale', 'V_ebitda', 'dpd_V', 'report_adj_close']].set_index('date_id'), on='date_id')
        base = base.join(dailys[['close', 'high', 'low', 'open', 'adj_factor', 'total_mv']], on='date_id')
        base.fillna(method='ffill', inplace=True)
        # print('base=', base)
        base.dropna(subset=['report_adj_close', 'V', 'V_sale', 'V_ebitda'], inplace=True)
        # print('base1=', base)
        base = base.join(recom_logs[['date_id', 'flag', 'step', 'nice', 'holdernum_2inc', 'cashcover_ratio', 'chance_times']].set_index('date_id'),
                         on='date_id')
        base.fillna(method='ffill', inplace=True)
        base = base[base['cal_date'] >= start_date]

        adj_close = base['close'] * base['adj_factor']
        adj_high = base['high'] * base['adj_factor']
        adj_low = base['low'] * base['adj_factor']

        # d_dates = pd.Series(index=base.index)
        # for i in range(len(base)):
        #     sd = datetime.strptime(base.iloc[i]['f_ann_date'], '%Y%m%d')
        #     ed = datetime.strptime(base.iloc[i]['cal_date'], '%Y%m%d')
        #     d_dates.iloc[i]= (ed-sd).days
        #
        # had_benefit = (1 + base['roe_sale_mv'] /100 * d_dates / 365)

        data = pd.DataFrame()
        data['end_date'] = base['end_date']
        data['cal_date'] = base['cal_date']
        data['code_id'] = code_id
        data['total_mv'] = base['total_mv']
        data['adj_factor'] = base['adj_factor']
        data['rev_pct_yearly'] = base['rev_pct_yearly']

        pb = base['pb'] * adj_close / base['report_adj_close']
        pe = base['pe'] * adj_close / base['report_adj_close']
        data['pb'] = pb
        data['pe'] = pe
        data['cashcover_ratio'] = base['cashcover_ratio']
        data['pp'] = round(base['V'] / pb, 2)
        data['pp_ci0'] = round(base['V_ci0'] / pb, 2)
        data['pp_ci1'] = round(base['V_ci1'] / pb, 2)
        data['pp_adj'] = round(base['V_adj'] / pb, 2)
        data['pp_rd'] = round(base['V_rd'] / pb, 2)
        data['pp_sale'] = round(base['V_sale'] / pb, 2)
        data['pp_ebitda'] = round(base['V_ebitda'] / pb, 2)
        data['pp_comp'] = round((data['pp_sale'] + data['pp_ebitda']) / 2, 2)
        data['dpd_RR'] = round(base['dpd_V'] / pe, 2)
        data['chance_times'] = round((base['chance_times'] + 1) * base['report_adj_close'] / adj_close - 1, 2)

        adj_MP = round(adj_close * data['pp_sale'], 2)
        adj_MP = round(adj_MP, 2)
        adj_LP = round(adj_MP / 2, 2)
        adj_HP = round(2 * adj_MP, 2)

        win = (2 * (adj_close * data['pp']) - adj_close) / adj_close
        lose = ((adj_close * data['pp']) / 2 - adj_close) / adj_close
        odds_pp = round(((1 + win) * (1 + lose) - 1) * 100, 1)

        win_return2 = (base['HHP'] - adj_close) / adj_close
        lose_return2 = (base['LLP'] -adj_close) / adj_close
        odds2 = round(((1 + win_return2) * (1 + lose_return2) - 1) * 100, 1)

        LP_max = adj_LP/0.9
        HP_min = adj_HP/1.1
        LP_min = adj_LP
        HP_max = adj_HP

        win_return = (HP_min - adj_close) / adj_close
        lose_return = (LP_min - adj_close) / adj_close
        odds = round(((1 + win_return) * (1 + lose_return) - 1) * 100, 1)
        win_return = round(win_return * 100, 1)
        lose_return = round(lose_return * 100, 1)

        LP = round(adj_LP / base['adj_factor'], 2)
        MP = round(adj_MP / base['adj_factor'], 2)
        HP = round(adj_HP / base['adj_factor'], 2)


        data['LP'] = LP
        data['MP'] = MP
        data['HP'] = HP
        data['adj_LLP'] = base['LLP']
        data['adj_LP'] = adj_LP
        data['adj_MP'] = adj_MP
        data['adj_HP'] = adj_HP
        data['adj_HHP'] = base['HHP']
        data['MP_pct'] = base['MP_pct']

        data['adj_close'] = round(base['close'] * base['adj_factor'], 2)
        data['win_return'] = win_return
        data['lose_return'] = lose_return
        data['odds'] = odds
        data['odds2'] = odds2
        data['odds_pp'] = odds_pp

        data['flag'] = base['flag']
        data['step'] = base['step']
        data['holdernum_2inc'] = base['holdernum_2inc']

        position = pd.Series(index=base.index)
        nice = pd.Series(index=base.index)
        for i in range(len(base)):
            if adj_high.iloc[i] >= HP_max.iloc[i]:
                position.iloc[i] = 3
            elif HP_min.iloc[i] <= adj_high.iloc[i] < HP_max.iloc[i]:
                position.iloc[i] = 2
            elif adj_MP.iloc[i] <= adj_high.iloc[i] < HP_min.iloc[i]:
                position.iloc[i] = 1
            elif adj_low.iloc[i] <= LP_min.iloc[i]:
                position.iloc[i] = -3
            elif LP_min.iloc[i] <= adj_low.iloc[i] < LP_max.iloc[i]:
                position.iloc[i] = -2
            elif LP_max.iloc[i] <= adj_low.iloc[i] < adj_MP.iloc[i]:
                position.iloc[i] = -1
            else:
                position.iloc[i] = 0

            if (data.iloc[i]['pp'] + data.iloc[i]['pp_sale']) > 3 and data.iloc[i]['pp_adj'] > 1 and data.iloc[i]['dpd_RR'] > 1.8:
                nice.iloc[i] = 1
            elif (data.iloc[i]['pp'] + data.iloc[i]['pp_sale']) < 1 and data.iloc[i]['pp_adj'] < 0.5 and data.iloc[i]['dpd_RR'] < 1:
                nice.iloc[i] = -1
            else:
                nice.iloc[i] = 0

            data['nice'] = nice

        basis = position.diff()
        basis[basis == 0] = np.nan
        basis.fillna(method='ffill', inplace=True)

        data['position'] = position
        data['basis'] = basis

        data[data.isin([np.inf, -np.inf])] = np.nan
        data.fillna(0, inplace=True)
        # data.dropna(inplace=True)
        # new_rows = pd.concat([new_rows, data.reset_index(drop=True)], sort=False)
        if not data.empty:
            data.to_sql('fina_super', DB.engine, index=False, if_exists='append', chunksize=1000)

