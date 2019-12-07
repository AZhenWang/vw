from app.saver.logic import DB
from app.saver.service.fina import Fina
import numpy as np
import pandas as pd


def execute(start_date='', end_date=''):
    """
    企业分析统计
    :param start_date: 公告开始日期
    :param end_date: 公告结束日期
    :return:
    """
    new_rows = pd.DataFrame(columns=['code_id', 'date_id', 'comp_type', 'end_date',
                                     'total_mv',  'holdernum', 'holdernum_inc', 'holdernum_2inc', 'holder_unit',
                                     'roe',  'roe_rd', 'roe_sale', 'roe_mv', 'roe_std', 'roe_adj', 'roe_rd_mv', 'roe_sale_mv', 'op_pct',
                                     'mix_op_diff',
                                     'V', 'V_adj', 'V_rd', 'V_sale', 'V_tax', 'dpd_V',
                                     'chance_times', 'pp', 'pp_rd', 'pp_adj', 'pp_sale', 'pp_tax',
                                     'dpd_RR',
                                     'pe', 'pb', 'cashcover_ratio', 'EE', 'i_debt', 'share_ratio', 'IER',
                                     'oth_receiv_rate', 'money_cap_pct', 'st_borr_pct', 'st_borr_rate', 'capital_turn', 'oper_pressure', 'OPM',
                                     'Z',
                                     'dyr', 'dyr_or', 'dyr_mean',
                                     'freecash_mv', 'cash_gap', 'cash_gap_r', 'receiv_pct', 'receiv_rate', 'cash_act_in',
                                     'cash_act_out', 'cash_act_rate',
                                     'equity_pct', 'equity_pct_yearly', 'fix_asset_pct', 'revenue', 'rev_pct', 'rev_pct_yearly', 'share_pct', 'gross_rate',
                                     'income_rate', 'tax_rate', 'income_pct', 'tax_pct', 'tax_payable_pct',
                                     'def_tax_ratio',
                                     'dpba_of_gross', 'dpba_of_assets', 'rd_exp_or',
                                     'rev_pctmv', 'total_assets_pctmv', 'liab_pctmv',
                                     'income_pctmv', 'tax_payable_pctmv', 'equity_pctmv', 'fix_asset_pctmv',
                                     'LLP', 'LP', 'MP', 'HP', 'HHP', 'MP_pct',  'adj_close', 'price_pct',
                                     'win_return', 'lose_return', 'odds', 'win_return2', 'lose_return2', 'odds2', 'odds_pp',
                                     'adj_factor',
        'flag', 'step', 'nice', 'years', 'result', 'return_yearly'])

    # codes = DB.get_code_list(list_status='')
    # code_ids = codes['code_id']
    # code_ids = range(1, 500)
    # code_ids = range(2920, 3670)
    # code_ids = [2352, 1587, 1348, 2119, 1376, 3304, 1383, 2074, 2057]
    # code_ids = range(1, 3893)
    code_ids = [3, 56, 63, 65, 78, 94, 103, 104, 110, 111, 115, 132, 138, 156, 171, 177, 198, 208, 213, 220, 239, 250,
                256, 284, 290, 297, 309, 311, 313, 324, 330, 333, 337, 390, 396, 399, 410, 422, 431, 450, 453, 454, 460,
                465, 466, 468, 471, 483, 484, 486, 494, 501, 502, 508, 518, 525, 532, 556, 560, 563, 566, 571, 573, 582,
                589, 600, 609, 626, 629, 630, 637, 662, 683, 687, 690, 710, 714, 716, 738, 750, 757, 780, 789, 795, 804,
                812, 823, 824, 828, 833, 845, 849, 853, 856, 862, 870, 871, 872, 874, 875, 885, 887, 895, 896, 898, 900,
                905, 913, 916, 918, 939, 960, 725,
                1012, 1013, 1024, 1028, 1043, 1046, 1053, 1059, 1063, 1065, 1076, 1094, 1105, 1106, 1110, 1115, 1128,
                1131, 1137, 1142, 1145, 1150, 1155, 1170, 1187, 1196, 1200, 1206, 1208, 1211, 1229, 1239, 1256, 1258,
                1262, 1263, 1269, 1273, 1277, 1278, 1279, 1313, 1321, 1322, 1328, 1342, 1345, 1347, 1348, 1353, 1355,
                1360, 1362, 1368, 1376, 1383, 1386, 1389, 1392, 1398, 1399, 1402, 1409, 1413, 1420, 1422, 1424, 1432,
                1437, 1443, 1464, 1468, 1489, 1490, 1491, 1492, 1501, 1503, 1504, 1514, 1517, 1524, 1525, 1529, 1540,
                1553, 1556, 1563, 1575, 1580, 1581, 1585, 1587, 1595, 1597, 1614, 1619, 1620, 1624, 1625, 1627, 1628,
                1635, 1636, 1648, 1654, 1670, 1675, 1679, 1695, 1699, 1707, 1728, 1738, 1739, 1743, 1766, 1775, 1783,
                1784, 1785, 1791, 1808, 1813, 1815, 1816, 1818, 1825, 1831, 1832, 1838, 1842, 1860, 1861, 1864, 1866,
                1876, 1884, 1898, 1907, 1908, 1912, 1913, 1939, 1944, 1949, 1958, 1959, 1961, 1969, 1970, 1975, 1984,
                2003, 2006, 2009, 2010, 2012, 2014, 2015, 2025, 2027, 2048, 2049, 2055, 2056, 2057, 2071, 2074, 2076,
                2091, 2093, 2095, 2097, 2119, 2157, 2164, 2176, 2179, 2180, 2181, 2186, 2191, 2226, 2231, 2236, 2251,
                2252, 2283, 2288, 2296, 2300, 2306, 2311, 2333, 2344, 2349, 2350, 2352, 2357, 2361, 2372, 2374, 2381,
                2390, 2398, 2399, 2402, 2415, 2429, 2439, 2441, 2442, 2465, 2471, 2473, 2481, 2506, 2514, 2536, 2540,
                2543, 2550, 2551, 2555, 2572, 2580, 2600, 2606, 2624, 2629, 2643, 2652, 2668, 2673, 2720, 2721, 2745,
                2749, 2760, 2764, 2768, 2777, 2783, 2804, 2832, 2853, 2910, 2925, 2989, 2992, 2586,
                3834, 3828, 3827, 3826, 3050, 3114, 3121, 3135, 3149, 3161, 3164, 3165, 3186, 3192, 3199, 3209, 3215,
                3220, 3230, 3234, 3236, 3242, 3243, 3255, 3257, 3262, 3291, 3295, 3304, 3315, 3334, 3340, 3346, 3347,
                3352, 3353, 3355, 3360, 3390, 3394, 3395, 3403, 3404, 3409, 3422, 3430, 3432, 3466, 3485, 3496, 3497,
                3499, 3508, 3515, 3519, 3526, 3531, 3537, 3539, 3542, 3543, 3549, 3557, 3664, 3682, 3684, 3692, 3699,
                3702, 3704, 3718, 3731, 3734, 3753, 3759, 3780, 3793, 3794, 3810, 3822, 3835, 3839, 3842, 3843, 3844,
                3849, 3860, ]

    # code_ids = range(3873, 3887)
    # code_ids = range(3559, 3670)
    # code_ids = [2381]
    # code_ids = [1348,2761,2352,214, 1648, 161]
    # code_ids = [1398, 2760,1386,1861,1707,2352,885,2344,2543,1524,1504,1975,2283,1392,1563,3242,2095,3192,3682,1273,1743,3315,1728,1783,]
    # code_ids = range(3800, 3820)
    start_date_id = DB.get_date_id(start_date)
    end_date_id = DB.get_date_id(end_date)

    for code_id in code_ids:
        print('code_id=', code_id)
        DB.delete_code_logs(code_id, tablename='fina_recom_logs')
        logs = Fina.get_report_info(code_id=code_id, start_date_id=start_date_id, end_date_id=end_date_id, TTB='fina_sys')
        logs = logs.dropna(subset=['total_mv', 'adj_close'])
        logs.fillna(0, inplace=True)
        if logs.empty:
            continue
        logs['price_pct'] = round(logs['adj_close'].pct_change() * 100, 2)
        logs.reset_index(inplace=True, drop=True)
        audits = Fina.get_report_info(code_id=code_id, start_date_id=start_date_id, end_date_id=end_date_id, TTB='fina_audit')
        logs = logs.join(audits[['end_date', 'audit_result', 'audit_fees', 'audit_agency']].set_index('end_date'), on='end_date')
        Y = logs['adj_close']
        point_args = np.diff(np.where(np.diff(Y) > 0, 0, 1))
        logs = logs.join(pd.Series(point_args, name='point').shift())
        for j in range(len(logs)):
            log = logs.iloc[j]
            index = logs.index[j]
            # 先判断这个企业是不是历史表现良好
            flag = 0

            if log['liab_pctmv'] < log['rev_pctmv'] * 2 \
                    and log['equity_pctmv'] > 10 and log['tax_payable_pct'] > 0 \
                    and log['receiv_pct'] < 25 and log['Z'] > 1 \
                    and log['cash_act_in'] > 8 \
                    and log['cash_act_out'] > 0 \
                    and log['i_debt'] < 50 \
                    and log['roe_mv'] > 10 \
                    and log['roe_sale_mv'] > 10\
                    and log['IER'] > 8\
                    and log['oth_receiv_rate'] < 10:
                flag = 1

            elif log['receiv_pct'] > 25 or log['Z'] < 1 \
                    or log['cash_act_in'] < 0 \
                    or log['cash_act_out'] < 0 \
                    or (log['i_debt'] > 40 and log['IER'] < 8) \
                    or log['i_debt'] > 60 \
                    or log['roe_mv'] < 10 \
                    or log['IER'] < 4\
                    or log['oth_receiv_rate'] > 20 \
                    or log['st_borr_rate'] > 40 \
                    or (log['st_borr_pct'] > 50 and log['money_cap_pct'] > 50):
                flag = -1
            # 再看这个企业的业务是不是在突飞猛进
            step = 0
            if j >= 1:
                pre_roe = logs.iloc[j-1]['roe']
                pre_total_turn = logs.iloc[j - 1]['total_turn']
                pre_gross_rate = logs.iloc[j - 1]['gross_rate']
            else:
                pre_roe = log['roe_mv']
                pre_total_turn = log['total_turn']
                pre_gross_rate = log['gross_rate']

            # if (log['roe_sale'] >= log['roe_sale_mv'] or log['roe_sale'] >= pre_roe_sale or log['roe_mv'] > pre_roe_mv)\
            #         and (log['roe'] >= log['roe_mv'] or log['roe'] >= pre_roe or log['roe_sale_mv'] > pre_roe_sale_mv)\
            #         and log['capital_turn'] > pre_capital_turn:
            #     step = 1

            # if log['roe'] > pre_roe and log['roe_sale'] > log['roe_sale_mv'] \
            #     and log['rev_pct_yearly'] > 10 \
            #     and log['capital_turn'] > pre_capital_turn:
            #
            #     step = 1
            # elif log['roe'] < pre_roe \
            #         and log['rev_pct_yearly'] < 0 \
            #         and log['capital_turn'] < pre_capital_turn:
            #     step = -1

            if log['roe'] - pre_roe > -1 \
                    and log['gross_rate'] > pre_gross_rate\
                    and log['total_turn'] > pre_total_turn:

                step = 1
            elif log['roe'] - pre_roe > -1 \
                    and log['gross_rate'] < pre_gross_rate\
                    and log['total_turn'] < pre_total_turn:
                step = -1

            # 三看年报性价比
            nice = 0
            if (log['pp'] + log['pp_rd']) > 3 \
                    and log['dpd_RR'] > 1.8:
                nice = 1
            elif (log['pp'] + log['pp_rd']) < 1 \
                or log['dpd_RR'] < 1:
                nice = -1
            holdernum_2inc = log['holdernum_inc']
            if j >= 1 and logs.iloc[j-1]['holdernum_inc'] * holdernum_2inc > 0:
                holdernum_2inc += logs.iloc[j-1]['holdernum_inc']
            if log['holdernum'] > 0:
                holder_unit = round(log['total_mv'] / log['holdernum'], 1)
            else:
                holder_unit = np.nan
            logs.at[index, 'flag'] = flag
            logs.at[index, 'step'] = step
            logs.at[index, 'nice'] = nice
            logs.at[index, 'holder_unit'] = holder_unit
            logs.at[index, 'price_pct'] = log['price_pct']
            logs.at[index, 'holdernum_2inc'] = holdernum_2inc

            today = logs.iloc[j]['end_date']
            later_logs = logs[logs.end_date > today]
            corner = later_logs[later_logs['point'] != 0]
            if not corner.empty:

                next_act = corner.iloc[0]
                next_close = next_act['adj_close']
                next_date = next_act['end_date']
                today_close = logs.iloc[j]['adj_close']

                result = round(next_close / today_close, 2)
                years = np.ceil((int(next_date) - int(today)) / 10000)
                logs.at[index, 'result'] = result
                logs.at[index, 'years'] = years
                logs.at[index, 'return_yearly'] = round((result**(1/years) - 1)*100, 2)
        new_rows = pd.concat([new_rows, logs], sort=False)
        # print(new_rows[['end_date', 'adj_close', 'holder_unit', 'price_pct', 'holdernum_inc', 'holdernum_2inc', 'flag', 'step', 'nice', 'result', 'years', 'return_yearly', ]])
        # os.ex
    new_rows[new_rows.isin([np.inf, -np.inf])] = np.nan
    if not new_rows.empty:
        new_rows.to_sql('fina_recom_logs', DB.engine, index=False, if_exists='append', chunksize=1000)

