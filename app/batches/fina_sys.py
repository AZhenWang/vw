from app.saver.logic import DB
from app.saver.service.fina import Fina
from app.saver.tables import fields_map
import numpy as np
import pandas as pd
from app.models.finance import get_reports, fina_kpi

def execute(start_date='', end_date=''):
    """
    企业分析
    :param start_date: 公告开始日期
    :param end_date: 公告结束日期
    :return:
    """
    start_date_id = DB.get_date_id(start_date)
    end_date_id = DB.get_date_id(end_date)
    # codes = DB.get_latestopendays_code_list(
    #     latest_open_days=244*2, date_id=end_date_id)
    # code_ids = codes['code_id']
    # new_rows = pd.DataFrame(columns=fields_map['fina_sys'])
    # code_ids = [1398,2760,1386,1861,1707,2352,885,2344,2543,1524,1504,1975,2283,1392,1563,3242,2095,3192,3682,1273,1743,3315,1728,1783,]
    # code_ids = [3840, 1348, 2352, 3304, 1376, 2057, 494]
    code_ids = [3, 56, 63, 65, 78, 94, 103, 104, 110, 111, 115, 132, 138, 156, 171, 177, 198, 208, 213, 220, 239, 250,
                256, 284, 290, 297, 309, 311, 324, 330, 333, 337, 390, 396, 399, 410, 422, 431, 450, 453, 454, 460,
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

    # code_ids = [1348,2761,2352,214, 1648, 161]
    # 智飞生物：1504
    # 康泰生物： 1975
    # 沃森生物： 1524
    # 易尚展示： 1209
    # code_ids = [2352, 1587, 1348, 2119, 1376, 3304, 1383, 2074, 2057]
    # code_ids = [214, 2381, 1348, 2352]
    # code_ids = [2352,1348,1907,1738,1969,2057]
    # 迈瑞医疗：2119， 理邦仪器：1587， 昂利康：1383， 天宇股份：2074， 2057：艾德
    # code_ids = range(3873, 3887)
    # code_ids = [1376, 3304, 1707, 1348, 2057, 214, 2, 132]
    # code_ids = [494,1670,1785,1818,2057]
    # code_ids = [1383, 1348, 1376, 3304, 2057]
    # code_ids = [2352, 1587, 1348, 2119, 1376, 3304, 1383, 2074, 2057,3394]
    # code_ids = [3304, 2344, 1376, 1348, 2057, 2074]
    # code_ids = [2352, 1587, 1348, 2119, 1376, 3304, 1383, 2074, 2057,3394]
    # code_ids = range(3807, 3820)
    # code_ids = range(1877, 3868)
    # code_ids = range(3709, 3868)
    # code_ids = range(1, 3982)
    # 1648：尔康制药，2352：恒瑞制药, 2543:华海药业
    # 大博医疗：1348， '002901.SZ', 明德生物：1376，'002932.SZ', 基蛋生物：3304，'603387.SH',
    #  安图生物：3394: '603658.SH'，华大基因：2048， '300676.SZ', 万孚生物：1861，'300482.SZ'，
    #  理邦仪器：1587：300206.SZ， 艾德生物：2057：300685.SZ
    code_ids = [3340]
    # code_ids = [2074]
    for ci in code_ids:
        print('code_id=', ci)
        Fina.delete_logs_by_date_id(code_id=ci, start_date_id=start_date_id, end_date_id=end_date_id, tablename='fina_sys')
        incomes, balancesheets, cashflows, fina_indicators, holdernum, code_info, cash_divs = get_reports(ci, start_date_id=start_date_id, end_date_id=end_date_id)
        if incomes.empty or balancesheets.iloc[0]['comp_type'] != 1:
            continue

        data = fina_kpi(incomes, balancesheets, cashflows, fina_indicators, holdernum, code_info, cash_divs)
        if data.empty:
            continue
        data['comp_type'] = balancesheets.iloc[0]['comp_type']
        data['code_id'] = ci
        data['end_date'] = data.index
        data[data.isin([np.inf, -np.inf])] = np.nan
        # new_rows = pd.concat([new_rows, data.reset_index(drop=True)], sort=False)
        if not data.empty:
            data.to_sql('fina_sys', DB.engine, index=False, if_exists='append', chunksize=1000)

