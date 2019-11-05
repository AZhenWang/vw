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
    # code_ids = [2, 214,132, 3,56,63,65,78,94,103,104,110,111,115,132,138,156,171,177,198,208,213,220,239,250,256,259,284
    #     ,290,297,309,311,313,324,330,333,337,390,396,399,410,422,431,450,453,454,460,465,466,468,471,483,484,486,494,501,
    #             502,508,518,525,532,556,560,563,566,571,573,582,589,600,609,626,629,630,637,662,683,687,690,710,714,716,
    #             725,738,750,757,780,789,795,804,812,823,824,828,833,845,849,853,856,862,870,871,872,874,875,885,887,895,
    #             896,898,900,905,913,916,918,939,960,1012,1013,1024,1028,1043,1046,1053,1059,1063,1065,1076,1094,1105,1106,
    #             1110,1115,1128,1131,1137,1142,1145,1150,1155,1170,1187,1196,1200,1206,1208,1211,1229,1239,1256,1258,1262,1263,
    #             1269,1273,1277,1278,1279,1313,1321,1322,1328,1342,1345,1347,1348,1353,1355,1360,1362,1368,1376,1383,1386,1389,
    #             1392,1398,1399,1402,1409,1413,1420,1422,1424,1432,1437,1443,1464,1468,1489,1490,1491,1492,1501,1503,1504,1514,
    #             1517,1524,1525,1529,1540,1553,1556,1563,1575,1580,1581,1585,1587,1595,1597,1614,1619,1620,1624,1625,1627,1628,
    #             1635,1636,1648,1654,1670,1675,1679,1695,1699,1707,1728,1738,1739,
    #             3839, 3843, 3842,]
    # code_ids = [494, 1142]
    # code_ids = [3863]
    # 智飞生物：1504
    # 康泰生物： 1975
    # 沃森生物： 1524
    # 易尚展示： 1209
    # code_ids = [1376, 3304, 1707, 1348, 2057, 214, 2, 132]
    # code_ids = [494,1670,1785,1818,2057]
    # code_ids = [1348, 1376, 3304, 1707]
    # code_ids = range(3807, 3820)
    # code_ids = range(1877, 3868)
    code_ids = range(3709, 3868)
    # code_ids = range(1, 3868)
    # 1648：尔康制药，2352：恒瑞制药, 2543:华海药业

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

