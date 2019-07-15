from matplotlib import pyplot as plt
from app.saver.logic import DB


def execute(start_date, end_date):
    # 测试recommend效率
    start_date = '19901219'
    end_date = '20190715'
    codes = DB.get_code_list()
    code_ids = codes['code_id']
    for code_id in code_ids:
       dailys = DB.get_code_info(code_id=code_id, start_date=start_date, end_date=end_date)
       pre_dailys = dailys.shift()
       change_place = dailys[pre_dailys['adj_factor'] != dailys['adj_factor']]
       pct_chg = round((dailys['close'] * dailys['adj_factor'] - pre_dailys['close'] * pre_dailys['adj_factor']) * 100 / (pre_dailys['close'] * pre_dailys['adj_factor']), 3)
       for i in range(1, len(change_place)):
           date_id = change_place.index[i]
           pc = pct_chg.loc[date_id]
           DB.update_pct_chg(code_id=code_id, date_id=date_id, pct_chg=pc)