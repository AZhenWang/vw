from matplotlib import pyplot as plt
from app.saver.logic import DB


def execute(start_date, end_date):
    codes = DB.get_code_list()
    code_ids = codes['code_id']
    for code_id in code_ids:
        if code_id < 2720:
            continue
        daily = DB.test_select(code_id=code_id)
        daily_prev = daily.shift()
        pct_chgs = (daily.close - daily_prev.close)/daily_prev.close * 100
        pct_chgs.fillna(value=0, inplace=True)
        pct_chgs = round(pct_chgs, 4)
        for date_id in pct_chgs.index:
            DB.test_update(code_id=code_id, date_id=date_id, pct_chg=pct_chgs.loc[date_id])


    # trade_cal = DB.get_open_cal_date(end_date=end_date, period=1)
    # end_date_id = trade_cal.iloc[0]['date_id']
    # code_list = DB.get_up_stocks_by_threshold(date_id=end_date_id)
    #
    # for code_id in code_list['code_id']:
    #     data = DB.get_code_info(code_id=code_id, end_date=end_date)
    #     adj_close = data['close'] * data['adj_factor']
    #
    #     threshold = DB.get_thresholds(code_id, end_date_id=end_date_id)
    #
    #     fig, ax = plt.subplots(1, 1, figsize=(15, 10))
    #     ax.plot(adj_close, 'b-')
    #     ax2 = ax.twinx()
    #     ax2.plot(threshold['simple_threshold_v'], 'r-')
    #
    #
    # plt.show()
