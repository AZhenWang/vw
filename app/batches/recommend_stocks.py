from app.saver.logic import DB
from app.orm.classified_v import classified as cr
import numpy as np
import pandas as pd
from app.saver.tables import fields_map


def execute(start_date='', end_date=''):
    # # start_date = '20181011'
    # # trade_cal
    # = DB.get_open_cal_date(start_date, end_date)
    # # for cal_date in trade_cal['cal_date']:
    # #     knn.execute(start_date=cal_date, end_date=cal_date)
    # # start_date = '20181011'
    # # end_date = '20181206'
    # # limit = 2
    # trade_cal = DB.get_open_cal_date(end_date=end_date, period=2)
    # for i in range(1, len(trade_cal)):
    #     current_date_id = trade_cal.iloc[i]['date_id']
    #     last_date_id = trade_cal.iloc[i-1]['date_id']
    #     stocks = DB.recommend_stock_by_classifier('1', current_date_id, last_date_id)
    #     if not stocks.empty:
    #         # code_ids = stocks['code_id'].unique()
    #         stocks = stocks.sort_values(by='r2_score', ascending=False)
    #         stocks = stocks.drop_duplicates(subset='code_id', keep='first')
    #         data = pd.DataFrame(columns=fields_map['recommend_stocks'])
    #         for j in range(len(stocks)):
    #             code_id = stocks.iloc[j]['code_id']
    #             max_log = DB.get_max_r2_score(code_id, date_id=current_date_id, limit=3)
    #
    #             if stocks.iloc[j]['id'] in max_log['id'].values:
    #                 data.loc[j] = {
    #                     'code_id': code_id,
    #                     'date_id': current_date_id,
    #                     'recommend_type': 'classified_v',
    #                     'recommend_id': stocks.iloc[j]['id'],
    #                     'recommend_v': stocks.iloc[j]['classifier_v']
    #                 }
    #         data.to_sql('recommend_stocks', DB.engine, index=False, if_exists='append', chunksize=1000)

    trade_cal = DB.get_open_cal_date(end_date=end_date, period=2)
    for i in range(1, len(trade_cal)):
        current_date_id = trade_cal.iloc[i]['date_id']
        last_date_id = trade_cal.iloc[i - 1]['date_id']

    code_list = DB.get_code_list(list_status='L')
    data = pd.DataFrame(columns=fields_map['recommend_stocks'])
    j = 0

    for code_id in code_list['code_id']:
        #获取前五个回报值最高的group
        top_five_group = cr.get_info(code_id=code_id,date_id=current_date_id,limit=5)
        groups = []
        info = {}
        for val in top_five_group:
            groups.append(val.feature_group_number)
            info[val.feature_group_number] = val
        #获取这五个昨天的回报值
        last_top_five_group = cr.get_info(code_id=code_id, date_id=last_date_id, limit=5,feature_group_number=groups)

        star = []
        for val in last_top_five_group:
            yester = val.classifier_v
            today = info[val.feature_group_number].classifier_v
            if yester > 0.01 and yester < today:
                star.append(yester)
        star_idx = len(star)
        if star_idx > 0:
            average = np.array(star).mean()
            data.loc[j] = {
                                'code_id': code_id,
                                'date_id': current_date_id,
                                'recommend_type': 'classified_v',
                                'star_idx': star_idx,
                                'average': average,
                            }
            data.to_sql('recommend_stocks', DB.engine, index=False, if_exists='append', chunksize=1000)
        j += 1


