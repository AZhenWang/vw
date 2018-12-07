from app.saver.logic import DB
import pandas as pd
from app.saver.tables import fields_map
from datetime import datetime, timedelta


def execute(start_date='', end_date=''):
    # start_date = '20181011'
    # trade_cal = DB.get_open_cal_date(start_date, end_date)
    # for cal_date in trade_cal['cal_date']:
    #     knn.execute(start_date=cal_date, end_date=cal_date)
    # start_date = '20181011'
    # end_date = '20181206'
    # limit = 2
    trade_cal = DB.get_open_cal_date(end_date=end_date, period=2)
    for i in range(1, len(trade_cal)):
        current_date_id = trade_cal.iloc[i]['date_id']
        last_date_id = trade_cal.iloc[i-1]['date_id']
        stocks = DB.recommend_stock_by_classifier('1', current_date_id, last_date_id)
        if not stocks.empty:
            # code_ids = stocks['code_id'].unique()
            stocks = stocks.sort_values(by='r2_score', ascending=False)
            stocks = stocks.drop_duplicates(subset='code_id', keep='first')
            data = pd.DataFrame(columns=fields_map['recommend_stocks'])
            for j in range(len(stocks)):
                code_id = stocks.iloc[j]['code_id']
                max_log = DB.get_max_r2_score(code_id, date_id=current_date_id, limit=3)

                if stocks.iloc[j]['id'] in max_log['id'].values:
                    data.loc[j] = {
                        'code_id': code_id,
                        'date_id': current_date_id,
                        'recommend_type': 'classified_v',
                        'recommend_id': stocks.iloc[j]['id'],
                        'recommend_v': stocks.iloc[j]['classifier_v']
                    }
            data.to_sql('recommend_stocks', DB.engine, index=False, if_exists='append', chunksize=1000)
