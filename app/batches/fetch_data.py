from app.fetcher import ts
from datetime import datetime, timedelta


def execute(start_date='', end_date=''):
    now = datetime.now()
    today = now.strftime('%Y%m%d')
    if not end_date or end_date >= today:
        yesterday = (now - timedelta(2)).strftime('%Y%m%d')
        hour = now.hour
        if hour < 17:
            end_date = yesterday
        else:
            end_date = today

    worker = ts.Ts(start_date, end_date)
    worker.set_code_list()
    if not worker.trade_dates.empty:
        # ts_apis = ['daily', 'daily_basic', 'adj_factor']
        ts_apis = ['adj_factor']
        for api in ts_apis:
            worker.query(api)
