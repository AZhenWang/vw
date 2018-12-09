from app.fetcher import ts


def execute(start_date='', end_date=''):
    worker = ts.Ts(start_date, end_date)

    worker.update_stock_basic()
    worker.update_trade_cal()
    worker.set_trade_dates()
    worker.set_code_list()
    if not worker.trade_dates.empty:
        ts_apis = ['daily', 'daily_basic', 'adj_factor']
        for api in ts_apis:
            worker.query(api)
