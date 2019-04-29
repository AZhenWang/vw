from app.fetcher import ts


def execute(start_date='', end_date=''):
    worker = ts.Ts(start_date, end_date)

    worker.update_index_basic()
    worker.set_index_list()

    worker.update_stock_basic()
    worker.set_code_list()

    worker.update_trade_cal()
    worker.set_trade_dates()

    if not worker.trade_dates.empty:
        idx_apis = ['index_daily', 'index_dailybasic']
        for idx_api in idx_apis:
            worker.query_index(idx_api)
        #
        ts_apis = ['daily', 'daily_basic', 'adj_factor', 'moneyflow']
        for api in ts_apis:
            worker.query(api)
