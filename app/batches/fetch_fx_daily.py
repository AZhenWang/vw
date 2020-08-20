from app.fetcher import ts


def execute(start_date='', end_date=''):
    worker = ts.Ts(start_date, end_date)

    worker.set_trade_dates(is_open='')

    # worker.update_fx_obasic()
    worker.set_fx_list()

    if not worker.trade_dates.empty:

        ts_apis = ['fx_daily']
        for api in ts_apis:
            worker.query_fx(api)



