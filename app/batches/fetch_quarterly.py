from app.fetcher import ts


def execute(start_date='', end_date=''):
    worker = ts.Ts(start_date, end_date)

    worker.update_stock_basic()
    worker.set_code_list()
    #
    worker.update_trade_cal()
    worker.set_trade_dates()

    if not worker.trade_dates.empty:

        ts_apis = ['income', 'cashflow', 'balancesheet']
        for api in ts_apis:
            worker.query_finance(api, report_type='1')

        ts_apis = ['income', 'cashflow']
        for api in ts_apis:
            worker.query_finance(api, report_type='2')

        ts_apis = ['fina_indicator']
        for api in ts_apis:
            worker.query_finance(api=api, need_fields=True)

        ts_apis = ['fina_audit', 'stk_holdernumber']
        for api in ts_apis:
            worker.query_finance(api=api, need_fields=True)

        ts_apis = ['express']
        for api in ts_apis:
            worker.query_finance(api=api, need_fields=True)

        ts_apis = ['fina_mainbz']
        for api in ts_apis:
            worker.query_fina_mainbz(api)

        # ts_apis = ['dividend']
        # for api in ts_apis:
        #     worker.query_by_ann_date(api)

