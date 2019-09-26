from app.fetcher import ts


def execute(start_date='', end_date=''):
    """
    提前一个月公布新股信息，及时把新股的财务数据拉取下来，
    :param start_date:
    :param end_date:
    :return:
    """
    worker = ts.Ts(start_date, end_date)

    worker.update_new_share()
    worker.update_stock_basic()
    worker.set_code_list(list_status='N')

    worker.update_trade_cal()
    worker.set_trade_dates()

    # ts_apis = ['income', 'cashflow', 'balancesheet']
    # for api in ts_apis:
    #     worker.query_finance(api, report_type='1')
    #
    # ts_apis = ['income', 'cashflow']
    # for api in ts_apis:
    #     worker.query_finance(api, report_type='2')
    #
    # ts_apis = ['fina_indicator']
    # for api in ts_apis:
    #     worker.query_finance(api=api, need_fields=True)

    ts_apis = ['fina_audit', 'stk_holdernumber']
    for api in ts_apis:
        worker.query_finance(api=api, need_fields=True)