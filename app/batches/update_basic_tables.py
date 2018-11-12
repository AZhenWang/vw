from app.fetcher import ts


def execute(start_date='', end_date=''):
    worker = ts.Ts(start_date, end_date)
    worker.update_stock_basic()
    worker.update_trade_cal()
