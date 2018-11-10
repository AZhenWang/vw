from app.fetcher import ts


def execute():
    worker = ts.Ts()
    worker.update_stock_basic()
