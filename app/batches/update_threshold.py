from app.features.assembly import Assembly


def execute(start_date='', end_date=''):
    Assembly.update_threshold(start_date, end_date)
