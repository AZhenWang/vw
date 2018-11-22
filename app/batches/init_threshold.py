from app.features.assembly import Assembly


def execute(start_date='', end_date=''):
    Assembly.init_thresholds_table(end_date)
