from app.features.assembly import Assembly


def execute(start_date='', end_date=''):
    Assembly.init_features_table()
