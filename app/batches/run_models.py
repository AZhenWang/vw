from datetime import datetime, timedelta


def execute(start_date='', end_date=''):
    now = datetime.now()
    if not end_date or end_date >= now.strftime('%Y%m%d'):
        yesterday = (now - timedelta(1)).strftime('%Y%m%d')
        hour = now.hour
        if hour < 17:
            end_date = yesterday

    module_names = ['knn']
    for module_name in module_names:
        module = __import__('app.models.{}'.format(module_name), fromlist=['models', module_name])
        module.run(start_date, end_date)
