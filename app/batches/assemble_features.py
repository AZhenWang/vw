import app.common.function as CF
from datetime import datetime, timedelta


def execute(start_date='', end_date=''):
    delta = 0
    now = datetime.now()
    today = now.strftime('%Y%m%d')
    if not end_date or end_date >= today:
        hour = now.hour
        if hour < 17:
            delta = 1

    end_date = (datetime.strptime(end_date, '%Y%m%d') - timedelta(delta)).strftime('%Y%m%d')

    module_names = ['assembly']
    for module_name in module_names:
        module = __import__('app.features.{}'.format(module_name), fromlist=['features', module_name])
        class_name = CF.str2hump(module_name)
        class_instance = getattr(module, class_name)(end_date)
        class_instance.run()
