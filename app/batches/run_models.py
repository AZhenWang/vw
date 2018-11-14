import app.common.function as CF


def execute(start_date='', end_date=''):
    module_names = ['knn']
    for module_name in module_names:

        module = __import__('app.models.{}'.format(module_name), fromlist=['models', module_name])
        class_name = CF.str2hump(module_name)
        class_instance = getattr(module, class_name)(start_date, end_date)
        class_instance.run()
