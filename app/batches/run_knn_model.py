from app.saver.logic import DB
from app.models.knn import Knn
import json


def execute(start_date='', end_date=''):
    print('start_date=', start_date)
    print('end_date=', end_date)

    classifiers = DB.get_classifiers(classifier_type='knn')
    if not classifiers.empty:
        for classifier_id, params in classifiers[['id', 'params']].values:
            classifier_params = json.loads(params)
            sample_interval = classifier_params['sample_interval']
            sample_interval= 244*3
            pre_predict_interval = classifier_params['pre_predict_interval']
            memory_size = classifier_params['memory_size']

            model = Knn(
                      end_date=end_date,
                      classifier_id=classifier_id,
                      sample_interval=sample_interval,
                      pre_predict_interval=pre_predict_interval,
                      memory_size=memory_size,
            )
            model.run()


