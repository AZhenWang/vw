import app.common.function as CF
from app.saver.logic import DB
from app.models.knn import Knn


def execute(start_date='', end_date=''):
        classifier_id = '1'
        model = Knn(end_date=end_date, classifier_id=classifier_id)
        model.run()



