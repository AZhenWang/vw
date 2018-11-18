import app.common.function as CF
from app.saver.logic import DB
from app.models.knn import Knn


def execute(start_date='', end_date=''):
        sample_interval=12 * 20
        pre_predict_interval=5
        score_interval=60
        model = Knn(end_date=end_date, sample_interval=sample_interval, pre_predict_interval=pre_predict_interval, score_interval=score_interval)
        model.run()



