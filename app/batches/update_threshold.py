from app.saver.logic import DB
from app.models.knn import Knn
from app.features.assembly import Assembly
import json


def execute(start_date='', end_date=''):

    # 先更新threshold表
    print('start_date=', start_date)
    print('end_date=', end_date)
    Assembly.update_threshold_by_date(start_date, end_date)


