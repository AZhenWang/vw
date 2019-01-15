from app.models.pca import Pca
from app.saver.logic import DB


def execute(start_date='', end_date=''):
    pca = Pca(cal_date=end_date)

    # codes = DB.get_recommended_stocks(cal_date=end_date)
    # pca.run(codes=codes['code_id'])

    focus_codes = DB.get_focus_stocks()
    pca.run(codes=focus_codes['code_id'])
