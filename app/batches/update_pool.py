from app.models.tp import Tp
from app.saver.logic import DB
from app.models.pca import Pca
import numpy as np
import pandas as pd
from app.saver.tables import fields_map



def execute(start_date='', end_date=''):
    """
    筛选处于大的上升趋势的股票作为关注股票，推荐的股票就在这些关注股票里选
    :param start_date:
    :param end_date:
    :return:
    """
    period = 7
    trade_cal = DB.get_open_cal_date(end_date=end_date, period=period)
    start_date_id = trade_cal.iloc[0]['date_id']
    end_date_id = trade_cal.iloc[-1]['date_id']
    DB.update_pool(start_date_id, end_date_id=end_date_id, recommend_type='rf')