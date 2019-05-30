from app.models.pca import Pca
from app.saver.logic import DB
import pandas as pd
import numpy as np
from app.saver.tables import fields_map
from app.common.function import pack_daily_return

n_components = 2
pre_predict_interval = 5
# TTB = 'daily'
# TTB = 'weekly'
TTB = 'monthly'

def execute(start_date='', end_date=''):
    """
    计算股票beta值
    :param start_date:
    :param end_date:
    :return:
    """
    trade_cal = DB.get_open_cal_date(end_date=end_date, period=244*10)
    end_date_id = trade_cal.iloc[-1].date_id
    start_date_id = trade_cal.iloc[0].date_id
    start_date = trade_cal.iloc[0].cal_date
    # start_time = time.strptime(start_date, "%Y%m%d")
    # end_time = time.strptime(end_date, "%Y%m%d")
    # codes = DB.get_latestopendays_code_list(
    #     latest_open_days=244, date_id=trade_cal.iloc[0]['date_id'], TTB=TTB)
    # code_ids = codes['code_id']
    # code_ids = [213]
    code_ids = [213, 214, 432, 583, 1436, 1551, 1591, 1605, 1711, 2423, 2551, 2597]
    # 沪深300低贝塔
    # index_code = '000829.CSI'
    index_code = '000001.SH'
    pca = Pca(cal_date=end_date)
    new_rows = pd.DataFrame(columns=fields_map['beta'])

    index_daily = DB.get_index_daily(ts_code=index_code, start_date_id=start_date_id, end_date_id=end_date_id)
    index_prices = index_daily['close']
    index_daily_return = pack_daily_return(index_prices)
    index_daily_return.name = 'idr'
    index_id = index_daily.iloc[0].index_id

    i = 0
    for code_id in code_ids:
        print('code_id=', code_id)

        daily = DB.get_code_info(code_id=code_id, start_date=start_date, end_date=end_date)
        stock_prices = daily['close'] * daily['adj_factor']
        stock_daily_return = pack_daily_return(stock_prices)
        stock_daily_return.name = 'sdr'

        std_daily_return = np.std(stock_daily_return)
        mean_daily_return = np.mean(stock_daily_return)
        print('std_daily_return=', std_daily_return, 'mean_daily_return=', mean_daily_return)

        data = pd.concat([index_daily_return, stock_daily_return], axis=1)
        data.dropna(inplace=True)
        # beta= conv(ra, rb) / var(rb)
        conv_fz = np.cov(data.sdr, data.idr)
        print('conv_fz=', conv_fz)
        fz = conv_fz[0, 1]
        print('fz=', fz)
        fm = np.var(data.idr)
        # beta = corrcoef[0, 1]
        beta = fz/fm
        new_rows.loc[i] = {
            'date_id': end_date_id,
            'code_id': code_id,
            'index_id': index_id,
            'mean_daily_return': round(mean_daily_return, 3),
            'std_daily_return': round(std_daily_return, 3),
            'beta': round(beta, 3)
        }
        i += 1
    if not new_rows.empty:
        new_rows.to_sql('beta', DB.engine, index=False, if_exists='append', chunksize=1000)