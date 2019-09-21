from app.saver.logic import DB
from app.saver.service.fina import Fina
from app.saver.tables import fields_map
import numpy as np
import pandas as pd
from app.models.finance import get_reports, fina_kpi

def execute(start_date='', end_date=''):
    """
    行业分析
    :param start_date: 公告开始日期
    :param end_date: 公告结束日期
    :return:
    """
    init_start_date = str(int(start_date) - 10000)
    # Fina.delete_logs_by_end_date(code_id, start_date=start_date, end_date=end_date, tablename='industry_sys')
    logs = Fina.get_fina_logs(industry='', start_date=init_start_date, end_date=end_date,
                                      TTB='fina_recom_logs')

    # 对年报分析指标进行行业分类统计
    logs.sort_values(by=['industry'], inplace=True)
    group_data = logs.groupby(by=['industry'])

    Fina.delete_logs_by_end_date(start_date=start_date, end_date=end_date, tablename='industry_sys')

    for industry, ind_gp in group_data:

        year_gp = ind_gp.groupby('end_date')
        data = pd.DataFrame(columns=['industry', 'end_date', 'list_num', 'total_mv_mean', 'total_mv_std', 'total_mv_Q20',
                                     'roe_mean', 'roe_std', 'roe_Q20', 'roe_mv_mean', 'roe_mv_std', 'roe_mv_Q20',
                                     'pp_mean', 'pp_sale_mean', 'dpd_RR_mean',
                                     'list_num_inc', 'total_mv_pct', 'total_mv_Q20_pct', 'roe_pct', 'roe_Q20_pct',
                                     'roe_mv_pct', 'roe_mv_Q20_pct' ])
        i = 0
        for year, gp in year_gp:
            gp_mean = gp.mean()
            gp_std = gp.std()
            gp_mean = gp_mean.apply(np.round, decimals=2)
            gp_std = gp_std.apply(np.round, decimals=2)

            data.loc[i] = {
                'industry': industry,
                'end_date': year,
                'list_num': len(gp),
                'total_mv_mean': gp_mean['total_mv'],
                'total_mv_std': gp_std['total_mv'],
                'total_mv_Q20': round(np.percentile(gp['total_mv'], 20), 2),

                'roe_mean': gp_mean['roe'],
                'roe_std': gp_std['roe'],
                'roe_Q20': round(np.percentile(gp['roe'], 20)),
                'roe_mv_mean': gp_mean['roe_mv'],
                'roe_mv_std': gp_std['roe_mv'],
                'roe_mv_Q20': round(np.percentile(gp['roe_mv'], 20), 2),
                'pp_mean': gp_mean['pp'],
                'pp_sale_mean': gp_mean['pp_sale'],
                'dpd_RR_mean': gp_mean['dpd_RR'],
                'list_num_inc': np.nan,
                'total_mv_pct': np.nan,
                'total_mv_Q20_pct': np.nan,
                'roe_pct': np.nan,
                'roe_Q20_pct': np.nan,
                'roe_mv_pct': np.nan,
                'roe_mv_Q20_pct': np.nan
            }
            i += 1
        data['list_num_inc'] = data['list_num'].diff()
        data['total_mv_pct'] = round(data['total_mv_mean'].pct_change() * 100, 1)
        data['total_mv_Q20_pct'] = round(data['total_mv_Q20'].pct_change() * 100, 1)
        data['roe_pct'] = round(data['roe_mean'].pct_change() * 100, 1)
        data['roe_Q20_pct'] = round(data['roe_Q20'].pct_change() * 100, 1)
        data['roe_mv_pct'] = round(data['roe_mv_mean'].pct_change() * 100, 1)
        data['roe_mv_Q20_pct'] = round(data['roe_mv_Q20'].pct_change() * 100, 1)
        data[data.isin([np.inf, -np.inf])] = np.nan
        # print(data)
        if not data.empty:
            data.to_sql('industry_sys', DB.engine, index=False, if_exists='append', chunksize=1000)


