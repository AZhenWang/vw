from app.saver.logic import DB
import pandas as pd
import numpy as np
from app.saver.tables import fields_map
from app.common.function import pack_daily_return


def execute(start_date='', end_date=''):
    """
    对假设做校验：
    流程有3个：输入、处理、输出；
    输入：把每次的假设数据按sql的形式存储
    处理：处理假设sql生成的数据, 获取推荐的股票
    输出：输出3个衡量指标：
        1、价值指标：取未来1个月的每日收益率的平均值，平均值越大，表示潜在收益越大
        2、增长指标：取未来1个月的波动率，波动越大，表示潜在收益越大
        3、beta指标：取过去1年每日收益率与大盘的相关性，代表此股票的系统风险
    :param start_date:
    :param end_date:
    :return:
    """

    Policy = {
        'p1': 'p1',
        'p2': 'p2'
    }
    # 第一步：输入数据
    abt_sqls = DB.get_abtest_sqls()

    k = 0
    new_rows = pd.DataFrame(columns=fields_map['ab_test_logs'])
    for i in range(len(abt_sqls)):
        abt_sql = abt_sqls.iloc[i]
        sql_title = abt_sql.sql_title
        sql_content = abt_sql.sql_content
        sql_params = abt_sql.sql_params
        data = DB.sql_execute(sql_content=sql_content, sql_params=sql_params)
        if data.empty:
            continue

        for fun_name, note in Policy.items():
            code_rows, recommend_code_ids = eval(fun_name)(data)
            recommend_number = len(recommend_code_ids)
            if recommend_number < 1:
                continue
            log_means = code_rows.mean()
            log_min = np.min(code_rows.s_min_rate)
            log_max = np.max(code_rows.s_max_rate)
            shot_ratio = len(code_rows[code_rows['s_max_rate'] > 30]) / recommend_number
            new_rows.loc[k] = {
                'numbers': recommend_number,
                'codes': len(np.unique(recommend_code_ids)),
                'shot_ratio': round(shot_ratio, 2),
                'm_mean': round(log_means.s_mean, 2),
                'm_std': round(log_means.s_std, 1),
                'm_max_rate': int(log_means.s_max_rate),
                'm_min_rate': int(log_means.s_min_rate),
                'max_rate': int(log_max),
                'min_rate': int(log_min),
                'note': note,
                'sql_title': sql_title,
                'sql_content': sql_content,
                'sql_params': sql_params
            }
            k += 1
    if not new_rows.empty:
        new_rows.to_sql('ab_test_logs', DB.engine, index=False, if_exists='append', chunksize=1000)


# 不设置止损止盈点，坚守一个月
def p1(data):
    code_rows = pd.DataFrame(columns=['s_mean', 's_std', 's_max_rate', 's_min_rate'])
    recommend_code_ids = data.code_id
    recommend_dates = data.cal_date
    number = len(recommend_code_ids)
    for j in range(number):
        code_id = recommend_code_ids.iloc[j]
        recommend_date = recommend_dates.iloc[j]
        daily = DB.get_code_info(code_id=code_id, start_date=int(recommend_date)+1, period=20)
        stock_prices = daily['close'] * daily['adj_factor']
        daily_return = pack_daily_return(stock_prices)
        daily_return.name = 'sdr'

        s_mean = np.mean(daily_return)
        s_std = np.std(daily_return)
        max_price = stock_prices.max()
        min_price = stock_prices.min()
        s_max_rate = (max_price - stock_prices.iloc[0]) * 100 / min(max_price, stock_prices.iloc[0])
        s_min_rate = (min_price - stock_prices.iloc[0]) * 100 / min(min_price, stock_prices.iloc[0])

        code_rows.loc[j] = {
            's_mean': s_mean,
            's_std': s_std,
            's_max_rate': s_max_rate,
            's_min_rate': s_min_rate
        }
    return code_rows, recommend_code_ids


# 推荐2次以上，不设置止损止盈点，坚守一个月
def p2(data):
    from collections import defaultdict
    code_rows = pd.DataFrame(columns=['s_mean', 's_std', 's_max_rate', 's_min_rate'])
    recommend_dates = []
    recommend_code_ids = []
    data.sort_values(by='cal_date', inplace=True)
    duplicates = defaultdict(int)
    for i in range(len(data)):
        code_id = data.iloc[i].code_id
        cal_date = data.iloc[i].cal_date
        duplicates[code_id] += 1

        if duplicates[code_id] == 2:
            recommend_code_ids.append(code_id)
            recommend_dates.append(cal_date)
    number = len(recommend_code_ids)
    for j in range(number):
        code_id = recommend_code_ids[j]
        recommend_date = recommend_dates[j]
        daily = DB.get_code_info(code_id=code_id, start_date=int(recommend_date) + 1, period=20)
        stock_prices = daily['close'] * daily['adj_factor']
        daily_return = pack_daily_return(stock_prices)
        daily_return.name = 'sdr'

        s_mean = np.mean(daily_return)
        s_std = np.std(daily_return)
        max_price = stock_prices.max()
        min_price = stock_prices.min()
        s_max_rate = (max_price - stock_prices.iloc[0]) * 100 / min(max_price, stock_prices.iloc[0])
        s_min_rate = (min_price - stock_prices.iloc[0]) * 100 / min(min_price, stock_prices.iloc[0])

        code_rows.loc[j] = {
            's_mean': s_mean,
            's_std': s_std,
            's_max_rate': s_max_rate,
            's_min_rate': s_min_rate
        }
    return code_rows, recommend_code_ids
