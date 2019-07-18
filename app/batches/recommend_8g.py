from app.saver.logic import DB
import pandas as pd
from app.common.function import send_email
from email.mime.text import MIMEText
import numpy as np
from app.saver.common import Base as BS

recommend_type = 'moneyflow'


def execute(start_date='', end_date=''):
    """
    心得：
        在冲破年线压力之前，pv_elg 和 pv_lg 都大于0是必须条件，因为还有套牢盘在卖，股价如果要继续上涨，必须有大资金源源不断的支持。
        一旦冲破年线压力位，上方没有套牢盘，此时小散进去都是盈利的，大资金此时可以任意出逃，有发疯的小散接盘，股价依然会冲冲上涨，当小散的资金变缓时，此时卖出信号
    :param start_date:
    :param end_date:
    :return:
    """
    trade_cal = DB.get_open_cal_date(start_date=start_date, end_date=end_date)
    pre_trade_cal = DB.get_open_cal_date(end_date=start_date, period=3)
    end_date_id = trade_cal.iloc[-1]['date_id']
    start_date_id = pre_trade_cal.iloc[0]['date_id']
    # code_id = 3475
    # codes = [2772]
    # codes = [530]
    codes = range(1, 100)
    recommend_stocks = pd.DataFrame(columns=['date',
                                             # 'd_qqb', 'd_g', 'd_diff', 'd_p', 'd_b',
                                             'qqb', 'g', 'diff', 'p', 'b',
                                             'pre_qqb', 'pre_g', 'pre_p', 'pre_b',
                                             'm_qqb', 'm_g', 'm_diff', 'm_p', 'm_b',
                                             'pre_m_p', 'pre_m_b',
                                             'max', 'min'
                                             ])
    j = 0
    for code_id in codes:
        logs = BS.get_table_logs(code_id=code_id, start_date_id=start_date_id, end_date_id=end_date_id, table_name='YY8G')
        msgs = []

        gp = logs.groupby(by=['code_id'])
        for code_id, group_data in gp:
            gp2 = group_data.groupby(by='TTB')
            w_logs = pd.Series()
            m_logs = pd.Series()
            d_logs = pd.Series()
            for T, group_data2 in gp2:
                if T == 'weekly':
                    w_logs = group_data2
                    w_logs.set_index('date_id', inplace=True)
                elif T == 'monthly':
                    m_logs = group_data2
                    m_logs.set_index('date_id', inplace=True)
                # elif T == 'daily':
                #     d_logs = group_data2
                #     d_logs.set_index('date_id', inplace=True)
            if w_logs.empty:
                continue

            for i in range(1, len(w_logs)):
                log = w_logs.iloc[i]
                pre_log = w_logs.iloc[i-1]
                date_id = w_logs.index[i]

                pre_m_logs = m_logs[m_logs.index <= date_id]
                pre_d_logs = d_logs[d_logs.index <= date_id]
                if pre_m_logs.empty or pre_d_logs.empty or len(pre_m_logs) < 2:
                    continue
                else:
                    m_log = pre_m_logs.iloc[-1]
                    pre_m_log = pre_m_logs.iloc[-2]
                    d_log = pre_d_logs.iloc[-1]

                # if log['bottom'] > pre_log['bottom'] \
                #     and pre_m_log['bottom'] == 9 \
                #     and m_log['bottom'] > pre_m_log['bottom']:

                if log['qqb'] > pre_log['qqb'] >= 0 \
                    and log['bottom'] >= 10 \
                    and log['g_number'] >= 36 \
                    and pre_log['peak'] >= 60 \
                    and pre_log['bottom'] < 10 \
                    and m_log['bottom'] > pre_m_log['bottom'] \
                    :

                    dailys = DB.get_code_dailys(code_id=code_id, start_date_id=date_id, period=20*6)
                    min_price = (dailys['low'] * dailys['adj_factor']).min()
                    max_price = (dailys['high'] * dailys['adj_factor']).max()
                    price = dailys.iloc[0]['close'] * dailys.iloc[0]['adj_factor']
                    next5_min = round((min_price - price) * 100 / price)
                    next5_max = round((max_price - price) * 100 / price)
                    content = {
                        # 'code_id': code_id,
                        'date':  dailys.iloc[0]['cal_date'],
                        # 'd_qqb': d_log.qqb,
                        # 'd_g': d_log.g_number,
                        # 'd_diff': d_log.diff_g,
                        # 'd_p': d_log.peak,
                        # 'd_b': d_log.bottom,

                        'qqb': log['qqb'],
                        'g': log['g_number'],
                        'diff': log['diff_g'],
                        'p': log['peak'],
                        'b': log['bottom'],

                        'pre_qqb': pre_log['qqb'],
                        'pre_g': pre_log['g_number'],
                        'pre_p': pre_log['peak'],
                        'pre_b': pre_log['bottom'],

                        'm_qqb': m_log.qqb,
                        'm_g': m_log.g_number,
                        'm_diff': m_log.diff_g,
                        'm_p': m_log.peak,
                        'm_b': m_log.bottom,

                        'pre_m_p': pre_m_log.peak,
                        'pre_m_b': pre_m_log.bottom,

                        'max': next5_max,
                        'min': next5_min,

                    }
                    recommend_stocks.loc[j] = content
                    j += 1

    if not recommend_stocks.empty:
        recommend_stocks.sort_values(by=['min'],
                                     ascending=[True], inplace=True)
        recommend_stocks.reset_index(drop=True, inplace=True)
        # print(recommend_stocks)
        # os.ex
        recommend_text = recommend_stocks.to_string(index=True)

        msgs.append(MIMEText(recommend_text, 'plain', 'utf-8'))
        send_email(subject=end_date + '换手率预测', msgs=msgs)

