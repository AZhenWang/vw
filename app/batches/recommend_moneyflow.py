from app.saver.logic import DB
import pandas as pd
from app.common.function import send_email
from email.mime.text import MIMEText

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
    logs = DB.get_mv_moneyflows(start_date_id=start_date_id, end_date_id=end_date_id)
    msgs = []
    recommend_stocks = pd.DataFrame(columns=['ts_code', 'code_name', 'code_id', 'date', 'pct_chg', 'max2_trf2',
                                             'max6_trf2', 'trf2',  'beta_trf2', 'pv1', 'pv34', 'top'
                                             ])
    gp = logs.groupby(by='code_id')
    j = 0
    for code_id, group_data in gp:
        data_len = len(group_data)
        if data_len <= 1:
            continue
        for i in range(1, data_len):
            log = group_data.iloc[i]
            if log['net1'] > group_data.iloc[i-1]['net1'] \
                    and log['bt_times'] < 1 \
                    and log['qqb'] > group_data.iloc[i - 1]['qqb'] \
                    and log['net34'] < group_data.iloc[i - 1]['net34'] \
                    and log['pv1'] > group_data.iloc[i - 1]['pv1'] \
                    and log['pv34'] < group_data.iloc[i - 1]['pv34'] \
                    and log['trf2'] > 1 \
                    and log['beta_trf2'] > group_data.iloc[i - 1]['beta_trf2'] \
                    and log['trf2_v'] > 0 \
                    and -log['trf2_a'] < log['beta_trf2']\
                    and (pd.isnull(log['top']) or (log['pv1'] > 0 and log['pv34'] < 0)) \
                    :
                    # and log['beta_trf2'] > log['max2_trf2'] \
                content = {
                    'code_id': code_id,
                    'ts_code': log['ts_code'],
                    'code_name': log['name'],
                    'date': log['cal_date'],
                    'pct_chg': round(log['pct_chg'], 1),
                    'max2_trf2': log['max2_trf2'],
                    'max6_trf2': log['max6_trf2'],
                    'trf2': log['trf2'],
                    'beta_trf2': log['beta_trf2'],
                    'pv1': log['pv1'],
                    'pv34': log['pv34'],
                    'top': log['top'],
                }
                recommend_stocks.loc[j] = content
                j += 1

    if not recommend_stocks.empty:
        recommend_stocks.sort_values(by=['date', 'max6_trf2', 'beta_trf2'],
                                     ascending=[False, True, False], inplace=True)
        recommend_stocks.reset_index(drop=True, inplace=True)
        recommend_text = recommend_stocks.to_string(index=True)

        msgs.append(MIMEText(recommend_text, 'plain', 'utf-8'))
        send_email(subject=end_date + '换手率预测', msgs=msgs)

