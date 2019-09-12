from app.saver.logic import DB
from app.saver.service.fina import Fina
import numpy as np
import pandas as pd


def execute(start_date='', end_date=''):
    """
    企业分析统计
    :param start_date: 公告开始日期
    :param end_date: 公告结束日期
    :return:
    """
    new_rows = pd.DataFrame(columns=['code_id', 'comp_type', 'end_date',
                                     'f_ann_date', 'adj_close', 'total_mv', 'holdernum', 'holdernum_inc',
                                     'roe', 'roe_sale', 'roe_mv', 'roe_std', 'roe_adj', 'roe_sale_mv', 'op_pct',
                                     'mix_op_diff',
                                     'V', 'V_adj', 'V_sale', 'V_tax', 'dpd_V', 'pp', 'pp_adj', 'pp_sale', 'pp_tax',
                                     'dpd_RR',
                                     'pe', 'pb', 'i_debt', 'share_ratio', 'IER', 'capital_turn', 'oper_pressure', 'OPM',
                                     'X1', 'X2', 'X3', 'X4', 'X5', 'Z',
                                     'dyr', 'dyr_or', 'dyr_mean',
                                     'freecash_mv', 'cash_gap', 'cash_gap_r', 'receiv_pct', 'cash_act_in',
                                     'cash_act_out', 'cash_act_rate',
                                     'equity_pct', 'fix_asset_pct', 'rev_pct',
                                     'income_rate', 'tax_rate', 'income_pct', 'tax_pct', 'tax_payable_pct',
                                     'def_tax_ratio',
                                     'dpba_of_gross', 'dpba_of_assets', 'rd_exp_or',
                                     'rev_pctmv', 'total_assets_pctmv', 'total_turn_pctmv', 'liab_pctmv',
                                     'income_pctmv', 'tax_payable_pctmv', 'equity_pctmv', 'fix_asset_pctmv',
                                     'LP', 'MP', 'HP', 'win_return', 'lose_return', 'odds', 'adj_factor',
        'flag', 'step', 'nice', 'years', 'result', 'return_yearly'])

    # codes = DB.get_code_list(list_status='')
    # code_ids = codes['code_id']
    code_ids = range(1, 500)
    # code_ids = range(2920, 3670)
    # code_ids = [3296]
    for code_id in code_ids:
        # DB.delete_code_logs(code_id, tablename='fina_recom_logs')
        logs = Fina.get_report_info(code_id=code_id, start_date=start_date, end_date=end_date, TTB='fina_sys',
                                    end_date_type='%1231%')
        logs.dropna(inplace=True)
        logs.reset_index(inplace=True, drop=True)

        Y = logs['adj_close']
        point_args = np.diff(np.where(np.diff(Y) > 0, 0, 1))
        logs = logs.join(pd.Series(point_args, name='point').shift())
        for j in range(1, len(logs)):
            log = logs.iloc[j]
            pre_log = logs.iloc[j-1]
            index = logs.index[j]
            # 先判断这个企业是不是历史表现良好
            flag = 0

            if log['liab_pctmv'] < log['rev_pctmv'] * 1.5 \
                    and log['equity_pctmv'] > 18 and log['fix_asset_pctmv'] > -10 and log['total_assets_pctmv'] > 10 and log['tax_payable_pctmv'] > 5 \
                    and log['receiv_pct'] < 20 and log['Z'] > 1.2 \
                    and log['cash_act_in'] > 8 \
                    and log['i_debt'] < 50 \
                    and log['roe_mv'] > 12 \
                    and log['roe_sale_mv'] > 15:

                flag = 1
            # 再看这个企业的业务是不是在突飞猛进
            step = 0
            if log['roe_sale'] > log['roe_sale_mv']\
                    and log['roe_mv'] > pre_log['roe_mv']\
                    and log['rev_pct'] > 18\
                    and log['rev_pctmv'] > 18 :
                step = 1

            # 三看年报性价比
            nice = 0
            if log['pp_adj'] > 1\
                    and log['pp'] > 1.3 \
                    and log['pp_sale'] > 1.3 \
                    and log['dpd_RR'] > 2:
                nice = 1
            elif log['pp_adj'] <= 0.6 \
                or log['pp'] < 1 \
                or log['pp_sale'] < 1 \
                or log['dpd_RR'] < 1:
                nice = -1

            logs.at[index, 'flag'] = flag
            logs.at[index, 'step'] = step
            logs.at[index, 'nice'] = nice

            today = logs.iloc[j]['end_date']
            later_logs = logs[logs.end_date > today]
            corner = later_logs[later_logs['point'] != 0]
            if not corner.empty:
                next_act = corner.iloc[0]
                next_close = next_act['adj_close']
                next_date = next_act['end_date']
                today_close = logs.iloc[j]['adj_close']
                result = round(next_close / today_close, 2)
                years = (int(next_date) - int(today)) / 10000
                logs.at[index, 'result'] = result
                logs.at[index, 'years'] = years
                logs.at[index, 'return_yearly'] = round((result**(1/years) - 1)*100, 2)

        new_rows = pd.concat([new_rows, logs], sort=False)
    if not new_rows.empty:
        new_rows.to_sql('fina_recom_logs', DB.engine, index=False, if_exists='append', chunksize=1000)

