from app.saver.logic import DB
from app.saver.service.fina import Fina
from app.saver.tables import fields_map
import numpy as np
import pandas as pd


def execute(start_date='', end_date=''):
    """
    企业分析
    :param start_date: 公告开始日期
    :param end_date: 公告结束日期
    :return:
    """
    date_id = DB.get_date_id(start_date)
    # codes = DB.get_latestopendays_code_list(
    #     latest_open_days=244, date_id=date_id)

    # code_ids = codes['code_id']
    code_ids = [378, 214]
    for code_id in code_ids:
        DB.delete_comp_sys_logs(code_id, start_date, end_date)

        incomes = Fina.get_report_info(code_id=code_id, start_date=start_date, end_date=end_date, TTB='income', report_type='1', end_date_type='%1231')
        balancesheets = Fina.get_report_info(code_id=code_id, start_date=start_date, end_date=end_date, TTB='balancesheet', report_type='1', end_date_type='%1231')
        cashflows = Fina.get_report_info(code_id=code_id, start_date=start_date, end_date=end_date, TTB='cashflow', report_type='1', end_date_type='%1231')

        balancesheets = balancesheets[balancesheets['end_date'].isin(incomes.end_date)]
        cashflows = cashflows[cashflows['end_date'].isin(incomes.end_date)]
        if cashflows.empty or balancesheets.empty:
            return
        # 一、好生意
        # 1、销售毛利率: （营业收入-营业成本）/ 营业收入
        gross_profit_ratio = round((incomes['revenue'] - incomes['oper_cost']) * 100 / incomes['revenue'], 1)
        gross_profit_ratio.name = 'gross_profit_ratio'

        # 二、好管理团队
        # 1、成本费用利润率： （营业总收入-营业总成本）/营业总成本
        cogs_profit_rate = round((incomes['total_revenue'] - incomes['total_cogs']) * 100 / incomes['total_cogs'], 1)
        cogs_profit_rate.name = 'cogs_profit_rate'
        print(gross_profit_ratio, cogs_profit_rate)

        # 2、以销定产: 销量增速 / 存货增速 > 1
        # inventories_opercost_ratio = round((incomes['oper_cost'].diff() - balancesheets['inventories'].diff()) * 100 / incomes['oper_cost'].diff(), 1)
        # inventories_opercost_ratio.name = 'inventories_opercost_ratio'

        # 3、有息负债率: 有息负债/平均总资产
        # int_debt = balancesheets['total_cur_liab'] - balancesheets['notes_payable'] - balancesheets['acct_payable']\
        #            - balancesheets['adv_receipts'] - balancesheets['payroll_payable']- balancesheets['taxes_payable'] \
        #            - balancesheets['oth_payable']
        #
        # int_debt_ratio = round(int_debt * 2 * 100 / balancesheets['total_assets'], 1)
        # int_debt_ratio.fillna(method='ffill', inplace=True)
        # int_debt_ratio.name = 'int_debt_ratio'

        # 4、资金周转率：平均准备期+平均收账期-平均付账期
        # 存货周转天数：360*存货平均余额/主营成本
        inventories_days = 360 * balancesheets['inventories'] / incomes['oper_cost']
        # 应收账款周转天数：360×应收账款周转率=平均应收账款×360天/销售收入=平均应收账款/平均日销售额
        accounts_receiv_days = 360 * balancesheets['accounts_receiv'] /incomes['revenue']
        # 预收账款周转天数：360 * 预收平均余额/主营收入
        adv_receipts_days = 360 * balancesheets['adv_receipts'] /incomes['revenue']
        # 资金周转天数=存货周转天数+应收账款周转天数-预收账款周转天数
        capital_turnover_days = inventories_days + accounts_receiv_days - adv_receipts_days
        capital_turnover = round((360 - capital_turnover_days) * 100 / 360, 1)
        capital_turnover.name = 'capital_turnover'
        # 三、好结果
        # 1、净资产销售报酬率: 销售利润/年平均股东权益
        equity_return = round((incomes['revenue'] - incomes['oper_cost']) * 100 / balancesheets['total_hldr_eqy_inc_min_int'], 1)
        equity_return.name = 'equity_return'
        # 2、总资产报酬率: 净利润/年平均总资产
        asset_return = round(incomes['n_income'] * 100 / balancesheets['total_assets'], 1)
        asset_return.name = 'asset_return'
        new_rows = pd.DataFrame(columns=fields_map['recommend_stocks'])

        # 四、正常的现金流
        cashflows.fillna(value=0, inplace=True)
        cashflow_ratio = round(cashflows['n_cashflow_act'] * 100 / (abs(cashflows['n_cashflow_act']) + abs(cashflows['n_cash_flows_fnc_act']) + abs(cashflows['n_cashflow_inv_act'])), 1)
        cashflow_ratio.name = 'cashflow_ratio'

        # 五、以上比值之和
        ratio_sum = round((gross_profit_ratio + cogs_profit_rate + cashflow_ratio + capital_turnover + equity_return + asset_return)/7, 1)
        ratio_sum.name = 'ratio_sum'

        new_rows = pd.concat([incomes.end_date, gross_profit_ratio, cogs_profit_rate, cashflow_ratio,
                          capital_turnover, equity_return, asset_return, ratio_sum], axis=1)
        new_rows['code_id'] = code_id
        if not new_rows.empty:
            new_rows.to_sql('comp_sys', DB.engine, index=False, if_exists='append', chunksize=1000)
