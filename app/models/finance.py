from app.saver.service.fina import Fina
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
from app.saver.logic import DB
from app.common.function import get_rolling_mean, get_ratio


def get_reports(code_id, start_date='19900101', end_date='20190801'):
    incomes = Fina.get_report_info(code_id=code_id, start_date=start_date, end_date=end_date, TTB='income',
                                   report_type='1', end_date_type='%1231')

    if incomes.empty:
        nan_series = pd.Series()
        return nan_series, nan_series, nan_series, nan_series, nan_series, nan_series

    dividends = Fina.get_divdends(code_id=code_id, start_date=start_date, end_date=end_date)

    dividends['div_count'] = dividends['total_share'] * dividends['cash_div_tax']
    dividends.div_count.astype = 'float64'
    dividends.fillna(0)
    cash_divs = pd.Series(index=incomes['end_date'], name='cash_divs')
    dividends.set_index('end_date', inplace=True)
    for i in range(len(incomes)):
        ed = incomes.iloc[i]['end_date']
        sd = (datetime.strptime(ed, '%Y%m%d') - timedelta(days=364)).strftime('%Y%m%d')
        current_div = np.float(dividends.loc[sd:ed]['div_count'].sum())
        cash_divs.loc[ed] = current_div
    balancesheets = Fina.get_report_info(code_id=code_id, start_date=start_date, end_date=end_date, TTB='balancesheet',
                                         report_type='1', end_date_type='%1231')
    cashflows = Fina.get_report_info(code_id=code_id, start_date=start_date, end_date=end_date, TTB='cashflow',
                                     report_type='1', end_date_type='%1231')
    fina_indicators = Fina.get_report_info(code_id=code_id, start_date=start_date, end_date=end_date,
                                           TTB='fina_indicator', end_date_type='%1231')
    mainbzs = Fina.get_report_info(code_id=code_id, start_date=start_date, end_date=end_date, TTB='fina_mainbz',
                                   end_date_type='%1231')
    daily_basics = DB.get_table_logs(code_id=code_id, start_date_id=incomes.iloc[0]['date_id'],
                                     end_date_id=incomes.iloc[-1]['date_id'], table_name='daily_basic')
    dailys = DB.get_table_logs(code_id=code_id, start_date_id=incomes.iloc[0]['date_id'],
                               end_date_id=incomes.iloc[-1]['date_id'], table_name='daily')
    adj_factors = DB.get_table_logs(code_id=code_id, start_date_id=incomes.iloc[0]['date_id'],
                                    end_date_id=incomes.iloc[-1]['date_id'], table_name='adj_factor')

    base_date_id = np.sort(np.unique(np.concatenate((daily_basics.cal_date.values, incomes['end_date'].values))))
    base = pd.DataFrame(index=base_date_id)

    dailys['adj_close'] = dailys['close'] * adj_factors['adj_factor']
    base = base.join(dailys[['cal_date', 'close', 'adj_close']].set_index('cal_date'))
    base = base.join(daily_basics[['cal_date', 'pe', 'pb', 'total_mv', 'total_share']].set_index('cal_date'))
    base.fillna(method='ffill', inplace=True)

    code_info = base.loc[incomes['end_date']]

    balancesheets = balancesheets[balancesheets['end_date'].isin(incomes.end_date)]
    cashflows = cashflows[cashflows['end_date'].isin(incomes.end_date)]

    if cashflows.empty or balancesheets.empty:
        print('数据为空！')

    incomes.set_index('end_date', inplace=True)
    balancesheets.set_index('end_date', inplace=True)
    cashflows.set_index('end_date', inplace=True)

    fina_indicators.set_index('end_date', inplace=True)
    return incomes, balancesheets, cashflows, fina_indicators, code_info, cash_divs


def fina_kpi(incomes, balancesheets, cashflows, fina_indicators, code_info, cash_divs):
    if incomes.empty:
        nan_series = pd.Series()
        return nan_series, nan_series,
    incomes.fillna(value=0, inplace=True)
    balancesheets.fillna(value=0, inplace=True)
    cashflows.fillna(value=0, inplace=True)
    # 一、好生意
    # 1、销售毛利率: （营业收入-营业成本）/ 营业收入
    if incomes.iloc[0]['comp_type'] != 1:
        print('sorry, 此方法只支持一般工商企业')

        return np.nan, np.nan

    gross_profit_ratio = round((incomes['revenue'] - incomes['oper_cost']) * 100 / incomes['revenue'], 1)
    gross_profit_ratio.name = 'gross'
    #     print('毛利率：gross_profit_ratio=', gross_profit_ratio)

    # 2、净利率
    n_income_ratio = round(incomes['n_income'] * 100 / incomes['revenue'], 1)
    n_income_ratio.name = 'n_income_ratio'

    goodwill = balancesheets['goodwill']
    goodwill.name = 'goodwill'

    # 2、净资本增长率
    equity = balancesheets['total_assets'] - balancesheets['total_liab'] - goodwill
    equity_inc = round(get_ratio(equity.shift(), equity), 2)
    equity_inc.name = 'equity_inc'

    # 二、好管理团队
    # 1、成本费用利润率： （营业总收入-营业总成本）/营业总成本
    cogs_profit_rate = round((incomes['total_revenue'] - incomes['total_cogs']) * 100 / abs(incomes['total_cogs']), 1)
    cogs_profit_rate.name = 'cogs_profit_rate'
    #     print('成本费用利润率：cogs_profit_rate=', cogs_profit_rate)

    # 营运资金
    oper_fun = balancesheets['total_cur_assets'] - goodwill - balancesheets['total_cur_liab']

    # 4、资金周转率：平均准备期+平均收账期-平均付账期
    # 存货周转天数：360*存货平均余额/主营成本
    inventories_days = 360 * balancesheets['inventories'] / incomes['revenue']
    # 应收账款周转天数：360×应收账款周转率=平均应收账款×360天/销售收入=平均应收账款/平均日销售额
    accounts_receiv_days = 360 * balancesheets['accounts_receiv'] / incomes['revenue']
    # 预收账款周转天数：360 * 预收平均余额/主营收入
    adv_receipts_days = 360 * balancesheets['adv_receipts'] / incomes['revenue']
    # 资金周转天数=存货周转天数+应收账款周转天数-预收账款周转天数
    capital_tdays = round(inventories_days + accounts_receiv_days - adv_receipts_days)
    capital_turn = round(incomes['revenue'] / abs(
        balancesheets['inventories'] + balancesheets['accounts_receiv'] - balancesheets['adv_receipts']), 2)
    oper_pressure = round((balancesheets['accounts_receiv'] + balancesheets['prepayment'] + balancesheets['oth_receiv']
                           - balancesheets['acct_payable'] - balancesheets['adv_receipts'] - balancesheets[
                               'oth_payable']) * capital_turn / incomes['revenue'], 2)
    capital_turn.name = 'capital_turn'
    capital_tdays.name = 'capital_tdays'
    oper_fun.name = 'oper_fun'
    oper_pressure.name = 'oper_pressure'
    #     print('存货周转天数: inventories_days=', inventories_days, '应收账款周转天数: accounts_receiv_days=', accounts_receiv_days,
    #           '预收账款周转天数:adv_receipts_days=', adv_receipts_days, '资金周转天数:capital_turn_days=', capital_turn_days)
    #     print('资金周转率：capital_turn=', capital_turn)

    #     4、ebitda利息倍数：衡量偿债能力，息税折旧摊销前利润/利息支出，越高，表示盈利能力相比利息越强
    ebitda_mul = round(fina_indicators['ebitda'] / abs(fina_indicators['interst_income']), 2)
    ebitda_mul.name = 'ebitda_mul'

    # 三、好结果

    # 1、净资产销售报酬率: 销售利润/年平均股东权益
    equity_return = round((incomes['revenue'] - incomes['oper_cost']) * 100 * 2 / (equity.shift() + equity), 1)
    equity_return.name = 'equity_return'
    #     print('净资产销售报酬率: equity_return=', equity_return)

    # 2、总资产报酬率: 净利润/年平均总资产
    asset_return = round(incomes['n_income'] * 100 / balancesheets['total_assets'], 1)
    asset_return.name = 'asset_return'
    #     print('总资产报酬率:asset_return=', asset_return)

    #     # 3、红利比率

    #     4、每股收益
    share_return = round(incomes['n_income_attr_p'] / balancesheets['total_share'], 2)
    share_return.name = 'share_return'
    # 四、正常的现金流
    cashflows.fillna(value=0, inplace=True)
    cashflow_act_ratio = round(cashflows['n_cashflow_act'] * 100 / (
                abs(cashflows['n_cashflow_act']) + abs(cashflows['n_cash_flows_fnc_act']) + abs(
            cashflows['n_cashflow_inv_act'])), 1)
    cashflow_act_ratio.name = 'cashflow_act_ratio'
    cashflow_fnc_ratio = round(cashflows['n_cash_flows_fnc_act'] * 100 / (
                abs(cashflows['n_cashflow_act']) + abs(cashflows['n_cash_flows_fnc_act']) + abs(
            cashflows['n_cashflow_inv_act'])), 1)
    cashflow_fnc_ratio.name = 'cashflow_fnc_ratio'
    cashflow_inv_ratio = round(cashflows['n_cashflow_inv_act'] * 100 / (
                abs(cashflows['n_cashflow_act']) + abs(cashflows['n_cash_flows_fnc_act']) + abs(
            cashflows['n_cashflow_inv_act'])), 1)
    cashflow_inv_ratio.name = 'cashflow_inv_ratio'
    int_exp_inc_ratio = round((incomes['ebit'] - incomes['income_tax'] - incomes['total_profit']) / incomes['revenue'],
                              2)
    int_exp_inc_ratio.name = 'int_exp_inc_ratio'
    share_inc_ratio = balancesheets['total_share'] - balancesheets['total_share'].shift()
    share_inc_ratio.name = 'share_inc_ratio'
    #     print('经营现金流比率：cashflow_ratio=', cashflow_ratio)

    # 五、增长率
    #   1、营业收入增长率
    #     revenue_mv = incomes['revenue'].rolling(window=4).sum()
    #     revenue_inc_ratio = get_ratio(revenue_mv.shift(), revenue_mv)
    revenue_inc_ratio = get_ratio(incomes['revenue'].shift(), incomes['revenue'])
    revenue_inc_ratio = round(revenue_inc_ratio, 2)
    revenue_inc_ratio.name = 'revenue_inc_ratio'

    #     2、费用增长率
    #     total_cogs_mv = incomes['total_cogs'].rolling(window=4).sum()
    #     cost_inc_ratio = get_ratio(total_cogs_mv.shift(), total_cogs_mv)
    cost_inc_ratio = get_ratio(incomes['total_cogs'].shift(), incomes['total_cogs'])
    cost_inc_ratio = round(cost_inc_ratio, 2)
    cost_inc_ratio.name = 'cost_inc_ratio'

    #     3、所得税占税前利润率: 保持正常比例才正常，突然的减少反而可能是内部会计造假
    profit_tax_ratio = round(incomes['income_tax'] * 100 / incomes['total_profit'], 2)
    profit_tax_ratio.name = 'profit_tax_ratio'

    #     4、杜邦分析
    total_assets = balancesheets['total_assets']

    receiv_inc = balancesheets['accounts_receiv'] - balancesheets['accounts_receiv'].shift()
    receiv_pct = round(receiv_inc / incomes['revenue'], 2)
    receiv_pct[receiv_pct < 0.2] = 0

    roe = round(incomes['n_income_attr_p'] * 100 * 2 / (equity.shift() + equity), 2)
    ret = round(incomes['n_income_attr_p'] * 100 * 2 / (total_assets.shift() + total_assets), 2)
    # 投入资产回报率 = （营业利润 - 新增应收帐款 * 新增应收账款占收入比重）/总资产
    iocc = round((incomes['operate_profit'] - receiv_pct * receiv_inc) * 100 * 2 / (total_assets.shift() + total_assets), 2)
    iocc.fillna(method='backfill', inplace=True)
    em = round(total_assets * 2 / (equity.shift() + equity), 2)
    income_rate = round((incomes['n_income_attr_p']) * 100 / incomes['total_revenue'], 2)
    total_turn = round(incomes['revenue'] * 2 / (total_assets.shift() + total_assets), 2)
    total_assets.name = 'total_assets'
    pe = round(code_info['pe'], 2)
    pb = round(code_info['pb'], 2)
    total_mv = round(code_info['total_mv'], 2)
    act_income = round(cashflows['n_cashflow_act'] / incomes['n_income'], 2)
    receiv_income = round(balancesheets['accounts_receiv'] / incomes['n_income'], 2)
    adv_income = round(balancesheets['adv_receipts'] / incomes['n_income'], 2)
    #     dt_eps = round(fina_indicators['dt_eps'], 2)
    eps = round((incomes['n_income_attr_p'].shift() + incomes['n_income_attr_p']) / (
                balancesheets['total_share'].shift() + balancesheets['total_share']), 2)

    mv_eps = eps.rolling(window=4).mean()
    mv_eps_d = round(mv_eps.diff(), 2)
    eps_mul = round(code_info['close'] / eps, 2)
    #     eps_to_ct = round(get_ratio(eps_times.pct_change() / capital_turn.pct_change(), 2)
    eps_mul_pct = round(get_ratio(eps_mul.shift(), eps_mul), 2)
    cap_turn_pct = round(get_ratio(capital_turn.shift(), capital_turn), 2)
    revenue_pct = round(get_ratio(incomes['revenue'].shift(), incomes['revenue']), 2)
    Tassets_inc = round(get_ratio(total_assets.shift(), total_assets), 2)
    libwithinterest = balancesheets['total_liab'] - balancesheets['acct_payable'] - balancesheets['adv_receipts']
    i_debt = round(libwithinterest * 100 / total_assets, 2)

    free_cash = (cashflows['n_cashflow_inv_act'] + cashflows['n_cashflow_act'])

    free_cash_mv = get_rolling_mean(free_cash, window=4)
    lib_cash = round(free_cash_mv / balancesheets['total_cur_liab'], 2)

    dyr = round(cash_divs * 100 / total_mv, 2)
    roe_inc = get_roe_inc(total_turn, iocc, em)
    OPM = get_rolling_mean(oper_pressure, window=4)
    OPM = round(OPM, 2)
    V = value_stock(roe, roe_inc, OPM)
    glem_V = glem_value_stock(roe, OPM)
    dpd_V = dpd_value_stock(roe, OPM)
    RR = round(V / eps_mul, 2)
    glem_RR = round(glem_V / eps_mul, 2)
    dpd_RR = round(dpd_V / eps_mul, 2)

    # 破产风险Z=0.717*X1 + 0.847*X2 + 3.11*X3 + 0.420*X4 + 0.998*X5，低于1.2：即将破产，1.2-2.9: 灰色区域，大于2.9:没有破产风险
    # X1= 营运资本/总资产
    X1 = round(oper_fun / total_assets, 2)
    # X2 = 留存收益/总资产
    X2 = round((balancesheets['special_rese'] + balancesheets['surplus_rese'] + balancesheets[
        'undistr_porfit']) / total_assets, 2)
    # X3 = 息税前利润 / 总资产
    X3 = round(incomes['ebit'] / total_assets, 2)
    # X4 = 股东权益 / 负债
    # X4 = equity / balancesheets['total_liab']
    X4 = round(equity / libwithinterest, 2)
    # X5 = 销售收入 / 总资产
    X5 = round(incomes['revenue'] / total_assets, 2)
    Z = round(0.717 * X1 + 0.847 * X2 + 3.11 * X3 + 0.420 * X4 + 0.998 * X5, 2)

    X1.name = 'X1'
    X2.name = 'X2'
    X3.name = 'X3'
    X4.name = 'X4'
    X5.name = 'X5'
    Z.name = 'Z'
    free_cash_mv.name = 'free_cash_mv'
    eps_mul.name = 'eps_mul'
    eps_mul_pct.name = 'eps_mul_pct'
    cap_turn_pct.name = 'cap_turn_pct'
    lib_cash.name = 'lib_cash'

    roe.name = 'roe'
    ret.name = 'ret'
    em.name = 'em'
    income_rate.name = 'income_rate'
    total_turn.name = 'total_turn'
    pe.name = 'pe'
    pb.name = 'pb'
    dyr.name = 'dyr'
    receiv_income.name = 'receiv_income'
    V.name = 'V'
    roe_inc.name = 'roe_inc'
    OPM.name = 'OPM'
    RR.name = 'RR'
    glem_RR.name = 'glem_RR'
    glem_V.name = 'glem_V'
    dpd_RR.name = 'dpd_RR'
    dpd_V.name = 'dpd_V'
    i_debt.name = 'i_debt'
    receiv_pct.name = 'receiv_pct'

    data = pd.concat(
        [code_info['adj_close'],  total_mv, income_rate, roe,  eps_mul,  V, glem_V, dpd_V, dyr,
         roe_inc, RR, glem_RR, dpd_RR,
         pe, pb, i_debt, oper_pressure, OPM,
         Z, X1, X2, X3, X4, X5,
         free_cash_mv, lib_cash, receiv_pct], axis=1)

    return data


def get_roe_inc(total_turn, iocc, equity_times):
    iocc_mv = get_rolling_mean(iocc, window=10)
    total_turn_mv = get_rolling_mean(total_turn, window=10)

    total_return = iocc_mv*total_turn_mv
    total_return_inc = total_return - total_return.shift()
    roe_inc = round(total_return_inc * equity_times, 2)
    return roe_inc


def value_stock(IR, IR_inc, OPM):
    """
    总投资10年，前5年保持现有增长率，后5年不增长，收入平稳
    """
    IR_inc.fillna(value=0, inplace=True)
    V = pd.Series(index=IR.index)
    L = 0.85
    for k in IR.index:
        v = 1
        E = 1
        if k not in OPM.index:
            continue
        ir = IR.loc[k] / 100
        ir_inc = IR_inc.loc[k] / 100

        for i in range(1, 11):
            # 贴现率
            if i <= 5:
                ir = ir + ir_inc

            E = E * (1 + ir)

            v += L ** i * E

        v = v * (1 - OPM.loc[k] * (1 - L))

        V.loc[k] = v

    V = round(V, 2)
    return V


def glem_value_stock(IR, OPM):
    """
    格雷厄姆的价值投资
    每股内在价值=每股收益*（1*预期未来的年增长率+8.5）
    """
    V = 2*IR+8.5
    V = V * (1 - OPM * 0.15)
    V = round(V, 2)
    return V


def dpd_value_stock(IR, OPM):
    """
    邓普顿的价值投资
    每股内在价值判断标准：当前股价/未来5年的每股收益<5
    """
    IR = IR / 100
    sn = (1 - (1 + IR) ** 6) / (-IR)
    V = sn * 5
    V = V * (1 - OPM * 0.15)
    V = round(V, 2)
    return V