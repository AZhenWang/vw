from app.saver.service.fina import Fina
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
from app.saver.logic import DB
from app.common.function import get_ratio


def get_reports(code_id, start_date_id='', end_date_id=''):

    balancesheets = Fina.get_report_info(code_id=code_id, start_date_id=start_date_id, end_date_id=end_date_id, TTB='balancesheet',
                                         report_type='1')
    incomes = Fina.get_report_info(code_id=code_id, start_date_id=start_date_id, end_date_id=end_date_id, TTB='income',
                                   report_type='2')
    incomes12 = Fina.get_report_info(code_id=code_id, start_date_id=start_date_id, end_date_id=end_date_id, TTB='income',
                                   report_type='1', end_date_type='%1231')
    cashflows2= Fina.get_report_info(code_id=code_id, start_date_id=start_date_id, end_date_id=end_date_id, TTB='cashflow',
                                     report_type='2')
    cashflows12 = Fina.get_report_info(code_id=code_id, start_date_id=start_date_id, end_date_id=end_date_id, TTB='cashflow',
                                     report_type='1',  end_date_type='%1231')
    fina_indicators = Fina.get_report_info(code_id=code_id, start_date_id=start_date_id, end_date_id=end_date_id,
                                           TTB='fina_indicator', end_date_type='%1231')
    stk_holdernumbers = Fina.get_report_info(code_id=code_id, start_date_id=start_date_id, end_date_id=end_date_id,
                                             TTB='stk_holdernumber')

    fina_indicators.set_index('end_date', inplace=True)
    stk_holdernumbers.set_index('end_date', inplace=True)

    incomes.set_index('end_date', inplace=True)
    incomes12.set_index('end_date', inplace=True)
    balancesheets.set_index('end_date', inplace=True)
    cashflows2.set_index('end_date', inplace=True)
    cashflows12.set_index('end_date', inplace=True)

    # 组装incomes
    incomes = incomes[
        ['n_income', 'revenue', 'oper_cost', 'income_tax', 'total_profit', 'operate_profit', 'n_income_attr_p',
         'int_exp', 'ebitda']].rolling(window=4).sum()

    for i in range(len(incomes12)):
        ed = incomes12.index[i]
        incomes.loc[ed] = incomes12.loc[ed]
    incomes.sort_index(inplace=True)
    incomes.dropna(subset=['n_income', 'revenue'], inplace=True)

    oneyearago = pd.Series()
    for i in range(len(incomes)):
        ed = incomes.index[i]
        oneyearago.loc[ed] = str(int(ed) - 10000)
        # print('ed=', ed, 'str(int(ed) - 10000)=', str(int(ed) - 10000))
    incomes['oneyearago'] = oneyearago

    #组装cashflow
    cashflows2.fillna(0, inplace=True)
    cashflows2['equity_in_fnc'] = cashflows2['stot_cash_in_fnc_act'] - cashflows2['oth_cash_recp_ral_fnc_act'] - cashflows2['proc_issue_bonds'] - cashflows2['c_recp_borrow']

    cashflows12['equity_in_fnc'] = cashflows12['stot_cash_in_fnc_act'] - cashflows12['oth_cash_recp_ral_fnc_act'] - \
                                   cashflows12['proc_issue_bonds'] - cashflows12['c_recp_borrow']

    cashflows = cashflows2[
        ['equity_in_fnc', 'depr_fa_coga_dpba', 'free_cashflow', 'incr_def_inc_tax_liab', 'end_bal_cash',
                                        'decr_def_inc_tax_assets','c_inf_fr_operate_a', 'st_cash_out_act', 'n_cashflow_act']].rolling(window=4).sum()
    for i in range(len(cashflows12)):
        ed = cashflows12.index[i]
        cashflows.loc[ed] = cashflows12.loc[ed]
    cashflows.sort_index(inplace=True)
    cashflows.dropna(subset=['c_inf_fr_operate_a', 'st_cash_out_act'], inplace=True)
    cashflows.fillna(method='ffill', inplace=True)

    balancesheets = balancesheets[balancesheets.index.isin(incomes.index)]
    cashflows = cashflows[cashflows.index.isin(incomes.index)]

    if balancesheets.empty or incomes.empty or cashflows.empty:
        nan_series = pd.Series()
        return nan_series, nan_series, nan_series, nan_series, nan_series, nan_series,  nan_series

    # 组装股价
    sd = balancesheets.iloc[0]['date_id'] - 10
    ed = balancesheets.iloc[-1]['date_id']
    daily_basics = DB.get_table_logs(code_id=code_id, start_date_id=sd,
                                     end_date_id=ed, table_name='daily_basic')
    dailys = DB.get_table_logs(code_id=code_id, start_date_id=sd,
                               end_date_id=ed, table_name='daily')
    adj_factors = DB.get_table_logs(code_id=code_id, start_date_id=sd,
                                    end_date_id=ed, table_name='adj_factor')

    cal_dates = DB.get_cal_date_by_id(start_date_id=start_date_id, end_date_id=end_date_id)
    # base_date_id = np.sort(np.unique(np.concatenate((daily_basics.index, balancesheets.date_id.values))))
    base = pd.DataFrame(index=cal_dates.cal_date.values)
    dailys['adj_close'] = dailys['close'] * adj_factors['adj_factor']
    base = base.join(adj_factors[['cal_date', 'adj_factor']].set_index('cal_date'))
    base = base.join(dailys[['cal_date', 'close', 'adj_close']].set_index('cal_date'))
    base = base.join(daily_basics[['cal_date', 'pe', 'pb', 'total_mv', 'total_share']].set_index('cal_date'))
    base.fillna(method='ffill', inplace=True)
    code_info = base.loc[balancesheets.f_ann_date]
    code_info['end_date'] = balancesheets.index
    code_info.set_index('end_date', inplace=True)

    dates_base = pd.DataFrame(index=balancesheets.index)
    incomes = dates_base.join(incomes)
    cashflows = dates_base.join(cashflows)

    stk_holdernumbers = dates_base.join(stk_holdernumbers)
    holdernum = stk_holdernumbers['holder_num']
    fina_indicators = dates_base.join(fina_indicators)
    fina_indicators = fina_indicators[['rd_exp', 'interst_income']]


    # 组装分红和权益性融资
    dividends = Fina.get_divdends(code_id=code_id, start_date_id=start_date_id, end_date_id=end_date_id)
    dividends.dropna(subset=['pay_date'])
    dividends['div_count'] = dividends['total_share'] * dividends['cash_div_tax'] * 10000
    dividends.div_count.astype = 'float64'
    dividend_dates = np.sort(np.unique(np.concatenate((dividends.pay_date.values, incomes.index))))
    dividend_base = pd.DataFrame(index=dividend_dates)
    dividend_base = dividend_base.join(dividends[['pay_date', 'div_count']].set_index('pay_date'))
    cash_divs = pd.Series(index=incomes.index, name='cash_divs')

    # cashflows2.fillna(0, inplace=True)
    # equity_in_fnc_base = cashflows2['stot_cash_in_fnc_act'] - cashflows2['oth_cash_recp_ral_fnc_act'] - cashflows2['proc_issue_bonds'] - cashflows2['c_recp_borrow']
    # equity_in_fnc = pd.Series(index=incomes.index, name='equity_in_fnc')

    for i in range(1, len(incomes)):
        ed = incomes.index[i]
        sd = str(int(incomes.index[i-1]) + 1)
        current_div = np.float(dividend_base.loc[sd:ed]['div_count'].sum())
        cash_divs.loc[ed] = current_div
        # current_equity_in_fnc = np.float(equity_in_fnc_base.loc[sd:ed].sum())
        # equity_in_fnc.loc[ed] = current_equity_in_fnc

    # cashflows['equity_in_fnc'] = equity_in_fnc
    cashflows['cash_divs_rolling'] = cash_divs.rolling(window=4).sum()
    return incomes, balancesheets, cashflows, fina_indicators, holdernum, code_info, cash_divs


def fina_kpi(incomes, balancesheets, cashflows, fina_indicators, holdernum, code_info, cash_divs):
    adj_close = code_info['adj_close']
    adj_factor = code_info['adj_factor']

    if len(incomes) < 3 or len(balancesheets) < 3:
        nan_series = pd.Series()
        return nan_series
    incomes.fillna(method='ffill', inplace=True)
    incomes.fillna(method='backfill', inplace=True)
    incomes.fillna(value=0, inplace=True)
    balancesheets.fillna(method='ffill', inplace=True)
    balancesheets.fillna(method='backfill', inplace=True)
    balancesheets.fillna(value=0, inplace=True)
    cashflows.fillna(method='ffill',  inplace=True)
    cashflows.fillna(value=0, inplace=True)
    fina_indicators.fillna(method='ffill', inplace=True)
    fina_indicators.fillna(value=0, inplace=True)
    # cash_divs.fillna(method='ffill', inplace=True)
    cash_divs.fillna(value=0, inplace=True)
    goodwill = balancesheets['goodwill']
    goodwill.name = 'goodwill'


    total_mv = round(code_info['total_mv'] * 10000, 2)

    total_assets = balancesheets['total_assets']
    TEV = total_assets - goodwill
    equity = balancesheets['total_assets'] - balancesheets['total_liab']
    pure_equity = balancesheets['total_hldr_eqy_exc_min_int'] - goodwill
    normal_equity = pure_equity - balancesheets['oth_eqt_tools_p_shr']

    ret = round(incomes['n_income'] * 100 / total_assets, 2)
    pe = round(code_info['pe'], 2)
    pb = round(total_mv/normal_equity, 2)
    total_share = balancesheets['total_share']

    print('ss2')
    # 更新刚IPO时的数据,
    ipo_log = DB.get_new_share_log(balancesheets['code_id'].iloc[0])

    if not ipo_log.empty:
        ipo_before_equity = normal_equity[normal_equity.index < ipo_log.issue_date]
        if not ipo_before_equity.empty:
            ipo_lastest_date = ipo_before_equity.index[-1]
            incr_equity = ipo_log.price * ipo_log.amount * 10000

            ipo_lastest_normal_equity = ipo_before_equity.loc[ipo_lastest_date]
            ipo_lastest_normal_equity += incr_equity
            ipo_lastest_total_mv = ipo_log.price * (
                        ipo_log.amount * 10000 + balancesheets.loc[ipo_lastest_date]['total_share'])

            pb.loc[ipo_lastest_date] = round(ipo_lastest_total_mv / ipo_lastest_normal_equity, 2)
            total_mv.loc[ipo_lastest_date] = ipo_lastest_total_mv
            pe.loc[ipo_lastest_date] = ipo_log.pe
            pb.loc[ipo_lastest_date] = round(ipo_lastest_total_mv / normal_equity.loc[ipo_lastest_date],2)
            adj_close.loc[ipo_lastest_date] = ipo_log.price
            adj_factor.loc[ipo_lastest_date] = 1
            total_share.loc[ipo_lastest_date] = ipo_log.amount * 10000 + balancesheets.loc[ipo_lastest_date]['total_share']
            # equity.loc[ipo_lastest_date] = equity.loc[ipo_lastest_date] + incr_equity
            # pure_equity.loc[ipo_lastest_date] = pure_equity.loc[ipo_lastest_date] + incr_equity

    adj_close_pure = adj_close.dropna()
    share_pct = round(total_share.pct_change() * 100, 2)

    # 营运资金
    oper_fun = balancesheets['total_cur_assets'] - balancesheets['total_cur_liab']

    # 4、资金周转率：平均准备期+平均收账期-平均付账期
    # 存货周转天数：360*存货平均余额/主营成本
    inventories_days = 360 * balancesheets['inventories'] / incomes['revenue']
    # 应收账款周转天数：360×应收账款周转率=平均应收账款×360天/销售收入=平均应收账款/平均日销售额
    accounts_receiv_days = 360 * balancesheets['accounts_receiv'] / incomes['revenue']
    # 预收账款周转天数：360 * 预收平均余额/主营收入
    adv_receipts_days = 360 * balancesheets['adv_receipts'] / incomes['revenue']
    # 资金周转天数=存货周转天数+应收账款周转天数-预收账款周转天数
    capital_tdays = round(inventories_days + accounts_receiv_days - adv_receipts_days)
    capital_turn = round(incomes['revenue'] / (balancesheets['inventories'] + balancesheets['accounts_receiv'] +  balancesheets['notes_receiv'] - balancesheets['adv_receipts']), 2)
    oper_pressure = round((balancesheets['accounts_receiv'] + balancesheets['prepayment']
                           + balancesheets['oth_receiv'] + balancesheets['notes_receiv']
                           - balancesheets['acct_payable'] - balancesheets['adv_receipts']
                           - balancesheets['oth_payable'] - balancesheets['notes_payable'])
                          / equity, 2)
    capital_turn.name = 'capital_turn'
    inventory_turn = round(incomes['revenue'] / balancesheets['inventories'], 2)
    inventory_turn.name = 'inventory_turn'

    # 如果货币资金与短期借款同步增加，且资金周转率下降，表示企业货币资金有无法动用之处，或者未来有大笔开支，且已经传导到经营层面，影响到供应商的信用授信，形成压力
    money_cap_pct = round(balancesheets['money_cap'].pct_change() * 100, 1)
    st_borr_pct = round(balancesheets['st_borr'].pct_change() * 100, 1)
    st_borr_rate = round(balancesheets['st_borr'] * 100 / pure_equity, 1)

    money_cap_pct.name = 'money_cap_pct'
    st_borr_pct.name = 'st_borr_pct'
    st_borr_rate.name = 'st_borr_rate'

    capital_tdays.name = 'capital_tdays'
    oper_fun.name = 'oper_fun'
    oper_pressure.name = 'oper_pressure'
    #     print('存货周转天数: inventories_days=', inventories_days, '应收账款周转天数: accounts_receiv_days=', accounts_receiv_days,
    #           '预收账款周转天数:adv_receipts_days=', adv_receipts_days, '资金周转天数:capital_turn_days=', capital_turn_days)
    #     print('资金周转率：capital_turn=', capital_turn)

    #     4、杜邦分析
    # receiv_inc = balancesheets['accounts_receiv'] - balancesheets['accounts_receiv'].shift()
    receiv = balancesheets['accounts_receiv'] + balancesheets['notes_receiv']
    receiv_inc = receiv - receiv.shift()
    receiv_pct = round(receiv_inc * 100 / incomes['revenue'])
    receiv_rate = round(receiv * 100 / incomes['revenue'].shift())
    print('ss3')
    # 调整折旧费用
    gross = incomes['revenue'] - incomes['oper_cost']
    gross_rate = round(gross * 100 / incomes['revenue'], 2)
    fix_assets = balancesheets['fix_assets'].fillna(method='ffill')
    dpba_of_assets = round(cashflows['depr_fa_coga_dpba'] * 100 / fix_assets, 2)
    dpba_of_gross = round(cashflows['depr_fa_coga_dpba'] * 100 / gross, 2)

    # 企业财务造假的原因在于普通人只看净利润，也就是每股盈利，而不关注净利润的来源，是来源于把一些费用从盈余项目扣除，还是真正的到手的利润，
    # 假装好的东西都是把好的一面呈现在你面前，侧面和后面看里面是烂的，而真正好的东西，360度看都是好的。
    # 所以，像二郎神一样，用三只眼立体来看一个企业
    # 第一只眼： 净利润，评估平均每股收益
    # 第二只眼： 收入增速，评估未来10年价值，评估企业价值
    # 第三支眼： 销售收入现金流增速，评估未来10年现金流入，评估企业价值
    # 第四只眼： 所得税缴纳比例正常

    # 研发费用，除去某一年的突然研发费用干扰， 研发费用+营业利润的7年的平均值，作为利润的增长基础值
    rd_exp = fina_indicators['rd_exp']
    rd_exp.fillna(0, inplace=True)
    rd_exp_or = round(rd_exp * 100 / incomes['revenue'], 2)
    fix_asset_pct = round(fix_assets.pct_change()*100, 2)

    # 所得税缴纳基数
    tax_rate = round(incomes['income_tax'] * 100 / incomes['total_profit'], 2)
    # 应纳税额
    def_tax = cashflows['incr_def_inc_tax_liab'] + cashflows['decr_def_inc_tax_assets']
    def_tax.fillna(0, inplace=True)
    tax_payable = incomes['income_tax'] - def_tax
    tax_payable[tax_payable.isna()] = 0
    tax_payable_pct = round(tax_payable.pct_change()*100, 2)
    tax_pct = round(incomes['income_tax'].pct_change()*100, 2)
    tax_diff = tax_payable - tax_payable.shift()
    tax_base = tax_payable.shift()[tax_diff >= 0].add(tax_payable[tax_diff < 0], fill_value=0.01)
    def_tax_ratio = round(def_tax * 100 / tax_base, 2)
    # 短期营业收入是否加速
    adj_ebit = (incomes['operate_profit'] + rd_exp + cashflows['depr_fa_coga_dpba'])
    # adj_ebit = tax_payable
    ebit_mv = get_rolling_mean(adj_ebit, window=7)
    ebit_mv_short = get_rolling_mean(adj_ebit,  window=4)
    ebit_mv_short_pct = round((ebit_mv_short - ebit_mv_short.shift()) * 100 / ebit_mv_short.shift(), 2)
    ebit_mv_pct = round((ebit_mv - ebit_mv.shift()) * 100 / ebit_mv.shift(), 2)
    mix_op_diff = ebit_mv_short_pct - ebit_mv_pct
    print('ss5=')
    # 收入，利润，负债，税的长期率
    rev_pct = get_rev_pct(incomes['revenue'], cash_divs)

    income_pct = round((incomes['n_income_attr_p'] + rd_exp).pct_change() * 100,2)
    tax_payable_pct[tax_payable_pct > 100] = 100
    tax_payable_pct[tax_payable_pct < -50] = -50
    tax_payable_pct[tax_payable_pct.isna()] = 0
    print('ss50=')

    income_rate = round((incomes['n_income_attr_p'] + rd_exp) * 100 / incomes['revenue'], 2)
    total_turn = round(incomes['revenue'] / total_assets, 2)

    print('ss52=')
    OPM = get_rolling_median(oper_pressure, window=5)
    OPM = round(OPM, 2)
    opm_coef = get_opm_coef(OPM)

    libwithinterest = balancesheets['st_borr'] + balancesheets['lt_borr'] + balancesheets['bond_payable'] + balancesheets['lt_payable'] + balancesheets['st_bonds_payable'] + balancesheets['non_cur_liab_due_1y'] + balancesheets['notes_payable']
    # 短期借款＋一年内到期的长期负债＋长期借款＋应付债券＋长期应付款
    i_debt = round(libwithinterest * 100 / total_assets, 2)
    # 利息保障倍数
    interst = pd.concat([fina_indicators['interst_income'], incomes['int_exp']], axis=1).max(axis=1)
    IER = round((adj_ebit) / (interst + 1), 2)
    equity_pct = round((equity + cash_divs - equity.shift()) * 100 / equity.shift(), 1)
    pure_equity_pct = round((pure_equity + cash_divs - pure_equity.shift()) * 100 /pure_equity.shift(), 1)

    rev_pct_yearly = pd.Series(index=balancesheets.index)
    cash_act_in_pct_yearly = pd.Series(index=balancesheets.index)
    cash_act_out_pct_yearly = pd.Series(index=balancesheets.index)
    equity_pct_yearly = pd.Series(index=balancesheets.index)
    pure_equity_pct_yearly = pd.Series(index=balancesheets.index)
    tax_payable_pct_yearly = pd.Series(index=balancesheets.index)
    income_pct_yearly = pd.Series(index=balancesheets.index)
    fix_asset_pct_yearly = pd.Series(index=balancesheets.index)
    total_assets_pct_yearly = pd.Series(index=balancesheets.index)
    liab_pct_yearly = pd.Series(index=balancesheets.index)
    freecash_pct_yearly = pd.Series(index=balancesheets.index)
    equity_mean = pd.Series(index=balancesheets.index)
    for i in range(1,len(incomes)):
        print('ssa2')
        ed = incomes.index[i]
        c_date = datetime.strptime(ed, '%Y%m%d')
        oneyearago = (c_date - timedelta(days=368)).strftime('%Y%m%d')
        equity_mean.loc[ed] = equity.loc[oneyearago:ed].mean()

        rev_pct_yearly.loc[ed] = round(((incomes['revenue'].loc[ed] -
                                               incomes['revenue'].loc[oneyearago:ed][0]) * 100 /
                                              incomes['revenue'].loc[oneyearago:ed][0]), 2)

        equity_pct_yearly.loc[ed] = round((equity.loc[ed] + cashflows['cash_divs_rolling'].loc[ed] - cashflows['equity_in_fnc'].loc[ed] - equity.loc[oneyearago:ed][0]) * 100 / equity.loc[oneyearago:ed][0], 1)
        # equity_pct_yearly.loc[ed] = round((equity.loc[ed] + cash_divs.loc[ed] - equity.loc[oneyearago:ed][0]) * 100 /equity.loc[oneyearago:ed][0], 1)
        pure_equity_pct_yearly.loc[ed] = round((pure_equity.loc[ed] + cashflows['cash_divs_rolling'].loc[ed] - pure_equity.loc[oneyearago:ed][0]) * 100 /pure_equity.loc[oneyearago:ed][0], 1)
        cash_act_in_pct_yearly.loc[ed] = round(((cashflows['c_inf_fr_operate_a'].loc[ed] -cashflows['c_inf_fr_operate_a'].loc[oneyearago:ed][0]) * 100 /cashflows['c_inf_fr_operate_a'].loc[oneyearago:ed][0]), 2)
        cash_act_out_pct_yearly.loc[ed] = round(((cashflows['st_cash_out_act'].loc[ed] -cashflows['st_cash_out_act'].loc[oneyearago:ed][0]) * 100 /cashflows['st_cash_out_act'].loc[oneyearago:ed][0]), 2)
        tax_payable_pct_yearly.loc[ed] = round(((tax_payable.loc[ed] -tax_payable.loc[oneyearago:ed][0]) * 100 /tax_payable.loc[oneyearago:ed][0]), 2)
        income_pct_yearly.loc[ed] = round(((incomes['n_income_attr_p'].loc[ed] - incomes['n_income_attr_p'].loc[oneyearago:ed][0]) * 100 /incomes['n_income_attr_p'].loc[oneyearago:ed][0]), 2)
        fix_asset_pct_yearly.loc[ed] = round(((fix_assets.loc[ed] - fix_assets.loc[oneyearago:ed][0]) * 100 /fix_assets.loc[oneyearago:ed][0]), 2)
        total_assets_pct_yearly.loc[ed] = round(((total_assets.loc[ed] - total_assets.loc[oneyearago:ed][0]) * 100 /total_assets.loc[oneyearago:ed][0]), 2)
        liab_pct_yearly.loc[ed] = round(((balancesheets['total_liab'].loc[ed] - balancesheets['total_liab'].loc[oneyearago:ed][0]) * 100 /balancesheets['total_liab'].loc[oneyearago:ed][0]), 2)
        freecash_pct_yearly.loc[ed] = round(((cashflows['free_cashflow'].loc[ed] - cashflows['free_cashflow'].loc[oneyearago:ed][0]) * 100 / cashflows['free_cashflow'].loc[oneyearago:ed][0]), 2)

    equity_pctmv = get_mean_of_complex_rate(equity_pct_yearly)
    # print(pd.concat([equity, equity_pct, cash_divs, cashflows['cash_divs_rolling'], cashflows['equity_in_fnc'], equity_pct_yearly, equity_pctmv], axis=1))
    # os.ex
    rev_pctmv = get_mean_of_complex_rate(rev_pct_yearly)

    pure_equity_pctmv = get_mean_of_complex_rate(pure_equity_pct_yearly)
    cash_act_in = get_mean_of_complex_rate(cash_act_in_pct_yearly)
    cash_act_out = get_mean_of_complex_rate(cash_act_out_pct_yearly)
    tax_payable_pctmv = get_mean_of_complex_rate(tax_payable_pct_yearly)
    income_pctmv = get_mean_of_complex_rate(income_pct_yearly)
    fix_asset_pctmv = get_mean_of_complex_rate(fix_asset_pct_yearly)
    total_assets_pctmv = get_mean_of_complex_rate(total_assets_pct_yearly)
    liab_pctmv = get_mean_of_complex_rate(liab_pct_yearly)

    freecash_mv = round(get_mean_of_complex_rate(freecash_pct_yearly), 1)
    cash_act_rate = round(cash_act_in / cash_act_out, 2)
    # print(pd.concat([adj_close, rev_pct,  rev_pct_yearly, rev_pctmv, pure_equity_pct, pure_equity_pct_yearly, pure_equity_pctmv, ], axis=1))
    # os.ex
    op_pct = pd.concat([rev_pctmv - rev_pctmv.shift(), income_pctmv - income_pctmv.shift()], axis=1).min(axis=1)
    op_pct[op_pct > 50] = 50
    op_pct[op_pct < -50] = -50

    equity_mean.fillna(method='backfill', inplace=True)
    roe = round(incomes['n_income_attr_p'] * 100 / equity_mean, 2)
    roe[roe > 50] = 50
    roe[roe < -50] = -50
    roe_rd_pure = round((rd_exp) * 100 / equity_mean, 2)
    roe_rd_pure[roe_rd_pure > 50] = 50
    roe_rd_pure[roe_rd_pure < -50] = -50
    roe_rd = round((incomes['n_income_attr_p'] + rd_exp) * 100 / equity_mean, 2)
    roe_rd[roe_rd > 50] = 50
    roe_rd[roe_rd < -50] = -50
    roe_sale = round((incomes['operate_profit'] - tax_payable + rd_exp) * 100 / equity_mean, 2)
    roe_sale[roe_sale > 50] = 50
    roe_sale[roe_sale < -50] = -50

    roe_ebitda = round((adj_ebit) * 100 / (total_assets - incomes['n_income_attr_p']), 2)
    roe_ebitda[roe_ebitda > 50] = 50
    roe_ebitda[roe_ebitda < -50] = -50

    # 求移动平均收益率
    roe_mv = pd.Series(index=balancesheets.index)
    roe_rd_mv = pd.Series(index=balancesheets.index)
    roe_rd_pure_mv = pd.Series(index=balancesheets.index)
    roe_sale_mv = pd.Series(index=balancesheets.index)
    roe_ebitda_mv = pd.Series(index=balancesheets.index)

    pre_date_idx = adj_close_pure.index[0]
    pre_date = datetime.strptime(pre_date_idx, '%Y%m%d')
    roe_mv.loc[pre_date_idx] = roe.loc[pre_date_idx] / 100
    roe_rd_mv.loc[pre_date_idx] = roe_rd.loc[pre_date_idx] / 100
    roe_rd_pure_mv.loc[pre_date_idx] = roe_rd_pure.loc[pre_date_idx] / 100
    roe_sale_mv.loc[pre_date_idx] = roe_sale.loc[pre_date_idx] / 100
    roe_ebitda_mv.loc[pre_date_idx] = roe_ebitda.loc[pre_date_idx] / 100

    for i in range(1, len(adj_close_pure)):
        date_idx = adj_close_pure.index[i]
        c_date = datetime.strptime(date_idx, '%Y%m%d')
        date_delta = c_date - pre_date
        pre_date = c_date
        date_coef = 365 / date_delta.days

        c1 = round(1 / date_coef, 2)
        c2 = 2 - c1

        roe_mv.loc[date_idx] = ((1 + roe.loc[date_idx] / 100) ** c1 * (1 + roe_mv.loc[pre_date_idx]) ** c2) ** (1 / 2) - 1
        roe_rd_mv.loc[date_idx] = ((1 + roe_rd.loc[date_idx] / 100) ** c1 * (1 + roe_rd_mv.loc[pre_date_idx]) ** c2) ** (1 / 2) - 1
        roe_rd_pure_mv.loc[date_idx] = ((1 + roe_rd_pure.loc[date_idx] / 100) ** c1 * (1 + roe_rd_pure_mv.loc[pre_date_idx]) ** c2) ** (1 / 2) - 1
        roe_sale_mv.loc[date_idx] = ((1 + roe_sale.loc[date_idx] / 100) ** c1 * (1 + roe_sale_mv.loc[pre_date_idx]) ** c2) ** (1 / 2) - 1
        roe_ebitda_mv.loc[date_idx] = ((1 + roe_ebitda.loc[date_idx] / 100) ** c1 * (1 + roe_ebitda_mv.loc[pre_date_idx]) ** c2) ** (1 / 2) - 1

        pre_date_idx = date_idx


    roe_mv = round(roe_mv * 100, 2)
    roe_rd_mv = round(roe_rd_mv * 100, 2)
    roe_rd_pure_mv = round(roe_rd_pure_mv * 100, 2)
    roe_sale_mv = round(roe_sale_mv * 100, 2)
    roe_ebitda_mv = round(roe_ebitda_mv * 100, 2)

    print('ss55=')
    roe_std = round(get_rolling_std(roe_sale_mv, window=10), 2)
    roe_adj = round(roe_sale_mv - roe_std, 2)
    # 未来10年涨幅倍数
    V = value_stock2(roe_mv, OPM, opm_coef)
    V_rd_pure = value_stock2(roe_rd_pure_mv, OPM, opm_coef)
    V_adj = value_stock2(roe_adj,  OPM, opm_coef)
    V_rd = value_stock2(roe_rd_mv, OPM, opm_coef)
    V_sale = value_stock2(roe_sale_mv, OPM, opm_coef)
    V_ebitda = value_stock2(roe_ebitda_mv, OPM, opm_coef)
    V_tax = value_stock2(tax_payable_pctmv, OPM, opm_coef)
    total_turn_pctmv = round((rev_pctmv / total_assets_pctmv).pct_change()*100, 2)
    # 赔率= 未来10年涨幅倍数/市现率
    pp = round(V / pb, 2)
    # 赔率，考虑roe_std波动
    pp_adj = round(V_adj / pb, 2)
    # 赔率，基于可持续的主营业务
    pp_rd = round(V_rd / pb, 2)
    pp_sale = round(V_sale / pb, 2)

    pp_ebitda = round(V_ebitda / pb, 2)
    # 税率验证：税的价值比总价值，要和税率相差无几，比如企业税率25%，那么这pp_tax的值不能离25太远
    pp_tax = round(V_tax * 100 / (V_tax + V_adj), 1)
    # 未来10年总营业增速
    dpd_V = dpd_value_stock(roe_sale_mv, OPM, opm_coef)
    # 未来10年总营业增速/市盈率
    dpd_RR = round(dpd_V / pe, 2)
    print('ss56=')
    MP = round(adj_close * pp_sale, 2)
    MP_pct = MP.pct_change()
    MP_pct[MP_pct > 1] = 1
    MP_pct[MP_pct < -1] = -0.99
    MP_pct.fillna(0, inplace=True)
    LLP = round((1 + MP_pct) * MP / 2, 2)
    HHP = round(2 * (1 + MP_pct) * MP)

    # HHP = round(2 * (1 + MP_pct + MP_pct_inc) * MP)
    MP = round(MP, 2)
    LP = round(MP / 2, 2)
    HP = round(2 * MP, 2)

    MP_pct = round(MP_pct * 100, 2)

    win = (2 * (adj_close * pp) - adj_close) / adj_close
    lose = ((adj_close * pp) / 2 - adj_close) / adj_close
    odds_pp = round(((1 + win) * (1 + lose) - 1) * 100, 1)

    win_return = (HP - adj_close) / adj_close
    lose_return = (LP - adj_close) / adj_close
    odds = round(((1 + win_return) * (1 + lose_return) - 1)*100, 1)
    win_return = round(win_return*100, 1)
    lose_return = round(lose_return*100, 1)

    win_return2 = (HHP - adj_close) / adj_close
    lose_return2 = (LLP - adj_close) / adj_close
    odds2 = round(((1 + win_return2) * (1 + lose_return2) - 1) * 100, 1)
    win_return2 = round(win_return2 * 100, 1)
    lose_return2 = round(lose_return2 * 100, 1)

    EE = round((total_assets - balancesheets['trad_asset'] - balancesheets['money_cap']) / incomes['ebitda'], 1)

    print('ss57=')
    em = round(total_assets / equity, 2)
    receiv_income = round(balancesheets['accounts_receiv'] / incomes['n_income'], 2)
    # 其他应收款存放关联交易或者保证金之类的杂项，超过5%，有妖

    oth_receiv_rate = round(balancesheets['oth_receiv'] * 100 / TEV, 1)
    oth_receiv_rate.name = 'oth_receiv_rate'
    # print(pd.concat(
    #     [adj_close, equity_mean, roe, roe_mv, tax_payable_pct, tax_payable_pct_yearly, tax_payable_pctmv],
    #     axis=1))
    # # os.ex
    # import matplotlib.pylab as plt
    # fig = plt.figure(figsize=(20, 16))
    # ax1 = fig.add_subplot(111, facecolor='#07000d')
    # ax1.plot(roe, label='roe', color='red')
    # ax1.plot(roe_mv, label='roe_mv', color='blue')
    # ax2 = ax1.twinx()
    # # ax2.plot(adj_close, label='adj_close', color='yellow')
    # ax2.plot(adj_close.pct_change()*100, label='adj_close', color='yellow')
    # ax2.plot(equity_pctmv, label='equity_pct', color='white', alpha=0.5)
    # ax2.plot(equity_pct.cumsum(), label='equity_pct2', color='green', alpha=0.5)
    # ax2.axhline(0, color='gray')
    # plt.show()
    # os.ex
    # 普通股在资本总额中的占比
    share_ratio = round(total_mv * 100 / (balancesheets['oth_eqt_tools_p_shr'] + balancesheets['bond_payable'] + total_mv), 1)

    st_cash_out_act_next = (1+cash_act_out/100)*cashflows['st_cash_out_act']
    cash_gap = (cashflows['end_bal_cash'] + balancesheets['nca_within_1y'] - balancesheets['non_cur_liab_due_1y'] \
                   + cashflows['n_cashflow_act'] \
                   # + cashflows['c_fr_sale_sg'] * adj_cash_in/100 \
                   - cashflows['st_cash_out_act'] * cash_act_out/100 \
                   )
    cash_gap_r = round(cash_gap * 12 / st_cash_out_act_next, 2)
    # 股息率
    dyr = round(cash_divs * 100 / total_mv, 2)
    # 股息占当年收入比率
    dyr_or = round(cash_divs * 100 / incomes['revenue'], 2)
    dyr_mean = round(get_mean_of_complex_rate(dyr), 2)
    money_cap = round(balancesheets['money_cap'] * 100 / total_assets)
    holdernum_inc = round(holdernum.pct_change()*100, 1)
    # holdernum_inc = round(get_ratio(holdernum), 2)
    # 破产风险Z=0.717*X1 + 0.847*X2 + 3.11*X3 + 0.420*X4 + 0.998*X5，低于1.2：即将破产，1.2-2.9: 灰色区域，大于2.9:没有破产风险
    # X1= 营运资本/总资产
    X1 = round(oper_fun / total_assets, 2)
    # X2 = 留存收益/总资产
    X2 = round((balancesheets['special_rese'] + balancesheets['surplus_rese'] + balancesheets[
        'undistr_porfit']) / total_assets, 2)
    # X3 = 息税前利润 / 总资产
    X3 = round(adj_ebit / total_assets, 2)
    # X4 = 股东权益 / 负债
    X4 = round((balancesheets['total_assets'] - balancesheets['total_liab']) / balancesheets['total_liab'], 2)
    # X4 = round((balancesheets['total_assets'] - balancesheets['total_liab'] - goodwill) / libwithinterest, 2)
    # X5 = 销售收入 / 总资产
    X5 = round(incomes['revenue'] / total_assets, 2)
    Z = round(0.717 * X1 + 0.847 * X2 + 3.11 * X3 + 0.420 * X4 + 0.998 * X5, 2)
    print('ss58=')
    Z.name = 'Z'
    cash_gap_r.name = 'cash_gap_r'
    pp.name = 'pp'
    pp_rd.name = 'pp_rd'
    pp_adj.name = 'pp_adj'
    pp_sale.name = 'pp_sale'
    pp_ebitda.name = 'pp_ebitda'
    pp_tax.name = 'pp_tax'
    cash_gap.name = 'cash_gap'

    roe.name = 'roe'
    roe_sale.name = 'roe_sale'
    roe_rd.name = 'roe_rd'
    roe_mv.name = 'roe_mv'
    roe_adj.name = 'roe_adj'
    roe_rd_mv.name = 'roe_rd_mv'
    roe_sale_mv.name = 'roe_sale_mv'
    roe_ebitda.name = 'roe_ebitda'
    roe_ebitda_mv.name = 'roe_ebitda_mv'

    ret.name = 'ret'
    em.name = 'em'
    gross_rate.name = 'gross_rate'
    income_rate.name = 'income_rate'
    total_turn.name = 'total_turn'
    EE.name = 'EE'
    pe.name = 'pe'
    pb.name = 'pb'
    dyr.name = 'dyr'
    receiv_income.name = 'receiv_income'
    rev_pct_yearly.name = 'rev_pct_yearly'
    V.name = 'V'
    V_adj.name = 'V_adj'
    V_sale.name = 'V_sale'
    V_rd.name = 'V_rd'
    V_ebitda.name = 'V_ebitda'
    V_tax.name = 'V_tax'
    OPM.name = 'OPM'
    dpd_RR.name = 'dpd_RR'
    dpd_V.name = 'dpd_V'
    i_debt.name = 'i_debt'
    receiv_pct.name = 'receiv_pct'
    receiv_rate.name = 'receiv_rate'
    money_cap.name = 'money_cap'
    dyr_or.name = 'dyr_or'
    dyr_mean.name = 'dyr_mean'
    op_pct.name = 'op_pct'
    holdernum_inc.name = 'holdernum_inc'
    holdernum.name = 'holdernum'
    freecash_mv.name = 'freecash_mv'
    equity_pct.name = 'equity_pct'
    equity_pct_yearly.name = 'equity_pct_yearly'
    share_pct.name = 'share_pct'
    pure_equity_pct.name = 'pure_equity_pct'
    pure_equity_pctmv.name = 'pure_equity_pctmv'
    tax_rate.name = 'tax_rate'
    mix_op_diff.name = 'mix_op_diff'
    dpba_of_assets.name = 'dpba_of_assets'
    dpba_of_gross.name = 'dpba_of_gross'
    IER.name = 'IER'
    rd_exp_or.name = 'rd_exp_or'
    roe_std.name = 'roe_std'
    share_ratio.name = 'share_ratio'
    tax_payable_pct.name = 'tax_payable_pct'
    def_tax_ratio.name = 'def_tax_ratio'
    income_pct.name = 'income_pct'
    fix_asset_pct.name = 'fix_asset_pct'
    rev_pct.name = 'rev_pct'
    tax_pct.name = 'tax_pct'
    total_assets.name = 'total_assets'

    tax_payable_pct.name = 'tax_payable_pct'
    rev_pctmv.name = 'rev_pctmv'
    total_turn_pctmv.name = 'total_turn_pctmv'
    liab_pctmv.name = 'liab_pctmv'
    income_pctmv.name = 'income_pctmv'
    tax_payable_pctmv.name = 'tax_payable_pctmv'
    equity_pctmv.name = 'equity_pctmv'
    total_assets_pctmv.name = 'total_assets_pctmv'
    fix_asset_pctmv.name = 'fix_asset_pctmv'
    cash_act_in.name = 'cash_act_in'
    cash_act_out.name = 'cash_act_out'
    cash_act_rate.name = 'cash_act_rate'
    win_return.name = 'win_return'
    lose_return.name = 'lose_return'
    odds.name = 'odds'
    win_return2.name = 'win_return2'
    lose_return2.name = 'lose_return2'
    odds2.name = 'odds2'
    odds_pp.name = 'odds_pp'
    LLP.name = 'LLP'
    LP.name = 'LP'
    MP.name = 'MP'
    HP.name = 'HP'
    HHP.name = 'HHP'
    MP_pct.name = 'MP_pct'
    data = pd.concat(
        [round(adj_close,2), round(adj_factor, 3), balancesheets['date_id'], total_mv/10000, incomes['revenue']/10000, gross_rate, income_rate,
         roe, roe_adj, roe_rd, roe_sale, roe_ebitda, roe_mv, roe_rd_mv, roe_sale_mv, roe_ebitda_mv, pp, pp_adj, pp_rd, pp_sale, pp_ebitda, pp_tax,
         holdernum, holdernum_inc,
         V, V_adj, V_rd, V_sale, V_ebitda, V_tax, dpd_V, dyr, dyr_or, dyr_mean, dpd_RR,
         pe, pb, EE, i_debt, share_ratio, oth_receiv_rate, money_cap_pct, st_borr_pct, st_borr_rate,  capital_turn, inventory_turn, oper_pressure, OPM,
         Z,
         receiv_pct, receiv_rate, cash_act_in, cash_act_out, cash_act_rate, cash_gap, cash_gap_r,
         freecash_mv,  op_pct, mix_op_diff, tax_rate,
         dpba_of_assets, dpba_of_gross, IER, rd_exp_or, roe_std,
         fix_asset_pct, tax_payable_pct, def_tax_ratio,
         rev_pct, share_pct,  rev_pct_yearly, total_turn, total_turn_pctmv, tax_pct, income_pct,
         rev_pctmv, total_assets_pctmv, liab_pctmv, income_pctmv, tax_payable_pctmv,
         pure_equity_pct, pure_equity_pctmv, equity_pct, equity_pct_yearly,  equity_pctmv, fix_asset_pctmv,
         LLP, LP, MP, HP, HHP, MP_pct, win_return, lose_return, odds, win_return2, lose_return2, odds2, odds_pp,
         ], axis=1, sort=True)
    return data

def get_right_mean(v, l=0.9):
    """
    时间加权平均值
    :param v: Series
    :param l: 时间加权系数
    :return: IR; 平均值
    """
    data = pd.Series(index=v.index)
    coefs = pd.Series(l, index=v.index)
    data.fillna(0, inplace=True)
    for j in range(1, len(data)):
        data.iloc[j] = (v[:j+1] * coefs[:j+1]).mean()
    return data

def get_rolling_mean(v, window=7, mtype=0):
    """
    移动平均值
    :param v:
    :param window:
    :param mtype: 为了保守估计而设置，保留今天的最小值：type=1, 保留今天的最大值：type=2, 去除包括今天的最大最小值：mtype=3
    不去掉最大最小值：type=0
    :return:
    """
    v.fillna(method='backfill', inplace=True)
    v.fillna(method='ffill', inplace=True)
    v.fillna(0, inplace=True)
    data = pd.Series(index=v.index)
    for j in range(2, len(data)):

        if mtype == 1:
            if j <= window:
                data.iloc[j] = (v[:j+1].sum() - np.max(v[:j+1]) - np.min(v[:j])) / (j - 1)
            else:
                data.iloc[j] = (v[j-window:j+1].sum() - np.max(v[j-window:j+1]) - np.min(v[j-window:j])) / (window - 1)

        elif mtype == 2:
            if j <= window:
                data.iloc[j] = (v[:j + 1].sum() - np.max(v[:j]) - np.min(v[:j + 1])) / (j - 1)
            else:
                data.iloc[j] = (v[j - window:j + 1].sum() - np.max(v[j - window:j]) - np.min(v[j - window:j + 1])) / (
                            window - 1)
        elif mtype == 3:
            if j <= window:
                data.iloc[j] = (v[:j + 1].sum() - np.max(v[:j+1]) - np.min(v[:j + 1])) / (j - 1)
            else:
                data.iloc[j] = (v[j - window:j + 1].sum() - np.max(v[j - window:j+1]) - np.min(v[j - window:j + 1])) / (window - 1)
        else:
            if j <= window:
                data.iloc[j] = v[:j+1].mean()
            else:
                data.iloc[j] = v[j-window+1:j + 1].mean()

    return round(data, 3)

def get_rolling_std(v, window=7):
    """
    移动方差
    :param v:
    :param window:
    :return:
    """
    v.fillna(method='backfill', inplace=True)
    v.fillna(method='ffill', inplace=True)
    v.fillna(0, inplace=True)
    data = pd.Series(index=v.index)
    for j in range(2, len(data)):
        if j <= window:
            data.iloc[j] = v[:j+1].std()
        else:
            data.iloc[j] = v[j-window+1:j + 1].std()
    return data

def get_rolling_median(v, window=7):
    """
    移动中值
    :param v:
    :param window:
    :return:
    """
    v.fillna(method='backfill', inplace=True)
    v.fillna(method='ffill', inplace=True)
    v.fillna(0, inplace=True)
    data = pd.Series(index=v.index)
    for j in range(2, len(data)):
        if j <= window:
            data.iloc[j] = v[:j+1].median()
        else:
            data.iloc[j] = v[j-window+1:j + 1].median()
    return data


def get_mean_of_complex_rate(d):
    """
    移动复合率的平均值
    :param v:
    :return:
    """
    v = d.copy()
    base = pd.DataFrame(index=v.index)
    v[v.isin([np.inf, -np.inf])] = np.nan
    if v.isna().all():
        return v
    # v.dropna(0, inplace=True)
    v[v < -99] = -99
    # v[v > 100] = np.nan
    v.fillna(method='ffill', inplace=True)
    v.dropna(inplace=True)
    v = v/100
    data = pd.Series(index=v.index, name='data')
    data.iloc[0] = v.iloc[0]
    pre_date = datetime.strptime(data.index[0], '%Y%m%d')
    for i in range(1, len(data)):
        date_idx = data.index[i]
        c_date = datetime.strptime(date_idx, '%Y%m%d')
        date_delta = c_date - pre_date
        pre_date = c_date
        date_coef = 365 / date_delta.days
        c1 = round(1 / date_coef, 2)
        c2 = 2 - c1
        data.iloc[i] = ((1 + v.iloc[i]) ** c1 * (1 + data.iloc[i - 1]) ** c2) ** (1 / 2) - 1
    base = base.join(data)
    return round(base['data'] * 100, 2)


def get_rev_pct(revenue, cash_divs):
    """
    收入增长率，考虑现金分红
    :param revenue:
    :param cash_divs:
    :return:
    """
    cash_divs.fillna(0, inplace=True)
    pct = pd.Series(index=revenue.index)
    for i in range(1, len(revenue)):
        pct.iloc[i] = (revenue.iloc[i] + cash_divs.iloc[i] - revenue.iloc[i - 1])/ revenue.iloc[i - 1]
    pct = round(pct * 100, 2)
    return pct


def get_opm_coef(OPM):
    """
    0.3是个人对被占用资金的惩罚系数，如果没有资金压力，占用资金的时间价值0.15, 赚钱难，亏钱容易
    :param OPM:
    :return:
    """
    opm_coef = pd.Series(index=OPM.index)
    opm_coef[OPM >= 0] = 0.3
    opm_coef[OPM < 0] = 0.15
    return opm_coef

def value_stock2(IR, OPM, opm_coef, years=10):
    """
       总投资10年，前5年保持现有增长率，后5年不增长，收入平稳

       """

    V = pd.Series(index=IR.index)

    L = 0
    for k in IR.index:
        v = 0
        base_v = 1
        if k not in OPM.index:
            continue
        ir = IR.loc[k] / 100

        for i in range(1, years + 1):
            if ir >= 0.5:
                ir = 0.5
            elif ir <= 0:
                ir = 0

            v += base_v * ir / (1 + L) ** i
            base_v = base_v * (1 + ir)
        v = v * (1 - OPM.loc[k] * opm_coef.loc[k] / 100)

        V.loc[k] = v
    V = round(V, 2)
    return V

def value_stock(IR, IR_a, OPM, opm_coef, years=10):
    """
    总投资10年，前5年保持现有增长率，后5年不增长，收入平稳

    """
    V = pd.Series(index=IR.index)

    L = 0
    # IR_a[IR_a > 5] = 5
    # IR_a[IR_a < -5] = -5
    IR_a.fillna(value=0, inplace=True)
    for k in IR.index:
        v = 0
        base_v = 1
        if k not in OPM.index:
            continue
        ir = IR.loc[k] / 100
        a = IR_a.loc[k] / 100

        for i in range(1, years+1):
            # 贴现率
            # print('i=', i, ',ir=', ir, 'a=',a, ',v=', v)
            if i < 4:
                ir = ir*(1+a)
            if ir >= 0.5:
                ir = 0.5
            elif ir <= 0:
                ir = 0

            # v = v * (1+ir) / (1+L)
            v += base_v * ir / (1 + L)**i
            base_v = base_v * (1 + ir)
            # print("\\n")
        # v = E*(1+ir)**10
        # print('v1=', v)
        v = v * (1 - OPM.loc[k] * opm_coef.loc[k] / 100)
        # print('v2=', v, 'OPM.loc[k] =', OPM.loc[k],',IR.loc[k]=', IR.loc[k])

        V.loc[k] = v
    V = round(V, 2)
    return V

def value_stock3(rev_pct_yearly, income, equity, OPM, opm_coef, years=10):
    """
    总投资10年，前5年保持现有增长率，后5年不增长，收入平稳

    """
    V = pd.Series(index=rev_pct_yearly.index)
    income_c = income.dropna()
    L = 0
    for k in income_c.index:
        v = 0
        base_v = 1
        income_return = income_c.loc[k]
        e = equity.loc[k]
        if k not in OPM.index:
            continue

        for i in range(years):
            if i < 3:
                income_return = income_return * (1 + rev_pct_yearly.loc[k] / 100)
                ir = income_return / e
                e = equity.loc[k] + income_return

            # 贴现率

            if ir >= 0.5:
                ir = 0.5
            elif ir <= 0:
                ir = 0

            # v = v * (1+ir) / (1+L)
            v += base_v * ir / (1 + L)**i
            base_v = base_v * (1 + ir)
            # print('i=', i, 'k=', k, ',income_return=', income_return, 'rev_pct_yearly.loc[k]=', rev_pct_yearly.loc[k], income_return, 'ir=', ir, ',e=', e, ', v=', v)
            # print("\\n")
        # v = E*(1+ir)**10
        # print('v1=', v)
        v = v * (1 - OPM.loc[k] * opm_coef.loc[k] / 100)
        # print('v2=', v, 'OPM.loc[k] =', OPM.loc[k],',IR.loc[k]=', IR.loc[k])

        V.loc[k] = v
    V = round(V, 2)
    return V


def glem_value_stock(IR, OPM, opm_coef):
    """
    格雷厄姆的价值投资
    每股内在价值=每股收益*（1*预期未来的年增长率+8.5）
    """
    V = 2*IR+8.5
    V = V * (1 - OPM * opm_coef)
    V = round(V, 2)
    return V


def dpd_value_stock(IR, OPM, opm_coef):
    """
    邓普顿的价值投资
    每股内在价值判断标准：当前股价/未来5年的每股收益<5
    """
    IR = IR / 100
    sn = (1 - (1 + IR) ** 6) / (-IR)
    V = sn * 5
    # V = V * (1 - OPM * opm_coef)
    V = round(V, 2)
    return V


def remove_outlier(roe):
    # 对于每一个特征，找到值异常高或者是异常低的数据点
    # 所有单特征有异常的行
    print('roe=', roe)
    logs = roe.dropna()
    logs = logs.sort_values()
    records = []
    # TODO: 计算给定特征的Q1（数据的25th分位点）
    Q1 = np.percentile(logs, 20)

    # TODO: 计算给定特征的Q3（数据的75th分位点）
    Q3 = np.percentile(logs, 80)

    # TODO: 使用四分位范围计算异常阶（1.5倍的四分位距）
    # step = (Q3 - Q1)
    step = 0
    outliers = logs[~((logs >= Q1 - step) & (logs <= Q3 + step))].index

    # 显示异常点
    print('异常点:Q1=',Q1, ', Q3=', Q3, ', records=', outliers)

    # 以下代码会移除outliers中索引的数据点
    logs[outliers] = np.nan
    print(logs)
    os.ex