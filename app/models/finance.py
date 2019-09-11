from app.saver.service.fina import Fina
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
from app.saver.logic import DB
from app.common.function import get_ratio


def get_reports(code_id, start_date='19900101', end_date='20190801'):

    balancesheets = Fina.get_report_info(code_id=code_id, start_date=start_date, end_date=end_date, TTB='balancesheet',
                                         report_type='1', end_date_type='%1231')
    incomes = Fina.get_report_info(code_id=code_id, start_date=start_date, end_date=end_date, TTB='income',
                                   report_type='1', end_date_type='%1231')
    cashflows = Fina.get_report_info(code_id=code_id, start_date=start_date, end_date=end_date, TTB='cashflow',
                                     report_type='1', end_date_type='%1231')

    balancesheets = balancesheets[balancesheets['end_date'].isin(incomes.end_date)]
    cashflows = cashflows[cashflows['end_date'].isin(incomes.end_date)]

    if balancesheets.empty or incomes.empty or cashflows.empty:
        nan_series = pd.Series()
        return nan_series, nan_series, nan_series, nan_series, nan_series, nan_series,  nan_series

    dividends = Fina.get_divdends(code_id=code_id, start_date=start_date, end_date=end_date)

    dividends['div_count'] = dividends['total_share'] * dividends['cash_div_tax'] * 10000
    dividends.div_count.astype = 'float64'
    dividends.fillna(0)
    cash_divs = pd.Series(index=incomes['end_date'], name='cash_divs')
    dividends.set_index('end_date', inplace=True)
    for i in range(len(balancesheets)):
        ed = balancesheets.iloc[i]['end_date']
        sd = (datetime.strptime(ed, '%Y%m%d') - timedelta(days=364)).strftime('%Y%m%d')
        current_div = np.float(dividends.loc[sd:ed]['div_count'].sum())
        cash_divs.loc[ed] = current_div

    fina_indicators = Fina.get_report_info(code_id=code_id, start_date=start_date, end_date=end_date,
                                           TTB='fina_indicator', end_date_type='%1231')
    stk_holdernumbers = Fina.get_report_info(code_id=code_id, start_date=start_date, end_date=end_date,
                                           TTB='stk_holdernumber', end_date_type='%1231')
    daily_basics = DB.get_table_logs(code_id=code_id, start_date_id=incomes.iloc[0]['date_id'],
                                     end_date_id=incomes.iloc[-1]['date_id'], table_name='daily_basic')
    dailys = DB.get_table_logs(code_id=code_id, start_date_id=incomes.iloc[0]['date_id'],
                               end_date_id=incomes.iloc[-1]['date_id'], table_name='daily')
    adj_factors = DB.get_table_logs(code_id=code_id, start_date_id=incomes.iloc[0]['date_id'],
                                    end_date_id=incomes.iloc[-1]['date_id'], table_name='adj_factor')

    base_date_id = np.sort(np.unique(np.concatenate((daily_basics.cal_date.values, incomes['end_date'].values))))
    base = pd.DataFrame(index=base_date_id)
    dailys['adj_close'] = dailys['close'] * adj_factors['adj_factor']
    base = base.join(adj_factors[['cal_date', 'adj_factor']].set_index('cal_date'))
    base = base.join(dailys[['cal_date', 'close', 'adj_close']].set_index('cal_date'))
    base = base.join(daily_basics[['cal_date', 'pe', 'pb', 'total_mv', 'total_share']].set_index('cal_date'))
    base.fillna(method='ffill', inplace=True)
    code_info = base.loc[incomes['end_date']]

    incomes.set_index('end_date', inplace=True)
    balancesheets.set_index('end_date', inplace=True)
    cashflows.set_index('end_date', inplace=True)
    fina_indicators.set_index('end_date', inplace=True)
    stk_holdernumbers.set_index('end_date', inplace=True)

    dates_base = pd.DataFrame(index=balancesheets.index)
    incomes = dates_base.join(incomes)
    cashflows = dates_base.join(cashflows)
    fina_indicators = dates_base.join(fina_indicators)
    stk_holdernumbers = dates_base.join(stk_holdernumbers)

    holdernum = stk_holdernumbers['holder_num']
    return incomes, balancesheets, cashflows, fina_indicators, holdernum, code_info, cash_divs


def fina_kpi(incomes, balancesheets, cashflows, fina_indicators, holdernum, code_info, cash_divs):
    if incomes.empty or len(balancesheets) < 4:
        nan_series = pd.Series()
        return nan_series
    incomes.fillna(method='backfill', inplace=True)
    incomes.fillna(value=0, inplace=True)
    balancesheets.fillna(value=0, inplace=True)
    cashflows.fillna(value=0, inplace=True)
    fina_indicators.fillna(value=0, inplace=True)
    goodwill = balancesheets['goodwill']
    goodwill.name = 'goodwill'

    equity = balancesheets['total_assets'] - balancesheets['total_liab']
    total_assets = balancesheets['total_assets']

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
    capital_tdays.name = 'capital_tdays'
    oper_fun.name = 'oper_fun'
    oper_pressure.name = 'oper_pressure'
    #     print('存货周转天数: inventories_days=', inventories_days, '应收账款周转天数: accounts_receiv_days=', accounts_receiv_days,
    #           '预收账款周转天数:adv_receipts_days=', adv_receipts_days, '资金周转天数:capital_turn_days=', capital_turn_days)
    #     print('资金周转率：capital_turn=', capital_turn)

    #     4、杜邦分析
    # receiv_inc = balancesheets['accounts_receiv'] - balancesheets['accounts_receiv'].shift()
    receiv = balancesheets['accounts_receiv'] + balancesheets['notes_receiv'] - balancesheets['adv_receipts']
    receiv_inc = receiv - receiv.shift()
    receiv_pct = round(receiv_inc * 100 / incomes['revenue'])

    # 调整折旧费用
    gross = incomes['revenue'] - incomes['oper_cost']
    dpba_of_assets = round(cashflows['depr_fa_coga_dpba'] * 100 / balancesheets['fix_assets'], 2)
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
    rd_exp_or = round(rd_exp * 100 / incomes['revenue'], 2)
    pure_equity = balancesheets['total_assets'] - balancesheets['total_liab'] - goodwill
    equity_pct = round((pure_equity - pure_equity.shift() + cash_divs) * 100/pure_equity.shift(), 1)
    fix_asset_pct = round(balancesheets['fix_assets'].pct_change()*100, 2)
    freecash = cashflows['free_cashflow'] + fina_indicators['rd_exp'].fillna(0)
    freecash_rate = round(freecash * 100 / (equity), 2)
    freecash_mv = round(get_mean_of_complex_rate(freecash_rate, window=10), 1)
    # 所得税缴纳基数
    tax_rate = round(incomes['income_tax'] * 100 / incomes['total_profit'], 2)
    # 应纳税额
    def_tax = cashflows['incr_def_inc_tax_liab'] + cashflows['decr_def_inc_tax_assets']
    def_tax.fillna(0, inplace=True)
    tax_payable = incomes['income_tax'] - def_tax
    tax_payable_pct = round(tax_payable.pct_change()*100, 2)
    tax_pct = round(incomes['income_tax'].pct_change()*100, 2)

    tax_diff = tax_payable - tax_payable.shift()
    tax_base = tax_payable.shift()[tax_diff >= 0].add(tax_payable[tax_diff < 0], fill_value=0.01)
    def_tax_ratio = round(def_tax * 100 / tax_base, 2)
    # 短期营业收入是否加速
    adj_ebit = (incomes['operate_profit'] + rd_exp + cashflows['depr_fa_coga_dpba'])
    # adj_ebit = tax_payable
    ebit_mv = get_rolling_mean(adj_ebit, mtype=3,  window=7)
    ebit_mv_short = get_rolling_mean(adj_ebit, mtype=3,  window=4)
    ebit_mv_short_pct = round((ebit_mv_short - ebit_mv_short.shift()) * 100 / ebit_mv_short.shift(), 2)
    ebit_mv_pct = round((ebit_mv - ebit_mv.shift()) * 100 / ebit_mv.shift(), 2)
    mix_op_diff = ebit_mv_short_pct - ebit_mv_pct

    # 收入，利润，负债，税的长期率
    rev_pct = get_rev_pct(incomes['revenue'], cash_divs)
    liab_pct = balancesheets['total_liab'].pct_change()*100
    liab_pctmv = get_mean_of_complex_rate(liab_pct)
    income_pct = incomes['n_income'].pct_change() * 100
    tax_payable_pct[tax_payable_pct > 100] = 100
    tax_payable_pct[tax_payable_pct < -50] = -50
    tax_payable_pctmv = get_mean_of_complex_rate(tax_payable_pct)
    rev_pctmv = get_mean_of_complex_rate(rev_pct)
    income_pctmv = get_mean_of_complex_rate(income_pct)
    equity_pctmv = get_mean_of_complex_rate(equity_pct)
    fix_asset_pctmv = get_mean_of_complex_rate(fix_asset_pct)
    income_rate = round((incomes['n_income_attr_p']) * 100 / incomes['revenue'], 2)
    total_turn = round(incomes['revenue'] / total_assets, 2)
    total_assets_pct = round(total_assets.pct_change()*100, 2)
    total_assets_pctmv = get_mean_of_complex_rate(total_assets_pct)
    total_assets.name = 'total_assets'
    OPM = get_rolling_median(oper_pressure, window=5)
    OPM = round(OPM, 2)
    opm_coef = get_opm_coef(OPM)
    op_pct = pd.concat([rev_pctmv - rev_pctmv.shift(), income_pctmv-income_pctmv.shift()], axis=1).min(axis=1)
    op_pct[op_pct > 30] = 30
    op_pct[op_pct < -30] = -30
    total_mv = round(code_info['total_mv'] * 10000, 2)
    roe = round(incomes['n_income_attr_p'] * 100 / balancesheets['total_hldr_eqy_exc_min_int'], 2)
    roe[roe > 30] = 30
    roe[roe < -30] = -30

    roe_std = round(get_rolling_std(roe, window=10), 2)
    ret = round(incomes['n_income'] * 100 / total_assets, 2)
    pe = round(code_info['pe'], 2)
    normal_equity = balancesheets['total_hldr_eqy_exc_min_int'] - balancesheets['oth_eqt_tools_p_shr'] - goodwill
    pb = total_mv / normal_equity
    roe_rd = round((incomes['n_income_attr_p']+rd_exp) * 100 / balancesheets['total_hldr_eqy_exc_min_int'], 2)
    roe_rd[roe_rd > 30] = 30
    roe_rd[roe_rd < -50] = -50
    roe_rd_mv = get_mean_of_complex_rate(roe_rd, window=10)

    # roe_mv = get_mean_of_complex_rate(roe, window=10)
    roe_mv = pd.Series(index=balancesheets.index)
    roe_mv.fillna(0, inplace=True)
    roe_mv.iloc[2] = roe_rd.iloc[2] / 100
    for i in range(3, len(roe_rd)):
        roe_mv.iloc[i] = (1 + roe_rd.iloc[i] / 100) ** (1 / 8) * (1 + roe_mv.iloc[i - 1]) ** (7 / 8) - 1
    roe_mv = round(roe_mv * 100, 2)

    libwithinterest = balancesheets['total_liab'] - balancesheets['acct_payable'] - balancesheets['adv_receipts']
    i_debt = round(libwithinterest * 100 / total_assets, 2)
    # 利息保障倍数
    interst = pd.concat([fina_indicators['interst_income'], incomes['int_exp']], axis=1).max(axis=1)
    IER = round((incomes['ebitda']) / (interst + 1), 2)
    cash_act_in = get_mv_pct(cashflows['c_inf_fr_operate_a'].pct_change() * 100)
    cash_act_out = get_mv_pct(cashflows['st_cash_out_act'].pct_change() * 100)
    cash_act_rate = round(cash_act_in / cash_act_out, 2)
    roe_adj = round(roe_mv * cash_act_rate, 2)

    # 未来10年涨幅倍数
    V = value_stock2(roe_mv, OPM, opm_coef)
    V_adj = value_stock(roe_mv, cash_act_in.diff(), OPM, opm_coef)
    V_tax = value_stock2(tax_payable_pctmv, OPM, opm_coef)
    total_turn_pctmv = round((rev_pctmv/ total_assets_pctmv).pct_change()*100, 2)
    # 赔率= 未来10年涨幅倍数/市现率
    pp = round(V / pb, 2)
    # 赔率，不计算加速度
    pp_adj = round(V_adj / pb, 2)
    # 税率验证：税的价值比总价值，要和税率相差无几，比如企业税率25%，那么这pp_tax的值不能离25太远
    pp_tax = round(V_tax * 100 / (V_tax + V_adj), 1)
    # 未来10年总营业增速
    dpd_V = dpd_value_stock(roe_mv, OPM, opm_coef)
    # 未来10年总营业增速/市盈率
    dpd_RR = round(dpd_V / pe, 2)

    next_V0 = V
    total_share = code_info['total_share'] * 10000

    LP = round(next_V0 / 2 * code_info['adj_factor'] * normal_equity / total_share, 2)
    MP = round(next_V0 * code_info['adj_factor'] * normal_equity / total_share, 2)
    HP = round(next_V0 * 2 * code_info['adj_factor'] * normal_equity / total_share, 2)
    win_base = pd.concat([HP, code_info['adj_close']], axis=1).min(axis=1)
    lose_base = pd.concat([LP, code_info['adj_close']], axis=1).min(axis=1)
    win_return = round((HP - code_info['adj_close'])*100/win_base, 2)
    lose_return = round((LP - code_info['adj_close']) * 100 / lose_base, 2)
    odds = win_return + lose_return

    # 投入资产回报率 = （营业利润 - 新增应收帐款 * 新增应收账款占收入比重）/总资产
    sale_rate = round((incomes['operate_profit']) * 100 / incomes['revenue'], 2)
    sale_rate.fillna(method='backfill', inplace=True)
    em = round(total_assets / equity, 2)
    receiv_income = round(balancesheets['accounts_receiv'] / incomes['n_income'], 2)


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
    X3 = round(incomes['ebit'] / total_assets, 2)
    # X4 = 股东权益 / 负债
    X4 = round((balancesheets['total_assets'] - balancesheets['total_liab'] - goodwill) / libwithinterest, 2)
    # X4 = round((balancesheets['total_assets'] - balancesheets['total_liab'] - goodwill) / libwithinterest, 2)
    # X5 = 销售收入 / 总资产
    X5 = round(incomes['revenue'] / total_assets, 2)
    Z = round(0.717 * X1 + 0.847 * X2 + 3.11 * X3 + 0.420 * X4 + 0.998 * X5, 2)
    X1.name = 'X1'
    X2.name = 'X2'
    X3.name = 'X3'
    X4.name = 'X4'
    X5.name = 'X5'
    Z.name = 'Z'
    cash_gap_r.name = 'cash_gap_r'
    pp.name = 'pp'
    pp_adj.name = 'pp_adj'
    pp_tax.name = 'pp_tax'
    cash_gap.name = 'cash_gap'

    roe.name = 'roe'
    roe_mv.name = 'roe_mv'
    roe_adj.name = 'roe_adj'
    ret.name = 'ret'
    em.name = 'em'
    income_rate.name = 'income_rate'
    total_turn.name = 'total_turn'
    pe.name = 'pe'
    pb.name = 'pb'
    dyr.name = 'dyr'
    receiv_income.name = 'receiv_income'
    V.name = 'V'
    V_adj.name = 'V_adj'
    V_tax.name = 'V_tax'
    OPM.name = 'OPM'
    dpd_RR.name = 'dpd_RR'
    dpd_V.name = 'dpd_V'
    i_debt.name = 'i_debt'
    receiv_pct.name = 'receiv_pct'
    money_cap.name = 'money_cap'
    dyr_or.name = 'dyr_or'
    dyr_mean.name = 'dyr_mean'
    op_pct.name = 'op_pct'
    holdernum_inc.name = 'holdernum_inc'
    holdernum.name = 'holdernum'
    freecash_mv.name = 'freecash_mv'
    equity_pct.name = 'equity_pct'
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
    LP.name = 'LP'
    MP.name = 'MP'
    HP.name = 'HP'
    data = pd.concat(
        [round(code_info['adj_close'],2), round(code_info['adj_factor'], 2), balancesheets['f_ann_date'], total_mv/10000, income_rate,
         roe, roe_adj, roe_mv, pp, pp_adj, pp_tax,
         holdernum, holdernum_inc,
         V, V_adj, V_tax, dpd_V, dyr, dyr_or, dyr_mean,  dpd_RR,
         pe, pb, i_debt, share_ratio, capital_turn, oper_pressure, OPM,
         Z, X1, X2, X3, X4, X5,
         receiv_pct, cash_act_in, cash_act_out, cash_act_rate, cash_gap, cash_gap_r,
         freecash_mv,  op_pct, mix_op_diff, tax_rate,
         dpba_of_assets, dpba_of_gross, IER, rd_exp_or, roe_std,
         equity_pct, fix_asset_pct, tax_payable_pct, def_tax_ratio,
         rev_pct, total_turn_pctmv, tax_pct, income_pct,
         rev_pctmv, total_assets_pctmv, liab_pctmv, income_pctmv, tax_payable_pctmv, equity_pctmv, fix_asset_pctmv,
         LP, MP, HP, win_return, lose_return, odds,
         ], axis=1)

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


def get_mean_of_complex_rate(v, window=10):
    """
    移动复合率的平均值
    :param v:
    :param window:
    :return:
    """
    base = pd.DataFrame(index=v.index)
    if v.isna().all():
        return v
    v.dropna(0, inplace=True)
    v[v <= -100] = -99
    v[v > 100] = 100
    v = v/100
    data = pd.Series(index=v.index, name='data')
    if len(v) == 1:
        data.iloc[0] = v.iloc[0]
        base = base.join(data)
        return base['data']

    mv = (1 + v.iloc[0]) * (1 + v.iloc[1])

    for i in range(2, len(data)):
        mv = mv * (1 + v.iloc[i])
        if i < window:
            max_v = v[0:i+1].max()
            min_v = v[0:i+1].min()
            t = mv / ((1+max_v) * (1+min_v))
            t = t**(1/(i-1)) - 1
        else:
            mv = mv / (1+v.iloc[i - window])
            t = mv**( 1 / window) - 1
        data.iloc[i] = round(t*100, 2)
    base = base.join(data)
    return base['data']


def get_mv_pct(pct):
    """
    获取移动的平均变化比例
    :param pct:
    :return:
    """
    pct[pct.isin([np.inf, -np.inf])] = np.nan
    pct.fillna(0, inplace=True)
    pct = pct / 100
    first_index = pct[pct != 0].index[0]
    first_loc = pct.index.get_loc(first_index)
    pct_mv = pd.Series(index=pct.index)
    pct_mv.iloc[first_loc] = pct.iloc[first_loc]
    for i in range(first_loc + 1, len(pct)):
        pct_mv.iloc[i] = (1 + pct.iloc[i]) ** (1 / 8) * (1 + pct_mv.iloc[i - 1]) ** (
                    7 / 8) - 1
    pct_mv = round(pct_mv * 100, 2)
    return pct_mv


def get_sale_roe(total_turn, sale_rate, equity_times):
    sale_rate_mv = get_rolling_mean(sale_rate, window=7, mtype=3)
    total_turn_mv = get_rolling_mean(total_turn, window=7, mtype=3)

    total_return = sale_rate_mv*total_turn_mv
    sale_roe = round(get_rolling_mean(total_return * equity_times), 2)
    # print(pd.concat([iocc, iocc_mv, equity_times, total_turn_mv, total_turn_pct, sale_roe], axis=1))
    # import matplotlib.pylab as plt
    # plt.plot(total_return, label='total_return')
    # plt.plot(iocc_mv, label='iocc_mv')
    # plt.plot(total_turn_mv, label='total_turn_mv')
    # plt.plot(equity_times, label='equity_times')
    # plt.plot(get_rolling_mean(total_return * equity_times), label='roe_mv')
    #
    # plt.legend()
    # plt.show()
    # os.ex

    return sale_roe


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
            if ir >= 0.3:
                ir = 0.3
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
    IR_a[IR_a > 5] = 5
    IR_a[IR_a < -5] = -5
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
            if i < 6:
                ir = ir*(1+a)
            if ir >= 0.3:
                ir = 0.3
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

