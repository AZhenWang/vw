from app.saver.service.fina import Fina
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
from app.saver.logic import DB
from app.common.function import get_ratio, get_mean, adj_infation_rate


def get_reports(code_id, start_date='19900101', end_date='20190801'):

    balancesheets = Fina.get_report_info(code_id=code_id, start_date=start_date, end_date=end_date, TTB='balancesheet',
                                         report_type='1', end_date_type='%1231')
    if balancesheets.empty:
        nan_series = pd.Series()
        return nan_series, nan_series, nan_series, nan_series, nan_series, nan_series

    dividends = Fina.get_divdends(code_id=code_id, start_date=start_date, end_date=end_date)

    dividends['div_count'] = dividends['total_share'] * dividends['cash_div_tax']
    dividends.div_count.astype = 'float64'
    dividends.fillna(0)
    cash_divs = pd.Series(index=balancesheets['end_date'], name='cash_divs')
    dividends.set_index('end_date', inplace=True)
    for i in range(len(balancesheets)):
        ed = balancesheets.iloc[i]['end_date']
        sd = (datetime.strptime(ed, '%Y%m%d') - timedelta(days=364)).strftime('%Y%m%d')
        current_div = np.float(dividends.loc[sd:ed]['div_count'].sum())
        cash_divs.loc[ed] = current_div

    incomes = Fina.get_report_info(code_id=code_id, start_date=start_date, end_date=end_date, TTB='income',
                                   report_type='1', end_date_type='%1231')
    cashflows = Fina.get_report_info(code_id=code_id, start_date=start_date, end_date=end_date, TTB='cashflow',
                                     report_type='1', end_date_type='%1231')
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
    base = base.join(dailys[['cal_date', 'close', 'adj_close']].set_index('cal_date'))
    base = base.join(daily_basics[['cal_date', 'pe', 'pb', 'total_mv', 'total_share']].set_index('cal_date'))
    base.fillna(method='ffill', inplace=True)

    code_info = base.loc[incomes['end_date']]

    incomes = incomes[incomes['end_date'].isin(balancesheets.end_date)]
    cashflows = cashflows[cashflows['end_date'].isin(balancesheets.end_date)]

    if cashflows.empty or incomes.empty:
        print('数据为空！')

    balancesheets.fillna(method='backfill', inplace=True)
    incomes.set_index('end_date', inplace=True)
    balancesheets.set_index('end_date', inplace=True)
    cashflows.set_index('end_date', inplace=True)
    fina_indicators.set_index('end_date', inplace=True)
    stk_holdernumbers.set_index('end_date', inplace=True)
    holdernum = stk_holdernumbers['holder_num']
    return incomes, balancesheets, cashflows, fina_indicators, holdernum, code_info, cash_divs


def fina_kpi(incomes, balancesheets, cashflows, fina_indicators, holdernum, code_info, cash_divs):
    if incomes.empty:
        nan_series = pd.Series()
        return nan_series, nan_series,
    incomes.fillna(value=0, inplace=True)
    balancesheets.fillna(value=0, inplace=True)
    cashflows.fillna(value=0, inplace=True)

    goodwill = balancesheets['goodwill']
    goodwill.name = 'goodwill'

    equity = balancesheets['total_assets'] - balancesheets['total_liab']
    total_assets = balancesheets['total_assets']

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
    # receiv_pct[receiv_pct < 0] = 0


    # 企业财务造假的原因在于普通人只看净利润，也就是每股盈利，而不关注净利润的来源，是来源于把一些费用从盈余项目扣除，还是真正的到手的利润，
    # 假装好的东西都是把好的一面呈现在你面前，侧面和后面看里面是烂的，而真正好的东西，360度看都是好的。
    # 所以，像二郎神一样，用三只眼立体来看一个企业
    # 第一只眼： 净利润，评估平均每股收益
    # 第二只眼： 收入增速，评估未来10年价值，评估企业价值
    # 第三支眼： 销售收入现金流增速，评估未来10年现金流入，评估企业价值

    # 研发费用，除去某一年的突然研发费用干扰， 研发费用+营业利润的7年的平均值，作为利润的增长基础值
    rd_exp = get_rolling_mean(fina_indicators['rd_exp'].fillna(0), mtype=3)
    pure_equity = balancesheets['total_assets'] - balancesheets['total_liab'] - goodwill
    equity_pct = round((pure_equity - pure_equity.shift() + cash_divs) * 100/pure_equity, 1)
    freecash = cashflows['n_cashflow_act'] - cashflows['c_pay_acq_const_fiolta'] + fina_indicators['rd_exp'].fillna(0)
    freecash_rate = round(freecash * 100 / (equity), 1)
    freecash_mv = get_rolling_mean(freecash_rate, window=7)

    adj_ebit = (incomes['operate_profit'] + rd_exp)
    op_mv = get_rolling_mean(adj_ebit, window=7)
    op_mv_short = get_rolling_mean(adj_ebit, window=3)
    op_pct = round((op_mv_short - op_mv_short.shift() - (op_mv - op_mv.shift())) * 100 / op_mv, 1)
    compr_mv = get_rolling_mean(incomes['n_income_attr_p'], window=7)

    roe = round(compr_mv * 100 / equity, 2)
    ret = round(compr_mv * 100 / total_assets, 2)

    total_share = balancesheets['total_share'].fillna(method='backfill')

    eps = compr_mv / total_share.iloc[0]
    dt_eps = compr_mv / total_share

    eps_mul = code_info['close'] / dt_eps

    OPM = get_rolling_mean(oper_pressure, window=4, mtype=2)
    OPM = round(OPM, 2)
    opm_coef = get_opm_coef(oper_pressure)
    V = value_stock(roe, op_pct, OPM, opm_coef)

    # value_five_years = (1 + roe_op/100) ** 5
    pp = round(V * (1 + op_pct/100) * 8.5 / eps_mul, 2)
    peg = round(op_pct / 10, 2)
    eps = round(eps, 2)
    dt_eps = round(dt_eps, 2)
    eps_mul = round(eps_mul, 2)
    # print(round(pd.concat([code_info['adj_close'], op_mv_short, op_mv,  OPM, roe, op_pct, V, eps_mul, eps, pp, peg], axis=1), 2))
    # os.ex
    # 投入资产回报率 = （营业利润 - 新增应收帐款 * 新增应收账款占收入比重）/总资产
    sale_rate = round((incomes['operate_profit']) * 100 / incomes['revenue'], 2)
    sale_rate.fillna(method='backfill', inplace=True)
    em = round(total_assets / equity, 2)
    income_rate = round((incomes['n_income_attr_p']) * 100 / incomes['total_revenue'], 2)
    total_turn = round(incomes['revenue'] / total_assets, 2)
    total_assets.name = 'total_assets'
    pe = round(code_info['pe'], 2)
    pb = round(code_info['pb'], 2)
    total_mv = round(code_info['total_mv'], 2)
    receiv_income = round(balancesheets['accounts_receiv'] / incomes['n_income'], 2)

    libwithinterest = balancesheets['total_liab'] - balancesheets['acct_payable'] - balancesheets['adv_receipts']
    i_debt = round(libwithinterest * 100 / total_assets, 2)

    cash_act_in = round(get_ratio(cashflows['c_fr_sale_sg'].shift(), cashflows['c_fr_sale_sg']), 2)
    cash_act_out = round(get_ratio(cashflows['st_cash_out_act'].shift(), cashflows['st_cash_out_act']), 2)
    # 持续投入增速如果少于0，不能认定下年度持续减少下去，需要满足最低10%的投入增速期望值
    adj_cash_out = cash_act_out.copy()
    adj_cash_out[cash_act_out < 0] = 10
    adj_cash_out[cash_act_out > 30] = 0

    st_cash_out_act_next = (1+adj_cash_out/100)*cashflows['st_cash_out_act']
    cash_gap = (cashflows['end_bal_cash'] + balancesheets['nca_within_1y'] - balancesheets['non_cur_liab_due_1y'] \
                   + cashflows['n_cashflow_act'] \
                   # + cashflows['c_fr_sale_sg'] * adj_cash_in/100 \
                   - cashflows['st_cash_out_act'] * adj_cash_out/100 \
                   )
    cash_gap_r = round(cash_gap * 12 / st_cash_out_act_next, 2)

    # 股息率
    dyr = round(cash_divs * 100 / total_mv, 2)
    # 股息占当年收入比率
    dyr_or = round(cash_divs * 10000 * 100 / incomes['revenue'], 2)
    dyr_mean = round(get_rolling_mean(dyr), 2)

    roe_mean = round(get_mean(roe), 2)

    glem_V = glem_value_stock(roe, OPM, opm_coef)
    dpd_V = dpd_value_stock(roe, OPM, opm_coef)
    RR = round(V / eps_mul, 2)
    glem_RR = round(glem_V / eps_mul, 2)
    dpd_RR = round(dpd_V / eps_mul, 2)
    money_cap = round(balancesheets['money_cap'] * 100/ total_assets)
    holdernum_inc = round(get_ratio(holdernum.shift(), holdernum), 2)
    cash_act_in.name = 'cash_act_in'
    cash_act_out.name = 'cash_act_out'

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
    X4 = round((balancesheets['total_assets'] - balancesheets['total_liab'] - goodwill) / libwithinterest, 2)
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
    eps.name = 'eps'
    dt_eps.name = 'dt_eps'
    eps_mul.name = 'eps_mul'
    pp.name = 'pp'
    cash_gap.name = 'cash_gap'

    roe.name = 'roe'
    roe_mean.name = 'roe_mean'
    ret.name = 'ret'
    em.name = 'em'
    income_rate.name = 'income_rate'
    total_turn.name = 'total_turn'
    pe.name = 'pe'
    pb.name = 'pb'
    dyr.name = 'dyr'
    receiv_income.name = 'receiv_income'
    V.name = 'V'
    OPM.name = 'OPM'
    RR.name = 'RR'
    glem_RR.name = 'glem_RR'
    glem_V.name = 'glem_V'
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
    peg.name = 'peg'
    freecash_mv.name = 'freecash_mv'
    equity_pct.name = 'equity_pct'

    data = pd.concat(
        [round(code_info['adj_close'],2),  total_mv, income_rate,
         roe,  roe_mean, eps, dt_eps, eps_mul, pp, peg,
         holdernum, holdernum_inc,
         V, glem_V, dpd_V, dyr, dyr_or, dyr_mean,
         RR, glem_RR, dpd_RR,
         pe, pb, i_debt, capital_turn, oper_pressure, OPM,
         Z, X1, X2, X3, X4, X5,
         receiv_pct,cash_act_in, cash_act_out,cash_gap, cash_gap_r,
         freecash_mv, equity_pct, op_pct], axis=1)

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
    print(coefs)
    data.fillna(0, inplace=True)
    for j in range(1, len(data)):
        data.iloc[j] = (v[:j+1] * coefs[:j+1]).mean()
        print(data.iloc[j])
    return data

def get_rolling_mean(v, window=10, mtype=0):
    """
    获取运营资金压力移动平均值，为了对经营恶化保持更高灵明度，当前报告期如果是最大值，就不能减去
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
                data.iloc[j] = v[j-window:j + 1].mean()

    return round(data, 3)


def get_sale_roe(total_turn, sale_rate, equity_times):
    sale_rate_mv = get_rolling_mean(sale_rate, window=10, mtype=3)
    total_turn_mv = get_rolling_mean(total_turn, window=10, mtype=3)

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


def value_stock(IR, IR_a, OPM, opm_coef):
    """
    总投资10年，前5年保持现有增长率，后5年不增长，收入平稳

    """
    V = pd.Series(index=IR.index)

    L = 0.85
    for k in IR.index:
        v = 1
        E = 1
        if k not in OPM.index:
            continue
        ir = IR.loc[k] / 100
        a = IR_a.loc[k] / 100

        # for i in range(1, 11):
        #     # 贴现率
        #     if a < 5:
        #         ir = ir * (1+a)
        #         # print('ir=', ir, 'a=', a)
        #     E = E * (1 + ir)
        #     print('e=', E)
        #     if ir <= 0:
        #         E = 0
        #
        #     v += L ** i * E

        print('v0', v)
        v = E*(1+ir)**10
        v = v * (1 - OPM.loc[k] * opm_coef.loc[k])
        print('v=', v, 'opm=',OPM.loc[k], 'opm_coef=', opm_coef.loc[k])

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
    V = V * (1 - OPM * opm_coef)
    V = round(V, 2)
    return V

