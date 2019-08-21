# 表可操作字段

fields_map = {

    # 股票基本信息表
    'daily': [
        'date_id', 'code_id', 'open', 'high', 'close', 'low', 'vol', 'amount', 'pct_chg'
    ],
    'weekly': [
        'date_id', 'code_id', 'open', 'high', 'close', 'low', 'vol', 'amount', 'pct_chg'
    ],
    'monthly': [
        'date_id', 'code_id', 'open', 'high', 'close', 'low', 'vol', 'amount', 'pct_chg'
    ],
    'daily_basic': [
        'date_id', 'code_id', 'close', 'turnover_rate', 'turnover_rate_f', 'pe', 'pe_ttm',
        'pb', 'ps', 'ps_ttm', 'total_share', 'float_share', 'free_share', 'total_mv', 'circ_mv',
    ],
    'adj_factor': [
        'date_id', 'code_id', 'adj_factor',
    ],
    'moneyflow': [
        'date_id', 'code_id', 'buy_sm_vol', 'buy_sm_amount', 'sell_sm_vol', 'sell_sm_amount', 'buy_md_vol', 'buy_md_amount',
        'sell_md_vol', 'sell_md_amount',  'buy_lg_vol', 'buy_lg_amount', 'sell_lg_vol', 'sell_lg_amount',
        'buy_elg_vol', 'buy_elg_amount', 'sell_elg_vol', 'sell_elg_amount', 'net_mf_vol', 'net_mf_amount'
    ],
    'block_trade': [
        'date_id', 'code_id', 'price', 'vol', 'amount', 'buyer', 'seller'
    ],

    'stock_basic': [
        'ts_code', 'name', 'area', 'industry', 'market',
        'curr_type', 'list_status', 'list_date', 'delist_date', 'is_hs',
    ],
    'trade_cal': [
        'cal_date', 'is_open', 'pretrade_date',
    ],

    'mv_moneyflow': [
        'code_id', 'date_id', 'trf2', 'max1_trf2', 'max6_trf2', 'trf2_a', 'trf2_v',
        'beta_trf2',  'peak', 'bottom', 'qqb','bt_times', 'bt_amounts',
        'net12', 'net2', 'net34', 'pv12', 'pv2', 'pv34',
        'net12_sum2', 'net12_sum6', 'net2_sum2', 'net2_sum6',
        'diff_12', 'diff_2', 'fluctuate'
    ],

    '2line': [
        'code_id', 'date_id', 'sm_l1', 'sm_l2', 'lg_l1', 'lg_l2', 'top', 'bot', 'from_top', 'from_bot'
    ],

    # 指数
    'index_basic': [
        'ts_code', 'name', 'market', 'publisher', 'category', 'base_date', 'base_point',
        'weight_rule', 'desc', 'exp_date'
    ],
    'index_daily': [
        'index_id', 'date_id', 'close', 'open', 'high', 'low', 'pre_close', 'change', 'pct_chg', 'vol', 'amount'
    ],
    'index_dailybasic': [
        'index_id', 'date_id', 'total_mv', 'float_mv', 'total_share', 'float_share', 'free_share',
        'turnover_rate', 'turnover_rate_f', 'pe', 'pe_ttm', 'pb'
    ],

    # 期货表
    'fut_basic': [
        'ts_code', 'symbol', 'exchange', 'name', 'fut_code', 'multiplier', 'trade_unit', 'quote_unit', 'quote_unit_desc',
        'd_mode_desc', 'list_date', 'delist_date', 'd_month', 'last_ddate', 'trade_time_desc'
    ],

    'fut_daily': [
        'fut_id', 'date_id', 'pre_close', 'pre_settle', 'open', 'high', 'low', 'close', 'settle', 'change1', 'change2',
        'vol', 'amount', 'oi', 'oi_chg',
    ],

    # 公司报表
    'income': [
        'code_id', 'date_id', 'f_ann_date', 'end_date', 'report_type', 'comp_type',
        'basic_eps', 'diluted_eps', 'total_revenue',
        'revenue','int_income','prem_earned','comm_income','n_commis_income','n_oth_income','n_oth_b_income',
        'prem_income','out_prem','une_prem_reser','reins_income','n_sec_tb_income','n_sec_uw_income',
        'n_asset_mg_income','oth_b_income','fv_value_chg_gain','invest_income','ass_invest_income','forex_gain',
        'total_cogs','oper_cost','int_exp','comm_exp','biz_tax_surchg','sell_exp','admin_exp','fin_exp',
        'assets_impair_loss','prem_refund','compens_payout','reser_insur_liab','div_payt','reins_exp',
        'oper_exp','compens_payout_refu','insur_reser_refu','reins_cost_refund','other_bus_cost','operate_profit',
        'non_oper_income','non_oper_exp','nca_disploss','total_profit','income_tax','n_income','n_income_attr_p',
        'minority_gain','oth_compr_income','t_compr_income','compr_inc_attr_p','compr_inc_attr_m_s',
        'ebit','ebitda','insurance_exp','undist_profit','distable_profit',
    ],

    'balancesheet': [
        'code_id', 'date_id', 'f_ann_date', 'end_date', 'report_type', 'comp_type',
        'total_share',  'cap_rese', 'undistr_porfit', 'surplus_rese', 'special_rese', 'money_cap', 'trad_asset',
        'notes_receiv', 'accounts_receiv', 'oth_receiv', 'prepayment', 'div_receiv', 'int_receiv', 'inventories',
        'amor_exp', 'nca_within_1y', 'sett_rsrv', 'loanto_oth_bank_fi', 'premium_receiv', 'reinsur_receiv',
        'reinsur_res_receiv', 'pur_resale_fa', 'oth_cur_assets', 'total_cur_assets', 'fa_avail_for_sale',
        'htm_invest', 'lt_eqt_invest', 'invest_real_estate', 'time_deposits', 'oth_assets',
        'lt_rec', 'fix_assets', 'cip', 'const_materials', 'fixed_assets_disp', 'produc_bio_assets',
        'oil_and_gas_assets', 'intan_assets', 'r_and_d', 'goodwill', 'lt_amor_exp', 'defer_tax_assets',
        'decr_in_disbur', 'oth_nca', 'total_nca', 'cash_reser_cb', 'depos_in_oth_bfi', 'prec_metals',
        'deriv_assets', 'rr_reins_une_prem', 'rr_reins_outstd_cla', 'rr_reins_lins_liab', 'rr_reins_lthins_liab',
        'refund_depos', 'ph_pledge_loans', 'refund_cap_depos', 'indep_acct_assets', 'client_depos', 'client_prov',
        'transac_seat_fee', 'invest_as_receiv', 'total_assets', 'lt_borr', 'st_borr', 'cb_borr', 'depos_ib_deposits',
        'loan_oth_bank', 'trading_fl', 'notes_payable', 'acct_payable', 'adv_receipts', 'sold_for_repur_fa',
        'comm_payable', 'payroll_payable', 'taxes_payable', 'int_payable', 'div_payable', 'oth_payable', 'acc_exp',
        'deferred_inc', 'st_bonds_payable', 'payable_to_reinsurer', 'rsrv_insur_cont', 'acting_trading_sec',
        'acting_uw_sec', 'non_cur_liab_due_1y', 'oth_cur_liab', 'total_cur_liab', 'bond_payable', 'lt_payable',
        'specific_payables', 'estimated_liab', 'defer_tax_liab', 'defer_inc_non_cur_liab', 'oth_ncl', 'total_ncl',
        'depos_oth_bfi', 'deriv_liab', 'depos', 'agency_bus_liab', 'oth_liab', 'prem_receiv_adva',
        'depos_received', 'ph_invest', 'reser_une_prem', 'reser_outstd_claims', 'reser_lins_liab', 'reser_lthins_liab',
        'indept_acc_liab', 'pledge_borr', 'indem_payable', 'policy_div_payable', 'total_liab',
        'treasury_share', 'ordin_risk_reser', 'forex_differ', 'invest_loss_unconf', 'minority_int',
        'total_hldr_eqy_exc_min_int', 'total_hldr_eqy_inc_min_int', 'total_liab_hldr_eqy', 'lt_payroll_payable',
        'oth_comp_income', 'oth_eqt_tools', 'oth_eqt_tools_p_shr', 'lending_funds', 'acc_receivable', 'st_fin_payable',
        'payables', 'hfs_assets', 'hfs_sales',
    ],

    'cashflow': [
        'code_id', 'date_id', 'f_ann_date', 'end_date', 'report_type', 'comp_type', 'net_profit',
        'finan_exp','c_fr_sale_sg','recp_tax_rends','n_depos_incr_fi','n_incr_loans_cb',
        'n_inc_borr_oth_fi','prem_fr_orig_contr','n_incr_insured_dep','n_reinsur_prem','n_incr_disp_tfa',
        'ifc_cash_incr','n_incr_disp_faas','n_incr_loans_oth_bank','n_cap_incr_repur','c_fr_oth_operate_a',
        'c_inf_fr_operate_a','c_paid_goods_s','c_paid_to_for_empl','c_paid_for_taxes','n_incr_clt_loan_adv',
        'n_incr_dep_cbob','c_pay_claims_orig_inco','pay_handling_chrg','pay_comm_insur_plcy','oth_cash_pay_oper_act',
        'st_cash_out_act','n_cashflow_act','oth_recp_ral_inv_act','c_disp_withdrwl_invest','c_recp_return_invest',
        'n_recp_disp_fiolta','n_recp_disp_sobu','stot_inflows_inv_act','c_pay_acq_const_fiolta','c_paid_invest',
        'n_disp_subs_oth_biz','oth_pay_ral_inv_act','n_incr_pledge_loan','stot_out_inv_act','n_cashflow_inv_act',
        'c_recp_borrow','proc_issue_bonds','oth_cash_recp_ral_fnc_act','stot_cash_in_fnc_act','free_cashflow',
        'c_prepay_amt_borr','c_pay_dist_dpcp_int_exp','incl_dvd_profit_paid_sc_ms','oth_cashpay_ral_fnc_act',
        'stot_cashout_fnc_act','n_cash_flows_fnc_act','eff_fx_flu_cash','n_incr_cash_cash_equ','c_cash_equ_beg_period',
        'c_cash_equ_end_period','c_recp_cap_contrib','incl_cash_rec_saims','uncon_invest_loss','prov_depr_assets',
        'depr_fa_coga_dpba','amort_intang_assets','lt_amort_deferred_exp','decr_deferred_exp','incr_acc_exp',
        'loss_disp_fiolta','loss_scr_fa','loss_fv_chg','invest_loss','decr_def_inc_tax_assets','incr_def_inc_tax_liab',
        'decr_inventories','decr_oper_payable','incr_oper_payable','others','im_net_cashflow_oper_act',
        'conv_debt_into_cap','conv_copbonds_due_within_1y','fa_fnc_leases','end_bal_cash','beg_bal_cash',
        'end_bal_cash_equ','beg_bal_cash_equ','im_n_incr_cash_equ',
    ],


    'fina_indicator': [
        'code_id', 'date_id', 'end_date', 'eps', 'dt_eps', 'total_revenue_ps',
        'revenue_ps', 'interst_income', 'ebit', 'ebitda', 'bps', 'ocfps', 'rd_exp',
        'current_ratio', 'quick_ratio', 'cash_ratio', 'assets_turn', 'debt_to_eqt',
        'dt_eps_yoy', 'tr_yoy', 'or_yoy', 'q_op_yoy', 'q_op_qoq', 'equity_yoy', 'netprofit_margin',
        'grossprofit_margin', 'cogs_of_sales', 'expense_of_sales', 'ebit_of_gr'

        ],

    'fina_audit': [
        'code_id', 'date_id', 'end_date', 'audit_result', 'audit_fees', 'audit_agency', 'audit_sign',
    ],

    'fina_mainbz': [
        'code_id', 'end_date', 'type', 'bz_item', 'bz_sales', 'bz_profit', 'bz_cost', 'curr_type'
    ],

    'dividend': [
        'code_id', 'date_id', 'end_date', 'div_proc', 'stk_div', 'stk_bo_rate', 'stk_co_rate', 'cash_div',
        'cash_div_tax', 'record_date', 'ex_date', 'pay_date', 'div_listdate', 'imp_ann_date'
    ],

    'fina_sys': [
        'code_id', 'comp_type', 'end_date', 'adj_close', 'total_mv',
        'income_rate', 'roe', 'sale_roe', 'eps', 'eps_mul', 'pp',
        'V', 'glem_V', 'dpd_V', 'dyr', 'dyr_or', 'dyr_mean',
        'RR', 'glem_RR', 'dpd_RR',
        'pe', 'pb', 'i_debt', 'capital_turn', 'oper_pressure', 'OPM',
        'X1', 'X2', 'X3', 'X4', 'X5', 'Z',
        'free_cash_mv', 'lib_cash', 'receiv_pct', 'money_cap'
    ],
    'fina_recom_logs': [
        'code_id', 'end_date', 'act', 'adj_close', 'years', 'result'
    ],

    'features': [
        'name', 'remark',
    ],
    'features_groups': [
        'feature_id', 'group_number'
    ],
    'classifiers': [
        'class_name', 'params',
    ],
    'classified_v': [
        'date_id', 'code_id', 'classifier_id', 'classifier_v', 'feature_group_number',
        'r2_score', 'cum_return', 'holding'
    ],

    # 股票预测
    'recommend_stocks': [
        'date_id', 'code_id', 'recommend_type', 'star_idx', 'average', 'qqb', 'moods', 'flag',
        'pre4_sum', 'pre40_sum', 'pre40_positive_mean', 'pre40_negative_mean'
    ],

    'focus_stocks': [
        'code_id', 'star_idx', 'predict_rose', 'recommend_type',
        'recommended_date_id', 'closed_date_id', 'holding_date_id', 'star_count'
    ],
    'tp_logs': [
        'date_id', 'code_id', 'today_v', 'tomorrow_v',
        'diff', 'mean', 'std', 'pca_diff', 'pca_mean', 'pca_min', 'pca_diff_mean', 'pca_diff_std'
    ],
    'pool': [
        'code_id',
    ],
    'macro_pca': [
        'date_id', 'code_id', 'TTB', 'flag', 'qqb', 'std0', 'std1',
        'Y0', 'Y1', 'Y0_chg', 'Y1_chg', 'Y0_line', 'Y0_pre_line', 'Y1_line', 'Y1_pre_line'
    ],
    'beta': [
        'date_id', 'code_id', 'index_id', 'mean_daily_return', 'std_daily_return', 'beta'
    ],
    'ab_test': [
        'sql_title', 'sql_content', 'sql_params', 'is_open'
    ],
    'ab_test_logs': [
        'note', 'numbers', 'codes', 'shot_ratio',
        'm_mean', 'm_std', 'm_max_rate', 'm_min_rate', 'max_rate', 'min_rate',
        'sql_title', 'sql_content', 'sql_params'
    ],

    # 八卦
    'YY8G': [
       'date_id', 'code_id', 'TTB', 'qqb', 'section', 'g_number', 'diff_g', 'g_type', 'g_object', 'peak', 'bottom'
    ],
}