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
        'code_id','date_id','ann_date','f_ann_date','report_type','comp_typ','basic_eps','diluted_eps','total_revenue',
        'revenue','int_income','prem_earned','comm_income','n_commis_income','n_oth_income','n_oth_b_income',
        'prem_income','out_prem','une_prem_reser','reins_income','n_sec_tb_income','n_sec_uw_income',
        'n_asset_mg_income','oth_b_income','fv_value_chg_gain','invest_income','ass_invest_income','forex_gain',
        'total_cogs','oper_cost','int_exp','comm_exp','biz_tax_surchg','sell_exp','admin_exp','fin_exp',
        'assets_impair_loss','prem_refund','compens_payout','reser_insur_liab','div_payt','reins_exp',
        'oper_exp','compens_payout_refu','insur_reser_refu','reins_cost_refund','other_bus_cost','operate_profit',
        'non_oper_income','non_oper_exp','nca_disploss','total_profit','income_tax','n_income','n_income_attr_p',
        'minority_gain','oth_compr_income','t_compr_income','compr_inc_attr_p','compr_inc_attr_m_s',
        'ebit','ebitda','insurance_exp','undist_profit','distable_profit','update_flag',
    ],

    # 股票预测信息表
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
    ]
}