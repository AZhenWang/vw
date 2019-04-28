# 表可操作字段

fields_map = {

    # 股票基本信息表
    'daily': [
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

    'stock_basic': [
        'ts_code', 'name', 'area', 'industry', 'market',
        'curr_type', 'list_status', 'list_date', 'delist_date', 'is_hs',
    ],
    'trade_cal': [
        'cal_date', 'is_open', 'pretrade_date',
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
}