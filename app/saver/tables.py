# 表可操作字段

fields_map = {

    # 股票基本信息表
    'daily': [
        'date_id', 'code_id', 'open', 'high', 'close', 'low', 'vol', 'amount',
    ],
    'daily_basic': [
        'date_id', 'code_id', 'close', 'turnover_rate', 'turnover_rate_f', 'pe', 'pe_ttm',
        'pb', 'ps', 'ps_ttm', 'total_share', 'float_share', 'free_share', 'total_mv', 'circ_mv',
    ],
    'adj_factor': [
        'date_id', 'code_id', 'adj_factor',
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
        'date_id', 'code_id', 'classifier_id', 'feature_group_id',
        'classifier_v', 'metric_type', 'metric_v'
    ],
}

