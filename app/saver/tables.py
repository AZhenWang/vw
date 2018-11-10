
fields_map = {
    'daily': [
        'ts_code', 'trade_date', 'open', 'high', 'close', 'low', 'vol', 'amount',
    ],
    'daily_basic': [
        'ts_code', 'trade_date', 'close', 'turnover_rate', 'turnover_rate_f', 'pe', 'pe_ttm',
        'pb', 'ps', 'ps_ttm', 'total_share', 'float_share', 'free_share', 'total_mv', 'circ_mv',
    ],
    'adj_factor': [
        'ts_code', 'trade_date', 'adj_factor',
    ],
    'stock_basic': [
        'ts_code', 'name', 'area', 'industry', 'market',
        'curr_type', 'list_status', 'list_date', 'delist_date', 'is_hs',
    ],
    'classifiers': [
        'ts_code', 'trade_date', 'knn_5',
    ],
    'thresholds': [
        'ts_code', 'trade_date', 'threshold_20',
    ],
}
