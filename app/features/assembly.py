import pandas as pd
from app.saver.logic import DB
from app.common import function as CF


class Assembly(object):

    def __init__(self, end_date='', sample_interval=12*20, pre_predict_interval=5):
        self.end_date = end_date
        self.period = sample_interval + pre_predict_interval + 20
        self.pre_predict_interval = pre_predict_interval
        self.sample_interval = sample_interval
        self.features = DB.get_features()
        self.code_id = ''
        self.adj_close = []
        self.date_idxs = []

    @staticmethod
    def init_features_table():
        features = {
            'RSI5': '5日移动RSI',
            'RSI10': '10日移动RSI',
            'Adj_SMA20_ratio': '复权价与20日移动平均线的比',
            'Adj_SMA10_ratio': '复权价与10日移动平均线的比',
            'Adj_SMA5_ratio': '复权价与5日移动平均线的比',
            'Turnover_rate': '换手率（自由流通股）',
            'Boll_ratio': '20日移动平均线的布尔比率',
            'Volume_SMA': '20日移动平均交易量',
            'Amplitude': '日实际变化幅度占当天总波动比率',
            'Adj_close': '复权收盘价'
        }
        DB.truncate_features()
        for name in features:
            DB.insert_features(name=name, remark=features[name])

        features = DB.get_features()
        DB.truncate_features_groups()
        ids = features['id'].astype('str')
        combined_cols_set = CF.combine_cols(ids)

        group_id = 0
        group_prefix = 'daily_'
        for combined_cols in combined_cols_set:
            for cols in combined_cols:
                group_id += 1
                group_number = group_prefix+str(group_id)
                for feature_id in cols:
                    DB.insert_features_groups(feature_id, group_number)

    @staticmethod
    def update_threshold(start_date='', end_date=''):
        year_period = 244
        up_threshold = -0.05
        down_threshold = -0.03

        trade_dates = DB.get_open_cal_date(start_date, end_date)
        if not trade_dates.empty:
            for date_id, cal_date in trade_dates[['date_id', 'cal_date']].values:
                codes = DB.get_trade_codes(date_id)
                for code_id in codes['code_id']:
                    data = DB.get_code_info(code_id=code_id,  end_date=cal_date, period=2*year_period+1)
                    data = data[data['vol'] != 0]
                    adj_close = data['close'] * data['adj_factor']

                    next_adj_close = adj_close.shift(-1)
                    fm = pd.concat([next_adj_close, adj_close], axis=1).min(axis=1)
                    rate = (next_adj_close - adj_close) / fm

                    rate_sma_supershort = rate.rolling(window=20).sum()
                    rate_sma_long = rate.rolling(window=year_period).sum()
                    if not rate_sma_long.empty and not rate_sma_long.dropna().empty:
                        latest_idx = rate_sma_long.index[-2]
                        SMS_month = rate_sma_supershort.loc[latest_idx]
                        SMS_year = rate_sma_long.loc[latest_idx]
                        simple_threshold_v = down_threshold
                        if SMS_month > 0.05 and SMS_year > 0.15:
                            simple_threshold_v = up_threshold
                        DB.insert_threshold(code_id=code_id, date_id=latest_idx, SMS_month=SMS_month, SMS_year=SMS_year,
                                            simple_threshold_v=simple_threshold_v)


    def pack_features(self, code_id):
        self.code_id = code_id
        data = DB.get_code_info(code_id=code_id, end_date=self.end_date, period=self.period)
        data = data[data['vol'] != 0]
        Adj_close = data['close'] * data['adj_factor']

        dr_window5 = Adj_close.rolling(window=5)
        dr_window10 = Adj_close.rolling(window=10)
        dr_window20 = Adj_close.rolling(window=20)
        SMA20 = dr_window20.mean()
        SMA10 = dr_window10.mean()
        SMA5 = dr_window5.mean()
        Adj_SMA20_ratio = Adj_close / SMA20
        Adj_SMA10_ratio = Adj_close / SMA10
        Adj_SMA5_ratio = Adj_close / SMA5

        std = dr_window20.std()
        Boll_ratio = (Adj_close - SMA20) / (2 * std)

        Volume_SMA = data['vol'] / data['vol'].rolling(window=20).mean()

        pre_adj_close = Adj_close.shift(1)
        fm = pd.concat([Adj_close, pre_adj_close], axis=1).min(axis=1)
        daily_return = (Adj_close - pre_adj_close) / fm

        dr_positive = daily_return.copy()
        dr_positive[dr_positive < 0] = 0
        dr_nagetive = daily_return.copy()
        dr_nagetive[dr_positive > 0] = 0
        dr_position_SMA5 = dr_positive.rolling(window=5).mean()
        dr_nagetive_SMA5 = dr_nagetive.rolling(window=5).mean()
        dr_position_SMA10 = dr_positive.rolling(window=10).mean()
        dr_nagetive_SMA10 = dr_nagetive.rolling(window=10).mean()
        RSI5 = dr_position_SMA5 / (dr_position_SMA5 - dr_nagetive_SMA5)
        RSI10 = dr_position_SMA10 / (dr_position_SMA10 - dr_nagetive_SMA10)

        Amplitude = (data['close'] - data['open']) / (data['high'] - data['low'])
        Amplitude.fillna(1, inplace=True)
        Turnover_rate = data['turnover_rate_f']

        feature_dict = {}
        for feature in self.features['name']:
            feature_dict[feature] = eval(feature)

        X = pd.DataFrame(feature_dict).dropna()
        self.adj_close = Adj_close
        self.date_idxs = X.index

        return X

    def pack_targets(self):
        thresholds = DB.get_thresholds(code_id=self.code_id, start_date_id=self.date_idxs[0],
                                       end_date_id=self.date_idxs[-1])
        threshold = pd.Series(-0.03, index=self.date_idxs)
        threshold.loc[thresholds.index] = thresholds['simple_threshold_v']
        target_max = self.adj_close.shift(-self.pre_predict_interval).rolling(self.pre_predict_interval).max()
        target_min = self.adj_close.shift(-self.pre_predict_interval).rolling(self.pre_predict_interval).min()
        target_max = target_max[self.date_idxs]
        target_min = target_min[self.date_idxs]
        adj_close = self.adj_close[self.date_idxs].copy()

        compare_v = (target_min - adj_close) / adj_close
        refer_target = target_min[compare_v <= threshold].add(target_max[compare_v > threshold], fill_value=0)
        diff = refer_target - self.adj_close
        fm = adj_close[diff >= 0].add(refer_target[diff < 0], fill_value=0)
        targets = diff / fm

        targets = targets[self.date_idxs]

        return targets
