import pandas as pd
from app.saver.logic import DB
from app.common import function as CF

class Assembly(object):

    def __init__(self, end_date='', period=21):
        self.end_date = end_date
        self.period = period
        self.features = DB.get_features()
        self.adj_close = []


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

        group_number = 0
        for combined_cols in combined_cols_set:
            for cols in combined_cols:
                group_number += 1
                for feature_id in cols:
                    DB.insert_features_groups(feature_id, group_number)

    def pack_features(self, code_id):
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
        return X

    def get_threshold(self):
        up_threshold = -0.05
        down_threshold = -0.03
        next_adj_close = self.adj_close.shift(-1)
        fm = pd.concat([next_adj_close, self.adj_close], axis=1).min(axis=1)
        rate = (next_adj_close - self.adj_close) / fm

        rate_sma_supershort = rate.rolling(window=20).sum()
        rate_sma_long = rate.rolling(window=240).sum()
        threshold = pd.Series(down_threshold, index=self.adj_close.index)
        threshold[(rate_sma_supershort > 0.05) & (rate_sma_long > 0.15)] = up_threshold
        return threshold

    def pack_targets(self, pre_predict_interval):
        threshold = self.get_threshold()  # when the trend is up, it is 0.05, when the trend is downward, it is 0.03
        target_max = self.adj_close.shift(-pre_predict_interval).rolling(pre_predict_interval).max()
        target_min = self.adj_close.shift(-pre_predict_interval).rolling(pre_predict_interval).min()
        compare_v = (target_min - self.adj_close) / self.adj_close
        refer_target = target_min[compare_v <= threshold].add(target_max[compare_v > threshold], fill_value=0)
        diff = refer_target - self.adj_close
        fm = self.adj_close[diff >= 0].add(refer_target[diff < 0], fill_value=0)
        targets = diff / fm

        return targets
