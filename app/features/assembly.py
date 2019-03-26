import pandas as pd
from conf.myapp import init_date
from app.saver.logic import DB
from app.common import function as CF


class Assembly(object):

    year_period = 244
    up_threshold = -0.05
    down_threshold = -0.03

    def __init__(self, end_date='', pre_predict_interval=5):
        self.end_date = end_date
        self.pre_predict_interval = pre_predict_interval
        self.features = DB.get_features()
        self.code_id = ''
        self.data = []
        self.adj_close = []
        self.date_idxs = []

    @staticmethod
    def init_features_table():
        features = {
            'RSI5': '5日移动RSI',
            'RSI10': '10日移动RSI',
            'Adj_SMA10_ratio': '复权价与10日移动平均线的比',
            'Adj_SMA5_ratio': '复权价与5日移动平均线的比',
            'Boll_ratio': '20日移动平均线的布尔比率',
            'Volume_SMA': '20日移动平均交易量',
            'Amplitude': '日实际变化幅度占当天总波动比率',
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

    @classmethod
    def init_thresholds_table(cls, end_date):
        # 只在初始化项目的时候使用
        DB.truncate_thresholds()
        codes = DB.get_code_list(list_status='L')
        for code_id in codes['code_id']:
            cls.update_threshold(code_id, cal_date=end_date)

    @classmethod
    def update_threshold(cls, code_id, cal_date, period=''):
        data = DB.get_code_info(code_id=code_id, end_date=cal_date, period=period)
        data = data[data['vol'] != 0]
        adj_close = data['close'] * data['adj_factor']

        # next_adj_close = adj_close.shift(-1)
        # fm = pd.concat([next_adj_close, adj_close], axis=1).min(axis=1)
        # rate = (next_adj_close - adj_close) / fm

        pre_adj_close = adj_close.shift()
        fm = pd.concat([pre_adj_close, adj_close], axis=1).min(axis=1)
        rate = (adj_close - pre_adj_close) / fm

        rate_sma_month = rate.rolling(window=20).sum()
        rate_sma_year = rate.rolling(window=cls.year_period).sum()

        simple_threshold_v = pd.Series(cls.down_threshold, index=adj_close.index)
        simple_threshold_v[(rate_sma_month > 0.06) & (rate_sma_year > 0.15)] = cls.up_threshold
        thresholds = pd.DataFrame({
            'date_id': data.index,
            'code_id': code_id,
            'SMS_month': rate_sma_month,
            'SMS_year': rate_sma_year,
            'simple_threshold_v': simple_threshold_v
        }).dropna()
        if not thresholds.empty:
            old_thresholds = DB.get_thresholds(code_id=code_id, start_date_id=thresholds.index[0], end_date_id=thresholds.index[-1])
            if not old_thresholds.empty:
                thresholds = thresholds[~thresholds['date_id'].isin(old_thresholds['date_id'])]
            if not thresholds.empty:
                thresholds.to_sql('thresholds', DB.engine, index=False, if_exists='append', chunksize=1000)

    @classmethod
    def update_threshold_by_date(cls, start_date='', end_date=''):
        trade_dates = DB.get_open_cal_date(start_date, end_date)
        if not trade_dates.empty:
            for date_id, cal_date in trade_dates[['date_id', 'cal_date']].values:
                codes = DB.get_trade_codes(date_id)
                for code_id in codes['code_id']:
                    cls.update_threshold(code_id=code_id, cal_date=cal_date, period=cls.year_period+1)

    def pack_features(self, code_id):
        self.code_id = code_id
        data = DB.get_code_info(code_id=code_id, start_date=init_date, end_date=self.end_date)
        data = data[data['vol'] != 0]

        Adj_close = data['close'] * data['adj_factor']
        Adj_open = data['open'] * data['adj_factor']

        dr_window5 = Adj_close.rolling(window=5)
        dr_window10 = Adj_close.rolling(window=10)
        dr_window20 = Adj_close.rolling(window=20)
        SMA20 = dr_window20.mean()
        SMA10 = dr_window10.mean()
        SMA5 = dr_window5.mean()
        Adj_SMA10_ratio = Adj_close / SMA10
        Adj_SMA5_ratio = Adj_close / SMA5

        std = dr_window20.std()
        Boll_ratio = (Adj_close - SMA20) / (2 * std)

        Volume_SMA = data['vol'] / data['vol'].rolling(window=5).mean()

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

        self.date_idxs = X.index
        self.adj_close = Adj_close[self.date_idxs]
        self.data = data.loc[self.date_idxs]

        return X

    def pack_targets(self):
        thresholds = DB.get_thresholds(code_id=self.code_id, start_date_id=self.date_idxs[0],
                                       end_date_id=self.date_idxs[-1])

        threshold = thresholds['simple_threshold_v']
        threshold = threshold.reindex(self.date_idxs, method='ffill', fill_value=-0.03)
        target_max = self.adj_close.shift(-self.pre_predict_interval).rolling(self.pre_predict_interval).max()
        target_min = self.adj_close.shift(-self.pre_predict_interval).rolling(self.pre_predict_interval).min()
        target_max = target_max[self.date_idxs]
        target_min = target_min[self.date_idxs]
        adj_close = self.adj_close[self.date_idxs].copy()

        compare_v = (target_min - adj_close) / adj_close
        refer_target = target_min[compare_v <= threshold].add(target_max[compare_v > threshold], fill_value=0)
        diff = refer_target - self.adj_close
        fm = adj_close[diff >= 0].add(refer_target[diff < 0], fill_value=0)
        targets = (diff / fm * 100).fillna(value=0)

        targets = targets[self.date_idxs]

        return targets
