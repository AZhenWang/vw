from app.common.globalvar import GL
import pandas as pd
import sqlalchemy as sa
from app.saver.logic import DB

class Assembly(object):
    def __init__(self, end_date='', period=3):
        self.end_date = end_date
        self.period = period
        self.trade_dates = DB.get_open_cal_date('', end_date, period)
        self.start_date = self.trade_dates.iloc[-1]['cal_date']

    def run(self):
        data = pd.read_sql(
            sa.text(
                ' SELECT d.open, d.close, d.high, d.low, d.vol, db.turnover_rate_f, af.adj_factor FROM daily d '
                ' left join daily_basic db on db.date_id = d.date_id and db.code_id = d.code_id'
                ' left join adj_factor af on af.date_id = d.date_id and af.code_id = d.code_id'
                ' left join trade_cal tc on tc.id = d.date_id'
                ' where tc.cal_date >= :sd and tc.cal_date <= :ed'),
            DB.engine,
            params={'sd': self.start_date, 'ed': self.end_date}
        )

        data = data[data['vol'] != 0]
        adjclose = data['close'] * data['adj_factor']

        dr_window5 = adjclose.rolling(window=5)
        dr_window10 = adjclose.rolling(window=10)
        dr_window20 = adjclose.rolling(window=20)
        SMA20 = dr_window20.mean()
        SMA10 = dr_window10.mean()
        SMA5 = dr_window5.mean()
        Adj_SMA20_ratio = adjclose / SMA20
        Adj_SMA10_ratio = adjclose / SMA10
        Adj_SMA5_ratio = adjclose / SMA5

        std = dr_window20.std()
        Boll_ratio = (adjclose - SMA20) / (2 * std)

        Volume_SMA = data['vol'] / data['vol'].rolling(window=20).mean()

        pre_adj_close = adjclose.shift(1)
        fm = pd.concat([adjclose, pre_adj_close], axis=1).min(axis=1)
        daily_return = (adjclose - pre_adj_close) / fm

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

        features = pd.DataFrame({
            'RSI5': RSI5,
            'RSI10': RSI10,
            'Adj_SMA20_ratio': Adj_SMA20_ratio,
            'Adj_SMA10_ratio': Adj_SMA10_ratio,
            'Adj_SMA5_ratio': Adj_SMA5_ratio,
            'Turnover_rate': data['turnover_rate_f'],
            'Boll_ratio': Boll_ratio,
            'Volume_SMA': Volume_SMA,
            'Amplitude': Amplitude,
        }).dropna()

        feature_names = features.columns


        return features
