from app.models.common import Interface

from sklearn.neighbors import KNeighborsRegressor, NearestNeighbors
from sklearn.metrics import r2_score
import pandas as pd
import numpy as np
import random

random.seed(1)


class Knn(Interface):
    def __init__(self, start_date='', end_date='', sample_interval=12*20, pre_predict_interval=5, score_interval=60):
        """

        :param sample_interval: 样本区间天数
        :param pre_predict_interval: 向前预测间隔天数
        :param score_interval: 计算R2_SCORE的区间天数
        """
        self.start_date = start_date
        self.end_date = end_date
        self.sample_interval = sample_interval
        self.pre_predict_interval = pre_predict_interval
        self.score_interval = score_interval

    def run(self):
        pass
        
    
    def knn_predict(self, features, predict_date):
        """predict one date by knn algorithm

        Args:
            features: normalized features with DataFrame type
            predict_date:  datetime.date object, or a string with 'year-month-day' format

        Returns:
            predict_value: the value predicted by knn algorithm

        """
        try:
            predict_iloc = features.index.get_loc(predict_date)
        except KeyError:
            return np.nan

        if predict_iloc <= self.pre_predict_interval:
            return np.nan

        if predict_iloc <= self.sample_interval:
            sample_start_iloc = self.pre_predict_interval
        else:
            sample_start_iloc = predict_iloc - self.sample_interval

        sample_end_iloc = predict_iloc - self.pre_predict_interval

        predict_idx = features.index.get_loc(predict_date)

        train_X = features.iloc[sample_start_iloc:sample_end_iloc]
        test_x = features.iloc[predict_iloc]
        train_Y = self.targets.iloc[sample_start_iloc:sample_end_iloc]
        knn = KNeighborsRegressor(n_neighbors=5)
        #         knn = KNeighborsRegressor(n_neighbors=5, weights='distance')
        knn.fit(train_X, train_Y)
        predict_Y = knn.predict([test_x])
        predict_value = predict_Y[0]

        return predict_value

    def batch_knn_predict(self, columns):
        """batch predict by knn algorithm

        Args:
            columns: normalized features with DataFrame type
            plot: plot predicted values with line pictures

        Returns:
            predict_Y: predict values
            true_Y: true values
        """
        features = self.features[columns]
        predict_Y = pd.Series(index=pd.date_range(self.predict_start_date, self.predict_end_date))
        for predict_date in pd.date_range(start=self.predict_start_date, end=self.predict_end_date):
            result = self.knn_predict(features, predict_date)
            predict_Y.loc[predict_date] = result

        predict_Y.dropna(inplace=True)
        return predict_Y

    def combine_cols(self):
        """combine columns orderly
        """

        def combine(l, n):
            answers = []
            one = [0] * n

            def next_c(li=0, ni=0):
                if ni == n:
                    answers.append(one.copy())
                    return
                for lj in range(li, len(l)):
                    one[ni] = l[lj]
                    next_c(lj + 1, ni + 1)

            next_c()
            return answers

        self.columns.sort()
        length = len(self.columns)
        for i in range(1, length + 1):
            self.combined_cols_set.append(combine(self.columns, i))

    def test(self,  predict_start_date, predict_end_date, features, targets, log_n=1):
        """test

        """

        self.predict_start_date = predict_start_date
        max_date = max(features.index).date()
        if predict_end_date > max_date:
            self.predict_end_date = max_date
        else:
            self.predict_end_date = predict_end_date

        self.features = features
        self.targets_len = targets.index.size
        self.targets = targets
        self.columns = features.columns.values
        self.combined_cols_set = []
        self.combine_cols()
        self.n = log_n
        self.max_score_set = [-10000] * log_n
        self.max_score_cols = [-10000] * log_n
        self.min_score_set = [10000] * log_n
        self.min_score_cols = [10000] * log_n

        true_Y = self.targets[self.predict_start_date:self.predict_end_date]
        for combined_cols in self.combined_cols_set:
            for cols in combined_cols:
                # predict Y values
                predict_Y = self.batch_knn_predict(columns=cols)
                # r2 score
                score = r2_score(true_Y.dropna(), predict_Y[true_Y.notna()])
                for j in range(self.n):
                    if score > self.max_score_set[j]:
                        self.max_score_set[j] = score
                        self.max_score_cols[j] = cols
                        break
                    elif score < self.min_score_set[j]:
                        self.min_score_set[j] = score
                        self.min_score_cols[j] = cols
                        break
