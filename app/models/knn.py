from app.models.common import Interface
from app.features.assembly import Assembly
from app.saver.logic import DB
from app.saver.tables import fields_map
from app.common.function import get_cum_return_rate

from sklearn.neighbors import KNeighborsRegressor
from sklearn.metrics import r2_score
import pandas as pd
import numpy as np
import random
from sklearn import preprocessing

random.seed(1)


class Knn(Interface):
    def __init__(self,
                 end_date='',
                 classifier_id='',
                 sample_interval=244,
                 pre_predict_interval=5,
                 memory_size=40,
                 single_code_id='',
                 ):
        """ knn 预测模型
        """

        self.end_date = end_date
        self.classifier_id = classifier_id
        self.sample_interval = sample_interval
        self.pre_predict_interval = pre_predict_interval
        self.memory_size = memory_size
        self.feature_assembly = Assembly(end_date=self.end_date,
                                         pre_predict_interval=self.pre_predict_interval)

        if single_code_id == '':
            codes = DB.get_latestopendays_code_list(
                latest_open_days=self.sample_interval + self.feature_assembly.year_period + 1)
            self.codes = codes['code_id']
            self.store = True   # 如果不传single_code_id,就遍历所有股票，存储结果，
        else:
            self.codes = [single_code_id]
            self.store = False  # 如果传了单只股票id,就不保存结果到数据库，而是返回预测结果

    def run(self):
        # 取2-5个特征一组
        min_portfolio = 7
        max_portfolio = 7
        features_groups = DB.get_features_groups()
        gp = features_groups.groupby('group_number')
        for code_id in self.codes:
            features = self.feature_assembly.pack_features(code_id)
            X = pd.DataFrame(preprocessing.MinMaxScaler().fit_transform(features), columns=features.columns,
                             index=features.index)
            Y = self.feature_assembly.pack_targets()
            Y_true = Y[-self.memory_size:]
            trade_dates = Y_true.index

            for group_number, group_data in gp:
                if group_data.shape[0] > max_portfolio or group_data.shape[0] < min_portfolio:
                    continue

                existed_classified_v = DB.get_classified_v(code_id, group_number, self.classifier_id)

                expired_classified_v = existed_classified_v[
                    ~existed_classified_v['date_id'].isin(trade_dates)]
                old_classified_v = existed_classified_v[
                    existed_classified_v['date_id'].isin(trade_dates)]
                new_dates = trade_dates[~trade_dates.isin(existed_classified_v['date_id'])]
                if not new_dates.empty:
                    new_classified_v = pd.DataFrame(columns=fields_map['classified_v'])
                    for predict_date_id in new_dates:
                        y_hat = self.knn_predict(X[group_data['name']].copy(), Y, predict_idx=predict_date_id)
                        if not np.isnan(y_hat):
                            # 保存Y_hat到数据库
                            new_classified_v.loc[predict_date_id] = {
                                'date_id': predict_date_id,
                                'code_id': code_id,
                                'classifier_id': self.classifier_id,
                                'classifier_v': y_hat,
                                'feature_group_number': group_number,
                                'r2_score': -0,
                                'cum_return': -0,
                                'holding': 0,
                            }

                    if not new_classified_v.empty:
                        Y_hat = old_classified_v['classifier_v'].append(new_classified_v['classifier_v'])
                        score = r2_score(Y_true.dropna(), Y_hat.dropna()[Y_true.notna()])
                        adj_prices = self.feature_assembly.adj_close[trade_dates]
                        holdings = self.get_holdings(Y_hat.values)
                        cum_return_set = get_cum_return_rate(prices=adj_prices, holdings=holdings)
                        cum_return = cum_return_set[-1]

                        new_classified_v.iloc[-1, -3] = str(round(score, 3))
                        new_classified_v.iloc[-1, -2] = str(round(cum_return, 3))
                        new_classified_v.holding = holdings

                        if not new_classified_v.empty:
                            DB.batch_delete_classified_v(expired_classified_v['id'])
                            new_classified_v.to_sql('classified_v', DB.engine, index=False, if_exists='append', chunksize=1000)

    def knn_predict(self, X, Y, predict_idx):
        """predict one date by knn algorithm

        Args:
            X: normalized features with DataFrame type
            Y: datetime.date object, or a string with 'year-month-day' format
            predict_idx: the index to predict

        Returns:
            y_hat: the value predicted by knn algorithm

        """
        try:
            predict_iloc = X.index.get_loc(predict_idx)
        except:
            return np.nan
        if predict_iloc < self.sample_interval:
            sample_start_iloc = 0
        else:
            sample_start_iloc = predict_iloc - self.sample_interval
        sample_end_iloc = predict_iloc - self.pre_predict_interval + 1
        training_X = X.iloc[sample_start_iloc:sample_end_iloc]
        training_Y = Y.iloc[sample_start_iloc:sample_end_iloc]
        testing_x = X.iloc[predict_iloc]
        knn = KNeighborsRegressor(n_neighbors=5)
        knn.fit(training_X, training_Y)
        predicted_Y = knn.predict([testing_x])
        y_hat = predicted_Y[0]
        return y_hat

    @staticmethod
    def get_holdings(Y_hat):
        holdings = [0] * 2
        holding = 0
        for i in range(2, len(Y_hat)):
            if Y_hat[i] <= 0 and Y_hat[i - 1] <= 0:
                holding = 0
            elif (Y_hat[i] > Y_hat[i - 1]) and (Y_hat[i - 1] > 0.01):
                holding = 1

            holdings.append(holding)

        return holdings
