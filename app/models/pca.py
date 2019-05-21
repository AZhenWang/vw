from app.features.assembly import Assembly
from sklearn.decomposition import PCA
import pandas as pd
from sklearn import preprocessing
import numpy as np


class Pca(object):
    def __init__(self,
                 cal_date='',
                 ):
        """ PCA 预测模型
        """

        self.cal_date = cal_date
        self.explained_variance_ratio_ = []

    def run(self, code_id, n_components=1, pre_predict_interval=5, return_y=False, TTB='daily'):
        feature_assembly = Assembly(end_date=self.cal_date, pre_predict_interval=pre_predict_interval, TTB=TTB)
        X = feature_assembly.pack_features(code_id)

        sample_prices = feature_assembly.adj_close
        X = pd.DataFrame(preprocessing.MinMaxScaler().fit_transform(X), columns=X.columns, index=X.index)

        pca = PCA(n_components=n_components)
        pca.fit(X)
        self.explained_variance_ratio_ = pca.explained_variance_ratio_
        # pca_X = pca.fit_transform(X)
        # samples_pca = pca_X.iloc[-length:]
        sample_pca = pd.DataFrame(pca.transform(X),
                                  columns=['col_' + str(i) for i in range(n_components)])

        diff_Y0 = np.where(np.diff(sample_pca.col_0) > 0, 1, -1)
        diff_Y1 = np.where(np.diff(sample_pca.col_1) > 0, 1, -1)
        diff_price = np.where(np.diff(sample_prices) > 0, 1, -1)
        dot_price_Y0 = np.dot(diff_Y0, diff_price)
        dot_price_Y1 = np.dot(diff_Y1, diff_price)
        if dot_price_Y0 < 0:
            print('转Y0')
            sample_pca.col_0 = (-1) * sample_pca.col_0
        if dot_price_Y1 < 0:
            print('转Y1')
            sample_pca.col_1 = (-1) * sample_pca.col_1

        if return_y:
            sample_Y = feature_assembly.pack_targets()
            return sample_pca, sample_prices, sample_Y, feature_assembly.data
        else:
            return sample_pca, sample_prices


