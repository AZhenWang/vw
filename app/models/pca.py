from app.features.assembly import Assembly
from sklearn.decomposition import PCA
import pandas as pd
from sklearn import preprocessing


class Pca(object):
    def __init__(self,
                 cal_date='',
                 ):
        """ PCA 预测模型
        """

        self.cal_date = cal_date
        self.explained_variance_ratio_ = []

    def run(self, code_id, n_components=1, pre_predict_interval=5, return_y=False):
        feature_assembly = Assembly(end_date=self.cal_date, pre_predict_interval=pre_predict_interval)
        X = feature_assembly.pack_features(code_id)

        # X = X[['Boll_ratio']]
        # X = X[['Boll_ratio', 'RSI5', 'Amplitude']]
        # X = X[['Adj_SMA10_ratio', 'RSI10', 'Volume_SMA']]
        # X = X[['Boll_ratio', 'Volume_SMA']]
        # X = X[['BELG_SAM5', 'BLG_SAM5', 'Boll_ratio', 'Volume_SMA']]
        # X = X[['BELG_SAM5', 'BLG_SAM5', 'BMD_SAM5', 'BSM_SAM5']]

        X = X[['RSI5', 'RSI10', 'Adj_SMA10_ratio', 'Adj_SMA5_ratio', 'Boll_ratio', 'Volume_SMA', 'Amplitude', 'BELG_SAM5']]
        # X = X[['RSI5', 'RSI10', 'Adj_SMA10_ratio', 'Adj_SMA5_ratio', 'Boll_ratio', 'Volume_SMA', 'Amplitude']]
        sample_prices = feature_assembly.adj_close
        X = pd.DataFrame(preprocessing.MinMaxScaler().fit_transform(X), columns=X.columns, index=X.index)

        pca = PCA(n_components=n_components)
        pca.fit(X)
        self.explained_variance_ratio_ = pca.explained_variance_ratio_
        # pca_X = pca.fit_transform(X)
        # samples_pca = pca_X.iloc[-length:]
        sample_pca = pd.DataFrame(pca.transform(X),
                                  columns=['col_' + str(i) for i in range(n_components)])

        if return_y:
            sample_Y = feature_assembly.pack_targets()
            return sample_pca, sample_prices, sample_Y, feature_assembly.data
        else:
            return sample_pca, sample_prices


