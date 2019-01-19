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

    def run(self, code_id, sample_len=240, n_components=2):
        feature_assembly = Assembly(end_date=self.cal_date)
        X = feature_assembly.pack_features(code_id)
        prices = feature_assembly.adj_close

        X = X.drop(columns=['Turnover_rate'])
        X = pd.DataFrame(preprocessing.MinMaxScaler().fit_transform(X), columns=X.columns, index=X.index)
        samples = X.iloc[-sample_len:]
        sample_prices = prices[-sample_len:]

        pca = PCA(n_components=n_components)
        pca.fit(X)
        self.explained_variance_ratio_ = pca.explained_variance_ratio_
        # pca_X = pca.fit_transform(X)
        # samples_pca = pca_X.iloc[-length:]
        sample_pca = pd.DataFrame(pca.transform(samples),
                                   columns=['col_'+str(i) for i in range(n_components)])
        return sample_pca, sample_prices


