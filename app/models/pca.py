from app.models.common import Interface
from app.features.assembly import Assembly
from scipy import stats
from sklearn.decomposition import PCA
import matplotlib.pylab as plt
import pandas as pd
import numpy as np
from app.common.function import get_cum_return_rate, get_buy_sell_points
from sklearn import preprocessing


class Pca(Interface):
    def __init__(self,
                 cal_date='',
                 ):
        """ PCA 预测模型
        """

        self.cal_date = cal_date

    def run(self, codes=[], length=80):
        for code_id in codes:
            feature_assembly = Assembly(end_date=self.cal_date)
            X = feature_assembly.pack_features(code_id)
            Adj_close = feature_assembly.adj_close
            # X['Turnover_rate'], _ = stats.boxcox(X['Turnover_rate'])
            # X['Volume_SMA'], _ = stats.boxcox(X['Volume_SMA'])
            # X = X.drop(columns=['Turnover_rate'])
            # X = X[['Turnover_rate', 'Volume_SMA']]
            X = pd.DataFrame(preprocessing.MinMaxScaler().fit_transform(X), columns=X.columns, index=X.index)

            samples = X.iloc[-length:]
            sample_closes = Adj_close[-length:]
            n_components = 2
            pca = PCA(n_components=n_components)
            pca.fit(X)
            # pca_X = pca.fit_transform(X)
            # samples_pca = pca_X.iloc[-length:]
            samples_pca = pd.DataFrame(pca.transform(samples),
                                       columns=['col_'+str(i) for i in range(n_components)])
            print(
                pd.concat([
                    X.mean(),
                    X.std(),
                    X.max(),
                    X.min()],
                keys=['mean', 'std', 'max', 'min'],
                axis=1)
            )

            print(
                pd.concat([
                    samples.mean(),
                    samples.std(),
                    samples.max(),
                    samples.min()],
                    keys=['mean', 'std', 'max', 'min'],
                    axis=1)
            )

            fig, ax = plt.subplots(2, 1, figsize=(16, 8))
            x_axis = [i for i in range(length)]

            pca_mean = samples_pca.mean()
            pca_std = samples_pca.std()
            print('pca_mean=', pca_mean)
            print('pca_std=', pca_std)

            column_loc = 0
            mean = pca_mean[column_loc]
            std = pca_std[column_loc]
            #
            # mean_1 = pca_mean[1]
            # std_1 = pca_std[1]

            ax1 = ax[0]
            ax1.plot(x_axis, samples_pca)
            ax1.set_ylabel('pca')
            ax1.axhline(mean - 3 * std, color='b')
            ax1.axhline(mean - 2 * std, color='c')
            # ax1.axhline(mean - 1.618 * std, color='gray')
            # ax1.axhline(mean - 1.5 * std, color='gray')
            ax1.axhline(mean - 1 * std, color='green')
            ax1.axhline(mean, color='black')
            ax1.axhline(mean + 1 * std, color='green')
            # ax1.axhline(mean + 1.618 * std, color='gray')
            # ax1.axhline(mean + 1.5 * std, color='gray')
            ax1.axhline(mean + 2 * std, color='c')
            ax1.axhline(mean + 3 * std, color='b')

            # ax1.axhline(mean_1 - 3 * std, color='gray')
            # ax1.axhline(mean_1 - 2 * std_1, color='gray')
            # # ax1.axhline(mean_1 - 1.618 * std, color='gray')
            # # ax1.axhline(mean_1 - 1.5 * std, color='gray')
            # # ax1.axhline(mean_1 - 1 * std_1, color='gray')
            # # ax1.axhline(0, color='black')
            # # ax1.axhline(mean_1, color='gray')
            # # ax1.axhline(mean_1 + 1 * std_1, color='gray')
            # # ax1.axhline(mean + 1.618 * std, color='gray')
            # # ax1.axhline(mean_1 + 1.5 * std, color='gray')
            # ax1.axhline(mean_1 + 2 * std_1, color='gray')
            # ax1.axhline(mean_1 + 3 * std, color='gray')

            # x_axis = mdates.date2num(data['cal_date'].apply(lambda x: dt.strptime(x, '%Y%m%d')))
            ax1_1 = ax1.twinx()
            ax1_1.plot(x_axis, sample_closes.iloc[-length:], 'bo-', label='price')
            ax1_1.set_ylabel('price')

            holdings = self.get_holdings(samples_pca['col_0'], mean, std)
            cum_return_rate_set = get_cum_return_rate(sample_closes, holdings)
            buy, sell = get_buy_sell_points(holdings)

            ax1_1.plot(x_axis, np.multiply(sample_closes, buy), 'r^', label='buy')
            ax1_1.plot(x_axis, np.multiply(sample_closes, sell), 'g^', label='sell')

            ax2 = ax[1]
            ax2.plot(x_axis, cum_return_rate_set, label=str(code_id) + '=' + str(round(cum_return_rate_set[-1], 2))
                                                                            + ', min=' + str(min(cum_return_rate_set)))
            ax2.set_title('Cumulative rate of return')
            plt.title(code_id)
            plt.legend()
            ax2.legend()
            plt.grid(axis='x')

            plt.show()

    @staticmethod
    def get_holdings(Y, mean, std):
        holdings = [0] * 3
        holding = 0
        for i in range(3, len(Y)):
            for dis in [mean-3*std, mean-2*std, mean-1*std, mean, mean+1*std, mean+2*std, mean+3*std]:
                if Y[i-1] > dis > Y[i]:
                    holding = 0
                if Y[i-1] < dis < Y[i]:
                    holding = 1
            holdings.append(holding)
        return holdings
