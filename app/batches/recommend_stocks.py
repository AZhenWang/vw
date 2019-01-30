from app.saver.logic import DB
import pandas as pd
from app.models.pca import Pca
from app.common.function import send_email
from app.common.function import knn_predict
from email.mime.text import MIMEText
from email.mime.image import MIMEImage

pre_predict_interval = 2
sample_interval = 122
n_components = 2

def execute(start_date='', end_date=''):
    trade_cal = DB.get_open_cal_date(end_date=end_date, period=2)
    current_date_id = trade_cal.iloc[-1]['date_id']
    yester_date_id = trade_cal.iloc[0]['date_id']
    cal_date = trade_cal.iloc[-1]['cal_date']
    logs = DB.get_recommended_stocks(cal_date=cal_date, recommend_type='pca')
    logs = logs[logs['star_idx'] > 1]
    msgs = []
    pca = Pca(cal_date=cal_date)
    recommend_stocks = pd.DataFrame(columns=['ts_code', 'star_idx', 'last_date', 'last_idx',
                                             'average', 'moods', 'amplitude', 'knn_v'])
    for i in range(len(logs)):
        code_id = logs.iloc[i]['code_id']
        recommended = True
        if logs.iloc[i]['star_idx'] == 2:
            if logs.iloc[i]['amplitude'] < 4:
                recommended = False
        if recommended:
            # 获取上一次的推荐记录
            lastestrecommend_logs = DB.get_latestrecommend_logs(code_id=code_id, date_id=current_date_id, recommend_type='pca', number=1)
            last_recommend_date = ''
            last_recommend_star = ''
            if not lastestrecommend_logs.empty:
                last_recommend_date = lastestrecommend_logs.iloc[-1]['cal_date']
                last_recommend_star = lastestrecommend_logs.iloc[-1]['star_idx']
            content = {
                'ts_code': logs.iloc[i]['ts_code'],
                'star_idx': logs.iloc[i]['star_idx'],
                'last_date': last_recommend_date,
                'last_idx': last_recommend_star,
                'average': logs.iloc[i]['average'],
                'moods': logs.iloc[i]['moods'],
                'amplitude': logs.iloc[i]['amplitude'],
                'knn_v': '',
            }

            sample_pca, sample_prices, sample_Y = pca.run(code_id=code_id, sample_len=0,
                                                          n_components=n_components,
                                                          pre_predict_interval=pre_predict_interval,
                                                          return_y=True)
            pca = Pca(cal_date=cal_date)
            knn_v = ''
            for predict_idx in sample_Y.index[-5:]:
                y_hat = knn_predict(sample_pca, sample_Y, k=1, sample_interval=122,
                                    pre_predict_interval=pre_predict_interval, predict_idx=predict_idx)
                knn_v = knn_v + str(round(y_hat, 1)) + '  '
                content['knn_v'] = knn_v

            recommend_stocks.loc[i] = content



    recommend_text = recommend_stocks.to_string(index=False)

    msgs.append(MIMEText(recommend_text, 'plain', 'utf-8'))
    send_email(subject=end_date + '预测', msgs=msgs)

