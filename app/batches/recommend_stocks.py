from app.saver.logic import DB
from app.orm.classified_v import ClassifiedV as CV
import numpy as np
import pandas as pd
from app.saver.tables import fields_map
from app.common.function import send_sms


def execute(start_date='', end_date=''):
    trade_cal = DB.get_open_cal_date(end_date=end_date, period=2)
    code_list = DB.get_code_list(list_status='L')
    for i in range(1, len(trade_cal)):
        current_date_id = trade_cal.iloc[i]['date_id']
        last_date_id = trade_cal.iloc[i - 1]['date_id']
        # 清空当天的预测记录
        DB.delete_recommend_stock_logs(date_id=current_date_id)

        for j in range(len(code_list)):
            data = pd.DataFrame(columns=fields_map['recommend_stocks'])
            code_id = code_list.iloc[j]['code_id']
            # 获取前五个回报值最高的group
            top_five_group = CV.get_info(code_id=code_id, date_id=current_date_id, limit=5)
            groups = []
            info = {}
            for val in top_five_group:
                groups.append(val.feature_group_number)
                info[val.feature_group_number] = val
            # 获取这五个昨天的回报值
            last_top_five_group = CV.get_info(code_id=code_id, date_id=last_date_id, limit=5, feature_group_number=groups)
            star = []
            for val in last_top_five_group:
                yester = val.classifier_v
                today = info[val.feature_group_number].classifier_v
                if 0.01 < yester < today or (yester + today) > 0.10:
                    star.append(yester)
            star_idx = len(star)
            if star_idx > 0:
                average = np.array(star).mean()
                data.loc[j] = {
                                'code_id': code_id,
                                'date_id': current_date_id,
                                'recommend_type': 'classified_v',
                                'star_idx': star_idx,
                                'average': average,
                                }
                data.to_sql('recommend_stocks', DB.engine, index=False, if_exists='append', chunksize=1000)
