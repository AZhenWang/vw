from app.saver.common import Base
import pandas as pd


class YY8GSer(Base):

    @classmethod
    def delete_logs(cls, code_id, start_date_id, end_date_id, TTB, g_type):
        pd.io.sql.execute(' delete from YY8G '
                          ' where code_id = %s '
                          ' and date_id >= %s and date_id <= %s '
                          ' and TTB=%s'
                          ' and g_type = %s',
                          cls.engine, params=[str(code_id), str(start_date_id), str(end_date_id), TTB, g_type])
