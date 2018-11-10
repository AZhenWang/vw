from conf.myapp import db_config
from app.saver.tables import T
from app.saver import tables

from sqlalchemy import Column, String, create_engine
from sqlalchemy.orm import sessionmaker


class DB(object):

    # self.conn = mysql.connector.connect(**db_config)
    engine = create_engine('mysql+mysqlconnector://{user}:{password}@{host}:{port}/{database}'.format(**db_config), echo=True)
    # 创建DBSession类型:
    session = sessionmaker(bind=engine)()

    # @classmethod
    # def add(cls, table_model, kv):
    #     valid_fields = cls.validate_field(columns)
    #
    # def delete(self):
    #     pass
    #
    # def update(self):
    #     pass

    @classmethod
    def select(cls, cond={}):
        values = cls.session.execute('select * from %s where %s', (self.table_name, cond))
        return values

    @classmethod
    def check_newest(cls, table_model, ts_code=''):
        print(cls, table_model, ts_code)
        T.__table__.name = table_model
        t = cls.session.query(T).filter(T.ts_code == ts_code).all()
        print(t)
        return t

    @staticmethod
    def validate_field(columns):
        valid_fields = list((set(columns).union(set(self.fields))) ^ (set(columns) ^ set(self.fields)))
        return valid_fields


