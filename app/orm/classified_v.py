from sqlalchemy import String, Column, Integer, Float, TIMESTAMP
from app.orm.vm_engine import Base, Session


class M(Base):
    __tablename__ = "classified_v"
    id = Column(Integer, primary_key=True)
    date_id = Column(Integer)
    code_id = Column(Integer)
    classifier_id = Column(Integer)
    feature_group_number = Column(String(16))
    r2_score = Column(Float)
    classifier_v = Column(Float)
    cum_return = Column(Float)
    holding = Column(Integer)
    created_at = Column(TIMESTAMP)


class ClassifiedV(object):

    session = Session()
    instance = session.query(M)

    @classmethod
    def get_info(cls, code_id, date_id, feature_group_number=[], limit=1):
        model = cls.instance.filter(M.code_id == str(code_id), M.date_id == str(date_id))

        if len(feature_group_number) > 0:
            model = model.filter(M.feature_group_number.in_(feature_group_number))

        info = model.order_by(M.r2_score.desc()).limit(limit).all()
        return info



