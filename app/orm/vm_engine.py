from conf.myapp import db_config
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base

vm_engine = create_engine(
    'mysql+mysqlconnector://{user}:{password}@{host}:{port}/{database}'.format(**db_config), echo=True)

Base = declarative_base()

Base.metadata.create_all(vm_engine)

Session = sessionmaker(bind=vm_engine)

