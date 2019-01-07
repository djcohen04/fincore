import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

dbpath = 'postgresql://david:david@localhost/findb'
engine = create_engine(dbpath)
Session = sessionmaker(bind=engine)
Session.configure(bind=engine)

session = Session()
