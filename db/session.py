import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

engine = create_engine('sqlite:///db.db')
Session = sessionmaker(bind=engine)
Session.configure(bind=engine)

session = Session()
