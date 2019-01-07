from sqlalchemy import create_engine
from base import Base

if __name__ == '__main__':
    # Create the database:
    dbpath = 'postgresql://david:david@localhost/findb'
    engine = create_engine(dbpath)
    from models import *
    Base.metadata.create_all(engine)
