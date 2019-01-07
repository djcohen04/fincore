from sqlalchemy import create_engine
from base import Base

if __name__ == '__main__':
    # Create the database:
    engine = create_engine('sqlite:///db.db')
    from models import *
    Base.metadata.create_all(engine)
