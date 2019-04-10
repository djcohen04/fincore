import datetime
import traceback
from time import sleep
from db.models import *

'''
    API Specifications dictate that we only get 4/5 querys per minute, so
    after each query, we should sleep the program for ~15 seconds to avoid
    failed queries
'''


if __name__ == '__main__':

    prs = session.query(PriceRequest).filter_by(sent=False).all()
    trs = session.query(TechnicalRequest).filter_by(sent=False).all()

    print("Sending Price Queries")
    for pr in prs:
        try:
            pr.send()
            session.commit()
        except:
            print("Exception occured for price request: %s" % pr.id)
            print traceback.format_exc()
        print("Sleeping 15s, zzz")
        sleep(15.1)

    print("Sending Technical Indicator Queries")
    for tr in trs:
        try:
            cutoff = datetime.date(2018, 8, 1)
            tr.send(cutoff=cutoff)
            session.commit()
        except Exception as e:
            print traceback.format_exc()
            session.rollback()
            raise e
            # tr.sent = True
            # tr.successful = False
            # session.commit()

        print("Sleeping 15s, zzz")
        sleep(15.1)
