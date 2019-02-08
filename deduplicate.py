import json
import time
from db.models import *

def deduplicate_prices():
    tradables = session.query(Tradable).all()
    for tradable in tradables:
        start = time.time()

        prices = tradable.prices()

        times = prices[['id', 'time']]
        seen = set()
        duplicates = set()
        for i in range(len(times)):
            ts = times.iloc[i]['time']
            if ts in seen:
                id = times.iloc[i]['id']
                duplicates.add(id)
            else:
                seen.add(ts)

        # Delete duplicate ids:
        if duplicates:
            session.execute('''
                    DELETE FROM price WHERE id IN (%s);
                ''' % (
                    ','.join([str(id) for id in duplicates])
                )
            )
            session.commit()

            print "Deleted %s Duplicates for %s in %.2fs" % (len(duplicates), tradable, time.time() - start)
        else:
            print "No Duplicates Found for %s (%.2fs)" % (tradable, time.time() - start)

def deduplicate_techicals():
    tradables = session.query(Tradable).all()
    technicals = session.query(TechnicalIndicator).all()

    for tradable in tradables:
        for technical in technicals:
            print("Deduplicating %s %s..." % (tradable.name, str(technical)))

            # Collect the Indicator values for this Tradable/Techincal Pair:
            technical_requests = session.query(TechnicalRequest).filter_by(tradable_id=tradable.id, technical_indicator_id=technical.id).all()
            indicators = []
            for request in technical_requests:
                indicators += request.values

            count = 0
            dates = {}
            for indicator in indicators:
                if indicator.date in dates:
                    # Make sure this is indeed a duplicate:
                    oldval = json.loads(dates[indicator.date])
                    newval = json.loads(indicator.values)
                    if oldval == newval:
                        count += 1
                        session.delete(indicator)
                    else:
                        print 'WARNING: %s != %s (%s)' % (oldval, newval, indicator.date)
                else:
                    dates[indicator.date] = indicator.values
            session.commit()

            if count:
                print("Found %s duplicates" % count)





if __name__ == '__main__':
    deduplicate_prices()
    deduplicate_techicals()
