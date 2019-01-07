import time
from db.models import *

def deduplicate_prices():
    tradables = session.query(Tradable).all()
    for tradable in reversed(tradables):
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
        session.execute('''
                DELETE FROM price WHERE id IN (%s);
            ''' % (
                ','.join([str(id) for id in duplicates])
            )
        )
        session.commit()

        print "Deleted %s Duplicates for %s in %.2fs" % (len(duplicates), tradable, time.time() - start)


def deduplicate_techicals():
    tradables = session.query(Tradable).all()
    technicals = session.query(TechnicalIndicator).all()

    for tradable in tradables:
        for technical in technicals:
            print("Deduplicating %s %s" % (tradable.name, str(technical)))
            indicators = tradable.technicals(technical)
            count = 0
            dates = set()
            for indicator in indicators:
                if indicator.date in dates:
                    count += 1
                    session.delete(indicator)
                else:
                    dates.add(indicator.date)
            session.commit()

            if count:
                print("Found %s duplicates" % count)





if __name__ == '__main__':
    deduplicate_prices()
    # deduplicate_techicals()
