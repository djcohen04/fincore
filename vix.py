import pandas as pd
from db.models import Tradable
from db.session import session

class VIXData(object):
    @classmethod
    def get(cls, timeindex):
        ''' Get a DataFrame of VIX data with the given timestamps index
            This can be used for concatenating onto an existing pandas DataFrame,
            as it may add some additional useful information for model development

            eg.
            from format import FeatureData
            spy = FeatureData('SPY')
            vix = VIXData.get(spy.rawdata.time)
            spy.rawdata = pd.concat([spy.rawdata, vix], axis=1)

        '''
        print 'Fetching & Formatting Auxilliary VIX Market Data...'

        # Create Empty DataFrame:
        vixdf = pd.DataFrame(index=timeindex, columns=['VIX', 'VIX-Return'])

        vix = session.query(Tradable).filter_by(name='VIX').first()
        if vix is None:
            print 'Warning: No Tradable For Symbol VIX present in the databse'
            return vixdf

        # Get Base VIX Data:
        prices = vix.prices()
        closes = prices.close
        times = prices.time
        returns = closes.pct_change()

        # Loop through VIX Prices Timestamps, update main vixdf correspondingly:
        timeindexset = set(timeindex)
        for i, timestamp in enumerate(times):
            if timestamp in timeindexset:
                vixdf['VIX'][timestamp] = closes.iloc[i]
                vixdf['VIX-Return'][timestamp] = returns.iloc[i]

        return vixdf
