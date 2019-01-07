import time
import numpy as np
from db.models import *


class FeatureData(object):
    ''' Produces a formatted numpy array with input/output pairs, along with a
        list of feature descriptions

        Example:
            spydata = FeatureData('SPY')
            inputs, outputs, features = spydata.format()
        print "Input Shape: %s" % (inputs.shape,)
        print "Ouput Shape: %s" % (outputs.shape,)
        print "Features: %s" % len(features)
    '''
    def __init__(self, tradable):
        # Search for Tradable:
        self.tradable = session.query(Tradable).filter_by(name=tradable).first()
        if self.tradable is None:
            raise Exception("Tradable %s Note Found" % tradable)

        print "Found Tradable %s" % self.tradable
        self.fetchdata()

    def fetchdata(self):
        ''' Fetch raw tradable data directly from the database
        '''
        print "Fetching Raw Tradable Data from Database..."
        self.rawdata = self.tradable.data()

    def format(self, hard=False):
        ''' Returns a 3-tuple:
                - Input Feature Data (numpy array)
                - Output Data (numpy array)
                - List of Feature Descriptions (list)
        '''
        start = time.time()
        if hard:
            # Refetch database data
            self.fetchdata()

        # Collect the list of auxiliary features:
        nonaux = set(['close', 'open', 'high', 'low', 'time'])
        auxilliary = list(set(self.rawdata.columns).difference(nonaux))
        auxilliary.sort()

        inputs = []
        outputs = []
        period = 30
        length = len(self.rawdata)
        for i in range(length - period):
            if i % 1000 == 0:
                print "%.2f%%" % (100. * i / length)
            if i < period:
                continue

            window = self.rawdata[(i - period):i]

            times = list(window.time)
            ts = times[-1]
            if (ts - times[0]).total_seconds() > 60 * 60:
                # print "Skipping window (starts %s ends %s)" % (times[0], ts)
                continue
            times = [ts.minute, ts.hour, ts.weekday()]

            closes = list(window.close)
            opens = list(window.open)
            highs = list(window.high)
            lows = list(window.low)

            additional = list(window.iloc[-1][auxilliary])

            # Construct Feature Array:
            input = closes + opens + highs + lows + times + additional
            inputs.append(input)

            # Compute Target Value:
            output = (window.close + 1).cumprod().iloc[-1] - 1.
            outputs.append(output)

        # Get Features Array:
        features = [
            '%s -%s' % (metric, i)
            for metric in ['Close', 'Opens', 'Highs', 'Lows']
            for i in reversed(range(period))
        ] + ['Minute', 'Hours', 'Weekday'] + auxilliary

        inputs, outputs = np.array(inputs), np.array(outputs)
        print "Reformatted Data in %.2fs" % (time.time() - start)
        return inputs, outputs, features
