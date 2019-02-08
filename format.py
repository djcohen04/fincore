import time
import numpy as np
from vix import VIXData
from db.models import *


class Helpers():
    @classmethod
    def float(cls, value):
        ''' Try to typecast as a float
        '''
        try:
            return float(value)
        except:
            return value

    @classmethod
    def spans(cls, window, period):
        ''' Check if the given window spans the given period size
        '''
        current = window.time[-1]
        first = window.time[0]
        minutes = (current - first).total_seconds() / 60
        return (minutes == (period - 1))

    @classmethod
    def output(cls, window):
        ''' Get the return value over the given time series
            Note that since the 'close' values are given as
            minute-to-minute return values, we need to aggregate
            those values to get the cumulative return value
        '''
        return (window.close + 1).cumprod().iloc[-1] - 1.


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
    def __init__(self, symbol, dropna=True, getvix=False):
        self.symbol = symbol
        self.dropna = dropna
        self.getvix = getvix

        # Search for Tradable:
        self.tradable = session.query(Tradable).filter_by(name=symbol).first()
        if self.tradable is None:
            raise Exception("Tradable %s Note Found" % tradable)

        print "Found Tradable %s" % self.tradable
        self.fetchdata()

    def fetchdata(self):
        ''' Fetch raw tradable data directly from the database
        '''
        print "Fetching Raw Tradable Data from Database..."
        self.rawdata = self.tradable.data()

        # Make sure there are no duplicate timestamps in the timeseries:
        timeseries = self.rawdata.time
        if len(set(timeseries)) != len(timeseries):
            raise Exception('Error: Duplicate Time Stamps Exist in the Database, please deduplicate before proceeding')

        # Drop NaN values
        if self.dropna:
            self.rawdata = self.rawdata.dropna()

        if self.getvix:
            # Load & Concatenate VIX prices dataframe:
            vix = VIXData.get(self.rawdata.time)
            vix.index = self.rawdata.index
            self.rawdata = pd.concat([self.rawdata, vix], axis=1)

            # Might need to drop newly introduced NaN values:
            if self.dropna:
                self.rawdata = self.rawdata.dropna()

    def format(self, period=30, forecast=10):
        ''' Returns a 3-tuple:
                - Input Feature Data (numpy array)
                - Output Data (numpy array)
                - List of Feature Descriptions (list)

            The 'period' argument determines the number of minutes
        '''
        start = time.time()

        # Collect the list of auxiliary features:
        nonaux = set(['close', 'open', 'high', 'low', 'time', 'volume'])
        auxilliary = list(set(self.rawdata.columns).difference(nonaux))
        auxilliary.sort()

        inputs = []
        outputs = []
        windowsize = period + forecast
        valid = 0
        invalid = 0
        length = len(self.rawdata)
        for i in range(length - windowsize):
            # Occasionally Print The Progress:
            if i % 1000 == 0:
                print "%.2f%%" % (100. * i / length)

            # We need to have a full period worth of data before we start, so we
            # must wait until i >= period:
            if i < windowsize:
                continue

            # Get the full window of data:
            fullwindow = self.rawdata[(i - windowsize):i]
            # Make sure we have a consecutive window:
            if not Helpers.spans(fullwindow, windowsize):
                invalid += 1
                continue

            valid += 1

            # Get the feature/input window:
            window = fullwindow[:period]

            # Include a few temporal features:
            current = window.time[-1]
            times = [current.minute, current.hour, current.weekday()]

            # Inlcude each minute's close/high/low/open value for this tradable
            # in the last period:
            closes = list(window.close)
            opens = list(window.open)
            highs = list(window.high)
            lows = list(window.low)

            # Include any additional features:
            additional = list(window.iloc[-1][auxilliary])

            # Aggregate all features to create a Unified Feature Array:
            input = closes + opens + highs + lows + times + additional

            # Try to typecast all values as floats:
            input = [Helpers.float(v) for v in input]

            # Add new observation to full inputs list:
            inputs.append(input)

            # Compute & append new output/target value:
            output = Helpers.output(fullwindow[-forecast:])
            outputs.append(output)


        # Get Feature Descriptions Array:
        features = [
            '%s -%s' % (metric, i)
            for metric in ['Close', 'Opens', 'Highs', 'Lows']
            for i in reversed(range(period))
        ] + ['Minute', 'Hours', 'Weekday'] + auxilliary

        inputs, outputs = np.array(inputs), np.array(outputs)
        print 'Found & Formatted %s Valid and %s Invalid Feature Windows in %.2fs' % (valid, invalid, time.time() - start)
        return inputs, outputs, features
