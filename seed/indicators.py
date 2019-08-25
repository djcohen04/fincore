''' Seed Data Script: Technical Indicators
'''

from models import *

indicators = [{
    'function': 'SMA',
    'interval': 'daily',
    'time_period': [5, 10, 20, 50],
    'series_type': 'close'
},{
    'function': 'EMA',
    'interval': 'daily',
    'time_period': [5, 10, 20, 50],
    'series_type': 'close'
},{
    'function': 'MACD',
    'interval': 'daily',
    'series_type': 'close'
},{
    'function': 'STOCH',
    'interval': 'daily',
    'series_type': 'close'
},{
    'function': 'RSI',
    'interval': 'daily',
    'time_period': [5, 10, 20, 50],
    'series_type': 'close'
},{
    'function': 'ADX',
    'interval': 'daily',
    'time_period': [5, 10, 20, 50],
    'series_type': 'close'
},{
    'function': 'CCI',
    'interval': 'daily',
    'time_period': [5, 10, 20, 50],
    'series_type': 'close'
},{
    'function': 'RSI',
    'interval': 'daily',
    'time_period': [5, 10, 20, 50],
    'series_type': 'close'
},{
    'function': 'AROON',
    'interval': 'daily',
    'time_period': [5, 10, 20, 50],
    'series_type': 'close'
},{
    'function': 'OBV',
    'interval': 'daily',
    'series_type': 'close'
},{
    'function': 'RSI',
    'interval': 'daily',
    'time_period': [5, 10, 20, 50],
    'series_type': 'close'
}]

def indicator_exists(args):
    # Check if indicator already exists in the database:
    if session.query(TechnicalIndicator).filter_by(args=args).first():
        return True
    else:
        return False

if __name__ == '__main__':
    for ind in indicators:
        if ind.get('time_period'):
            for tp in ind.get('time_period'):
                # make copy of indicator & overwrite time_period with tp value
                data = dict(ind)
                data['time_period'] = tp
                args = json.dumps(data)
                if not indicator_exists(args):
                    session.add(TechnicalIndicator(args=args))
        else:
            args = json.dumps(ind)
            if not indicator_exists(args):
                session.add(TechnicalIndicator(args=args))

    session.commit()
