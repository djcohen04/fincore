# fincore
This is a repository built around the alphavantage [API](https://www.alphavantage.co) for building, storing, and managing a financial-data database.

## Initialization:
- Create a database for storage.  I would recommend running a postgres server in a docker container (see [here](https://hackernoon.com/dont-install-postgres-docker-pull-postgres-bee20e200198))
- Set the string value of `dbpath` in the `db/session` file to point towards your target database
- Get a free API Key [here](https://www.alphavantage.co/support/#api-key)
- Add a file `db/api_key.py`, and simply add one line: `API_KEY = '[Enter API Key Here]'`. Make sure this is kept private!
- Run `./bin/dbinit` from the command line. This should initialize your database and add some seed data
- Run `./bin/fetch` from the command line to run your first fetch.  If all works as planned, this should take a bit of time to fetch all the data from alphavantage, since there is a 4-request-per-minute limit for free users

These steps should get your local financial database up and running, and should give it some data to work with right away.


## Example Usage
```
from db.models import Tradable, session
spy = session.query(Tradable).filter_by(name='SPY').first()
prices = spy.prices()

#        open     close    low     high    time                       volume
# 58383  290.320  290.430  290.32  290.430 2019-04-22 15:54:00+00:00  384667.0
# 58773  290.430  290.360  290.31  290.435 2019-04-22 15:55:00+00:00  347707.0
# 58361  290.370  290.335  290.32  290.405 2019-04-22 15:56:00+00:00  322169.0
# 58575  290.330  290.240  290.23  290.340 2019-04-22 15:57:00+00:00  238364.0
# ...


```
