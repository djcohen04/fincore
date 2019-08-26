# FinCore
This is a database-wrapper repository, that fetches, stores, and cleans financial information that is freely available via the [Alphavantage API](https://www.alphavantage.co). 

## Requirements:
1. You'll need a database, and will need to create a file `db/dbpaths.py`, and set `dbpath='[db address]'`
2. The initial database configuration can be accomplished by simply running `dbinit.py`
3. You will need a free API Key, which is available [here](https://www.alphavantage.co/support/#api-key).  
4. You will need to create another config file `db/api_key.py`, and set `API_KEY='[your key]'`
5. Optionally run the files in the `seed/` folder. These data files can be edited to fit your project's needs.

## Getting Data:
Data download is as simple as running `python fetch.py`.  Note, however, that free API users can only request four times per minute, so this should take several minutes, depending on how many tradables are in your database.

