import requests
import datetime
from time import sleep
from db.models import *
from sqlalchemy import exc


def create_price_request(tradable, commit=False):
    print("Creating price Request for %s" % tradable.name)
    session.add(PriceRequest(tradable_id=tradable.id))

    if commit:
        session.commit()

def create_technical_request(tradable, technical, commit=False):
    print("Creating technical Request for %s: %s" % (tradable.name, technical.id))
    session.add(TechnicalRequest(
        tradable_id=tradable.id,
        technical_indicator_id=technical.id
    ))

    if commit:
        session.commit()


def maybe_add_price_request(tradable):
    now = datetime.datetime.now()
    latest = tradable.latest_price()
    if latest:
        # Check if latest price is less than 1 day old:
        seconds = (now - latest.time).total_seconds()
        if seconds > 60 * 60 * 24:
            create_price_request(tradable, commit=False)
    else:
        # No price data, create new price request:
        create_price_request(tradable, commit=False)


def maybe_add_technical_request(tradable, technical):
    latest = tradable.latest_technical(technical)
    today = datetime.date.today()
    if latest:
        # Check if latest data point is less than 7 days old
        days = (today - latest.date).days
        if days > 7:
            create_technical_request(tradable, technical, commit=False)
    else:
        # No data exists, create new request:
        create_technical_request(tradable, technical, commit=False)


if __name__ == '__main__':
    tradables = session.query(Tradable).all()
    technicals = session.query(TechnicalIndicator).all()

    for tradable in tradables:
        create_price_request(tradable, commit=False)
        # maybe_add_price_request(tradable)
        # for technical in technicals:
        #     create_technical_request(tradable, technical, commit=False)
            # maybe_add_technical_request(tradable, technical)

    session.commit()
