"""Functionality to assist handling of dates within huntsman-drp."""
from contextlib import suppress
from datetime import datetime, timedelta
from dateutil.parser import parse as parse_date_dateutil
from astropy.io.fits import getval

import pandas as pd


def get_date_range_from_files(fnames):
    """Take a list of fits file names and extract the `DATE-OBS` keyword
    value from the headers and determine the set of unique dates in the list.

    Args:
        fnames (list of str): filenames of the fits images to examine.
    Returns:
        unique_dates (set of datetime objects): Set of unique dates represented as datetime objects.
    """
    unique_dates = set()
    for f in fnames:
        date = getval(f, 'DATE-OBS', 0)
        unique_dates.add(parse_date(date))
    return unique_dates


def get_date_range_from_docs(docs):
    """Take a list of calib docs and determine the set of unique
    calib dates.

    Args:
        docs (list of huntsman.drp.document): List of huntsman calib documents.

    Returns:
        (set of datetime objects): Set of unique dates represented as datetime objects
    """
    return set([parse_date(doc['date']) for doc in docs])


def validity_range(dates, validity=None):
    """ Take a list of calib docs to be ingested into a butler repo and determine
    the max and min dates and then add or subtract the validity parameter
    to produce the min and max valid dates for the calibs.

    Args:
        dates (set of datetime objects): set of datetime objects.
        validity (int): Number of days outside the min/max calib date range
        for which the calibs are considered valid for use.
    Returns:
        begin_date:
        end_date:
    """
    if validity is None:
        validity = timedelta(days=1)
    else:
        validity = timedelta(days=validity)
    begin_date = min(dates) - validity
    end_date = max(dates) + validity
    return begin_date, end_date


def parse_date(date):
    """ Parse a date as a `datetime.datetime`.
    Args:
        date (Object): The object to parse.
    Returns:
        A `datetime.datetime` object.
    """
    if isinstance(date, int):
        return datetime.fromtimestamp(date / 1e3)

    if isinstance(date, pd.Timestamp):
        return datetime.fromtimestamp(date)

    with suppress(AttributeError):
        date = date.strip("(UTC)")

    if type(date) is datetime:
        return date

    return parse_date_dateutil(date)


def date_to_ymd(object):
    """ Convert a date to YYYY:MM:DD format.
    Args:
        object (Object): An object that can be parsed using `parse_date`.
    Returns:
        str: The converted date.
    """
    date = parse_date(object)
    return date.strftime('%Y-%m-%d')


def current_date():
    """Returns the UTC time now as a `datetime.datetime` object."""
    return datetime.utcnow()


def current_date_ymd():
    """ Get the UTC date now in YYYY-MM-DD format.
    Returns:
        str: The date.
    """
    date = current_date()
    return date_to_ymd(date)


def make_mongo_date_constraint(date=None, date_min=None, date_max=None):
    """ Convenience function to make a mongo date constraint.
    Args:
        date (object, optional): If provided, restrict to this date.
        date_min (object, optional): If provided, use this as the minimum date (inclusive).
        date_max (object, optional): If provided, use this as the maximum date (non-inclusive).
    Returns:
        dict: The date constraint.
    """
    # Add date range to criteria if provided
    date_constraint = {}

    if date_min is not None:
        date_constraint.update({"$gte": parse_date(date_min)})
    if date_max is not None:
        date_constraint.update({"$lt": parse_date(date_max)})
    if date is not None:
        date_constraint.update({"$eq": parse_date(date)})

    return date_constraint
