from datetime import datetime
from huntsman.drp.utils.date import current_date, parse_date, current_date_ymd


def test_parse_date_datetime():
    parse_date(datetime.today())


def test_date_to_ymd():
    date = current_date()
    assert current_date_ymd() == date.strftime('%Y-%m-%d')
