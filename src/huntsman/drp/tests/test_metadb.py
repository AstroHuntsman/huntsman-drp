import os
import pytest
from dateutil.parser import parse as parse_date


def test_query_by_date(metadatabase):
    date_min = parse_date("2021-08-20")
    date_max = parse_date("2022-08-20")
    filenames = metadatabase.query_files(date_min=date_min, date_max=date_max)
    assert len(filenames) == 6
    date_min = parse_date("2020-08-20")
    date_max = parse_date("2021-08-20")
    filenames = metadatabase.query_files(date_min=date_min, date_max=date_max)
    assert len(filenames) == 6
    date_min = parse_date("2020-08-20")
    date_max = parse_date("2022-08-20")
    filenames = metadatabase.query_files(date_min=date_min, date_max=date_max)
    assert len(filenames) == 12
