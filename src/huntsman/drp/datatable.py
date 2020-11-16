"""Code to interface with the Huntsman database."""
from copy import deepcopy
from collections import abc
from contextlib import suppress
from datetime import timedelta
from urllib.parse import quote_plus

import numpy as np
import pandas as pd

from pymongo import MongoClient
from pymongo.errors import ServerSelectionTimeoutError

from huntsman.drp.utils.date import parse_date, current_date
from huntsman.drp.utils.query import Criteria, QueryCriteria, encode_mongo_value
from huntsman.drp.base import HuntsmanBase


class DataTable(HuntsmanBase):
    """ """
    _required_columns = None

    def __init__(self, **kwargs):
        HuntsmanBase.__init__(self, **kwargs)
        self._date_key = self.config["mongodb"]["date_key"]

        # Initialise the DB
        self._table_name = self.config["mongodb"]["tables"][self._table_key]
        db_name = self.config["mongodb"]["db_name"]
        self._initialise(db_name, self._table_name)

    def _initialise(self, db_name, table_name):
        """
        Initialise the datebase.
        Args:
            db_name (str): The name of the (mongo) database.
            table_name (str): The name of the table (mongo collection).
        """
        # Connect to the mongodb
        hostname = self.config["mongodb"]["hostname"]
        port = self.config["mongodb"]["port"]
        if "username" in self.config["mongodb"].keys():
            username = quote_plus(self.config["mongodb"]["username"])
            password = quote_plus(self.config["mongodb"]["password"])
            uri = f"mongodb://{username}:{password}@{hostname}/{db_name}?ssl=true"
            self._client = MongoClient(uri)
        else:
            self._client = MongoClient(hostname, port)
        try:
            self._client.server_info()
            self.logger.debug(f"Connected to mongodb at {hostname}:{port}.")
        except ServerSelectionTimeoutError as err:
            self.logger.error(f"Unable to connect to mongodb at {hostname}:{port}.")
            raise err
        self._db = self._client[db_name]
        self._table = self._db[table_name]

    def query(self, criteria=None):
        """ Get data for one or more matches in the table.
        Args:
            criteria (dict, optional): The query criteria.
        Returns:
            pd.DataFrame: The query result.
        """
        if criteria is not None:
            criteria = QueryCriteria(criteria).to_mongo()
        cursor = self._table.find(criteria)
        # Convert to pd.DataFrame
        df = pd.DataFrame(list(cursor))
        self.logger.debug(f"Query returned {df.shape[0]} results.")
        return df

    def query_latest(self, days=0, hours=0, seconds=0, criteria=None):
        """
        Convenience function to query the latest files in the db.
        Args:
            days (int): default 0.
            hours (int): default 0.
            seconds (int): default 0.
            criteria (dict, optional): Criteria for the query.
        Returns:
            list: Query result.
        """
        date_now = current_date()
        date_start = date_now - timedelta(days=days, hours=hours, seconds=seconds)
        return self.query(date_start=date_start, criteria=criteria)

    def update(self, metadata):
        """ Insert one or multiple single entry into the table.
        Args:
            metadata (dict): The document to insert.
        Returns:
            pymongo.results.UpdateResult: The update result object.
        """
        VALIDATE DOCUMENT!!!
        if isinstance(metadata, abc.Mapping):
            self._check_query_count(metadata, 1)
            update_result = self._table.update_one(deepcopy(metadata))
        elif isinstance(metadata, abc.Iterable):
            for item in metadata:
                self.update(item)
        else:
            raise TypeError(f"Invalid data type for update: {type(metadata)}.")
        return update_result

    def delete(self, metadata):
        """ Delete one or more entries from the table.
        Args:
            metadata (abc.Mapping or abc.Iterable): The document(s) to insert.
        Returns:
            pymongo.results.DeleteResult: The delete result object.
        """
        if isinstance(metadata, abc.Mapping):
            self._check_query_count(metadata, 1)
            self.logger.debug(f"Deleting {metadata} from {self}.")
            delete_result = self._table.delete_one(metadata)
        elif isinstance(metadata, abc.Iterable):
            for item in metadata:
                self.delete(item)
        else:
            raise TypeError(f"Invalid data type for update: {type(metadata)}.")
        return delete_result

    def _check_query_count(self, criteria, expected_count):
        """
        """
        query_count = self.query(criteria=criteria).shape[0]
        if not isinstance(expected_count, abc.Iterable):
            expected_count = list(expected_count)
        if query_count not in expected_count:
            raise RuntimeError(f"Unexpected query result size: {query_count} not in"
                               f" {expected_count}.")


class RawDataTable(DataTable):
    """Table to store metadata for raw data synced via NiFi from Huntsman."""
    _table_key = "raw_data"
    _date_key = "taiObs"
    _allow_edits = False

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._required_columns = self.config["fits_header"]["required_columns"]


class RawQualityTable(DataTable):
    """ Table to store data quality metadata for raw data. """
    _table_key = "raw_quality"
    _required_columns = ("filename",)
    _allow_edits = True

    def __init__(self, **kwargs):
        super().__init__(**kwargs)


class MasterCalibTable(DataTable):
    """ Table to store metadata for master calibs. """
    _table_key = "master_calib"
    _required_columns = ("filename", "calibDate")
    _allow_edits = True

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
