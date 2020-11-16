"""Code to interface with the Huntsman mongo database."""
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


def _apply_action(func, metadata):
    """
    """
    if isinstance(metadata, abc.Mapping):
        func(metadata)
    elif isinstance(metadata, abc.Iterable):
        for item in metadata:
            func(metadata)
    raise TypeError(f"Invalid metadata data type: {type(metadata)}.")


class DataTable(HuntsmanBase):
    """ The primary goal of DataTable objects is to provide a minimal, easily-configurable and
    user-friendly interface between the mongo database and the DRP."""
    _required_columns = None
    _unique_columns = ("filename", )  # Required to identify a unique document

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

    def insert(self, metadata):
        """
        """
        return _apply_action(self._insert, metadata)

    def update(self, metadata):
        """
        """
        return _apply_action(self._update, metadata)

    def delete(self, metadata):
        """
        """
        return _apply_action(self._delete, metadata)

    def _insert(self, metadata):
        """
        """
        # Ensure required columns exist
        if self.required_columns is None:
            return
        for column_name in self._required_columns:
            if column_name not in metadata.keys():
                raise ValueError(f"New document missing required column: {column_name}.")
        # Ensure new document is unique
        unique_id = {k: metadata[k] for k in self._unique_columns}
        query_result = self.query(criteria=unique_id)
        if query_result.shape[0] != 0:
            raise ValueError(f"Document already exists for {unique_id}.")
        # Insert the new document
        self._table.insert_one(deepcopy(metadata))

    def _update(self, metadata, upsert=True):
        """ Update one or multiple single entry into the table. MongoDB edits the first matching
        document, so we need to check we are only matching with a single document.
        Args:
            metadata (dict): The document to insert.
        Returns:
            pymongo.results.UpdateResult: The update result object.
        """
        if isinstance(metadata, abc.Mapping):
            query_result = self.query(criteria=metadata)
            if upsert and (query.shape[0] == 0):
                return self._insert(metadata)

            update_result = self._table.update_one(deepcopy(metadata))
        elif isinstance(metadata, abc.Iterable):
            for item in metadata:
                self.update(item)
        else:
            raise TypeError(f"Invalid data type for update: {type(metadata)}.")
        return update_result

    def _delete(self, metadata):
        """ Delete one or more entries from the table. MongoDB deletes the first matching document,
        so we need to check we are only matching with a single document.
        Args:
            metadata (abc.Mapping or abc.Iterable): The document(s) to insert.
        Returns:
            pymongo.results.DeleteResult: The delete result object.
        """
        if isinstance(metadata, abc.Mapping):
            query_count = self.query(criteria=metadata).shape[0]
            if query_count == 0:
                self.logger.warning(f"Tried to delete non-existent document from {self}:"
                                    f" {metadata}.")
                return
            elif query_count == 1:
                self.logger.debug(f"Deleting {metadata} from {self}.")
                return self._table.delete_one(metadata)
            else:
                raise RuntimeError(f"Unexpected query result size: {query_count}>1.")
        elif isinstance(metadata, abc.Iterable):
            for item in metadata:
                self.delete(item)
        else:
            raise TypeError(f"Invalid data type for update: {type(metadata)}.")

    def _validate_document(self, metadata):
        """
        """
        if self.required_columns is None:
            return
        for column_name in self._required_columns:
            if column_name not in metadata.keys():
                raise ValueError(f"Document missing required column: {column_name}.")


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
