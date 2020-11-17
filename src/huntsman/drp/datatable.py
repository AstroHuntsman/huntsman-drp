"""Code to interface with the Huntsman mongo database."""
from collections import abc
from functools import partial
from datetime import timedelta
from urllib.parse import quote_plus

import pandas as pd

from pymongo import MongoClient
from pymongo.errors import ServerSelectionTimeoutError

from huntsman.drp.base import HuntsmanBase
from huntsman.drp.utils.date import current_date
from huntsman.drp.utils.query import QueryCriteria, encode_mongo_value


def _apply_operation(func, metadata):
    """ Apply a function to the metadata. metadata can either be a mappable, in which case the
    function is called with metadata as its first argument, or it can be an iterable, in which
    case the function will be successively applied to each of its items (assumed to be mappings).
    Args:
        func (Function): The function to apply.
        metadata (abc.Mapping or abc.Iterable): The metadata to process.
    """
    if isinstance(metadata, abc.Mapping):
        func(encode_mongo_value(metadata))
    elif isinstance(metadata, abc.Iterable):
        for item in metadata:
            func(encode_mongo_value(item))
    raise TypeError(f"Invalid metadata type: {type(metadata)}.")


class DataTable(HuntsmanBase):
    """ The primary goal of DataTable objects is to provide a minimal, easily-configurable and
    user-friendly interface between the mongo database and the DRP that enforces standardisation
    of new documents."""
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
        # Convert to a DataFrame object
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
        """ Insert a new document into the table after ensuring it is valid and unique.
        Args:
            data_id (dict): The dictionary specifying the single document to delete.
        """
        return _apply_operation(self._insert_one, metadata)

    def update(self, data_id, metadata):
        """ Update a single document in the table.
        Args:
            data_id (dict): The data ID of the document to update.
            metadata (dict): The new metadata to be inserted.
        """
        fn = partial(self._update_one, data_id=encode_mongo_value(data_id))
        return _apply_operation(fn, metadata)

    def delete(self, metadata):
        """ Delete one document from the table.
        Args:
            data_id (dict): The dictionary specifying the single document to delete.
        """
        return _apply_operation(self._delete_one, metadata)

    def _insert_one(self, metadata):
        """ Insert a new document into the table after ensuring it is valid and unique.
        Args:
            data_id (dict): The dictionary specifying the single document to delete.
        """
        # Ensure required columns exist
        if self.required_columns is None:
            return
        if self._required_columns is not None:
            for column_name in self._required_columns:
                if column_name not in metadata.keys():
                    raise ValueError(f"New document missing required column: {column_name}.")

        # Ensure new document is unique
        unique_id = {k: metadata[k] for k in self._unique_columns}
        query_count = self.query(criteria=unique_id).shape[0]
        if query_count != 0:
            raise ValueError(f"Document already exists for {unique_id}.")

        # Insert the new document
        self.logger.debug(f"Inserting new document into {self}: {metadata}.")
        self._table.insert_one(metadata)

    def _update_one(self, data_id, metadata):
        """ Update a single document in the table. MongoDB edits the first matching
        document, so we need to check we are only matching with a single document. A new document
        will be created if there are no matches in the table.
        Args:
            data_id (dict): The data ID of the document to update.
            metadata (dict): The new metadata to be inserted.
        """
        query_count = self.query(criteria=data_id).shape[0]
        if query_count > 1:
            raise RuntimeError(f"data ID matches with more than one document: {data_id}.")
        elif query_count == 0:
            new_metadata = data_id.copy().update(metadata.copy())
            return self._insert_one(new_metadata)
        else:
            self._table.update_one(data_id, {'$set': metadata})

    def _delete_one(self, data_id):
        """ Delete one document from the table. MongoDB deletes the first matching document,
        so we need to check we are only matching with a single document. A warining is logged if
        no document is matched.
        Args:
            data_id (dict): The dictionary specifying the single document to delete.
        """
        query_count = self.query(criteria=data_id).shape[0]
        if query_count > 1:
            raise RuntimeError(f"Metadata matches with more than one document: {data_id}.")
        elif query_count == 0:
            self.logger.warning(f"Tried to delete non-existent document from {self}:"
                                f" {data_id}.")
        elif query_count == 1:
            self.logger.debug(f"Deleting {data_id} from {self}.")
            self._table.delete_one(data_id)


class RawDataTable(DataTable):
    """Table to store metadata for raw data synced via NiFi from Huntsman."""
    _table_key = "raw_data"

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._required_columns = self.config["fits_header"]["required_columns"]


class RawQualityTable(DataTable):
    """ Table to store data quality metadata for raw data. """
    _table_key = "raw_quality"

    def __init__(self, **kwargs):
        super().__init__(**kwargs)


class MasterCalibTable(DataTable):
    """ Table to store metadata for master calibs. """
    _table_key = "master_calib"
    _required_columns = ("filename", "calibDate")

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
