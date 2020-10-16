"""Code to interface with the Huntsman database."""
from datetime import datetime, timedelta
from urllib.parse import quote_plus
from pymongo import MongoClient
from pymongo.errors import ServerSelectionTimeoutError

from huntsman.drp.utils import parse_date
from huntsman.drp.base import HuntsmanBase


class DataTable(HuntsmanBase):
    """ """
    _required_columns = None
    _allow_edits = True

    def __init__(self, **kwargs):
        HuntsmanBase.__init__(self, **kwargs)
        self._date_key = self.config["mongodb"]["date_key"]

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

    def query(self, date_start=None, date_end=None, **kwargs):
        """
        Query the table, optionally with a date range.

        Args:
            date_start (optional): The earliest date of returned rows.
            date_end (optional): The latest date of returned rows.
            **kwargs: Parsed to the query.
        Returns:
            list of dict: Dictionary of query results.
        """
        query_dict = {key: value for key, value in kwargs.items() if value is not None}
        result = list(self._table.find(query_dict))
        # TODO remove this in favour of pymongo date handling
        if date_start is not None:
            result = [r for r in result if parse_date(r[self._date_key]) >= parse_date(date_start)]
        if date_end is not None:
            result = [r for r in result if parse_date(r[self._date_key]) < parse_date(date_end)]
        self.logger.debug(f"Query returned {len(result)} results.")
        return result

    def query_column(self, column_name, **kwargs):
        """
        Convenience function to query database and return entries for a specific column.

        Args:
            column_name (str): The column name.
        Returns:
            List: List of column values matching the query.
        """
        query_results = self.query(**kwargs)
        return [q[column_name] for q in query_results]

    def insert_one(self, entry, **kwargs):
        """
        Insert a single entry into the table.

        Args:
            entry (dict): The document to insert.
        """
        self._validate_edit(**kwargs)
        self._table.insert_one(entry)

    def insert_many(self, entries, **kwargs):
        """
        Insert a single entry into the table.

        Args:
            entry (list of dict): The documents to insert.
        """
        self._validate_edit(**kwargs)
        self._table.insert_many(entries)

    def query_latest(self, days=0, hours=0, seconds=0, column_name=None, **kwargs):
        """
        Convenience function to query the latest files in the db.

        Args:
            days (int): default 0.
            hours (int): default 0.
            seconds (int): default 0.
            column_name (int, optional): If given, call `datatable.query_column` with
                `column_name` as its first argument.
            **kwargs: Passed to the query.
        Returns:
            list: Query result.
        """
        date_now = datetime.utcnow()
        date_start = date_now - timedelta(days=days, hours=hours, seconds=seconds)
        if column_name is not None:
            return self.query_column(column_name, date_start=date_start, **kwargs)
        return self.query(date_start=date_start, **kwargs)

    def update_document(self, data_id, data, **kwargs):
        """
        Update the document associated with the data_id.
        Args:
            data_id (dict): Dictionary of key: value pairs identifying the document.
            data (dict): Dictionary of key: value pairs to update in the database. The field will
                be created if it does not already exist.
        Returns:
            `pymongo.results.UpdateResult`: The result of the update operation.
        """
        self._validate_edit(**kwargs)
        self._validate_update()
        result = self._table.update_one(data_id, {'$set': data}, upsert=False)
        if result.matched_count == 0:
            raise ValueError("No matches in database for update.")
        if result.matched_count > 1:
            raise ValueError("Multiple matches in database for update.")
        return result

    def delete_document(self, data_id, **kwargs):
        """
        Delete the document associated with the data_id.
        Args:
            data_id (dict): Dictionary of key: value pairs identifying the document.
        Returns:
            `pymongo.results.UpdateResult`: The result of the delete operation.
        """
        self._validate_edit(**kwargs)
        result = self._table.delete_one(data_id)
        if result.matched_count == 0:
            raise ValueError("No matches in database for update.")
        if result.matched_count > 1:
            raise ValueError("Multiple matches in database for update.")
        return result

    def update_file_data(self, filename, data):
        """
        Update the metadata associated with a file in the database.
        Args:
            filename (str): Modify the metadata for this file.
            data (dict): Dictionary of key: value pairs to update in the database. The field will
                be created if it does not already exist.
        Returns:
            `pymongo.results.UpdateResult`: The result of the update operation.
        """
        data_id = {'filename': filename}
        return self.update_document(data_id, data)

    def delete_file_data(self, filename, **kwargs):
        """
        Delete the metadata associated with a file in the database.
        Args:
            filename (str): Modify the metadata for this file.
            data (dict): Dictionary of key: value pairs to update in the database. The field will
                be created if it does not already exist.
        Returns:
            `pymongo.results.UpdateResult`: The result of the delete operation.
        """
        data_id = {'filename': filename}
        return self.delete_document(data_id, **kwargs)

    def _validate_edit(self, bypass_allow_edits=False):
        if not bypass_allow_edits:
            if not self._allow_edits:
                raise PermissionError("Edits are not allowed by-default for this table. If you are"
                                      "sure you want to do this, use `bypass_allow_edits=True`.")

    def _validate_update(self, metadata):
        """Make sure the required columns are in the metadata."""
        if self._required_columns is None:
            return
        missing = [k for k in self._required_columns if k not in metadata.keys()]
        if len(missing) != 0:
            raise ValueError(f"Missing columns for update: {missing}.")


class RawDataTable(DataTable):
    """ """
    _table_key = "raw_data"
    _date_key = "taiObs"
    _allow_edits = False

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._required_columns = self.config["fits_header"]["required_columns"]

        # Initialise the DB
        db_name = self.config["mongodb"]["db_name"]
        table_name = self.config["mongodb"]["tables"][self._table_key]
        self._initialise(db_name, table_name)


class DataQualityTable(DataTable):
    """ """
    _table_name = "data_quality"
    _required_columns = ("filename",)
    _allow_edits = True

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        # Initialise the DB
        db_name = self.config["mongodb"]["db_name"]
        self._initialise(db_name, self._table_name)
