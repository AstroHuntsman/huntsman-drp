from contextlib import suppress
from datetime import timedelta
from urllib.parse import quote_plus

import pymongo
from pymongo.errors import ServerSelectionTimeoutError

from huntsman.drp.base import HuntsmanBase
from huntsman.drp.utils.date import current_date, make_mongo_date_constraint
from huntsman.drp.document import Document
from huntsman.drp.utils.mongo import mongo_logical_and


class Collection(HuntsmanBase):
    """ This class is used to interface with the mongodb. It is responsible for performing queries
    and inserting/updating/deleting documents, as well as validating new documents.
    """
    _index_fields = None
    _required_fields = None
    _DocumentClass = None

    def __init__(self, db_name=None, collection_name=None, **kwargs):
        super().__init__(**kwargs)

        # Get the name of the mongo database
        self._db_name = self.config["mongodb"]["db_name"] if db_name is None else db_name

        # Get the name of the collection in the mongo database
        if not collection_name:
            try:
                collection_name = self.config["collections"][self.__class__.__name__]["name"]
            except KeyError:
                raise ValueError("Unable to determine collection name.")
        self.collection_name = collection_name

        # Get the fields required for new documents
        with suppress(KeyError):
            self._required_fields = self.config["collections"][self.__class__.__name__
                                                               ]["required_fields"]
        # Get the fields used to create a lookup index
        # The combination of field values must be unique for each document
        with suppress(KeyError):
            self._index_fields = self.config["collections"][self.__class__.__name__
                                                            ]["index_fields"]
        # Connect to the DB and initialise the collection
        self._connect()

    def __str__(self):
        return f"{self.__class__.__name__} ({self.collection_name})"

    # Public methods

    def find(self, document_filter=None, key=None, quality_filter=False, limit=None, **kwargs):
        """Get data for one or more matches in the table.
        Args:
            document_filter (dict, optional): A dictionary containing key, value pairs to be
                matched against other documents, by default None
            key (str, optional):
                Specify a specific key to be returned from the query (e.g. filename), by default
                None.
            quality_filter (bool, optional): If True, only return documents that satisfy quality
                cuts. Default: False.
            limit (int): Limit the number of returned documents to this amount.
            **kwargs: Parsed to make_mongo_date_constraint.
        Returns:
            result (list): List of DataIds or key values if key is specified.
        """
        document_filter = Document(document_filter, copy=True)
        with suppress(KeyError):
            del document_filter["date_modified"]  # This might change so don't match with it

        # Add date range to criteria if provided
        date_constraint = make_mongo_date_constraint(**kwargs)
        if date_constraint:
            document_filter.update({self._date_key: date_constraint})

        mongo_filter = document_filter.to_mongo(flatten=True)

        # Add quality cuts to document filter
        if quality_filter:
            mongo_quality_filter = self._get_quality_filter()
            if mongo_quality_filter:
                mongo_filter = mongo_logical_and([mongo_filter, mongo_quality_filter])

        self.logger.debug(f"Performing mongo find operation with filter: {mongo_filter}.")

        # Do the mongo query and get results
        cursor = self._collection.find(mongo_filter, {"_id": False})
        if limit is not None:
            cursor = cursor.limit(limit)
        documents = list(cursor)

        self.logger.debug(f"Find operation returned {len(documents)} results.")

        if key is not None:
            return [d[key] for d in documents]

        # Skip validation to speed up - inserted documents should already be valid
        return [self._DocumentClass(d, validate=False, config=self.config) for d in documents]

    def find_one(self, *args, **kwargs):
        """ Find a single matching document. If multiple matches, raise a RuntimeError.
        Args:
            *args, **kwargs: Parsed to self.find.
        Returns:
            Document or None: If there is a match return the document, else None.
        """
        documents = self.find(*args, **kwargs)
        if not documents:
            return None
        if len(documents) > 1:
            raise RuntimeError("Matched with more than one document.")
        return documents[0]

    def insert_one(self, document):
        """ Insert a new document into the table after ensuring it is valid and unique.
        Args:
            document (dict): The document to be inserted into the table.
        """
        # Check the required columns exist in the new document
        doc = self._prepare_doc_for_insert(document)

        # Insert the document
        # Uniqueness is verified implicitly
        self.logger.debug(f"Inserting document into {self}: {doc}.")
        self._collection.insert_one(doc.to_mongo())

    def replace_one(self, document_filter, replacement, **kwargs):
        """ Replace a matching document with a new one.
        Args:
            document_filter (Document): dictionary containing key, value pairs used to identify
                the document to replace.
            replacement (Document): The document to replace with.
            **kwargs: Parsed to pymongo replace_one.
        Raises:
            RuntimeError: If document filter matches with more than one document.
        """
        document_filter = Document(document_filter)

        # Make sure the filter matches with at most one doc
        if self.count_documents(document_filter) > 1:
            raise RuntimeError(f"Document filter {document_filter} matches with multiple documents"
                               f" in {self}.")

        mongo_filter = document_filter.to_mongo()
        mongo_doc = self._prepare_doc_for_insert(replacement).to_mongo()  # Implicit validation

        self.logger.debug(f"Replacing {mongo_filter} with {mongo_doc}")

        self._collection.replace_one(mongo_filter, mongo_doc, **kwargs)

    def update_one(self, document_filter, to_update, upsert=False):
        """ Update a single document in the table.
        See: https://docs.mongodb.com/manual/reference/operator/update/set/#up._S_set
        Args:
            document_filter (dict): A dictionary containing key, value pairs used to identify
                the document to update, by default None.
            to_update (dict): The key, value pairs to update within the matched document.
            upsert (bool, optional): If True perform the insert even if no matching documents
                are found, by default False.
        """
        document_filter = Document(document_filter, copy=True)
        with suppress(KeyError):
            del document_filter["date_modified"]  # This might change so don't match with it

        count = self.count_documents(document_filter)
        if count > 1:
            raise RuntimeError(f"Multiple matches found for document in {self}: {document_filter}.")

        elif count == 0:
            if upsert:
                self.insert_one(to_update)
                return
            else:
                raise RuntimeError(f"No matches found for document {document_filter} in {self}. Use"
                                   " upsert=True to upsert.")

        to_update = Document(to_update)
        to_update["date_modified"] = current_date()

        # Use flattened version (dot notation) for nested updates to work properly
        mongo_update = to_update.to_mongo(flatten=True)

        self.logger.debug(f"Updating document with: {mongo_update}")
        self._collection.update_one(document_filter, {'$set': mongo_update}, upsert=False)

    def delete_one(self, document_filter, force=False):
        """Delete one document from the table.
        Args:
            document_filter (dict, optional): A dictionary containing key, value pairs used to
                identify the document to delete, by default None
            force (bool, optional): If True, ignore checks and delete all matching documents.
                Default False.
        """
        document_filter = Document(document_filter, validate=False)
        mongo_filter = document_filter.to_mongo()

        if not force:
            count = self.count_documents(document_filter)
            if count > 1:
                raise RuntimeError(f"Multiple matches found for document in {self}:"
                                   f" {document_filter}.")
            elif (count == 0):
                raise RuntimeError(f"No matches found for document in {self}: {document_filter}.")

        self.logger.debug(f"Deleting {document_filter} from {self}.")

        self._collection.delete_one(mongo_filter)

    def insert_many(self, documents, **kwargs):
        """Insert a new document into the table.
        Args:
            documents (list): List of dictionaries that specify documents to be inserted in the
                table.
        """
        for d in documents:
            self.insert_one(d, **kwargs)

    def delete_many(self, documents, **kwargs):
        """ Delete one document from the table.
        Args:
            documents (list): List of dictionaries that specify documents to be deleted from the
                table.
        """
        self.logger.debug(f"Deleting {len(documents)} documents from {self}.")

        for d in documents:
            self.delete_one(d, **kwargs)

    def find_latest(self, days=0, hours=0, seconds=0, **kwargs):
        """ Convenience function to query the latest files in the db.
        Args:
            days (int): default 0.
            hours (int): default 0.
            seconds (int): default 0.
        Returns:
            list: Query result.
        """
        date_now = current_date()
        date_min = date_now - timedelta(days=days, hours=hours, seconds=seconds)
        return self.find(date_min=date_min, **kwargs)

    def delete_all(self, really=False, **kwargs):
        """ Delete all documents from the collection. """
        if not really:
            raise RuntimeError("If you really want to do this, parse really=True.")
        docs = self.find()
        self.delete_many(docs, **kwargs)

    def count_documents(self, *args, **kwargs):
        """ Count the number of matching documents in the collection.
        Args:
            *args, **kwargs: Parsed to self.find.
        Returns:
            int: The number of matching documents in the collection.
        """
        return len(self.find(*args, **kwargs))

    # Private methods

    def _connect(self):
        """ Initialise the database.
        Args:
            db_name (str): The name of the (mongo) database.
            collection_name (str): The name of the table (mongo collection).
        """
        # Connect to the mongodb
        hostname = self.config["mongodb"]["hostname"]
        port = self.config["mongodb"]["port"]

        if "username" in self.config["mongodb"].keys():
            username = quote_plus(self.config["mongodb"]["username"])
            password = quote_plus(self.config["mongodb"]["password"])
            uri = f"mongodb://{username}:{password}@{hostname}/{self._db_name}?ssl=true"
            self._client = pymongo.MongoClient(uri)
        else:
            self._client = pymongo.MongoClient(hostname, port)
        try:
            self._client.server_info()
            self.logger.info(f"{self} connected to mongodb at {hostname}:{port}.")
        except ServerSelectionTimeoutError as err:
            self.logger.error(f"Unable to connect {self} to mongodb at {hostname}:{port}.")
            raise err

        self._db = self._client[self._db_name]
        self._collection = self._db[self.collection_name]

        # Create unique index
        # This leverages mongdb's server-side locking mechanism for thread-safety on inserts
        if self._index_fields is not None:
            self._collection.create_index([(k, pymongo.ASCENDING) for k in self._index_fields],
                                          unique=True)

    def _prepare_doc_for_insert(self, document):
        """ Prepare a document to be inserted into the database.
        Args:
            document (Document or dict): The document to prepare.
        Returns:
            Document: The prepared document of the appropriate type for this collection.
        """
        # Create and validate document
        doc = self._DocumentClass(document, copy=True, unflatten=True)
        self._validate_document(doc)

        # Add date records
        doc["date_created"] = current_date()
        doc["date_modified"] = current_date()

        return doc

    def _validate_document(self, document, required_fields=None):
        """ Validate a document for insersion.
        Args:
            document (Document): The document to validate.
            required_fields (iterable of str, optional): Fields required to exist in document.
                If not provided, use class attribute.
        Raises:
            ValueError: If the document is invalid.
        """
        if required_fields is None:
            required_fields = self._required_fields

        if not required_fields:
            return

        for field in required_fields:
            if field not in document:
                raise ValueError(f"Field {field} not in document. Cannot insert.")

    def _get_quality_filter(self):
        """ Return the Query object corresponding to quality cuts. """
        raise NotImplementedError
