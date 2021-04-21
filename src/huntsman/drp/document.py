""" Classes to represent dataIds. """
from collections import abc
from contextlib import suppress

from huntsman.drp.core import get_config
from huntsman.drp.utils.date import parse_date
from huntsman.drp.utils.mongo import encode_mongo_filter


class Document(abc.Mapping):
    """ A dataId behaves like a dictionary but makes it easier to compare between dataIds.
    DataId objects are hashable, whereas dictionaries are not. This allows them to be used in sets.
    """
    _required_keys = tuple()

    def __init__(self, document, validate=True, copy=False, **kwargs):
        super().__init__()

        if document is None:
            document = {}

        elif isinstance(document, Document):
            document = document._document

        if copy:
            document = document.copy()

        # Check all the required information is present
        if validate and self._required_keys:
            self._validate_document(document)

        self._document_dict = document

    # Special methods

    def __eq__(self, o):
        with suppress(KeyError):
            return all([self[k] == o[k] for k in self._required_keys])
        return False

    def __hash__(self):
        return hash(tuple([self[k] for k in self._required_keys]))

    def __getitem__(self, key):
        return self._document_dict[key]

    def __setitem__(self, key, item):
        self._document_dict[key] = item

    def __delitem__(self, item):
        del self._document_dict[item]

    def __iter__(self):
        return self._document_dict.__iter__()

    def __len__(self):
        return len(self._document_dict)

    def __repr__(self):
        return repr(self._document_dict)

    def __str__(self):
        return str(self._document_dict)

    # Properties

    @property
    def minimal_dict(self):
        return {k: self[k] for k in self._required_keys}

    # Public methods

    def values(self):
        return self._document_dict.values()

    def items(self):
        return self._document_dict.items()

    def keys(self):
        return self._document_dict.keys()

    def update(self, d):
        self._document_dict.update(d)

    def to_mongo(self):
        """ Get the full mongo filter for the document """
        return encode_mongo_filter(self._document_dict)

    def get_mongo_id(self):
        """ Get the unique mongo ID for the document """
        doc = {k: self[k] for k in self._required_keys}
        return encode_mongo_filter(doc)

    # Private methods

    def _validate_document(self, document):
        """
        """
        if not all([k in document for k in self._required_keys]):
            raise ValueError(f"Document does not contain all required keys: {self._required_keys}.")


class RawExposureDocument(Document):

    _required_keys = ["filename"]

    def __init__(self, document, config=None, **kwargs):

        if config is None:
            config = get_config()  # Do not store the config as we will be making many DataIds

        self._required_keys.extend(config["fits_header"]["required_columns"])

        super().__init__(document=document, **kwargs)

        if "date" not in self.keys():
            self["date"] = parse_date(self["dateObs"])

    def __repr__(self):
        return repr(self.minimal_dict)

    def __str__(self):
        return str(self.minimal_dict)


class CalibDocument(Document):

    _required_keys = ("calibDate", "datasetType", "filename", "ccd")

    _required_keys_type = {"flat": ("filter",)}

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        if "date" not in self.keys():
            self["date"] = parse_date(self["calibDate"])

    def _validate_document(self, document):
        """
        """
        super()._validate_document(document)

        keys = self._required_keys_type.get(document["datasetType"], None)
        if not keys:
            return

        if not all([k in document for k in keys]):
            raise ValueError(f"Document does not contain all required keys: {keys}.")

    def __repr__(self):
        return repr(self.minimal_dict)

    def __str__(self):
        return str(self.minimal_dict)
