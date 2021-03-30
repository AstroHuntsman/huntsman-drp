""" Classes to represent dataIds. """
from contextlib import suppress


class DataId(object):
    """ A dataId behaves like a dictionary but makes it easier to compare between dataIds.
    """
    _unique_keys = ()

    def __init__(self, document):

        # Check all the required information is present
        self._validate_document(document)

        self._document = document.copy()

    # Special methods

    def __eq__(self, o):
        with suppress(KeyError):
            return all([self[k] == o[k] for k in self._unique_keys])
        return False

    def __getitem__(self, key):
        return self._document[key]

    # Public methods

    def values(self):
        return self._document.values()

    def items(self):
        return self._document.items()

    def keys(self):
        return self.document.keys()

    def to_dict(self):
        return self._document.copy()

    # Private methods

    def _validate_document(self, document):
        """
        """
        if not all([k in document for k in self._unique_keys]):
            raise ValueError(f"Document does not contain all required keys: {self._unique_keys}.")


class CalibId(DataId):

    _unique_keys = ("calibDate", "datasetType", "filename")

    _unique_keys_type = {"bias": ("calibDate", "ccd"),
                         "dark": ("calibDate", "ccd"),
                         "flat": ("calibDate", "ccd", "filter")}

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def _validate_document(self, document):
        """
        """
        super()._validate_document(document)

        keys = self._unique_keys_type[document["datasetType"]]

        if not all([k in document for k in keys]):
            raise ValueError(f"Document does not contain all required keys: {keys}.")
