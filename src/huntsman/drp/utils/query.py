from contextlib import suppress
from collections import abc, MutableMapping
from copy import deepcopy
from collections import defaultdict

from huntsman.drp.utils.mongo import MONGO_OPERATORS, encode_mongo_query


def flatten_dict(d, **kwargs):
    """ Flatten a nested dictionary, for example to dot notation.
    Args:
        d (dict): The dictionary to flatten.
        parent_key (str, optional): Will be prepended to keys of flattened dict, by default None.
        sep (str, optional): Separater character between parent_key and key name, by default '.'.
    Returns:
        dict: The flattened dictionary.
    """
    return deepcopy(_flatten_dict(d, **kwargs))


def _flatten_dict(d, parent_key=None, sep='.'):
    """ Flatten a nested dictionary, for example to dot notation.
    Args:
        d (dict): The dictionary to flatten.
        parent_key (str, optional): Will be prepended to keys of flattened dict, by default None.
        sep (str, optional): Separater character between parent_key and key name, by default '.'.
    Returns:
        dict: The flattened dictionary.
    """
    items = []
    for k, v in d.items():
        new_key = parent_key + sep + k if parent_key else k
        if isinstance(v, MutableMapping):
            items.extend(_flatten_dict(v, new_key, sep=sep).items())
        else:
            items.append((new_key, v))
    return dict(items)


class Query():

    def __init__(self, document_filter=None):
        """ Query class for building a pymongo query.
        See: https://docs.mongodb.com/manual/reference/operator/query/
        Args:
            document_filter (dict, optional): A dictionary containing other search criteria which
                can include the operators defined in `huntsman.drp.utils.mongo.MONGO_OPERATORS`,
                by default None.
        """
        if document_filter is None:
            document_filter = {}

        with suppress(AttributeError):
            document_filter = document_filter.to_dict()

        if not isinstance(document_filter, abc.Mapping):
            raise TypeError(f"document_filter should be a mapping, got {type(document_filter)}.")

        # Store as flattened dictionaries using dot format recongnised by pymongo
        document_filter = flatten_dict(document_filter)

        self._mongo_query = self._to_mongo(document_filter)

    # Public methods

    def to_mongo(self):
        return self._mongo_query

    def logical_and(self, other):
        filters = [self.to_mongo(), other.to_mongo()]
        self._mongo_query = {"$and": [_ for _ in filters if _]}

    def logical_or(self, other):
        filters = [self.to_mongo(), other.to_mongo()]
        self._mongo_query = {"$or": [_ for _ in filters if _]}

    # Private methods

    def _to_mongo(self, document_filter):
        """ Builds pymongo query from inputs, transforming any document_filter that utilise
        mongo operators into the appropriate form.
        Returns:
            dict: The properly formatted pymongo query dict.
        """
        mongo_query = defaultdict(dict)

        # Map constraint operators to their mongodb forms
        for k, constraint in document_filter.items():

            encoded_value = encode_mongo_query(constraint)

            # Extract the key, operator pair from the flattened key
            split = k.split(".")
            try:
                operator = MONGO_OPERATORS[split[-1]]
            except KeyError:
                mongo_query[k] = encoded_value
                continue

            # Add the constraint to the constraint dict for this key
            key = ".".join(split[:-1])
            mongo_query[key][operator] = encoded_value

        return dict(mongo_query)
