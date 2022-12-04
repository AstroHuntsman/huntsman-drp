from collections import abc
from collections.abc import MutableMapping
from copy import deepcopy
from collections import defaultdict

import numpy as np
from astropy import units as u

# These are responsible for converting arbitary types into something mongo can store
MONGO_ENCODINGS = {np.bool_: bool,
                   np.float64: float,
                   np.float32: float,
                   np.int32: int,
                   np.int64: int}


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


def unflatten_dict(d, sep="."):
    """ Unflatten a flattened dictionary.
    This is useful for inserting nested, encoded documents into collections.
    Args:
        d (dict): The dictionary to flatten.
        sep (str, optional): Separater character, default: '.'.
    Returns:
        dict: The un-flattened (nested) dictionary.
    """
    result = {}
    for k, v in d.items():
        _unflatten_dict_split(k, v, result, sep=sep)
    return result


def _unflatten_dict_split(k, v, out, sep):
    """ Utility function for unflatten_dict. """
    k, *rest = k.split(sep, 1)
    if rest:
        _unflatten_dict_split(rest[0], v, out.setdefault(k, {}), sep=sep)
    else:
        out[k] = v


def encode_mongo_document(value):
    """ Encode object for a pymongo query.
    Args:
        value (object): The data to encode.
    Returns:
        object: The encoded data.
    """
    if isinstance(value, u.Quantity):
        return encode_mongo_document(value.value)
    if isinstance(value, abc.Mapping):
        for k, v in value.items():
            value[k] = encode_mongo_document(v)
    elif isinstance(value, str):
        pass  # Required because strings are also iterables
    elif isinstance(value, abc.Iterable):
        value = [encode_mongo_document(v) for v in value]
    else:
        for oldtype, newtype in MONGO_ENCODINGS.items():
            if isinstance(value, oldtype):
                value = newtype(value)
                break
    return value


def encode_mongo_filter(document_filter):
    """ Encode document filter into something that pymongo understands.
    Args:
        document_filter (abc.Mapping): The document filter to encode.
    Returns:
        dict: The properly formatted pymongo query dict.
    """
    document_filter = flatten_dict(document_filter)

    mongo_query = defaultdict(dict)

    # Map constraint operators to their mongodb forms
    for k, constraint in document_filter.items():

        encoded_value = encode_mongo_document(constraint)

        # Check if a mongo operator was specified
        split = k.split(".")
        if split[-1].startswith("$"):
            operator = split[-1]
            key = ".".join(split[:-1])
            mongo_query[key][operator] = encoded_value
        # If no operator specified, use direct equality
        else:
            mongo_query[k] = encoded_value
            continue

    return dict(mongo_query)


def mongo_logical_or(document_filters):
    """ Combine document filters with logical or operation.
    """
    if not any(document_filters):
        return None
    return {"$or": [_ for _ in document_filters if _]}


def mongo_logical_and(document_filters):
    """ Combine document filters with logical and operation.
    """
    if not any(document_filters):
        return None
    return {"$and": [_ for _ in document_filters if _]}
