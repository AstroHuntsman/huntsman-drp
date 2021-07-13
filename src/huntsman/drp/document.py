""" Classes to represent dataIds. """
from copy import deepcopy
from functools import reduce
from collections import abc
from contextlib import suppress

from astropy.coordinates import SkyCoord
from astropy import units as u
from astropy.wcs import WCS

from huntsman.drp.utils.fits import read_fits_header
from huntsman.drp.utils.mongo import encode_mongo_filter, unflatten_dict


class Document(abc.Mapping):
    """ A dataId behaves like a dictionary but makes it easier to compare between dataIds.
    DataId objects are hashable, whereas dictionaries are not. This allows them to be used in sets.
    """
    _required_keys = set()

    def __init__(self, document, copy=False, unflatten=True, **kwargs):
        super().__init__()

        if document is None:
            document = {}

        elif isinstance(document, Document):
            document = document._document

        if copy:
            document = document.copy()

        if unflatten:
            document = unflatten_dict(document)

        self._document = document

    # Special methods

    def __eq__(self, o):
        with suppress(KeyError):
            return all([self[k] == o[k] for k in self._required_keys])
        return False

    def __hash__(self):
        return hash(tuple([self[k] for k in self._required_keys]))

    def __getitem__(self, key):
        return self._document[key]

    def __setitem__(self, key, value):
        self._document.__setitem__(key, value)

    def __delitem__(self, key):
        self._document.__delitem__(key)

    def __iter__(self):
        return self._document.__iter__()

    def __len__(self):
        return len(self._document)

    def __str__(self):
        if self._required_keys:
            return str({k: self._document[k] for k in self._required_keys})
        return str(self._document)

    def __repr__(self):
        if self._required_keys:
            return repr({k: self._document[k] for k in self._required_keys})
        return repr(self._document)

    # Public methods

    def values(self):
        return self._document.values()

    def items(self):
        return self._document.items()

    def keys(self):
        return self._document.keys()

    def update(self, d):
        self._document.update(d)

    def get(self, key, default=None):
        """ Override get method to allow nested key searches.
        Args:
            key (str): The key name. Use dot notation for nested keys.
            default (optional): Return this object if no match. Default: None.
        """
        return reduce(lambda d, k: d.get(k, default) if isinstance(d, abc.Mapping) else default,
                      key.split("."), self._document)

    def to_mongo(self, flatten=False):
        """ Get the full mongo filter for the document.
        Args:
            flatten (bool, optional): If True, return flattened dictionary using dot notation.
                Default False.
        """
        d = encode_mongo_filter(self._document)
        if not flatten:
            return unflatten_dict(d)
        return d

    def get_mongo_id(self):
        """ Get the unique mongo ID for the document.
        Returns:
            dict: The encoded document.
        """
        doc = {k: self[k] for k in self._required_keys}
        return encode_mongo_filter(doc)

    def copy(self):
        """ Copy the document.
        Returns:
            Document: The copied document.
        """
        return deepcopy(self)


class ExposureDocument(Document):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def get_central_skycoord(self):
        """ Return the central celestial coordinate of the exposure using the WCS info.
        Returns:
            astropy.coordinates.SkyCoord: The central coordinate.
        """
        ra = self["metrics"]["ra_centre"] * u.deg
        dec = self["metrics"]["dec_centre"] * u.deg
        return SkyCoord(ra=ra, dec=dec)

    def get_wcs(self):
        """ Get the WCS object for this document.
        Returns:
            astropy.wcs.WCS: The WCS object.
        """
        # At the moment the easiest way is to read the FITS header again from file
        # TODO: In the future we should be able to do this from the document metadata
        header = read_fits_header(self["filename"])
        wcs = WCS(header)
        return wcs


class CalibDocument(Document):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
