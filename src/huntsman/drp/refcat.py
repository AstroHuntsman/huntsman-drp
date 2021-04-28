import os
import pickle
import serpent
from threading import Lock
from tempfile import NamedTemporaryFile
from contextlib import suppress

import numpy as np
import pandas as pd
from astroquery.utils.tap.core import TapPlus

import Pyro5.server
from Pyro5.api import Proxy

from huntsman.drp.base import HuntsmanBase
from huntsman.drp.utils.pyro.nameserver import NameServer

PYRO_NAME = "refcat"


class RefcatServer(HuntsmanBase):
    """ Class to expose reference catalogue over network. """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._lock = Lock()
        self._tap = TapReferenceCatalogue(config=self.config, logger=self.logger)

    @Pyro5.server.expose
    def make_reference_catalogue(self, *args, **kwargs):
        """ Thread-safe implementation of refcat query.
        Args:
            *args, **kwargs: Parsed to TapReferenceCatalogue.make_reference_catalogue.
        """
        # Get the data
        with self._lock:
            df = self._tap.make_reference_catalogue(*args, **kwargs)

        # Pickle the data and return it
        # This sends an encoded version over the network and may not be advisable for large files
        return pickle.dumps(df)


class RefcatClient(HuntsmanBase):
    """ Client-side interface to the thread-safe tap reference catalogue. """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        ns = NameServer(config=self.config, logger=self.logger)
        ns.connect()

        uri = ns.name_server.lookup(PYRO_NAME)
        self._proxy = Proxy(uri)

    def make_reference_catalogue(self, *args, **kwargs):
        """ Thread-safe implementation of refcat query.
        Args:
            *args, **kwargs: Parsed to TapReferenceCatalogue.make_reference_catalogue.
        """
        filename = kwargs.pop("filename", None)  # file needs to be stored on local volume

        # Get and decode the data sent over the network
        data = self._proxy.make_reference_catalogue(*args, **kwargs)
        df_bytes = serpent.tobytes(data)

        df = pickle.loads(df_bytes)

        # Save to a path on the local volume
        if filename is not None:
            self.logger.debug(f"Writing reference catalogue to {filename}.")
            os.makedirs(os.path.dirname(filename), exist_ok=True)
            df.to_csv(filename)

        return df


class TapReferenceCatalogue(HuntsmanBase):
    """ Class to download reference catalogues using Table Access Protocol (TAP). """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        # Extract attribute values from config
        self._cone_search_radius = self.config["refcat"]["cone_search_radius"]
        self._ra_key = self.config["refcat"]["ra_key"]
        self._dec_key = self.config["refcat"]["dec_key"]
        self._unique_key = self.config["refcat"]["unique_source_key"]

        self._tap_url = self.config["refcat"]["tap_url"]
        self._tap_table = self.config["refcat"]["tap_table"]
        self._tap_limit = self.config["refcat"].get("tap_limit", None)
        self._parameter_ranges = self.config["refcat"]["parameter_ranges"]

        # Create the tap object
        self._tap = TapPlus(url=self._tap_url)

    def cone_search(self, ra, dec, filename, radius_degrees=None):
        """ Query the reference catalogue, saving output to a .csv file.
        Args:
            ra (float): RA of the centre of the cone search in J2000 degrees.
            dec (float): Dec of the centre of the cone search in J2000 degrees.
            filename (str): Filename of the returned .csv file.
            radius_degrees (float, optional): Override search radius from config.
        Returns:
            pd.DataFrame: The source catalogue.
        """
        if radius_degrees is None:
            radius_degrees = self._cone_search_radius

        query = f"SELECT * FROM {self._tap_table}"

        # Apply cone search
        query += (f" WHERE 1=CONTAINS(POINT('ICRS', {self._ra_key}, {self._dec_key}),"
                  f" CIRCLE('ICRS', {ra}, {dec}, {radius_degrees}))")

        # Apply parameter ranges
        for param, prange in self._parameter_ranges.items():
            with suppress(KeyError):
                query += f" AND {param} >= {prange['lower']}"
            with suppress(KeyError):
                query += f" AND {param} < {prange['upper']}"
            with suppress(KeyError):
                query += f" AND {param} = {prange['equal']}"

        # Apply limit on number of returned rows
        if self._tap_limit is not None:
            query += f" LIMIT {int(self._tap_limit)}"

        # Start the query
        self.logger.debug(f"Cone search command: {query}.")

        self._tap.launch_job_async(query, dump_to_file=True, output_format="csv",
                                   output_file=filename)
        return pd.read_csv(filename)

    def make_reference_catalogue(self, ra_list, dec_list, filename=None, **kwargs):
        """ Create the master reference catalogue with no source duplications.
        Args:
            ra_list (iterable): List of RA in J2000 degrees.
            dec_list (iterable): List of Dec in J2000 degrees.
            filename (string, optional): Filename to save output catalogue.
        Returns:
            pandas.DataFrame: The reference catalogue.
        """
        result = None

        with NamedTemporaryFile(delete=True) as tempfile:
            for ra, dec in zip(ra_list, dec_list):

                # Do the cone search and get result
                df = self.cone_search(ra, dec, filename=tempfile.name, **kwargs)

                # First iteration
                if result is None:
                    result = df
                    continue

                # Remove existing sources & concat
                is_new = np.isin(df[self._unique_key].values, result[self._unique_key].values,
                                 invert=True)
                result = pd.concat([result, df[is_new]], ignore_index=False)

        self.logger.debug(f"{result.shape[0]} sources in reference catalogue.")

        if filename is not None:
            os.makedirs(os.path.dirname(filename), exist_ok=True)
            result.to_csv(filename)

        return result
