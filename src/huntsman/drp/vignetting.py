from multiprocessing import Pool
from functools import partial
from datetime import timedelta

import numpy as np
import matplotlib.pyplot as plt
from scipy.spatial import ConvexHull

from astropy.io import fits
from astropy import units as u
from astropy.coordinates import EarthLocation, AltAz, SkyCoord

from huntsman.drp.base import HuntsmanBase
from huntsman.drp.utils import parse_date
from huntsman.drp.datatable import RawDataTable


def is_vignetted(data, threshold=200, tolerance=0.05):
    """
    Calculate the vignetted fraction by applying a simple threshold. TODO: Automate threshold
    value.
    """
    return (data < threshold).mean() > tolerance


class VignettingAnalyser(HuntsmanBase):

    def __init__(self, date, nproc=8, query_kwargs={}, **kwargs):
        super().__init__(**kwargs)
        self._date_start = parse_date(date)
        self._date_end = date + timedelta(days=1)
        self._nproc = nproc
        self._field_name = self.config["vignetting"]["field_name"]
        self._location_name = self.config["vignetting"]["location_name"]
        self._earth_location = EarthLocation.of_site(self._location_name)
        self._hull = None
        self._get_metadata(**query_kwargs)

    def create_hull(self, plot_filename=None, **kwargs):
        """
        Create the map of vignetted coordinates.
        """
        # Calculate vignetted fractions
        self.is_vignetted = self._get_is_vignetted(self._filenames, **kwargs)
        # Create the hull
        if self.is_vignetted.any():
            self._hull = ConvexHull(self._coordinates[self.is_vignetted])
        # Make the plot
        if plot_filename is not None:
            self._make_summary_plot(plot_filename)

    def _get_metadata(self, query_dict=None):
        """
        Get a list of filenames for a vignetting sequence.
        """
        if query_dict is None:
            query_dict = {}
        query_dict["field"] = self._field_name
        datatable = RawDataTable(config=self.config)
        self._metadata = datatable.query(date_start=self._date_start, date_end=self._date_end,
                                         query_dict=query_dict)
        self._filenames = [_["filename"] for _ in self._metadata]
        self._coordinates = self._translate_altaz(self._metadata)

    def _translate_altaz(self, metadata_list):
        """
        Calculate the alt/az coordinates from the FITS header.
        """
        coordinates = np.zeros((len(metadata_list), 2), dtype="float")
        for i, metadata in enumerate(metadata_list):
            # Extract info from metadata
            ra = metadata["RA-MNT"] * u.degree
            dec = metadata["DEC-MNT"] * u.degree
            obsdate = parse_date(metadata["DATE-OBS"])
            # Transform into Alt/Az
            coord_radec = SkyCoord(ra=ra, dec=dec)
            transform = AltAz(location=self._earth_location, obstime=obsdate)
            coord_altaz = coord_radec.transform_to(transform)
            coordinates[i] = coord_altaz.alt.to_value(u.degree), coord_altaz.az.to_value(u.degree)
        return coordinates

    def _get_is_vignetted(self, filenames, **kwargs):
        """
        Calculate vignetted fractions for files using a process pool.
        """
        fn = partial(self._is_vignetted, **kwargs)
        with Pool(self._nproc) as pool:
            is_vignetted = pool.map(fn, filenames)
        return np.array(is_vignetted).astype("bool")

    def _is_vignetted(self, filename, **kwargs):
        """
        Read the data from FITS and calculate the vignetted fraction.
        """
        data = fits.getdata(filename)
        return is_vignetted(data, **kwargs)

    def _make_summary_plot(self, filename):
        """
        """
        fig, ax = plt.subplots()
        points_vignetted = self._coordinates[self.is_vignetted]
        ax.plot(self._coordinates[:, 0], self._coordinates[:, 1], "k+")
        ax.plot(points_vignetted[:, 0], points_vignetted[:, 1], "rx")
        if self._hull is not None:
            for simplex in self._hull.simplices:
                ax.plot(points_vignetted[simplex, 0], points_vignetted[simplex, 1], 'b-')
        ax.set_xlabel("Alt [Degrees]")
        ax.set_ylabel("Az [Degrees]")
        plt.savefig(filename, bbox_inches="tight", dpi=150)
