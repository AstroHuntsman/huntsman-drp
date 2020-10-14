"""
Script to read the data from a vignetting sequence, identify vignetted frames, and produce
a pickled object that can be used to predict vignetted alt/az coordinates.
"""
from multiprocessing import Pool
from functools import partial

import numpy as np
import matplotlib.pyplot as plt
from scipy.spatial import ConvexHull

from astropy.io import fits
from astropy import units as u
from astropy.coordinates import EarthLocation, AltAz, SkyCoord

from huntsman.drp.base import HuntsmanBase
from huntsman.drp.utils import parse_date
from huntsman.drp.datatable import RawDataTable


def calculate_vignetted_fraction(data, threshold=200):
    """
    Calculate the vignetted fraction by applying a simple threshold. TODO: Automate threshold
    value.
    """
    return (data < threshold).mean()


class VingettingAnalyser(HuntsmanBase):  # Yes, another class

    _field_name = "VigTest"
    _location_name = "Siding Spring Observatory"

    def __init__(self, date, nproc=8, **kwargs):
        self._date = date
        self._nproc = nproc
        self._earth_location = EarthLocation.of_site(self._location_name)
        self._hull = None
        self._get_metadata(**kwargs)

    def create_hull(self, tolerance=0.05, plot_filename=None, **kwargs):
        """
        Create the map of vignetted coordinates.
        """
        # Calculate vignetted fractions
        vfracs = self._calculate_vignetted_fractions(self._filenames, **kwargs)
        self._is_vignetted = vfracs >= tolerance
        # Create the hull
        if self._is_vignetted.any():
            self._hull = ConvexHull(self._coordinates[self._is_vignetted])
        # Make the plot
        if plot_filename is not None:
            self._make_summary_plot(plot_filename)

    def _get_metadata(self, **kwargs):
        """
        Get a list of filenames for a vignetting sequence.
        """
        datatable = RawDataTable()
        self._metadata = datatable.query(FIELD=self._field_name, date_start=self._date,
                                         date_end=self._date, **kwargs)
        self._filenames = [_["filename"] for _ in self._metadata]
        self._coordinates = self._translate_altaz(self._metadata)

    def _translate_altaz(self, metadata_list):
        """
        Calculate the alt/az coordinates from the FITS header.
        """
        coordinates = np.zeros(len(metadata_list), dtype="float")
        for i, metadata in enumerate(metadata_list):
            # Extract info from metadata
            ra = metadata["RA"] * u.degree
            dec = metadata["DEC"] * u.degree
            obsdate = parse_date(metadata["DATE-OBS"])
            # Transform into Alt/Az
            coord_radec = SkyCoord(ra=ra, dec=dec)
            transform = AltAz(location=self._earth_location, obstime=obsdate)
            coord_altaz = coord_radec.transform_to(transform)
            coordinates[i] = coord_altaz.alt.to_value(u.degree), coord_altaz.az.to_value(u.degree)
        return coordinates

    def _calculate_vignetted_fractions(self, filenames, **kwargs):
        """
        Calculate vignetted fractions for files using a process pool.
        """
        fn = partial(self._calculate_vignetted_fraction, **kwargs)
        with Pool(self._nproc) as pool:
            fractions = pool.map(fn, filenames)
        return np.array(fractions)

    def _calculate_vignetted_fraction(self, filename, **kwargs):
        """
        Read the data from FITS and calculate the vignetted fraction.
        """
        data = fits.getdata(filename)
        return calculate_vignetted_fraction(data, **kwargs)

    def _make_summary_plot(self, filename, points):
        """
        """
        fig, ax = plt.subplots()
        points_vignetted = self._coordinates[self._is_vignetted]
        ax.plot(self._coordinates[:, 0], self._coordinates[:, 1], "k+")
        ax.plot(points_vignetted[:, 0], points_vignetted[:, 1], "rx")
        if self._hull is not None:
            for simplex in self._hull.simplices:
                ax.plot(points[simplex, 0], points[simplex, 1], 'b-')
        ax.set_xlabel("Alt [Degrees]")
        ax.set_ylabel("Az [Degrees]")
        plt.savefig(filename, bbox_inches="tight", dpi=150)


if __name__ == "__main__":

    va = VingettingAnalyser(date="12-10-2020")
    va.create_hull(plot_filename="vignetting_plot.py")
