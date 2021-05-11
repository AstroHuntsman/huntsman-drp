"""
https://github.com/lsst/meas_algorithms/blob/master/python/lsst/meas/algorithms/subtractBackground.py
https://github.com/lsst/afw/blob/master/python/lsst/afw/math/_backgroundList.py

TODO:
  - Check if backgrounds are in counts / s
  - Figure out how to provide a background image to processCcd
  - Check if the background input bug exists in Docker image
"""
import os
from collections import defaultdict

import numpy as np
from astropy.io import fits

from huntsman.drp.reduction.lsst import LsstDataReduction


def make_median_lsst_backgroundList(bg_filename_list, output_filename):
    """ LSST stores backgrounds in a strange way.

    This is a complete hack.
    """
    with fits.open(bg_filename_list[0]) as hdulist:
        n_hdus = len(hdulist)  # Assumed to be the same for all files

    bg_list = defaultdict(list)
    var_list = defaultdict(list)

    # Extract background components from the FITS files
    # Note that these components are summed to create the final background image
    # However it's not a direct sum so we can't just add the components here

    for filename in bg_filename_list:
        for hdu_idx in range(0, n_hdus, 3):

            bg = fits.getdata(filename, ext=hdu_idx)
            var = fits.getdata(filename, ext=hdu_idx + 2)

            bg_list[hdu_idx].append(bg)
            var_list[hdu_idx].append(var)

    # Now we need to write these new values back into a FITS file
    # The easiest way to make sure the metadata is OK is to edit the data in a copied HDU list
    with fits.open(filename) as hdulist:

        # Swap the existing data for the averaged versions
        for hdu_idx in range(0, n_hdus, 3):

            hdulist[hdu_idx] = np.nanmedian(bg_list[hdu_idx])
            hdulist[hdu_idx + 1] = np.zeros_like(hdulist[hdu_idx], dtype="int32")
            hdulist[hdu_idx + 2] = np.nanmedian(var_list[hdu_idx + 2])

        # Now write the update HDU list to file
        os.makedirs(os.path.dirname(output_filename), exist_ok=True)
        hdulist.writeto(output_filename)


class SkyOffsetReduction(LsstDataReduction):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def prepare(self):
        super().prepare()

        # Get background docs

        # Ingest background docs

    def reduce(self):

        # extra_config = {"isr.doWrite": True}  # Outputs not written by default
        pass

    def measure_sky(self):
        """ Measure sky for each sky image. """

        # Get dataIds to reduce
        dataIds = [doc.to_lsst() for doc in self.background_docs]

        # Specify extra args for LSST subtask
        extra_config = {"calibrate.doPhotoCal": False}

        # Process the dataIds
        self._butler_repo.make_calexps(dataIds=dataIds, extra_config=extra_config)

    def get_master_sky(self, document, rerun="default"):
        """ Get a master sky frame for the specific document. """

        # Identify matching sky documents
        matching_sky_docs = None  # ???

        # Get background images from LSST
        bg_filename_list = []
        for doc in matching_sky_docs:

            dataId = doc.to_lsst()

            # TODO: Parse the rerun somehow
            bg_filename = self._butler_repo.get("calexpBackground_filename", dataId=dataId,
                                                rerun=rerun)
            bg_filename_list.append(bg_filename)

        # Get the filename in which to store the background for this exposure
        dataId = document.to_lsst()
        filename = self._butler_repo.get("calexpBackground_filename", dataId=dataId, rerun=rerun)

        # Combine the background images into a master and write to file
        # This will enable butler to retrieve the background as a calexpBackground object
        make_median_lsst_backgroundList(bg_filename_list, output_filename=filename)
