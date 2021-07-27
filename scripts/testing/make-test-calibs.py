""" Script to make master calibs from testdata.
This should not have to be run regularly, but only after e.g. upgrading the LSST stack, modifying
the testing data, or changing the LSST calib policy.
"""
import os
import shutil

from huntsman.drp.core import get_config
from huntsman.drp.services.calib import CalibService
from huntsman.drp.collection import CalibCollection
from huntsman.drp.utils.testing import create_test_exposure_collection


if __name__ == "__main__":

    # Make an exposure collection with just the test data
    raw = create_test_exposure_collection()

    # Make a master calib collection just for the test data
    calib = CalibCollection(collection_name="calib-test")

    # Override the config so we don't use the main collections
    config = get_config()
    config["collections"]["ExposureCollection"]["name"] = raw.collection_name
    config["collections"]["CalibCollection"]["name"] = calib.collection_name

    # Make the calibs
    date = raw.find(key="date")[0]
    calib_maker = CalibService(config=config, validity=1000)
    calib_maker.process_date(date)

    # Copy files from the archive into the test data dir
    idir = os.path.join(calib_maker.config["directories"]["archive"], "calib")
    odir = os.path.join(calib_maker.config["directories"]["root"], "tests", "data", "calib")

    shutil.copytree(idir, odir)
