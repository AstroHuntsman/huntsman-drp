import os
import pytest
import yaml
import numpy as np
from astropy.io import fits

from huntsman.drp.fitsutil import FitsHeaderTranslator
from huntsman.drp.metadb import RawDataTable
from huntsman.drp.butler import TemporaryButlerRepository


def make_test_data(filename, taiObs, dataType, camera=1, filter="g2", shape=(30, 50), bias=32,
                   ra=100, dec=-30, exposure_time=30):
    """Make fake FITS images with realistic headers."""
    # Make the fake image data
    if dataType == "science":
        data = np.ones(shape) * bias * 5
        data = np.random.poisson(data) + bias
        field = "A Science Field"
        image_type = "Light Frame"
    elif dataType == "flat":
        data = np.ones(shape, dtype="float32")
        field = "Flat Field"
        image_type = "Light Frame"
    elif dataType == "bias":
        data = bias * np.ones(shape, dtype="uint16")
        field = "Dark Field"
        image_type = "Dark Frame"
    hdu = fits.PrimaryHDU(data)
    # Add the header
    hdu.header["RA"] = ra
    hdu.header["dec"] = dec
    hdu.header['EXPTIME'] = exposure_time
    hdu.header['FILTER'] = filter
    hdu.header['FIELD'] = field
    hdu.header['DATE-OBS'] = taiObs
    hdu.header["IMAGETYP"] = image_type
    hdu.header["INSTRUME"] = f"TESTCAM{camera:02d}"
    hdu.header["IMAGEID"] = "TestImageId"
    # Write as a FITS file
    hdu.writeto(filename, overwrite=True)


@pytest.fixture(scope="session")
def test_data():
    """List of dictionaries of test data."""
    filename = os.path.join(os.environ["HUNTSMAN_DRP"], "tests", "test_data.yaml")
    with open(filename, 'r') as f:
        data = yaml.safe_load(f)
    return data


@pytest.fixture(scope="session")
def raw_data_directory(tmp_path_factory, test_data):
    """Create a temporary directory populated with fake FITS images."""
    tempdir = tmp_path_factory.mktemp("testdata")
    for i, data_dict in enumerate(test_data["raw_data"]):
        filename = os.path.join(tempdir, f"testdata_{i}.fits")
        # Make the FITS images
        make_test_data(filename=filename, **data_dict)
        # Add the filename to test_data
        test_data["raw_data"][i]["filename"] = filename
    return tempdir


@pytest.fixture(scope="session")
def raw_data_table(raw_data_directory, raw_test_data):
    """Create a data table with test data inserted."""
    data_table = RawDataTable()
    data_table.insert_many(raw_test_data)
    return data_table


@pytest.fixture(scope="session")
def fits_header_translator():
    return FitsHeaderTranslator()


@pytest.fixture(scope="function")
def temp_butler_repo():
    return TemporaryButlerRepository()
