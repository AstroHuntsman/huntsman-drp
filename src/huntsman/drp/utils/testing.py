import os
import yaml
import time
from datetime import timedelta
import numpy as np
from astropy.io import fits
from astropy import units as u

from huntsman.drp.core import get_config
from huntsman.drp.lsst.butler import ButlerRepository
from huntsman.drp.base import HuntsmanBase
from huntsman.drp.utils.date import parse_date
from huntsman.drp.collection import RawExposureCollection
from huntsman.drp.services.ingestor import FileIngestor


EXPTIME_BIAS = 1E-32  # Minimum exposure time for ZWO cameras is > 0


def load_test_config():
    """Load config for the tests themselves."""
    filename = os.path.join(os.environ["HUNTSMAN_DRP"], "config", "test_config.yaml")
    with open(filename, 'r') as f:
        test_config = yaml.safe_load(f)
    return test_config


def datetime_to_taiObs(date):
    """Convert datetime into a panoptes-style date string."""
    return date.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "(UTC)"


# Real test data


def get_testdata_dir(config):
    return os.path.join(config["directories"]["root"], "tests", "data")


def get_testdata_fits_filenames(config=None):
    """
    """
    if config is None:
        config = get_config()

    datadir = get_testdata_dir(config)

    # Get test data filenames
    filenames = []
    for filename in os.listdir(datadir):
        if filename.endswith(".fits"):
            filenames.append(os.path.join(datadir, filename))

    return filenames


def get_refcat_filename(config):
    """ Get the filename of the testing reference catalogue.
    Args:
        config (dict): The config dict.
    """
    datadir = os.path.join(config["directories"]["root"], "tests", "data")
    return os.path.join(datadir, "refcat.csv")


def create_test_bulter_repository(directory, config=None, **kwargs):
    """ Create a butler repository and ingest the testing dataset.
    Args:
        **kwargs: Parsed to ButlerRepository.
    """
    if config is None:
        config = get_config()

    br = ButlerRepository(directory=directory, config=config, **kwargs)

    filenames = get_testdata_fits_filenames(config=config)

    # Ingest test data into butler repository
    br.ingest_raw_data(filenames)

    # Ingest the refcat
    refcat_filename = get_refcat_filename(config)
    br.ingest_reference_catalogue([refcat_filename])

    return br


def create_test_exposure_collection(config, name="real_data", clear=True):
    """ Ingest real testing images into a RawExposureCollection
    """
    dir = get_testdata_dir(config)
    filenames = get_testdata_fits_filenames(config)

    exposure_collection = RawExposureCollection(config=config, collection_name=name)
    if clear:
        exposure_collection.delete_all(really=True)

    # Make and start FileIngestor object
    ing = FileIngestor(directory=dir, config=config, exposure_collection=exposure_collection)
    ing.start()

    i = 0
    while i < 30:
        if ing.status["processed"] == len(filenames):
            break
        time.sleep(1)
        i += 1

    ing.stop()

    assert ing.status["processed"] == len(filenames)
    assert ing.status["failed"] == 0
    assert len(exposure_collection.find()) == len(filenames)
    assert set(exposure_collection.find(key="filename")) == set(filenames)
    assert len(exposure_collection.find(screen=True)) == len(filenames)

    return exposure_collection


# Fake test data


def make_hdu(data, date, cam_name, exposure_time, field, image_type, ccd_temp=0, filter="Blank",
             imageId="TestImageId", ra=10, dec=-20, airmass=1, pixel_size=1):
    """Make a HDU with a minimal header for DRP to function."""
    hdu = fits.PrimaryHDU(data)
    hdu.header['EXPTIME'] = exposure_time
    hdu.header['FILTER'] = filter
    hdu.header['FIELD'] = field
    hdu.header['DATE-OBS'] = datetime_to_taiObs(date)
    hdu.header["IMAGETYP"] = image_type
    hdu.header["CAM-ID"] = cam_name
    hdu.header["IMAGEID"] = imageId
    hdu.header["CCD-TEMP"] = ccd_temp
    hdu.header["RA-MNT"] = ra
    hdu.header["DEC-MNT"] = dec
    hdu.header["AIRMASS"] = airmass
    hdu.header["CD1_1"] = pixel_size.to_value(u.degree / u.pixel)
    hdu.header["CD2_2"] = pixel_size.to_value(u.degree / u.pixel)
    hdu.header["CD1_2"] = 0
    hdu.header["CD2_1"] = 0
    hdu.header["BITDEPTH"] = 12
    return hdu


class FakeExposureSequence(HuntsmanBase):
    """
    The `FakeExposureSequence` is responsible for generating fake FITS files based on settings
    in the config. The basic idea is to create semi-realistic daily observation sets for testing
    purposes.
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.config = self.config["exposure_sequence"]
        self.file_count = 0
        self.shape = self.config["size_y"], self.config["size_x"]
        self.dtype = self.config["dtype"]
        self.saturate = self.config["saturate"]
        self.bias = self.config["bias"]
        self.pixel_size = self.config["pixel_size"] * u.arcsecond / u.pixel
        self.header_dict = {}

    def generate_fake_data(self, directory):
        """
        Create FITS files for the exposure sequence specified in the testing config and store
        their metadata.

        Args:
            directory (str): The name of the directory in which to store the FITS files.
        """
        exptime_sci = self.config["exptime_science"]
        exptime_flat = self.config["exptime_flat"]
        exptimes = [exptime_flat, exptime_sci]

        # Create n_days days-worth of fake observations
        for day in range(self.config["n_days"]):
            dtime = parse_date(self.config["start_date"]) + timedelta(days=day, hours=19)

            # Assume synchronous exposures between CCDs
            for cam_number in range(self.config["n_cameras"]):
                cam_name = f"TESTCAM{cam_number+1:02d}"

                # Loop over filters to create science exposures and flat fields
                for filter in self.config["filters"]:

                    # Create the flats
                    for flat in range(self.config["n_flat"]):
                        hdu = self._make_light_frame(date=dtime, cam_name=cam_name,
                                                     field="FlatDither0", filter=filter,
                                                     exposure_time=exptime_flat)
                        self._write_data(hdu=hdu, directory=directory)
                        dtime += timedelta(seconds=exptime_flat)  # Increment time

                    # Create the science exposures
                    for sci in range(self.config["n_science"]):
                        hdu = self._make_light_frame(date=dtime, cam_name=cam_name,
                                                     exposure_time=exptime_sci, filter=filter,
                                                     field="TestField0")
                        self._write_data(hdu=hdu, directory=directory)
                        dtime += timedelta(seconds=exptime_flat)  # Increment time

                # Create the dark frames using given exposure times
                for _ in range(self.config["n_bias"]):
                    hdu = self._make_dark_frame(date=dtime, cam_name=cam_name,
                                                exposure_time=EXPTIME_BIAS, field="Bias")
                    self._write_data(hdu=hdu, directory=directory)
                    dtime += timedelta(seconds=1)  # Increment time

                # Create the dark frames using given exposure times
                for _ in range(self.config["n_dark"]):
                    for exptime in exptimes:
                        hdu = self._make_dark_frame(date=dtime, cam_name=cam_name, field="Dark",
                                                    exposure_time=exptime)
                        self._write_data(hdu=hdu, directory=directory)
                        dtime += timedelta(seconds=exptime)  # Increment time

    def _get_bias_level(self, exposure_time, ccd_temp=0):
        # TODO: Implement realistic scaling with exposure time
        return self.bias

    def _get_target_brightness(self, exposure_time, filter):
        # TODO: Implement realistic scaling with exposure time
        return 0.5 * self.saturate

    def _make_light_frame(self, date, cam_name, exposure_time, filter, field):
        """Make a light frame (either a science image or flat field)."""

        adu = self._get_target_brightness(exposure_time=exposure_time, filter=filter)
        data = np.random.poisson(adu, size=self.shape) + self._get_bias_level(exposure_time)
        data[data > self.saturate] = self.saturate
        data = data.astype(self.dtype)
        assert (data > 0).all()

        hdu = make_hdu(data=data, date=date, cam_name=cam_name, exposure_time=exposure_time,
                       field=field, filter=filter, image_type="Light Frame",
                       pixel_size=self.pixel_size)
        return hdu

    def _make_dark_frame(self, date, cam_name, exposure_time, field):
        """Make a dark frame (bias or dark)."""

        adu = self._get_bias_level(exposure_time=exposure_time) + 1 * exposure_time
        data = np.random.poisson(adu, size=self.shape)
        data[data > self.saturate] = self.saturate
        data = data.astype(self.dtype)
        assert (data > 0).all()

        hdu = make_hdu(data=data, date=date, cam_name=cam_name, exposure_time=exposure_time,
                       field=field, image_type="Dark Frame", pixel_size=self.pixel_size)
        return hdu

    def _get_filename(self, directory):
        """ Get the filename for the next exposure in the sequence.
        Args:
            directory (str): The name of the directory in which to store the file.
        Returns:
            str: The filename.
        """
        return os.path.join(directory, f"testdata_{self.file_count}.fits")

    def _write_data(self, hdu, directory):
        """ Write the data to file, store the header and increment the file count.
        Args:
            directory (str): The name of the directory in which to store the file.
        """
        filename = self._get_filename(directory)
        hdu.writeto(filename, overwrite=True)
        # Read the header from file because astropy can modify the header during write
        self.header_dict[filename] = fits.getheader(filename)
        self.file_count += 1
