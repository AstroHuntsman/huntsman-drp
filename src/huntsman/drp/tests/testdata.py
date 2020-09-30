import os
import yaml
import numpy as np
from astropy.io import fits
from datetime import timedelta

from huntsman.drp.utils import parse_date


def load_test_config():
    """Load config for the tests themselves."""
    filename = os.path.join(os.environ["HUNTSMAN_DRP"], "config", "testing.yaml")
    with open(filename, 'r') as f:
        test_config = yaml.safe_load(f)
    return test_config


def datetime_to_taiObs(date):
    """Convert datetime into a panoptes-style date string."""
    return date.strftime("%m-%d-%YT%H:%M:%S.%f")[:-3] + "(UTC)"


def make_hdu(data, date, cam_name, exposure_time, field, ccd_temp=0, filter="Blank",
             imageId="TestImageId"):
    """ """
    hdu = fits.PrimaryHDU(data)
    hdu.header['EXPTIME'] = exposure_time
    hdu.header['FILTER'] = "Blank"
    hdu.header['FIELD'] = field
    hdu.header['DATE-OBS'] = datetime_to_taiObs(date)
    hdu.header["IMAGETYP"] = "Dark Frame"
    hdu.header["INSTRUME"] = cam_name
    hdu.header["IMAGEID"] = imageId
    hdu.header["CCD-TEMP"] = ccd_temp
    return hdu


class FakeExposureSequence():

    def __init__(self):
        self.config = load_test_config()["exposure_sequence"]
        self.file_count = 0
        self.shape = self.config["size_y"], self.config["size_x"]
        self.dtype = self.config["dtype"]
        self.saturate = self.config["saturate"]
        self.hdu_dict = {}

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
                cam_name = f"TESTCAM{cam_number:02d}"
                for filter in self.config["filters"]:

                    # Create the flats
                    for flat in range(self.config["n_flat"]):
                        hdu = self._make_flat_data(date=dtime, cam_name=cam_name,
                                                   exposure_time=exptime_flat, filter=filter)
                        self._write_data(hdu=hdu)
                        dtime += timedelta(seconds=exptime_flat)
                    # Create the science exposures
                    for sci in range(self.config["n_science"]):
                        hdu = self._make_sci_data(date=dtime, cam_name=cam_name,
                                                  exposure_time=exptime_sci, filter=filter)
                        self._write_data(hdu=hdu)
                        dtime += timedelta(seconds=exptime_flat)

                # Create the dark frames
                for exptime in exptimes:
                    hdu = self._make_dark_frame_data(date=dtime, cam_name=cam_name,
                                                     exposure_time=exptime)
                    self._write_data(hdu=hdu)
                    dtime += timedelta(seconds=exptime)

    def _get_bias_level(self, exposure_time, ccd_temp=0):
        # TODO: Implement realistic scaling with exposure time
        return self.bias

    def _get_target_brightness(self, exposure_time, filter):
        # TODO: Implement realistic scaling with exposure time
        return 0.5 * self.saturate

    def _make_sci_data(self, *args, **kwargs):
        return self._make_light_frame(*args, **kwargs, field="TestField0")

    def _make_flat_data(self, *args, **kwargs):
        return self._make_light_frame(*args, **kwargs, field="FlatDither0")

    def _make_light_frame(self, date, cam_name, exposure_time, filter, field):
        """Make a light frame (either a science image or flat field)."""
        adu = self._get_target_brightness(exposure_time=exposure_time, filter=filter)
        data = np.ones(self.shape, dtype=self.dtype) * adu
        data[:, :] = np.random.poisson(data) + self.get_bias_level(exposure_time)
        data[data > self.saturate] = self.saturate
        # Create the header object
        hdu = make_hdu(data=data, date=date, cam_nam=cam_name, exposure_time=exposure_time,
                       field=field, filter=filter)
        return hdu

    def _make_dark_frame(self, date, cam_name, exposure_time, field="Dark Field"):
        """Make a dark frame (bias or dark)."""
        adu = self._get_target_brightness(exposure_time=exposure_time, filter=filter)
        data = np.ones(self.shape, dtype=self.dtype) * adu
        data[:, :] = np.random.poisson(data) + self.get_bias_level(exposure_time)
        data[data > self.saturate] = self.saturate
        # Create the header object
        hdu = make_hdu(data=data, date=date, cam_nam=cam_name, exposure_time=exposure_time,
                       field=field)
        return hdu

    def _get_filename(self, directory):
        return os.path.join(directory, f"testdata_{self.file_count}.fits")

    def _write_data(self, hdu, directory):
        filename = self._get_filename(directory)
        hdu.writeto(filename, overwrite=True)
        self.hdu_dict[filename] = hdu
        self.file_count += 1
