""" The translator class is responsible for converting FITS header values into standardised
quantities understood by the LSST stack, as defined in the ObservationInfo class. """
import re
from datetime import datetime

from astropy.time import Time
from astropy import units as u
from astropy.coordinates import Angle

from panoptes.utils.utils import get_quantity_value

from huntsman.drp.core import get_config
from huntsman.drp.utils.date import parse_date
from huntsman.drp.utils import header as header_utils

from astro_metadata_translator.translator import cache_translation
from astro_metadata_translator.translators.fits import FitsTranslator


__all__ = ("HuntsmanTranslator", )


TRIVIAL_MAP = {"exposure_time": ("EXPTIME", dict(unit=u.s)),
               "detector_serial": "CAM-ID",
               "detector_name": "CAM-ID",
               "object": "FIELD",
               "boresight_airmass": "AIRMASS"}


def _get_camera_num_from_config(camera_name):
    """ Return the unique integer index of the cameras listed in the config.
    Args:
        camera_name (str): The camera name. Should be in the config.
    Returns:
        int: The index +1 of the camera in the config.
    Raises:
        RuntimeError: If the camera does not exist in the config.
    """
    HUNTSMAN_CONFIG = get_config()

    cam_num = None
    for i, cam_config in enumerate(HUNTSMAN_CONFIG["cameras"]["devices"]):
        if cam_config["camera_name"] == camera_name:
            cam_num = i + 1
            break
    if cam_num is None:
        raise RuntimeError(f"Camera {camera_name} not present in config.")
    return cam_num


class HuntsmanTranslator(FitsTranslator):
    """Metadata translator for DECam standard headers.
    """
    name = "Huntsman"
    supported_instrument = "Huntsman"

    # Maximum date / number of detectors to use for detector_exposure_id
    max_num_detectors = 99
    _max_date = datetime(year=2099, hour=23, day=31, month=12, minute=59, second=59,
                         microsecond=999999)

    # TODO: Implement units and "checker" functions (see base class documentation)
    _trivial_map = TRIVIAL_MAP

    # Constant mappings do not depend on the header
    # TODO: Figure out actual boresight rotation and speficy per-camera based on header
    _const_map = {"instrument": "Huntsman",
                  "detector_group": "Huntsman",
                  "science_program": "Huntsman",
                  "telescope": "Huntsman",
                  "boresight_rotation_angle": Angle(0 * u.deg),
                  "boresight_rotation_coord": "sky",
                  "temperature": 0 * u.Celsius,
                  "pressure": 0 * u.pascal,
                  "relative_humidity": 0.}

    # Class methods

    @classmethod
    def max_detector_exposure_id(cls):
        """ Return the maximum possible detector exposure ID.
        Returns:
            int: The maximum possible detector_exposure_id value.
        """
        max_exp_id = cls.max_exposure_id()
        return cls._get_detector_exposure_id(detector_num=cls.max_num_detectors,
                                             exposure_id=max_exp_id)

    @classmethod
    def max_exposure_id(cls):
        """ Return the maximum possible exposure ID.
        Returns:
            int: The maximum possible exposure_id value.
        """
        return cls._get_exposure_id(date=cls._max_date)

    # Translation methods

    @cache_translation
    def to_detector_num(self):
        """ Return a unique (for instrument) integer identifier for the sensor.
        Note: The detectors need to be present in the cameras config directory.
        Returns:
            int: The detector number.
        """
        camera_name = self.to_detector_name()
        return _get_camera_num_from_config(camera_name)

    @cache_translation
    def to_exposure_id(self):
        """ Calculate exposure ID.
        Unique (with instrument) integer identifier for this observation. For Huntsman, this is the
        datetime of the start of the observation to milisecond precision.
        Returns:
            int: ID of exposure.
        """
        key = "DATE-OBS"
        date = parse_date(self._header[key])
        exp_id = self._get_exposure_id(date)
        self._used_these_cards(key)
        return exp_id

    @cache_translation
    def to_visit_id(self):
        """ Return the visit ID integer.
        ID of the Visit this Exposure is associated with. For Huntsman, this is the datetime of the
        start of the observation to milisecond precision.
        Returns:
            int: The visit ID.
        """
        return self.to_exposure_id()

    @cache_translation
    def to_detector_exposure_id(self):
        """ Return a unique integer identifier for this detector in this exposure.
        Returns:
            int: The detector exposure ID.
        """
        exp_id = self.to_exposure_id()
        det_num = self.to_detector_num()
        return self._get_detector_exposure_id(detector_num=det_num, exposure_id=exp_id)

    @cache_translation
    def to_observation_id(self):
        """ Return a unique label identifying this observation.
        Returns:
            str: The observation ID.
        """
        return str(self.to_detector_exposure_id())

    # NOTE: We don't actually count exposures so this probably doesn't work properly
    @cache_translation
    def to_observation_counter(self):
        """ Return the lifetime exposure number.
        Counter of this observation. Can be counter within observing_day or a global counter.
        Returns:
            int: observation count.
        """
        return self.to_exposure_id()

    @cache_translation
    def to_datetime_begin(self):
        """ Calculate start time of observation.
        Returns:
            astropy.time.Time: Time corresponding to the start of the observation.
        """
        key = "DATE-OBS"
        date_begin = Time(parse_date(self._header[key]))
        self._used_these_cards(key)
        return date_begin

    @cache_translation
    def to_datetime_end(self):
        """ Calculate end time of observation.
        Returns:
            astropy.time.Time: Time corresponding to the end of the observation.
        """
        exptime = get_quantity_value(self.to_exposure_time(), u.second)
        return self.to_datetime_begin() + exptime

    @cache_translation
    def to_observation_type(self):
        """ Calculate the observation type.
        Returns:
            str: The observation type normalized to standard set (e.g. science, dark, bias).
        """
        obs_type, used_cards = header_utils.header_to_observation_type(self._header,
                                                                       get_used_cards=True)
        self._used_these_cards(*used_cards)
        return obs_type

    @cache_translation
    def to_location(self):
        """ Calculate the observatory location.
        Returns:
            astropy.coordinates.EarthLocation: An object representing the location of the telescope.
        """
        location, used_cards = header_utils.header_to_location(self._header, get_used_cards=True)
        self._used_these_cards(*used_cards)
        return location

    @cache_translation
    def to_tracking_radec(self):
        """ Requested RA/Dec to track.
        Returns:
            astropy.coordinates.SkyCoord: The ra dec object.
        """
        radec, used_cards = header_utils.header_to_radec(self._header, get_used_cards=True)
        self._used_these_cards(*used_cards)
        return radec

    @cache_translation
    def to_altaz_begin(self):
        """ Telescope boresight azimuth and elevation at start of observation.
        Returns:
            astropy.coordinates.AltAz: The alt az object.
        """
        radec, used_cards = header_utils.header_to_altaz(self._header, get_used_cards=True)
        self._used_these_cards(*used_cards)
        return radec

    @cache_translation
    def to_dark_time(self):
        """ Return the duration of the exposure with shutter closed.
        Returns:
            astropy.Quantity: The dark time.
        """
        key = "IMAGETYP"
        if self._header[key] == "Dark Frame":
            dark_time = self.to_exposure_time()
        else:
            dark_time = 0 * u.second
        self._used_these_cards(key)
        return dark_time

    @cache_translation
    def to_physical_filter(self):
        """ Return the name of the physical filter. Must be defined in filter definitions.
        Returns:
            filter (str): The physical filter name.
        """
        key = "FILTER"
        value = self._header[key].strip()
        self._used_these_cards(key)
        return value

    # Private methods

    @staticmethod
    def _get_exposure_id(date):
        """ Helper method to get exposure ID from date.
        Args:
            date (datetime.datetime): The datetime of the start of the observation.
        Returns:
            int: The exposure ID.
        """
        year = f"{int(str(date.year)[2:]):02d}"  # Strip off the first two digits
        month = f"{int(date.month):02d}"
        day = f"{int(date.day):02d}"
        hour = f"{int(date.hour):02d}"
        minute = f"{int(date.minute):02d}"
        second = f"{int(date.second):02d}"
        milisecond = f"{int(date.microsecond / 1E+3):03d}"

        datestr = f"{year}{month}{day}{hour}{minute}{second}{milisecond}"

        if len(datestr) != 15:
            raise ValueError("Date string should have length 15.")

        return int(datestr)

    @staticmethod
    def _get_detector_exposure_id(detector_num, exposure_id):
        """ Helper method to get detector exposure ID.
        Args:
            detector_num (int): The detector number.
            exposure_id (int): The exposure ID.
        Returns:
            int: The detector exposure ID.
        """
        return int(f"{detector_num:02d}{exposure_id}")
