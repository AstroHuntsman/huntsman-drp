from astropy.io import fits
from astro_metadata_translator import ObservationInfo

from huntsman.drp.utils.date import parse_date
from huntsman.drp.translator import HuntsmanTranslator


def read_fits_data(filename, dtype="float32", **kwargs):
    """ Read fits image into numpy array.
    Args:
        filename (str): The name of ther file to read.
        dtype (str, optional): The data type for the array. Default: float32.
        **kwargs: Parsed to fits.getdata.
    Returns:
        np.array: The image array.
    """
    return fits.getdata(filename, **kwargs).astype(dtype)


def read_fits_header(filename, **kwargs):
    """ Read the FITS header for a given filename.
    Args:
        filename (str): The filename.
        **kwargs: Parsed to fits.getheader.
    Returns:
        astropy.header.Header: The header object.
    """
    return fits.getheader(filename, **kwargs)


def parse_fits_header(header, **kwargs):
    """ Use the translator class to parse the FITS header.
    Certain objects (e.g. AltAz) are simplified for mongo ingestion.
    Args:
        header (dict): The FITS header.
        **kwargs: Parsed to astro_metadata_translator.ObservationInfo.
    Returns:
        dict: The parsed header.
    """
    md = ObservationInfo(header, translator_class=HuntsmanTranslator, **kwargs).to_simple()

    # Extract simplified AltAz
    md["alt"], md["az"] = md.pop("altaz_begin")

    # Extract simplified RaDec
    md["ra"], md["dec"] = md.pop("tracking_radec")

    # Remove other keys that cannot be stored in mongo DB
    for key in md.keys():
        pass

    # Make some extra fields that are used by LSST
    md["detector"] = md["detector_num"]
    md["exposure"] = md["exposure_id"]
    md["visit"] = md["visit_id"]

    # Add generic date field
    md["date"] = parse_date(header["DATE-OBS"])

    return md
