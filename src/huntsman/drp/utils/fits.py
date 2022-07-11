from contextlib import suppress

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
    with suppress(KeyError):
        if md["altaz_begin"] is None:
            md["alt"], md["az"] = None, None
        else:
            md["alt"], md["az"] = md.pop("altaz_begin")

    # Extract simplified RaDec
    with suppress(KeyError):
        if md["tracking_radec"] is None:
            md["ra"], md["dec"] = None, None
        else:
            md["ra"], md["dec"] = md.pop("tracking_radec")

    # Make some extra fields that are used by LSST
    with suppress(KeyError):
        md["detector"] = md["detector_num"]
    with suppress(KeyError):
        md["exposure"] = md["exposure_id"]
    with suppress(KeyError):
        md["visit"] = md["visit_id"]

    # Add generic date field
    with suppress(KeyError):
        md["date"] = parse_date(header["DATE-OBS"])

    return md
