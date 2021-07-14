from astropy.io import fits
from astro_metadata_translator import ObservationInfo

from huntsman.drp.translator import HuntsmanTranslator


def read_fits_data(filename, dtype="float32", **kwargs):
    """ Read fits image into numpy array.
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

    return md
