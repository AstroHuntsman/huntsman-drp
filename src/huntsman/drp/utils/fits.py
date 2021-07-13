from astropy.io import fits
from astro_metadata_translator import ObservationInfo

from huntsman.drp.translator import HuntsmanTranslator


def read_fits_data(filename, dtype="float32", **kwargs):
    """ Read fits image into numpy array.
    """
    return fits.getdata(filename, **kwargs).astype(dtype)


def read_fits_header(filename, ext="auto"):
    """ Read the FITS header for a given filename.
    Args:
        filename (str): The filename.
        ext (str or int): Which FITS extension to use. If 'auto' (default), will choose based on
            file extension. If 'all', will recursively extend the header with all extensions.
            Else, will use int(ext) as the ext number.
    Returns:
        dict: The header dictionary.
    """
    if ext == "all":
        header = fits.Header()
        i = 0
        while True:
            try:
                header.extend(fits.getheader(filename, ext=i))
            except IndexError:
                if i > 1:
                    return header
            i += 1
    elif ext == "auto":
        if filename.endswith(".fits"):
            ext = 0
        elif filename.endswith(".fits.fz"):  # <----- CHECK THIS
            ext = 1
        else:
            raise ValueError(f"Unrecognised FITS extension for {filename}.")
    else:
        ext = int(ext)
    return fits.getheader(filename, ext=ext)


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
