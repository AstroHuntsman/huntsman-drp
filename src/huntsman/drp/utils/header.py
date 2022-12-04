from astropy import units as u
from astropy.wcs import WCS
from astropy.coordinates import SkyCoord, EarthLocation, AltAz

from huntsman.drp.utils.date import parse_date


def header_to_radec(header, get_used_cards=False):
    """ Get the ra dec of the field centre from the FITS header.
    If a celestial WCS is present, will try and use that first. If not, will use approximate
    telescope pointing. If a telescope pointing was not captured in the header, return None.
    Args:
        header (abc.Mapping): The FITS header.
        get_used_cards (bool, optional): If True, return the keys used to obtain alt / az. This
            is used e.g. by LSST translator classes. Default: False.
    Returns:
        astropy.coordinates.SkyCoord: The ra dec object.
    """
    try:
        wcs = WCS(header)
        if not wcs.has_celestial:
            raise ValueError("Header does not have celestial WCS.")

        # Get pixel coordinates of image centre
        xkey = "NAXIS1"
        ykey = "NAXIS2"
        x = header[xkey] / 2
        y = header[ykey] / 2

        # Convert pixel coordinates to radec
        radec = wcs.pixel_to_world(x, y)

        # TODO: Include keys used to make WCS object
        used_keys = set([xkey, ykey])

    # If can't used WCS, get directly from header keys
    # This does not account for telescope pointing errors
    except Exception:
        ra_key = "RA-MNT"
        dec_key = "DEC-MNT"
        ra = header[ra_key]
        dec = header[dec_key]
        # Sometimes headers don't contain mount RA/DEC
        bad_vals = ('', None)
        if any(val in bad_vals for val in (ra, dec)):
            radec = None
        else:
            radec = SkyCoord(ra=ra * u.deg, dec=dec * u.deg)
        used_keys = set([ra_key, dec_key])

    if get_used_cards:
        return radec, used_keys

    return radec


def header_to_location(header, get_used_cards=False):
    """ Get the location object from the FITS header.
    Args:
        header (abc.Mapping): The FITS header.
        get_used_cards (bool, optional): If True, return the keys used to obtain alt / az. This
            is used e.g. by LSST translator classes. Default: False.
    Returns:
        astropy.coordinates.EarthLocation: The location object.
    """
    lat = header["LAT-OBS"] * u.deg
    lon = header["LONG-OBS"] * u.deg
    elevation = header["ELEV-OBS"] * u.m
    location = EarthLocation(lat=lat, lon=lon, height=elevation)

    if get_used_cards:
        return location, set(["LAT-OBS", "LONG-OBS", "ELEV-OBS"])

    return location


def header_to_altaz(header, get_used_cards=False):
    """ Get the alt az of the observation from the header.
    Args:
        header (abc.Mapping): The FITS header.
        get_used_cards (bool, optional): If True, return the keys used to obtain alt / az. This
            is used e.g. by LSST translator classes. Default: False.
    Returns:
        astropy.coordinates.AltAz: The alt / az object.
    """
    # Get the ra / dec of the observation
    radec, used_keys = header_to_radec(header, get_used_cards=True)

    # Get the location of the observation
    location, used_keys_location = header_to_location(header, get_used_cards=True)
    used_keys.update(used_keys_location)

    # Create the Alt/Az frame
    obstime = parse_date(header["DATE-OBS"])
    frame = AltAz(obstime=obstime, location=location)

    # Perform the transform
    if radec is None:
        altaz = None
    else:
        altaz = radec.transform_to(frame)

    used_keys.update(["DATE-OBS"])
    if get_used_cards:
        return altaz, used_keys

    return altaz


def header_to_observation_type(header, get_used_cards=False):
    """ Return the type of observation (e.g. science, bias, dark, flat).
    Args:
        header (abc.Mapping): The FITS header.
        get_used_cards (bool, optional): If True, return the keys used to obtain alt / az. This
            is used e.g. by LSST translator classes. Default: False.
    Returns:
        str: The observation type.
    """
    image_type = header['IMAGETYP']
    field_name = header["FIELD"]

    if image_type == 'Light Frame':
        if field_name.startswith("Flat"):
            obs_type = 'flat'
        else:
            obs_type = 'science'

    elif image_type == 'Dark Frame':
        if field_name == "Bias":
            obs_type = "bias"
        else:
            obs_type = "dark"
    else:
        raise ValueError(f"IMAGETYP not recongnised: {image_type}")

    if get_used_cards:
        return obs_type, set(["IMAGETYP", "FIELD"])

    return obs_type
