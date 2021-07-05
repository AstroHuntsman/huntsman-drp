from astropy import units as u
from astropy.coordinates import SkyCoord, EarthLocation, AltAz

from huntsman.drp.utils.date import parse_date


def header_to_radec(header, get_used_cards=False):
    """ Get the ra dec object from the FITS header.
    Args:
        header (abc.Mapping): The FITS header.
        get_used_cards (bool, optional): If True, return the keys used to obtain alt / az. This
            is used e.g. by LSST translator classes. Default: False.
    Returns:
        astropy.coordinates.SkyCoord: The ra dec object.
    """
    ra_key = "RA-MNT"
    dec_key = "DEC-MNT"
    crd = SkyCoord(ra=header[ra_key] * u.deg, dec=header[dec_key] * u.deg)

    if get_used_cards:
        return crd, set([ra_key, dec_key])

    return crd


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
    ra = header["RA-MNT"] * u.deg
    dec = header["DEC-MNT"] * u.deg
    radec = SkyCoord(ra=ra, dec=dec)

    # Get the location of the observation
    location, used_keys = header_to_location(header, get_used_cards=True)

    # Create the Alt/Az frame
    obstime = parse_date(header["DATE-OBS"])
    frame = AltAz(obstime=obstime, location=location)

    # Perform the transform
    altaz = radec.transform_to(frame)

    used_keys.update(["RA-MNT", "DEC-MNT", "DATE-OBS"])
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
