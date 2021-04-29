from astropy import stats
from astropy.wcs import WCS
from panoptes.utils.images.fits import get_solve_field
from huntsman.drp.fitsutil import FitsHeaderTranslator, read_fits_header

# TODO: Move this to config?
RAW_METRICS = ("get_wcs", "clipped_stats", "flipped_asymmetry")


def get_wcs(filename, header, timeout=60, downsample=4, radius=5, **kwargs):
    """Function to call get_solve_field on a file and verify if a WCS solution could be found.
    Args:
        filename (str): The filename.
        timeout (int, optional): How long to try and solve in seconds. Defaults to 60.
        downsample (int, optional): Downsample image by this factor. Defaults to 4.
        radius (int, optional): Search radius around mount Ra and Dec coords. Defaults to 5.
    Returns:
        dict: dictionary containing metadata.
    """
    # Skip if dataType is not science
    # TODO: Move this logic outside this function
    parsed_header = FitsHeaderTranslator().parse_header(header)
    if parsed_header['dataType'] != "science":
        return {"has_wcs": False}

    # Create dict of args to pass to solve_field
    solve_kwargs = {'--cpulimit': str(timeout),
                    '--downsample': downsample}

    # Try and get the Mount RA/DEC info to speed up the solve
    if ("RA-MNT" in header) and ("DEC-MNT" in header):
        solve_kwargs['--ra'] = header["RA-MNT"]
        solve_kwargs['--dec'] = header["DEC-MNT"]
        solve_kwargs['--radius'] = radius

    # Solve for wcs
    get_solve_field(filename, **solve_kwargs)

    # Check if the header now contians a wcs solution
    wcs = WCS(read_fits_header(filename))
    has_wcs = wcs.has_celestial

    result = {"has_wcs": has_wcs}

    # Calculate the central sky coordinates
    if has_wcs:
        x0_pix = header["NAXIS1"] / 2
        y0_pix = header["NAXIS2"] / 2
        ra, dec = wcs.wcs_pix_to_world([[x0_pix, y0_pix]], 0)[0]
        result["ra_cen"] = ra
        result["dec_cen"] = dec

    return result


def clipped_stats(filename, data, header):
    """Return sigma-clipped image statistics.

    Parameters
    ----------
    data : array
        Image data as stored as an array.
    header : dict
        Dictionary containing image metadata

    Returns
    -------
    dict
        Dictionary containing the calculated stats values.
    """
    mean, median, stdev = stats.sigma_clipped_stats(data)

    # Calculate the well fullness fraction using clipped median
    bit_depth = header["BITDEPTH"]
    saturate = 2**bit_depth - 1
    well_fullfrac = median / saturate

    return {"clipped_mean": mean, "clipped_median": median, "clipped_std": stdev,
            "well_fullfrac": well_fullfrac}


def flipped_asymmetry(filename, data, header):
    """Calculate the asymmetry statistics by flipping data in x and y directions.

    Parameters
    ----------
    data : array
        Image data as stored as an array.
    header : dict
        Dictionary containing image metadata

    Returns
    -------
    dict
        Dictionary containing the calculated stats values.
    """
    # Horizontal flip
    data_flip = data[:, ::-1]
    std_horizontal = (data - data_flip).std()
    # Vertical flip
    data_flip = data[::-1, :]
    std_vertical = (data - data_flip).std()
    return {"flip_asymm_h": std_horizontal, "flip_asymm_v": std_vertical}
