from astropy import stats
from astropy.wcs import WCS
from panoptes.utils.images.fits import get_solve_field
from huntsman.drp.fitsutil import FitsHeaderTranslator, read_fits_header

RAW_METRICS = ("has_wcs", "clipped_stats", "flipped_asymmetry")


def get_wcs(filename, timeout=60, downsample=4, *args):
    """Function to call get_solve_field on a file and verify
    if a WCS solution could be found.

    Args:
        filename (str): The filename.
    """
    has_wcs = False
    # first try and get the Mount RA/DEC info to speed up the solve
    try:
        hdr = read_fits_header(filename)
        parsed_hdr = FitsHeaderTranslator().parse_header(hdr)
        ra = hdr.get('RA-MNT')
        dec = hdr.get('DEC-MNT')
    except KeyError:
        pass

    # if file is not a science exposure, skip
    if parsed_hdr['dataType'] is not "science":
        return {"has_wcs": has_wcs}

    # Create list of args to pass to solve_field
    solve_kwargs = {'--cpulimit': str(timeout),
                    '--downsample': downsample,
                    '--ra': ra,
                    '--dec': dec,
                    '--radius': radius}
    # now solve for wcs
    try:
        wcs_info = get_solve_field(fname, *args, **kwargs, **solve_kwargs)
    except Exception as e:
        pass

    # finally check if the header now contians a wcs solution
    wcs = WCS(read_fits_header(filename))
    if wcs.has_celestial:
        has_wcs = True
    return {"has_wcs": has_wcs}


def clipped_stats(filename, data, file_info):
    """Return sigma-clipped image statistics.

    Parameters
    ----------
    data : array
        Image data as stored as an array.
    file_info : dict
        Dictionary containing image metadata

    Returns
    -------
    dict
        Dictionary containing the calculated stats values.
    """
    mean, median, stdev = stats.sigma_clipped_stats(data)

    # Calculate the well fullness fraction using clipped median
    bit_depth = file_info["BITDEPTH"]
    saturate = 2**bit_depth - 1
    well_fullfrac = median / saturate

    return {"clipped_mean": mean, "clipped_median": median, "clipped_std": stdev,
            "well_fullfrac": well_fullfrac}


def flipped_asymmetry(filename, data, file_info):
    """Calculate the asymmetry statistics by flipping data in x and y directions.

    Parameters
    ----------
    data : array
        Image data as stored as an array.
    file_info : dict
        Dictionary containing image metadata

    Returns
    -------
    dict
        Dictionary containing the calculated stats values.
    """
    # Horizontal flip
    data_flip = data[:, ::-1]
    std_horizontal = (data-data_flip).std()
    # Vertical flip
    data_flip = data[::-1, :]
    std_vertical = (data-data_flip).std()
    return {"flip_asymm_h": std_horizontal, "flip_asymm_v": std_vertical}
