from contextlib import suppress

from scipy.optimize import minimize
from astropy import stats
from astropy.wcs import WCS

from panoptes.utils.images.fits import get_solve_field

from huntsman.drp.utils.fits import parse_fits_header
from huntsman.drp.metrics.evaluator import MetricEvaluator

metric_evaluator = MetricEvaluator()


@metric_evaluator.add_function
def get_wcs(filename, header, timeout=60, downsample=4, radius=5, remake_wcs=False, **kwargs):
    """ Function to call get_solve_field on a file and verify if a WCS solution could be found.
    Args:
        filename (str): The filename.
        timeout (int, optional): How long to try and solve in seconds. Defaults to 60.
        downsample (int, optional): Downsample image by this factor. Defaults to 4.
        radius (int, optional): Search radius around mount Ra and Dec coords. Defaults to 5.
        remake_wcs (bool, optional): If True, remake WCS even if it already exists. Default False.
    Returns:
        dict: dictionary containing metadata.
    """
    # Skip if observation_type is not science
    parsed_header = parse_fits_header(header)
    if parsed_header["observation_type"] != "science":
        return {"has_wcs": False}

    # If there is already a WCS then don't make another one unless remake_wcs=True
    make_wcs = True
    with suppress(Exception):
        make_wcs = not WCS(header).has_celestial

    # Make the WCS if it doesn't already exist
    if make_wcs or remake_wcs:
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
    wcs = WCS(header)
    has_wcs = wcs.has_celestial

    result = {"has_wcs": has_wcs}

    # Calculate the central sky coordinates
    if has_wcs:
        x0_pix = header["NAXIS1"] / 2
        y0_pix = header["NAXIS2"] / 2
        coord = wcs.pixel_to_world(x0_pix, y0_pix)
        result["ra_centre"] = coord.ra.to_value("deg")
        result["dec_centre"] = coord.dec.to_value("deg")

    # Raise error so parent function knows it failed
    else:
        raise RuntimeError(f"Unable to determine WCS for {filename}.")

    return result


@metric_evaluator.add_function
def clipped_stats(filename, data, header, **kwargs):
    """Return sigma-clipped image statistics.
    Args:
        filename (str): The filename.
        data (np.array): The data array.
        header (abc.Mapping): The parsed FITS header.
    Returns:
        dict: The dict containing the metrics.
    """
    mean, median, stdev = stats.sigma_clipped_stats(data)

    # Calculate the well fullness fraction using clipped median
    bit_depth = header["BITDEPTH"]
    saturate = 2**bit_depth - 1
    well_fullfrac = median / saturate

    return {"clipped_mean": mean, "clipped_median": median, "clipped_std": stdev,
            "well_fullfrac": well_fullfrac}


@metric_evaluator.add_function
def flipped_asymmetry(filename, data, header, **kwargs):
    """ Calculate the asymmetry statistics by flipping data in x and y directions.
    Args:
        filename (str): The filename.
        data (np.array): The data array.
        header (abc.Mapping): The parsed FITS header.
    Returns:
        dict: The dict containing the metrics.
    """
    # Horizontal flip
    data_flip = data[:, ::-1]
    std_horizontal = (data - data_flip).std()

    # Vertical flip
    data_flip = data[::-1, :]
    std_vertical = (data - data_flip).std()
    return {"flip_asymm_h": std_horizontal, "flip_asymm_v": std_vertical}


@metric_evaluator.add_function
def reference_image_stats(filename, data, header, **kwargs):
    """ Compare an image to a reference image.
    Args:
        filename (str): The filename.
        data (np.array): The data array.
        header (abc.Mapping): The parsed FITS header.
    Returns:
        dict: The dict containing the metrics.
    """
    ref_image = kwargs.get("ref_image", None)
    if ref_image is None:
        return {}

    # Hack because for some reason LSST likes to add a pixel one each axis of master calibs
    ref_image = ref_image[:data.shape[0], :data.shape[1]]

    # First, we need to scale the data to the reference image
    def chi2(scaling):
        x = scaling * data
        return ((x - ref_image) ** 2 / ref_image).sum()

    scaling = minimize(chi2, x0=[1]).x[0]

    # Now calculate the reduced chi2 statistic
    chi2red = chi2(scaling) / data.size

    return {"ref_scaled_chi2r": chi2red}
