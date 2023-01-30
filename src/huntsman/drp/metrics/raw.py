from contextlib import suppress

import numpy as np
from scipy import signal, ndimage
from scipy.optimize import minimize
from astroscrappy import detect_cosmics

from astropy.coordinates import EarthLocation, AltAz
from astropy.io.fits import getheader
from astropy.wcs import WCS
from astropy import units as u
from astropy import stats

from panoptes.utils.images.fits import get_solve_field

from huntsman.drp.core import get_config
from huntsman.drp.utils.date import parse_date
from huntsman.drp.utils.fits import parse_fits_header, image_cutout
from huntsman.drp.metrics.evaluator import MetricEvaluator

metric_evaluator = MetricEvaluator()


@metric_evaluator.add_function
def get_wcs(filename, header, timeout=300, downsample=4, radius=3, remake_wcs=False, **kwargs):
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
        # Create list of args to pass to solve_field
        solve_opts = ['--cpulimit', str(timeout),
                      '--downsample', str(downsample)]

        # Try and get the Mount RA/DEC info to speed up the solve
        bad_vals = ('', None)
        if ("RA-MNT" in header and header["RA-MNT"] not in bad_vals) and ("DEC-MNT" in header and header["DEC-MNT"] not in bad_vals):
            solve_opts += ['--ra', header["RA-MNT"],
                           '--dec', header["DEC-MNT"],
                           '--radius', str(radius)]
        # Solve for wcs
        get_solve_field(filename, timeout=timeout, solve_opts=solve_opts)

    # Check if the header now contians a wcs solution
    header = getheader(filename)
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

    # First, we need to scale the data to the reference image
    def chi2(scaling):
        x = scaling * data
        return ((x - ref_image) ** 2 / ref_image).sum()

    scaling = minimize(chi2, x0=[1]).x[0]

    # Now calculate the reduced chi2 statistic
    chi2red = chi2(1) / data.size
    chi2red_scaled = chi2(scaling) / data.size

    return {"ref_chi2r": chi2red, "ref_chi2r_scaled": chi2red_scaled}


@metric_evaluator.add_function
def alt_az(filename, data, header, **kwargs):
    """ Get the alt az of the observation from the header.
    Args:
        filename (str): The filename.
        data (np.array): The data array.
        header (abc.Mapping): The parsed FITS header.
    Returns:
        dict: The dict containing the metrics.
    """
    # Skip if observation_type is not science
    header = getheader(filename)
    parsed_header = parse_fits_header(header)
    if parsed_header["observation_type"] != "science":
        return {}

    # If there is no wcs, we can't get the ra/dec so skip
    has_wcs = False
    with suppress(Exception):
        wcs = WCS(header)
        has_wcs = wcs.has_celestial

    if not has_wcs:
        return {}

    # get image dimensions
    img_dim = wcs.array_shape
    # get the ra,dec of the centre of the image
    radec = wcs.pixel_to_world(img_dim[1]/2, img_dim[0]/2)

    # Get the location of the observation
    try:
        lat = header["LAT-OBS"] * u.deg
        lon = header["LONG-OBS"] * u.deg
        elevation = header["ELEV-OBS"] * u.m
        location = EarthLocation(lat=lat, lon=lon, height=elevation)

        # Create the Alt/Az frame
        obstime = parse_date(header["DATE-OBS"])
        frame = AltAz(obstime=obstime, location=location)

        # Perform the transform
        altaz = radec.transform_to(frame)

        return {"alt": altaz.alt.to_value("deg"), "az": altaz.az.to_value("deg")}
    except Exception:
        # if something goes wrong just skip
        return {}


@metric_evaluator.add_function
def detect_star_trails(filename, data, **kwargs):
    """ Measure the autocorrelation signal of an image to determine if any star trailing
    or "double star" effects are present (due to bad tracking or mid exposure shaking of array).

    Args:
        data (array): Image data as stored as an array.

    Returns:
        (dict): Dictionary containing the calculated metric values.
    """
    # TODO? create config entry for hardcoded values
    config = get_config()['raw_metric_parameters']['detect_star_trails']
    # take of log of image so faint sources are easier to detect
    cutoutimg = np.log10(image_cutout(data, **config['cutout']).astype(float))

    # subtract off background
    mean, median, std = stats.sigma_clipped_stats(cutoutimg, sigma=config['sigma_clip'])
    cutoutimg = cutoutimg - median

    # create a binary mask of pixels above a threshold level
    mask = np.zeros(cutoutimg.shape)
    mask[np.where(cutoutimg > std)] = 1

    # thin out the mask with a binary erosion (get rid of hotpixels/cosmic rays)
    be_size = config['binary_erosion_size']
    mask = 1 * ndimage.binary_erosion(mask, structure=np.ones((be_size, be_size)))

    # calculate autocorrelation of the mask
    ac = signal.correlate2d(mask, mask, boundary='symm', mode='same')

    # extract metrics from autocorr image
    total_mean = np.nanmean(ac)
    # measure the sum in the central region of the autocorr image
    autocorr_box_size = config['autocorr_signal_box_size']
    length = config['cutout']['length']
    start = int(length / 2 - autocorr_box_size)
    stop = int(length / 2 + autocorr_box_size)
    centre_mean = np.nanmean(ac[start:stop, start:stop])
    return {'autocorr': {'total_mean': total_mean, 'centre_mean': centre_mean}}


@metric_evaluator.add_function
def cosmic_ray_density(filename, data, **kwargs):
    """ Measure the cosmic ray count/density within a subset of an image.

    https://astroscrappy.readthedocs.io/en/latest/api/astroscrappy.detect_cosmics.html#astroscrappy.detect_cosmics

    Args:
        data (array): Image data as stored as an array.

    Returns:
        (dict): Dictionary containing the calculated metric values.
    """
    config = get_config()
    cam_config = config['cameras']['presets']['zwo']
    cr_config = config['raw_metric_parameters']['cosmic_ray_density']
    cutoutimg = image_cutout(data, **cr_config['cutout'])
    mask, cleaned_img = detect_cosmics(cutoutimg,
                                       gain=cam_config['gain'],
                                       readnoise=cam_config['read_noise'],
                                       satlevel=cam_config['saturation'],
                                       **cr_config['detect_cosmics']
                                       )
    cr_count = np.sum(1 * mask)
    cr_density = cr_count / (mask.shape[0] * mask.shape[1])
    return {'cosmic_ray_density': {'cr_count': cr_count, 'cr_density': cr_density}}
