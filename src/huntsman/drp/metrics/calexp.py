""" Some parts of the code are adapted from the LSST stack club:
https://nbviewer.jupyter.org/github/LSSTScienceCollaborations/StackClub/blob/rendered/Validation/image_quality_demo.nbconvert.ipynb
"""
import numpy as np
from astropy import units as u

from lsst.afw.geom.ellipses import Quadrupole, SeparableDistortionTraceRadius

from huntsman.drp.utils.library import load_module


METRICS = ("zeropoint", "psf")


def calculate_metrics(task_result, metrics=METRICS):
    """ Evaluate metrics for a single calexp.
    Args:
        calexp: The LSST calexp object.
        metrics (list of str): list of metrics to calculate.
    Returns:
        dict: A dictionary of metric name: value.
    """
    result = {"charSucess": task_result["charSucess"],
              "isrSuccess": task_result["isrSuccess"],
              "calibSuccess": task_result["calibSuccess"],
              "psfSuccess": task_result["psfSuccess"]}

    for metric in METRICS:

        func = load_module(f"huntsman.drp.metrics.calexp.{metric}")
        metrics = func(task_result)

        for k, v in metrics.items():
            if k in metrics:
                raise KeyError(f"Key '{k}' already in metrics dict.")
            result[k] = v

    return result


def background(task_result):
    """ Calculate sky background statistics.
    Args:
        task_result
    Returns:
        dict:
    """
    bg = task_result["background"].getImage().getArray()
    return {"bg_median": np.median(bg), "bg_std": bg.std()}


def zeropoint(task_result):
    """ Get the magnitude zero point of the raw data.
    Args:
        calexp (lsst.afw.image.exposure): The calexp object.
    Returns:
        dict: Dict containing the zeropoint in mags.
    """
    calexp = task_result["exposure"]
    fluxzero = calexp.getPhotoCalib().getInstFluxAtZeroMagnitude()

    # Note the missing minus sign here...
    return {"zp_mag": 2.5 * np.log10(fluxzero) * u.mag}


def psf(task_result):
    """ Calculate PSF metrics.
    This formula (based on a code shared in the stack club) assumes a Gaussian PSF, so the returned
    FWHM is an approximation that can be used to monitor data quality.
    Args:
        calexp (lsst.afw.image.exposure): The calexp object.
    Returns:
        dict: Dict containing the PSF FWHM in arcsec and ellipticity.
    """
    calexp = task_result["exposure"]

    psf = calexp.getPsf()
    shape = psf.computeShape()  # At the average position of the stars used to measure it

    # FWHM (assumes Gaussian PSF)
    pixel_scale = calexp.getWcs().getPixelScale().asArcseconds()
    fwhm = 2 * np.sqrt(2. * np.log(2)) * shape.getTraceRadius() * pixel_scale

    # Ellipticity
    i_xx, i_yy, i_xy = shape.getIxx(), shape.getIyy(), shape.getIxy()
    q = Quadrupole(i_xx, i_yy, i_xy)
    s = SeparableDistortionTraceRadius(q)
    e1, e2 = s.getE1(), s.getE2()
    ell = np.sqrt(e1 ** 2 + e2 ** 2)

    return {"psf_fwhm_arcsec": fwhm * u.arcsecond, "psf_ell": ell}
