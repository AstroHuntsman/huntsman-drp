""" Some parts of the code are adapted from the LSST stack club:
https://nbviewer.jupyter.org/github/LSSTScienceCollaborations/StackClub/blob/rendered/Validation/image_quality_demo.nbconvert.ipynb
"""
import numpy as np
from astropy import units as u

from lsst.afw.geom.ellipses import Quadrupole, SeparableDistortionTraceRadius

from huntsman.drp.metrics.evaluator import MetricEvaluator

metric_evaluator = MetricEvaluator()


@metric_evaluator.add_function
def background_metadata(calexp, src, calexpBackground, **kwargs):
    """ Calculate sky background statistics.
    Args:
        task_result (dict): The result of ProcessCcdTask.
    Returns:
        dict: The dictionary of results
    """
    result = {}
    bg = calexpBackground.getImage().getArray()

    result["bg_median"] = np.median(bg)
    result["bg_std"] = bg.std()

    return result


@metric_evaluator.add_function
def source_metadata(calexp, src, **kwargs):
    """ Metadata from source catalogue.
    Args:
        task_result (dict): The result of ProcessCcdTask.
    Returns:
        dict: The dictionary of results
    """
    result = {}

    # Count the number of sources
    result["n_src_char"] = len(src)

    return result


@metric_evaluator.add_function
def photocal_metadata(calexp, src, **kwargs):
    """ Get the magnitude zero point of the raw data.
    Args:
        calexp (lsst.afw.image.exposure): The calexp object.
    Returns:
        dict: Dict containing the zeropoint in mags.
    """
    # Find number of sources used for photocal
    n_sources = sum(src["calib_photometry_used"])

    # Get the photo calib metadata
    pc = calexp.getPhotoCalib()

    # Get the magnitude zero point
    zp_flux = pc.getInstFluxAtZeroMagnitude()
    zp_mag = 2.5 * np.log10(zp_flux) * u.mag  # Note the missing minus sign here...

    # Record calibration uncertainty
    # See: https://hsc.mtk.nao.ac.jp/pipedoc/pipedoc_7_e/tips_e/mag_zeropoint.html
    zp_flux_err = zp_flux * pc.getCalibrationErr() / pc.getCalibrationMean()

    return {"zp_mag": zp_mag, "zp_flux": zp_flux, "zp_flux_err": zp_flux_err,
            "zp_n_src": n_sources}


@metric_evaluator.add_function
def psf_metadata(calexp, src, **kwargs):
    """ Calculate PSF metrics.
    This formula (based on a code shared in the stack club) assumes a Gaussian PSF, so the returned
    FWHM is an approximation that can be used to monitor data quality.
    Args:
        calexp (lsst.afw.image.exposure): The calexp object.
    Returns:
        dict: Dict containing the PSF FWHM in arcsec and ellipticity.
    """
    # Find number of sources used to measure PSF
    n_sources = sum(src["calib_psf_used"])

    psf = calexp.getPsf()
    shape = psf.computeShape()  # At the average position of the stars used to measure it

    # PSF FWHM (assumes Gaussian PSF)
    pixel_scale = calexp.getWcs().getPixelScale().asArcseconds()
    fwhm = 2 * np.sqrt(2. * np.log(2)) * shape.getTraceRadius() * pixel_scale

    # PSF ellipticity
    i_xx, i_yy, i_xy = shape.getIxx(), shape.getIyy(), shape.getIxy()
    q = Quadrupole(i_xx, i_yy, i_xy)
    s = SeparableDistortionTraceRadius(q)
    e1, e2 = s.getE1(), s.getE2()
    ell = np.sqrt(e1 ** 2 + e2 ** 2)

    return {"psf_fwhm_arcsec": fwhm * u.arcsecond, "psf_ell": ell, "psf_n_src": n_sources,
            "psfSuccess": True}
