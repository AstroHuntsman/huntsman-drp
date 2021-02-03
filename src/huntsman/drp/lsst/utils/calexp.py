import numpy as np
from astropy import units as u


def get_magzero(calexp):
    """
    """


def get_psf_fwhm(calexp):
    """ Calculate the PSF FWHM.
    This formula (based on a code shared in the stack club) assumes a Gaussian PSF, so the returned
    FWHM is an approximation that can be used to monitor data quality.
    Args:
        calexp (calexp)
    """
    psf = calexp.getPsf()
    pixel_scale = calexp.getWcs().getPixelScale().asArcseconds()
    fwhm = 2 * np.sqrt(2. * np.log(2)) * psf.computeShape().getTraceRadius() * pixel_scale
    return fwhm * u.arcsec
