import pytest
from astropy.wcs import WCS

from huntsman.drp.utils.fits import read_fits_header, read_fits_data
from huntsman.drp.metrics import raw


@pytest.fixture(scope="function")
def filename_with_wcs(exposure_collection_real_data):
    filename = exposure_collection_real_data.find({"observation_type": "science"})[0]["filename"]
    wcs = WCS(read_fits_header(filename))
    assert wcs.has_celestial
    return filename


@pytest.fixture(scope="function")
def header_with_wcs(filename_with_wcs):
    return read_fits_header(filename_with_wcs)


@pytest.fixture(scope="function")
def data(filename_with_wcs):
    return read_fits_data(filename_with_wcs)


def test_get_wcs_no_solve(filename_with_wcs, header_with_wcs):
    result = raw.get_wcs(filename_with_wcs, header_with_wcs)
    assert "ra_centre" in result
    assert "dec_centre" in result
    assert result["has_wcs"]


def test_cosmic_ray_density(data):
    result = raw.cosmic_ray_density(data)
    assert "cosmic_ray_density" in result
    assert "cr_count" in result['cosmic_ray_density']
    assert "cr_density" in result['cosmic_ray_density']
    assert result['cosmic_ray_density']['cr_count'] >= 0
    assert result['cosmic_ray_density']['cr_density'] >= 0
