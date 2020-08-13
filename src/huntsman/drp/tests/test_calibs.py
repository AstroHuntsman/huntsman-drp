#import os
import pytest
from .conftest import make_fake_image
from ..calibs import get_calib_data_qualities


@pytest.fixture
def fits_filename_list(tmpdir):
    flatfilenames = make_fake_image(tmpdir, 'flat', num_images=1, background=10000)[1]
    darkfilenames = make_fake_image(tmpdir, 'dark', num_images=1, n_sources=0, background=5)[1]
    return(flatfilenames + darkfilenames)


def test_get_calib_data_qualities(fits_filename_list):
    data_quality_dict = get_calib_data_qualities(fits_filename_list)

    for k in data_quality_dict.keys():
        if k.replace('flat', '') != k:
            assert data_quality_dict[k][0] == pytest.approx(10000, 10)
        else:
            assert data_quality_dict[k][0] == pytest.approx(5, 2)
