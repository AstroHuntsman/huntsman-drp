import os
import pytest
import numpy as np


@pytest.fixture(scope="module")
def sequence_date(config):
    return config["exposure_sequence"]["start_date"]


@pytest.fixture(scope="module")
def test_vignetting_data(config, raw_data_table, sequence_date, tmp_path_factory):
    """
    Make some fake non-vignetted / vignetted data.
    """
    field_name = config["vignetting"]["field_name"]

    xx, yy = np.meshgrid(np.linspace(0, 1, 3), np.linspace(0, 1, 3))
    is_vignetted = np.zeros_like(xx, dtype="bool")
    is_vignetted[1, 1] = True
    is_vignetted[1, 2] = True

    # Make a tempdir to store files
    tempdir = tmp_path_factory.mktemp("test_exposure_sequence")

    i = 0
    for ra in xx:
        for dec in yy:
            # Create filename
            filename = os.path.join(str(tempdir), f"vigdata_{i}.fits")
            # Write data to file

            # Add MD to datatable
            md = {"DATE-OBS": sequence_date, "RA-MNT": ra, "DEC-MNT": dec, "filename": filename,
                  "FIELD": field_name}
            raw_data_table.insert_one(md)
            i += 1
