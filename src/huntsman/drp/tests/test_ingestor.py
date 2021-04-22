import os
import time
import pytest

from huntsman.drp.ingestor import FileIngestor
from huntsman.drp.utils.ingest import METRIC_SUCCESS_FLAG, screen_success


@pytest.fixture(scope="function")
def ingestor(tempdir_and_exposure_table_with_uningested_files, config):
    """
    """
    tempdir, exposure_table = tempdir_and_exposure_table_with_uningested_files

    ingestor = FileIngestor(exposure_collection=exposure_table, sleep_interval=10,
                            status_interval=5, directory=tempdir, config=config)

    # Skip astrometry tasks as tests running in drp-lsst container
    ingestor._raw_metrics = [_ for _ in ingestor._raw_metrics if _ != "get_wcs"]

    yield ingestor

    ingestor.stop()


def test_file_ingestor(ingestor, tempdir_and_exposure_table_with_uningested_files, config):
    """This test runs on a directory where ~70% of the images have already been
    ingested into the datatable. The files already in the datatable should be
    identified as requiring screening. The uningested files should be ingested
    and then should be picked up for screening as well"""

    tempdir, exposure_table = tempdir_and_exposure_table_with_uningested_files

    n_to_process = len(os.listdir(tempdir))
    assert n_to_process > 0

    ingestor.start()
    i = 0
    timeout = 10

    while (i < timeout):
        if ingestor.is_running and ingestor.status["processed"] == n_to_process:
            break
        i += 1
        time.sleep(1)

    if i == timeout:
        raise RuntimeError(f"Timeout while waiting for processing of {n_to_process} images.")

    if not ingestor.is_running:
        raise RuntimeError("Ingestor has stopped running.")

    ingestor.stop(blocking=True)
    assert not ingestor.is_running

    assert ingestor._n_failed == 0

    for md in exposure_table.find():
        print(md)
        assert METRIC_SUCCESS_FLAG in md
        assert "quality" in md
        assert screen_success(md)

    for metric_value in md["quality"].values():
        assert metric_value is not None

    ingestor.stop(blocking=True)
    assert not ingestor.is_running
