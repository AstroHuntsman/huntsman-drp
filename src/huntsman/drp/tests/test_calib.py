import os
import time
import pytest

from panoptes.utils.time import CountdownTimer
from panoptes.utils import error

from huntsman.drp.utils.testing import FakeExposureSequence
from huntsman.drp.collection import ExposureCollection, CalibCollection
from huntsman.drp.services import CalibService


@pytest.fixture(scope="function")
def config(config):
    """ A config containing a smaller exposure sequence. """

    config["exposure_sequence"]["n_days"] = 1
    config["exposure_sequence"]["n_cameras"] = 1
    config["exposure_sequence"]["n_dark"] = 1
    config["exposure_sequence"]["n_bias"] = 1
    config["exposure_sequence"]["filters"] = ["g_band"]

    config["collections"]["ExposureCollection"]["name"] = "fake_data_lite"
    config["collections"]["CalibCollection"]["name"] = "calib_test"

    return config


@pytest.fixture(scope="function")
def empty_calib_collection(config):
    """ An empty master calib collection. """
    col = CalibCollection.from_config(config)
    yield col

    col.delete_all(really=True)


@pytest.fixture(scope="function")
def exposure_collection_lite(tmp_path_factory, config):
    """
    Create a temporary directory populated with fake FITS images, then parse the images into the
    raw data table.
    """
    # Generate the fake data
    tempdir = tmp_path_factory.mktemp("test_exposure_sequence")
    expseq = FakeExposureSequence(config=config)
    expseq.generate_fake_data(directory=tempdir)

    # Populate the database
    exposure_collection = ExposureCollection(config=config)
    for filename, header in expseq.header_dict.items():
        exposure_collection.ingest_file(filename)

    # Make sure table has the correct number of rows
    assert exposure_collection.count_documents() == expseq.file_count
    yield exposure_collection

    # Remove the metadata from the DB ready for other tests
    all_metadata = exposure_collection.find()
    exposure_collection.delete_many(all_metadata)


@pytest.fixture(scope="function")
def calib_service(config, exposure_collection_lite, empty_calib_collection):
    calib_service = CalibService.from_config(config)
    yield calib_service
    calib_service.stop()


def test_master_calib_service(calib_service, config):

    n_calib_dates = config["exposure_sequence"]["n_days"]
    n_cameras = config["exposure_sequence"]["n_cameras"]
    n_filters = len(config["exposure_sequence"]["filters"])

    n_flats = n_calib_dates * n_filters * n_cameras
    n_bias = n_calib_dates * n_cameras
    n_dark = n_calib_dates * n_cameras
    n_defect = n_dark

    calib_collection = calib_service.calib_collection
    assert not calib_collection.find()  # Check calib table is empty

    assert not calib_service.is_running
    calib_service.start()
    assert calib_service.is_running

    timer = CountdownTimer(duration=180)
    while not timer.expired():
        calib_service.logger.debug("Waiting for calibs...")

        dataset_types = calib_collection.find(key="datasetType")

        # Check if we are finished
        if len([d for d in dataset_types if d == "flat"]) == n_flats:
            if len([d for d in dataset_types if d == "bias"]) == n_bias:
                if len([d for d in dataset_types if d == "dark"]) == n_dark:
                    if len([d for d in dataset_types if d == "defects"]) == n_defect:
                        break

        for filename in calib_collection.find(key="filename"):
            assert os.path.isfile(filename)

        if not calib_service.is_running:
            raise RuntimeError("Calib maker has stopped running. Check the logs for details.")

        time.sleep(10)

    if timer.expired():
        raise error.Timeout("Timeout while waiting for calibs.")

    calib_service.stop()
    assert not calib_service.is_running
