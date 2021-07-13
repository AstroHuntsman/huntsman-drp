import pytest
import time
from copy import deepcopy

from huntsman.drp.core import get_config
from huntsman.drp.collection import ExposureCollection
from huntsman.drp import refcat as rc
from huntsman.drp.utils import testing

from huntsman.drp.utils.ingest import ingest_exposure

# ===========================================================================
# Config


@pytest.fixture(scope="session")
def session_config():
    """ Session scope config dict to be used for creating shared fixtures """

    config = get_config(ignore_local=True, testing=True)

    # Hack around so files pass screening and quality cuts
    # TODO: Move to testing config
    for k in ("bias", "dark", "flat", "science"):
        if k in config["quality"]["raw"]:
            del config["quality"]["raw"][k]

    return config


@pytest.fixture(scope="function")
def config(session_config):
    """ Function scope version of config_module that should be used in tests """
    return deepcopy(session_config)

# ===========================================================================
# Reference catalogue


@pytest.fixture(scope="session")
def refcat_filename(session_config):
    return testing.get_refcat_filename(config=session_config)


@pytest.fixture(scope="session")
def testing_refcat_server(session_config, refcat_filename):
    """ A testing refcat server that loads the refcat from file rather than downloading it.
    """
    refcat_kwargs = dict(refcat_filename=refcat_filename)

    # Yield the refcat server process
    refcat_service = rc.create_refcat_service(refcat_type=rc.TestingTapReferenceCatalogue,
                                              refcat_kwargs=refcat_kwargs,
                                              config=session_config)
    refcat_service.start()
    time.sleep(5)  # Allow some startup time
    yield refcat_service

    # Shutdown the refcat server after we are done
    refcat_service.stop()


# ===========================================================================
# Testing data


@pytest.fixture(scope="function")
def exposure_collection(tmp_path_factory, config):
    """
    Create a temporary directory populated with fake FITS images, then parse the images into the
    raw data table.
    """
    # Generate the fake data
    tempdir = tmp_path_factory.mktemp("test_exposure_sequence")
    expseq = testing.FakeExposureSequence(config=config)
    expseq.generate_fake_data(directory=tempdir)

    # Prepare the database
    exposure_collection = ExposureCollection(config=config, collection_name="fake_data")
    exposure_collection.delete_all(really=True)

    # Ingest the data into the collection
    for filename, header in expseq.header_dict.items():
        ingest_exposure(filename=filename, collection=exposure_collection)

    # Make sure table has the correct number of rows
    assert exposure_collection.count_documents() == expseq.file_count
    yield exposure_collection

    # Remove the metadata from the DB ready for other tests
    exposure_collection.delete_all(really=True)


@pytest.fixture(scope="session")
def exposure_collection_real_data(session_config):
    """
    Create a temporary directory populated with fake FITS images, then parse the images into the
    raw data table.
    """
    # Populate the database
    exposure_collection = testing.create_test_exposure_collection(session_config, clear=True)

    yield exposure_collection

    # Remove the metadata from the DB ready for other tests
    exposure_collection.logger.info("Deleting all documents after test.")
    exposure_collection.delete_all(really=True)
    assert not exposure_collection.find()


@pytest.fixture(scope="session")
def master_calib_collection_real_data(session_config):
    """ Make a master calib table by reducing real calib data.
    TODO: Store created files so they can be copied in for quicker tests.
    """
    calib_collection = testing.create_test_calib_collection(config=session_config)

    yield calib_collection

    # Remove the metadata from the DB ready for other tests
    calib_collection.delete_all(really=True)
    assert not calib_collection.find()


@pytest.fixture(scope="function")
def tempdir_and_exposure_collection_with_uningested_files(tmp_path_factory, config,
                                                          exposure_collection):
    """
    Create a temporary directory populated with fake FITS images, then parse the images into the
    raw data table.
    """
    # Clear the exposure collection of any existing documents
    exposure_collection.delete_all(really=True)

    # Generate the fake data
    tempdir = tmp_path_factory.mktemp("dir_with_uningested_files")
    expseq = testing.FakeExposureSequence(config=config)
    expseq.generate_fake_data(directory=tempdir)

    # Populate the database
    n_stop = len(expseq.header_dict) * 0.7 // 1  # ingest ~70% of the files
    n = 0
    for filename, header in expseq.header_dict.items():
        if n >= n_stop:
            break
        ingest_exposure(filename=filename, collection=exposure_collection, config=config)
        n += 1

    # Make sure table has the correct number of rows
    assert exposure_collection.count_documents() == n_stop
    yield (tempdir, exposure_collection)

    # Remove the metadata from the DB ready for other tests
    exposure_collection.delete_all(really=True)
