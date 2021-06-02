import time
import pytest

from huntsman.drp.services.health import HealthMonitor


@pytest.fixture(scope="function")
def health_monitor(config):

    hm = HealthMonitor(config=config)

    yield hm

    hm.stop()


def test_health_monitor(health_monitor, raw_exposure_collection):

    n_docs = len(raw_exposure_collection.find())

    # Insert a duplicate .fz file in to the DB and make sure the file actually exists
    doc = raw_exposure_collection.find()[0]
    assert doc.endswith(".fits")
    doc["filename"] = doc["filename"] + ".fz"
    raw_exposure_collection.insert_one(doc)

    # Insert a document with a filename that doesn't exist
    doc["filename"] = "thisfiledoesnotexist.fits"
    raw_exposure_collection.insert_one(doc)

    assert len(raw_exposure_collection.find()) == n_docs + 2

    # Start the monitor
    health_monitor.start()

    time.sleep(5)

    # Check the bad documents got deleted

    # Check they were the only documents to be deleted
    assert len(raw_exposure_collection.find()) == n_docs
