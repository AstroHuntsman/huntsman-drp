import pytest
import copy
from datetime import timedelta
import numpy as np

from huntsman.drp.collection import ExposureCollection
from huntsman.drp.utils.date import current_date, parse_date
from huntsman.drp.utils.fits import read_fits_header, parse_fits_header

from pymongo.errors import ServerSelectionTimeoutError, DuplicateKeyError


def test_mongodb_wrong_host_name(config):
    """ Test if an error is raised if the mongodb hostname is incorrect. """
    modified_config = copy.deepcopy(config)
    modified_config["mongodb"]["hostname"] = "nonExistantHostName"
    with pytest.raises(ServerSelectionTimeoutError):
        ExposureCollection(config=modified_config)


def test_datatable_query_by_date(exposure_collection, config):
    """ Test ability to query using datetime ranges. """

    # Get list of all dates in the database
    dates = [d["date"] for d in exposure_collection.find()]
    n_files = len(dates)

    dates_unique = np.unique(dates)  # Sorted array of unique dates
    date_max = dates_unique[-1]

    for date_min in dates_unique[:-1][:3]:

        # Get filenames between dates
        filenames = exposure_collection.find(key="filename", date_min=date_min,
                                             date_max=date_max)
        assert len(filenames) <= n_files  # This holds because we sorted the dates

        for filename in filenames:
            header = read_fits_header(filename)
            date = parse_fits_header(header)["date"]
            assert date >= parse_date(date_min)
            assert date < parse_date(date_max)

        n_files = len(filenames)


def test_query_latest(exposure_collection, config, tol=1):
    """Test query_latest finds the correct number of DB entries."""
    date_min = config["exposure_sequence"]["start_date"]
    n_days = config["exposure_sequence"]["n_days"]
    date_min = parse_date(date_min)
    date_now = current_date()

    if date_now <= date_min + timedelta(days=n_days):
        pytest.skip("Test does not work unless current date is later than all test exposures.")

    timediff = date_now - date_min
    # This should capture all the files
    qresult = exposure_collection.find_latest(days=timediff.days + tol)
    assert len(qresult) == len(exposure_collection.find())
    # This should capture none of the files
    qresult = exposure_collection.find_latest(days=0, hours=0, seconds=0)
    assert len(qresult) == 0


def test_update(exposure_collection):
    """Test that we can update a document specified by a filename."""
    data = exposure_collection.find()[0]
    # Get a filename to use as an identifier
    filename = data["filename"]
    # Get a key to update
    key = [_ for _ in data.keys() if _ not in ["filename"]][0]
    old_value = data[key]
    new_value = "ThisIsAnewValue"
    assert old_value != new_value  # Let's be sure...
    # Update the key with the new value
    update_dict = {key: new_value, "filename": filename}
    exposure_collection.update_one(data, update_dict)
    # Check the values match
    data_updated = exposure_collection.find()[0]
    assert data_updated[key] == new_value


def test_update_file_data_bad_filename(exposure_collection):
    """Test that we can update a document specified by a filename."""
    # Specify the bad filename
    filenames = exposure_collection.find(key="filename")
    filename = "ThisFileDoesNotExist"
    assert filename not in filenames
    update_dict = {"A Key": "A Value", "filename": filename}
    with pytest.raises(RuntimeError):
        exposure_collection.update_one(update_dict, update_dict, upsert=False)


def test_quality_filter(exposure_collection):
    """
    """
    document_filter = {"observation_type": "dark"}
    documents = exposure_collection.find(document_filter)
    n_docs = len(documents)

    for i, d in enumerate(documents):
        exposure_collection.update_one(d, {"TEST_METRIC_1": i})
    for i, d in enumerate(documents[::-1]):
        exposure_collection.update_one(d, {"TEST_METRIC_2": i})

    exposure_collection.config["quality"]["raw"]["dark"] = {"TEST_METRIC_1": {"$lt": 1}}
    matches = exposure_collection.find(document_filter, quality_filter=True)
    assert len(matches) == 1

    exposure_collection.config["quality"]["raw"]["dark"] = {"TEST_METRIC_1": {"$lt": 2}}
    matches = exposure_collection.find(document_filter, quality_filter=True)
    assert len(matches) == 2

    cond = {"TEST_METRIC_1": {"$lt": 1}, "TEST_METRIC_2": {"$gt": n_docs - 2}}
    exposure_collection.config["quality"]["raw"]["dark"] = cond
    matches = exposure_collection.find(document_filter, quality_filter=True)
    assert len(matches) == 1

    cond = {"TEST_METRIC_1": {"$lt": 1}, "TEST_METRIC_2": {"$lt": 1}}
    exposure_collection.config["quality"]["raw"]["dark"] = cond
    matches = exposure_collection.find(document_filter, quality_filter=True)
    assert len(matches) == 0


def test_insert_duplicate(exposure_collection):
    """ Check an exception is raised when inserting a duplicate document. """

    doc = exposure_collection.find()[0]

    with pytest.raises(DuplicateKeyError):
        exposure_collection.insert_one(doc)


def test_ingest_duplicate_fpack(exposure_collection):
    """ Check an exception is raised when inserting duplicate .fits/.fz document. """

    doc = exposure_collection.find()[0]

    doc1 = doc._document.copy()
    doc1["filename"] = "test_insert_duplicate.fits"

    doc2 = doc._document.copy()
    doc2["filename"] = "test_insert_duplicate.fits.fz"

    exposure_collection.insert_one(doc1)
    with pytest.raises(DuplicateKeyError):
        exposure_collection.insert_one(doc2)

    exposure_collection.delete_many(exposure_collection.find(), force=True)

    exposure_collection.insert_one(doc2)
    with pytest.raises(DuplicateKeyError):
        exposure_collection.insert_one(doc1)


def test_ingest_ref_image(exposure_collection_real_data, ref_calib_collection):
    """ Test that we can evaluate metrics that use a reference calib.
    """
    exposure_collection = exposure_collection_real_data
    exposure_collection.ref_calib_collection = ref_calib_collection

    doc = exposure_collection.find({"observation_type": "flat"})[0]
    exposure_collection.delete_one(doc)

    # Add the doc as a reference calib
    exposure_collection.ref_calib_collection.ingest_file(doc["filename"])
    calib_doc = exposure_collection.ref_calib_collection.get_matching_calib(doc)
    assert calib_doc is not None

    # Try re-ingesting the doc and make sure it matched with the reference calib
    exposure_collection.ingest_file(doc["filename"])
    new_doc = exposure_collection.find_one({"filename": doc["filename"]})

    assert "ref_chi2r_scaled" in new_doc["metrics"]
