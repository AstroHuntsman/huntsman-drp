from huntsman.drp.utils.date import current_date
from huntsman.drp.lsst.butler import TemporaryButlerRepository


def test_ingest(exposure_collection, config):
    """ Test ingest of raw files into a ButlerRepository and ensure they have the right type. """

    econfig = config["exposure_sequence"]  # TODO: Rename
    n_filters = len(econfig["filters"])

    filenames = exposure_collection.find(key="filename")

    with TemporaryButlerRepository(config=config) as br:

        # Count the number of ingested files
        data_ids = br.get_dataIds("raw")
        assert len(data_ids) == 0

        br.ingest_raw_files(filenames)
        data_ids = br.get_dataIds("raw")
        assert len(data_ids) == len(filenames)

        # Check we have the right number of each datatype
        n_flat = econfig["n_cameras"] * econfig["n_days"] * econfig["n_flat"] * n_filters
        data_ids = br.get_dataIds("raw", where="exposure.observation_type='flat'")
        assert len(data_ids) == n_flat

        n_sci = econfig["n_cameras"] * econfig["n_days"] * econfig["n_science"] * n_filters
        data_ids = br.get_dataIds("raw", where="exposure.observation_type='science'")
        assert len(data_ids) == n_sci

        n_bias = econfig["n_cameras"] * econfig["n_days"] * econfig["n_bias"]
        data_ids = br.get_dataIds("raw", where="exposure.observation_type='bias'")
        assert len(data_ids) == n_bias

        n_dark = econfig["n_cameras"] * econfig["n_days"] * econfig["n_dark"] * 2  # 2 exp times
        data_ids = br.get_dataIds("raw", where="exposure.observation_type='dark'")
        assert len(data_ids) == n_dark


def test_make_master_calibs(exposure_collection, config):
    """ Check we can create master calibs and in the correct number. """

    # Get documents for a single night and a single camera
    doc = exposure_collection.find()[0]
    doc_filter = {k: doc.get(k) for k in ["header.CAM-ID", "observing_day"]}
    doc_filter["observation_type"] = {"$nin": ["science"]}
    docs = exposure_collection.find(doc_filter)

    # Get corresponding calib documents
    calib_date = current_date()
    calib_docs = set([exposure_collection.raw_doc_to_calib_doc(d, calib_date) for d in docs])

    with TemporaryButlerRepository(config=config) as br:

        br.ingest_raw_files([d["filename"] for d in docs])

        for datasetType in config["calibs"]["types"]:

            # Defects are made from raw darks
            datasetType2 = "dark" if datasetType == "defects" else datasetType
            n_expected = len([d for d in calib_docs if d["datasetType"] == datasetType2])
            assert n_expected > 0

            # Make the calibs
            br.construct_calibs(datasetType)

            # Check the right number of calibs were produced
            assert len(br.get_dataIds(datasetType)) == n_expected
