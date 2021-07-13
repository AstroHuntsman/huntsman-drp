from huntsman.drp.utils.date import current_date
from huntsman.drp.lsst.butler import TemporaryButlerRepository


def test_ingest(exposure_collection, config):
    """ Test ingest of raw files into a ButlerRepository. """

    config = config["exposure_sequence"]  # TODO: Rename
    n_filters = len(config["filters"])

    filenames = exposure_collection.find(key="filename")

    with TemporaryButlerRepository(config=config) as br:

        # Count the number of ingested files
        data_ids = br.get_dataIds("raw")
        assert len(data_ids) == 0

        br.ingest_raw_files(filenames)
        data_ids = br.get_dataIds("raw")
        assert len(data_ids) == len(filenames)

        # Check we have the right number of each datatype
        n_flat = config["n_cameras"] * config["n_days"] * config["n_flat"] * n_filters
        data_ids = br.get_dataIds("raw", where="exposure.observation_type='flat'")
        assert len(data_ids) == n_flat

        n_sci = config["n_cameras"] * config["n_days"] * config["n_science"] * n_filters
        data_ids = br.get_dataIds("raw", where="exposure.observation_type='science'")
        assert len(data_ids) == n_sci

        n_bias = config["n_cameras"] * config["n_days"] * config["n_bias"]
        data_ids = br.get_dataIds("raw", where="exposure.observation_type='bias'")
        assert len(data_ids) == n_bias

        n_dark = config["n_cameras"] * config["n_days"] * config["n_dark"] * 2  # 2 exp times
        data_ids = br.get_dataIds("raw", where="exposure.observation_type='dark'")
        assert len(data_ids) == n_dark


def test_make_master_calibs(exposure_collection, config):
    """ Make sure the correct number of master bias frames are produced. """

    # Get documents for a single night and a single camera
    doc = exposure_collection.find()[0]
    doc_filter = {k: doc.get(k) for k in ["header.CAM-ID", "observing_day"]}
    docs = exposure_collection.find(doc_filter)

    # Get corresponding calib documents
    calib_docs = exposure_collection.get_calib_docs(current_date(), documents=docs,
                                                    validity=9999)

    for doc in docs:
        exposure_collection.logger.info(f"{doc['observing_day']} {doc['observation_type']}")

    with TemporaryButlerRepository(config=config) as br:
        br.ingest_raw_files([d["filename"] for d in docs])

        for datasetType in ("bias", "dark", "flat"):

            n_expected = len([d for d in calib_docs if d["datasetType"] == datasetType])
            assert n_expected > 0

            # Make the calibs
            br.construct_calibs(datasetType)

            # Check the right number of calibs were produced
            assert len(br.get_dataIds(datasetType)) == n_expected
