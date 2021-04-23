from huntsman.drp.services.base import ProcessQueue
from huntsman.drp.utils.library import load_module
from huntsman.drp.collection import RawExposureCollection, MasterCalibCollection
from huntsman.drp.lsst.butler import TemporaryButlerRepository
from huntsman.drp.metrics.calexp import METRICS


def get_quality_metrics(calexp):
    """ Evaluate metrics for a single calexp. This could probably be improved in future.
    TODO: Implement version control here.
    """
    result = {}
    for metric in METRICS:
        func = load_module(f"huntsman.drp.metrics.calexp.{metric}")
        result[metric] = func(calexp)
    return result


def _init_pool(function, config, logger, exp_coll_name, calib_coll_name):
    function.exposure_table = RawExposureCollection(collection_name=exp_coll_name, config=config,
                                                    logger=logger)
    function.calib_table = MasterCalibCollection(collection_name=calib_coll_name, config=config,
                                                 logger=logger)


def _process_file(document, refcat_filename):
    """ Create a calibrated exposure (calexp) for the given data ID and store the metadata.
    Args:
        document (RawExposureDocument): The document to process.
    """
    calib_table = _process_file.calib_table
    exposure_table = _process_file.exposure_table
    config = exposure_table.config
    logger = calib_table.logger

    # Get matching master calibs
    calib_docs = calib_table.get_matching_calibs(document)

    with TemporaryButlerRepository(logger=logger, config=config) as br:

        # Ingest raw science exposure into the bulter repository
        br.ingest_raw_data([document["filename"]])

        # Ingest the corresponding master calibs
        for calib_type, calib_doc in calib_docs.items():
            calib_filename = calib_doc["filename"]
            br.ingest_master_calibs(datasetType=calib_type, filenames=[calib_filename])

        # Make and ingest the reference catalogue
        if refcat_filename is None:
            br.make_reference_catalogue()
        else:
            br.ingest_reference_catalogue([refcat_filename])

        # Make the calexps, also getting the dataIds to match with their raw frames
        br.make_calexps()
        required_keys = br.get_keys("raw")
        calexps, dataIds = br.get_calexps(extra_keys=required_keys)

        # Evaluate metrics and insert into the database
        logger.debug(f"Calculating metrics for {document}")

        for calexp, calexp_id in zip(calexps, dataIds):

            metrics = get_quality_metrics(calexp)

            # Make the document and update the DB
            document = {k: calexp_id[k] for k in required_keys}
            to_update = {"quality": {"calexp": metrics}}
            exposure_table.update_one(document, to_update=to_update)


class CalexpQualityMonitor(ProcessQueue):
    """ Class to continually evauate and archive calexp quality metrics for raw exposures that
    have not already been processed. Intended to run as a docker service.
    """

    def __init__(self, nproc=None, refcat_filename=None, *args, **kwargs):
        """
        Args:
            refcat_filename (str, optional): The reference catalogue filename. If not provided,
                will create a new refcat.
            nproc (int): The number of processes to use. If None (default), will check the config
                item `calexp-monitor.nproc` with a default value of 1.
        """
        super().__init__(*args, **kwargs)

        self._refcat_filename = refcat_filename

        # Set the number of processes
        calexp_monitor_config = self.config.get("calexp-monitor", {})
        if nproc is None:
            nproc = calexp_monitor_config.get("nproc", 1)
        self._nproc = int(nproc)
        self.logger.debug(f"Calexp monitor using {nproc} processes.")

        self._pool_init_args = (_process_file, self.config, self.logger,
                                self._exposure_collection.collection_name,
                                self._calib_collection.collection_name)
        self._process_func_kwargs = dict(refcat_filename=self._refcat_filename)

    def _get_objs(self):
        """ Update the set of data IDs that require processing. """
        docs = self._exposure_table.find({"dataType": "science"}, screen=True, quality_filter=True)
        return [d for d in docs if self._requires_processing(d)]

    def _requires_processing(self, file_info):
        """ Check if a file requires processing.
        Args:
            file_info (dict): The file document from the exposure data table.
        Returns:
            bool: True if processing required, else False.
        """
        try:
            return "calexp" not in file_info["quality"]
        except KeyError:
            return True
