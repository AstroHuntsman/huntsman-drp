from functools import partial
from multiprocessing.pool import ThreadPool

from huntsman.drp.services.base import ProcessQueue
from huntsman.drp.utils.library import load_module
from huntsman.drp.lsst.butler import TemporaryButlerRepository
from huntsman.drp.metrics.calexp import METRICS


def _get_quality_metrics(calexp):
    """ Evaluate metrics for a single calexp. This could probably be improved in future.
    TODO: Implement version control here.
    """
    result = {}
    for metric in METRICS:
        func = load_module(f"huntsman.drp.metrics.calexp.{metric}")
        result[metric] = func(calexp)
    return result


def _process_document(document, exposure_collection, calib_collection, refcat_filename, timeout,
                      **kwargs):
    """ Create a calibrated exposure (calexp) for the given data ID and store the metadata.
    Args:
        document (RawExposureDocument): The document to process.
    """
    config = exposure_collection.config
    logger = calib_collection.logger

    logger.info(f"Processing document: {document}")

    calib_docs = calib_collection.get_matching_calibs(document)

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
        calexps, dataIds = br.get_calexps(extra_keys=required_keys, timeout=timeout)

        # Evaluate metrics and insert into the database
        logger.debug(f"Calculating metrics for {document}")

        for calexp, calexp_id in zip(calexps, dataIds):

            metrics = _get_quality_metrics(calexp)

            # Make the document and update the DB
            document_filter = {k: calexp_id[k] for k in required_keys}
            to_update = {"quality": {"calexp": metrics}}
            exposure_collection.update_one(document_filter, to_update=to_update)


class CalexpQualityMonitor(ProcessQueue):
    """ Class to continually evauate and archive calexp quality metrics for raw exposures that
    have not already been processed. Intended to run as a docker service.
    """
    _pool_class = ThreadPool  # Use ThreadPool as LSST code makes its own subprocesses
    _timeout = 600  # TODO: Move to config

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

    def _async_process_objects(self, *args, **kwargs):
        """ Continually process objects in the queue. """

        func = partial(_process_document, refcat_filename=self._refcat_filename,
                       timeout=self._timeout)

        return super()._async_process_objects(process_func=func)

    def _get_objs(self):
        """ Update the set of data IDs that require processing. """
        docs = self._exposure_collection.find({"dataType": "science"}, screen=True,
                                              quality_filter=True)
        return [d for d in docs if self._requires_processing(d)]

    def _requires_processing(self, document):
        """ Check if a document requires processing.
        Args:
            document (Document): The document to check.
        Returns:
            bool: True if processing required, else False.
        """
        try:
            return "calexp" not in document["quality"]
        except KeyError:
            return True
