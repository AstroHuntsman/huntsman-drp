from copy import deepcopy

from huntsman.drp.core import get_logger
from huntsman.drp.services.base import ProcessQueue
from huntsman.drp.collection import RawExposureCollection
from huntsman.drp.fitsutil import FitsHeaderTranslator, read_fits_header, read_fits_data
from huntsman.drp.utils.library import load_module
from huntsman.drp.metrics.raw import RAW_METRICS
from huntsman.drp.utils.ingest import METRIC_SUCCESS_FLAG, list_fits_files_recursive


def _pool_init(function, collection_name, config):
    """ Initialise the process pool.
    This allows a single mongodb connection per process rather than per file, which is inefficient.
    Args:
        function (Function): The function to parallelise.
        collection_name (str): The collection name for the raw exposures.
        config (dict): The config dictionary.
    """
    logger = get_logger()
    function.exposure_collection = RawExposureCollection(collection_name=collection_name,
                                                         config=config, logger=logger)
    function.fits_header_translator = FitsHeaderTranslator(config=config, logger=logger)


def _process_file(filename, metric_names):
    """ Process a single file.
    This function has to be defined outside of the FileIngestor class since we are using
    multiprocessing and class instance methods cannot be pickled.
    Args:
        filename (str): The name of the file to process.
        metric_names (list of str): The list of the metrics to process.
    Returns:
        bool: True if file was successfully processed, else False.
    """
    exposure_collection = _process_file.exposure_collection
    fits_header_translator = _process_file.fits_header_translator
    logger = exposure_collection.logger

    logger.debug(f"Processing {filename}.")

    # Read the header
    try:
        parsed_header = fits_header_translator.read_and_parse(filename)
    except Exception as err:
        logger.error(f"Exception while parsing FITS header for {filename}: {err}.")
        success = False
    else:
        # Get the metrics
        metrics, success = _get_raw_metrics(filename, metric_names=metric_names, logger=logger)
        to_update = {METRIC_SUCCESS_FLAG: success, "quality": metrics}

        # Update the document (upserting if necessary)
        to_update.update(parsed_header)
        to_update["filename"] = filename
        exposure_collection.update_one(parsed_header, to_update=to_update, upsert=True)

    return filename, success


def _get_raw_metrics(filename, metric_names, logger):
    """ Evaluate metrics for a raw/unprocessed file.
    Args:
        filename (str): The filename of the FITS image to be processed.
        metric_names (list of str): The list of the metrics to process.
    Returns:
        dict: Dictionary containing the metric names / values.
    """
    result = {}
    success = True

    # Read the FITS file
    try:
        header = read_fits_header(filename)
        data = read_fits_data(filename)  # Returns float array
    except Exception as err:
        logger.error(f"Unable to read {filename}: {err!r}")

    for metric in metric_names:
        func = load_module(f"huntsman.drp.metrics.raw.{metric}")
        try:
            result.update(func(filename, data=data, header=header))
        except Exception as err:
            logger.error(f"Exception while calculating {metric} for {filename}: {err!r}")
            success = False

    return result, success


class FileIngestor(ProcessQueue):
    """ Class to watch for new file entries in database and process their metadata. """

    # Work around so that tests can run without running the has_wcs metric
    _raw_metrics = deepcopy(RAW_METRICS)

    def __init__(self, directory=None, nproc=None, *args, **kwargs):
        """
        Args:
            directory (str): The top level directory to watch for new files, so they can
                be added to the relevant datatable.
            nproc (int): The number of processes to use. If None (default), will check the config
                item `screener.nproc` with a default value of 1.
            *args, **kwargs: Parsed to ProcessQueue initialiser.
        """
        super().__init__(*args, **kwargs)

        ingestor_config = self.config.get("ingestor", {})

        # Set the number of processes
        if nproc is None:
            nproc = ingestor_config.get("nproc", 1)
        self._nproc = int(nproc)

        # Set the monitored directory
        if directory is None:
            directory = ingestor_config["directory"]
        self._directory = directory
        self.logger.debug(f"Ingesting files in directory: {self._directory}")

        self._process_func_kwargs = dict(metric_names=self._raw_metrics)
        self._pool_init_args = (_process_file, self._collection_name, self.config)

    def _get_objs(self):
        """ Get list of files to process. """
        # Get set of all files in watched directory
        files_in_directory = set(list_fits_files_recursive(self._directory))
        self.logger.debug(f"Found {len(files_in_directory)} FITS files in {self._directory}.")

        # Get set of all files that are ingested and pass screening
        files_ingested = set(self._exposure_collection.find(screen=True, key="filename"))

        # Identify files that require processing
        files_to_process = files_in_directory - files_ingested
        self.logger.debug(f"Found {len(files_to_process)} files requiring processing.")

        return files_to_process
