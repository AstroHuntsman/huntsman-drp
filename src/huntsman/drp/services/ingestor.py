from functools import partial

from huntsman.drp.services.base import ProcessQueue
from huntsman.drp.metrics.raw import metric_evaluator
from huntsman.drp.utils.fits import read_fits_header, parse_fits_header, read_fits_data
from huntsman.drp.utils.ingest import METRIC_SUCCESS_FLAG, list_fits_files_recursive


def ingest_file(filename, collection, **kwargs):
    """ Process a single file.
    This function has to be defined outside of the FileIngestor class since we are using
    multiprocessing and class instance methods cannot be pickled.
    Args:
        filename (str): The name of the file to process.
        collection (ExposureCollection): The raw exposure collection.
    Returns:
        bool: True if file was successfully processed, else False.
    """
    config = collection.config
    logger = collection.logger
    logger.debug(f"Processing file: {filename}.")

    try:
        data = read_fits_data(filename)
        original_header = read_fits_header(filename)
    except Exception as err:
        logger.warning(f"Problem reading FITS file: {err!r}")
        metrics = {}
        success = False
    else:
        # Ignore certain metrics if required
        metrics_ignore = config.get("raw_metrics_ignore", ())
        for metric_name in metrics_ignore:
            metric_evaluator.remove_function(metric_name)

        # Get the metrics
        metrics, success = metric_evaluator.evaluate(filename, header=original_header, data=data,
                                                     **kwargs)
    metrics[METRIC_SUCCESS_FLAG] = success

    # Read the header
    # NOTE: The header is currently modified if WCS is measured
    header = read_fits_header(filename)

    # Parse the FITS header
    # NOTE: Parsed info goes in the top level of the mongo document
    parsed_header = parse_fits_header(header)

    to_update = {"filename": filename}
    to_update.update(parsed_header)
    to_update["header"] = dict(header)
    to_update["metrics"] = metrics

    # Use filename query as metrics etc can change
    collection.update_one({"filename": filename}, to_update=to_update, upsert=True)

    # Raise an exception if not success
    if not success:
        raise RuntimeError(f"Metric evaluation unsuccessful for {filename}.")


class FileIngestor(ProcessQueue):
    """ Class to watch for new file entries in database and process their metadata. """

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

    def _async_process_objects(self, *args, **kwargs):
        """ Continually process objects in the queue. """

        func = partial(ingest_file)

        return super()._async_process_objects(process_func=func)

    def _get_objs(self):
        """ Get list of files to process. """
        # Get set of all files in watched directory
        files_in_directory = set(list_fits_files_recursive(self._directory))
        self.logger.debug(f"Found {len(files_in_directory)} FITS files in {self._directory}.")

        # Get set of all files that are ingested and pass screening
        files_ingested = set(self.collection.find(screen=True, key="filename"))

        # Identify files that require processing
        files_to_process = files_in_directory - files_ingested
        self.logger.debug(f"Found {len(files_to_process)} files requiring processing.")

        return files_to_process
