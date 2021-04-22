import time
import queue
import atexit
import multiprocessing
from multiprocessing.queues import JoinableQueue as Queue
from copy import deepcopy
from contextlib import suppress
from threading import Thread

from panoptes.utils.time import CountdownTimer

from huntsman.drp.core import get_logger
from huntsman.drp.base import HuntsmanBase
from huntsman.drp.collection import RawExposureCollection
from huntsman.drp.fitsutil import FitsHeaderTranslator, read_fits_header, read_fits_data
from huntsman.drp.utils.library import load_module
from huntsman.drp.metrics.raw import RAW_METRICS
from huntsman.drp.utils.ingest import METRIC_SUCCESS_FLAG, list_fits_files_recursive


class UniqueQueue(Queue):
    """ Small override for multiprocessing.Queue to only put objects if they are not already in
    the queue.
    """

    def __init__(self, *args, **kwargs):
        ctx = multiprocessing.get_context()
        super().__init__(*args, **kwargs, ctx=ctx)
        self._unique_objs = set()  # Might be a cleaner way of doing this

    def put(self, obj, *args, **kwargs):
        if obj in self._unique_objs:
            return
        self._unique_objs.update([obj])
        super().put(obj, *args, **kwargs)

    def get(self, *args, **kwargs):
        obj = super().get(*args, **kwargs)
        self._unique_objs.remove(obj)
        return obj


def _pool_init(function, queue, collection_name, config):
    """ Initialise the process pool.
    This allows a single mongodb connection per process rather than per file, which is inefficient.
    Args:
        function (Function): The function to parallelise.
        queue (UniqueQueue): The multiprocessing file queue object.
        collection_name (str): The collection name for the raw exposures.
        config (dict): The config dictionary.
    """
    logger = get_logger()
    function.exposure_collection = RawExposureCollection(collection_name=collection_name,
                                                         config=config, logger=logger)
    function.fits_header_translator = FitsHeaderTranslator(config=config, logger=logger)
    function.queue = queue


def _process_file(metric_names, config):
    """ Process a single file.
    This function has to be defined outside of the FileIngestor class since we are using
    multiprocessing and class instance methods cannot be pickled.
    Args:
        filename (str): The name of the file to process.
    Returns:
        bool: True if file was successfully processed, else False.
    """
    exposure_collection = _process_file.exposure_collection
    fits_header_translator = _process_file.fits_header_translator
    logger = exposure_collection.logger

    filename = _process_file.queue.get(timeout=5)  # Raises queue.Empty if timeout occurs
    logger.debug(f"Processing {filename}.")

    # Read the header
    try:
        header = fits_header_translator.read_and_parse(filename)
    except Exception as err:
        logger.error(f"Exception while parsing FITS header for {filename}: {err}.")
        success = False
    else:
        # Get the metrics
        metrics, success = _get_raw_metrics(filename, metric_names=metric_names, logger=logger)
        to_update = {METRIC_SUCCESS_FLAG: success, "quality": metrics}

        # Update the document (upserting if necessary)
        to_update.update(header)
        to_update["filename"] = filename

        logger.info(f"{to_update}")

        exposure_collection.update_one(header, to_update=to_update, upsert=True)

    return filename, success


def _get_raw_metrics(filename, metric_names, logger):
    """ Evaluate metrics for a raw/unprocessed file.
    Args:
        filename (str): The filename of the FITS image to be processed.
    Returns:
        dict: Dictionary containing the metric values.
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


class FileIngestor(HuntsmanBase):
    """ Class to watch for new file entries in database and process their metadata
    """
    # Work around so that tests can run without running the has_wcs metric
    _raw_metrics = deepcopy(RAW_METRICS)

    def __init__(self, exposure_collection=None, sleep_interval=10, status_interval=60,
                 nproc=None, directory=None, *args, **kwargs):
        """
        Args:
            sleep_interval (float): The amout of time to sleep in between checking for new
                files to screen in seconds. Default 10.
            status_interval (float, optional): Sleep for this long between status reports. Default
                60s.
            directory (str): The top level directory to watch for new files, so they can
                be added to the relevant datatable.
            nproc (int): The number of processes to use. If None (default), will check the config
                item `screener.nproc` with a default value of 1.
            *args, **kwargs: Parsed to HuntsmanBase initialiser.
        """
        super().__init__(*args, **kwargs)

        ingestor_config = self.config.get("ingestor", {})

        # Set the number of processes
        if nproc is None:
            nproc = ingestor_config.get("nproc", 1)
        self._nproc = int(nproc)
        self.logger.info(f"File ingestor using {nproc} processes.")

        # Set the monitored directory
        if directory is None:
            directory = ingestor_config["directory"]
        self._directory = directory
        self.logger.debug(f"Ingesting files in directory: {self._directory}")

        # Setup the exposure collection
        if exposure_collection is None:
            exposure_collection = RawExposureCollection(config=self.config, logger=self.logger)
        self._exposure_collection = exposure_collection
        self._collection_name = exposure_collection.collection_name

        # Sleep intervals
        self._sleep_interval = sleep_interval
        self._status_interval = status_interval

        # Setup threads
        self._file_queue = UniqueQueue()
        self._status_thread = Thread(target=self._async_monitor_status)
        self._queue_thread = Thread(target=self._async_queue_files)
        self._process_thread = Thread(target=self._async_process_files)
        self._threads = [self._status_thread, self._queue_thread, self._process_thread]

        # Starting values
        self._n_processed = 0
        self._n_failed = 0
        self._stop = False

        atexit.register(self.stop)  # This gets called when python is quit

    @property
    def is_running(self):
        """ Check if the screener is running.
        Returns:
            bool: True if running, else False.
        """
        return all([t.is_alive() for t in self._threads])

    @property
    def status(self):
        """ Return a status dictionary.
        Returns:
            dict: The status dictionary.
        """
        status = {"status_thread": self._status_thread.is_alive(),
                  "queue_thread": self._queue_thread.is_alive(),
                  "process_thread": self._process_thread.is_alive(),
                  "processed": self._n_processed,
                  "failed": self._n_failed,
                  "queued": self._file_queue.qsize()}
        return status

    def start(self):
        """ Start the file ingestor. """
        self.logger.info("Starting file ingestor.")
        self._stop = False
        for thread in self._threads:
            thread.start()

    def stop(self, blocking=True):
        """ Stop the file ingestor.
        Args:
            blocking (bool, optional): If True (default), blocks until all threads have joined.
        """
        self.logger.info("Stopping file ingestor.")
        self._stop = True
        if blocking:
            for thread in self._threads:
                with suppress(RuntimeError):
                    thread.join()

    def _async_monitor_status(self):
        """ Report the status on a regular interval. """
        self.logger.debug("Starting status thread.")

        while True:
            if self._stop:
                self.logger.debug("Stopping status thread.")
                break

            # Get the current status
            status = self.status
            self.logger.info(f"Ingestor status: {status}")
            if not self.is_running:
                self.logger.warning("Ingestor is not running.")

            # Sleep before reporting status again
            timer = CountdownTimer(duration=self._status_interval)
            while not timer.expired():
                if self._stop:
                    break
                time.sleep(1)

    def _async_queue_files(self):
        """ Queue all existing files that are not in the exposure collection or have not passed
        screening.
        """
        self.logger.debug("Starting queue thread.")

        while True:
            if self._stop:
                self.logger.debug("Stopping queue thread.")
                return

            # Get set of all files in watched directory
            files_in_directory = set(list_fits_files_recursive(self._directory))
            self.logger.debug(f"Found {len(files_in_directory)} FITS files in {self._directory}.")

            # Get set of all files that are ingested and pass screening
            files_ingested = set(self._exposure_collection.find(screen=True, key="filename"))

            # Identify files that require processing
            files_to_process = files_in_directory - files_ingested
            self.logger.debug(f"Found {len(files_to_process)} files requiring processing.")

            # Update files to process
            for filename in files_to_process:
                self._file_queue.put(filename)  # Note that we are using UniqueQueue

            timer = CountdownTimer(duration=self._sleep_interval)
            while not timer.expired():
                if self._stop:
                    break
                time.sleep(1)

    def _async_process_files(self):
        """ Continually process files in the queue. """
        self.logger.debug(f"Starting process with {self._nproc} processes.")

        # Define the function to parallelise
        func_kwargs = dict(metric_names=self._raw_metrics, config=self.config)
        init_args = (_process_file, self._file_queue, self._collection_name, self.config)

        with multiprocessing.Pool(self._nproc, initializer=_pool_init, initargs=init_args) as pool:
            while True:
                if self._stop:
                    self.logger.debug("Stopping process thread.")
                    break

                # Process the file in the pool asyncronously
                pool.apply_async(_process_file, (), func_kwargs,
                                 callback=self._process_callback,
                                 error_callback=self._error_callback)

    def _process_callback(self, result):
        """ Function that is called after a file is processed.
        Args:
            result (tuple): The result of the completed function call.
        """
        filename, success = result
        self.logger.debug(f"Finished processing {filename}.")
        self._n_processed += 1
        if not success:
            self._n_failed += 1
        self._file_queue.task_done()

    def _error_callback(self, exception):
        if not isinstance(exception, queue.Empty):
            raise exception
