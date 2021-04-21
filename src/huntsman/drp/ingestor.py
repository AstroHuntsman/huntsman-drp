import time
import queue
import atexit
from multiprocessing import Pool, Queue
from copy import deepcopy
from contextlib import suppress
from threading import Thread

from huntsman.drp.base import HuntsmanBase
from huntsman.drp.collection import RawExposureCollection
from huntsman.drp.fitsutil import FitsHeaderTranslator, read_fits_header, read_fits_data
from huntsman.drp.utils.library import load_module
from huntsman.drp.metrics.raw import RAW_METRICS
from huntsman.drp.utils.ingest import METRIC_SUCCESS_FLAG, list_fits_files_recursive


class FileIngestor(HuntsmanBase):
    """ Class to watch for new file entries in database and process their metadata
    """
    # Work around so that tests can run without running the has_wcs metric
    _raw_metrics = deepcopy(RAW_METRICS)

    def __init__(self, exposure_table=None, sleep_interval=None, status_interval=60, nproc=None,
                 directory=None, *args, **kwargs):
        """
        Args:
            sleep_interval (float/int): The amout of time to sleep in between checking for new
                files to screen.
            status_interval (float, optional): Sleep for this long between status reports. Default
                60s.
            directory (str): The top level directory to watch for new files, so they can
                be added to the relevant datatable.
            nproc (int): The number of processes to use. If None (default), will check the config
                item `screener.nproc` with a default value of 1.
            *args, **kwargs: Parsed to HuntsmanBase initialiser.
        """
        super().__init__(*args, **kwargs)

        screener_config = self.config.get("screener", {})

        # Set the number of processes
        # This is a dummy for now
        if nproc is None:
            nproc = screener_config.get("nproc", 1)
        self._nproc = int(nproc)
        self.logger.info(f"Screener using {nproc} processes.")

        # Set the monitored directory
        if directory is None:
            directory = screener_config["directory"]
        self._directory = directory
        self.logger.debug(f"Screening directory: {self._directory}")

        # Setup the exposure collection
        if exposure_table is None:
            exposure_table = RawExposureCollection(config=self.config, logger=self.logger)
        self._exposure_collection = exposure_table

        # Make the FITS header translator
        self._fits_header_translator = FitsHeaderTranslator(config=self.config, logger=self.logger)

        # Sleep intervals
        self._sleep_interval = sleep_interval
        if self._sleep_interval is None:
            self._sleep_interval = 0

        self._status_interval = status_interval

        # Setup threads
        self._files_queue = Queue()
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
                  "queue_thread": self._ingest_queuer_thread.is_alive(),
                  "process_thread": self._screen_queuer_thread.is_alive(),
                  "processed": self._n_processed,
                  "failed": self._n_failed,
                  "queued": self._file_queue.qsize}
        return status

    def start(self):
        """ Start the file ingestor. """
        self.logger.info("Starting screening.")
        self._stop = False
        for thread in self._threads:
            thread.start()

    def stop(self, blocking=True):
        """ Stop the file ingestor.
        Args:
            blocking (bool, optional): If True (default), blocks until all threads have joined.
        """
        self.logger.info("Stopping screening.")
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
            self.logger.info(f"screener status: {status}")
            if not self.is_running:
                self.logger.warning("screener is not running.")

            # Sleep before reporting status again
            time.sleep(self._status_interval)

    def _async_queue_files(self):
        """ Queue all existing files that are not in the exposure collection or have not passed
        screening.
        """
        self.logger.debug("Starting queue thread.")

        while True:

            if self._stop_threads:
                self.logger.debug("Stopping queue thread.")
                return

            # Get set of all files in watched directory
            files_in_directory = set(list_fits_files_recursive(self._directory))

            # Get set of all files that are ingested and pass screening
            files_ingested = set(self._raw_collection.find(screen=True, key="filename"))

            # Identify files that require processing
            files_to_process = files_in_directory - files_ingested

            # Update files to process
            for filename in files_to_process:
                if filename not in self._file_queue:
                    self._file_queue.put(filename)

    def _async_process_files(self):
        """ Continually process files in the queue. """
        self.logger.debug(f"Starting process with {self._nproc} processes.")
        with Pool(self._nproc) as pool:
            pool.map(self._process_files, range(self._proc))

    def _process_files(self, process_number):
        """ Dummy method to process files via pool.map.
        Args:
            process_number (int): The process ID number.
        """
        self.logger.debug(f"Starting worker process {process_number}.")
        while True:
            if self._stop:
                self.logger.debug(f"Stopping worker process {process_number}.")
                return
            self._process_file()

    def _process_file(self):
        """ Process a single file from the queue. """
        try:
            filename = self._file_queue.get(timeout=10)  # Use timeout so we can gracefully exit
        except queue.Empty:
            self._n_failed += 1
            return
        self.logger.debug(f"Processing {filename}.")

        # Read the header
        try:
            header = self._fits_header_translator.read_and_parse(filename)
        except Exception as err:
            self.logger.error(f"Exception while parsing FITS header for {filename}: {err}.")
            return

        # Get the metrics
        metrics, success = self._get_raw_metrics(filename)
        to_update = {METRIC_SUCCESS_FLAG: success, "quality": metrics}

        # Update the document (upserting if necessary)
        to_update.update(header)
        self._exposure_collection.update_one(header, to_update=to_update, upsert=True)

        # Mark the file as complete
        self._file_queue.task_done()

        self._n_processed += 1
        if not success:
            self._n_failed += 1

        return

    def _get_raw_metrics(self, filename):
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
            self.logger.error(f"Unable to read {filename}: {err!r}")

        for metric in self._raw_metrics:
            func = load_module(f"huntsman.drp.quality.metrics.raw.{metric}")
            try:
                result.update(func(filename, data=data, header=header))
            except Exception as err:
                self.logger.error(f"Exception while calculating {metric} for {filename}: {err!r}")
                success = False

        return result, success
