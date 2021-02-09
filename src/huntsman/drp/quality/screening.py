""" Code for automated metadata processing of new files """
import time
import queue
import atexit
from contextlib import suppress
from threading import Thread
from astropy import units as u

from panoptes.utils import get_quantity_value
from panoptes.utils.time import current_time

from huntsman.drp.base import HuntsmanBase
from huntsman.drp.datatable import ExposureTable
from huntsman.drp.fitsutil import read_fits_header
from huntsman.drp.quality import metadata_from_fits
from huntsman.drp.quality.utils import get_quality_metrics, list_fits_files_in_directory
from huntsman.drp.quality.metrics.calexp import METRICS
from huntsman.drp.quality.utils import QUALITY_FLAG_NAME


def get_raw_metrics(filename):
    """Evaluate metrics for a raw/unprocessed file.

    Args:
        filename: filename of image to be characterised.

    Returns:
        dict: Dictionary containing the metric values.
    """
    result = {}
    # read the header
    try:
        hdr = read_fits_header(filename)
    except Exception as e:
        logger.error(f"Unable to read file header for {filename}: {e}")
        result[QUALITY_FLAG_NAME] = False
        return result
    # get the image data
    try:
        data = fits.getdata(filename).astype(dtype)
    except Exception as e:
        logger.error(f"Unable to read file {filename}: {err}")
        result[QUALITY_FLAG_NAME] = False
        return result

    for metric in METRICS:
        func = load_module(
            f"huntsman.drp.quality.metrics.rawexp.{metric}")
        result[metric] = func(data, hdr)
    return result


class Screener(HuntsmanBase):
    """ Class to watch for new file entries in database and process their metadata
    """

    def __init__(self, sleep_interval=None, status_interval=60, *args, **kwargs):
        """
        Args:
            sleep_interval (u.Quantity): The amout of time to sleep in between checking for new
                files to screen.
            status_interval (float, optional): Sleep for this long between status reports. Default
                60s.
            *args, **kwargs: Parsed to HuntsmanBase initialiser.
        """
        super().__init__(*args, **kwargs)

        self._table = ExposureTable(config=self.config, logger=self.logger)

        if sleep_interval is None:
            sleep_interval = 0
        self.sleep_interval = get_quantity_value(
            sleep_interval, u.minute) * u.minute

        self._status_interval = get_quantity_value(status_interval, u.second)

        self._n_screened = 0
        self._n_ingested = 0
        self._stop = False
        self._screen_queue = queue.Queue()
        self._ingest_queue = queue.Queue()

        self._status_thread = Thread(target=self._async_monitor_status)
        self._watch_thread = Thread(target=self._async_watch_table)
        self._ingest_thread = Thread(target=self._async_ingest_files)
        self._screen_thread = Thread(target=self._async_screen_files)
        self._threads = [self._status_thread, self._watch_thread, self._ingest_thread,
                         self._screen_thread]

        atexit.register(self.stop)  # This gets called when python is quit

    @property
    def is_running(self):
        return self.status["is_running"]

    @property
    def status(self):
        """ Return a status dictionary.
        Returns:
            dict: The status dictionary.
        """
        status = {"is_running": all([t.is_alive() for t in self._threads]),
                  "status_thread": self._status_thread.is_alive(),
                  "watch_thread": self._watch_thread.is_alive(),
                  "ingest_thread": self._ingest_thread.is_alive()
                  "screen_thread": self._status_thread.is_alive(),
                  "ingest_queued": self._ingest_queue.qsize(),
                  "screen_queued": self._screen_queue.qsize(),
                  "ingested": self._n_ingested,
                  "screened": self._n_screened}
        return status

    def start(self):
        """ Start screening. """
        self.logger.info("Starting screening.")
        self._stop = False
        for thread in self._threads:
            thread.start()

    def stop(self, blocking=True):
        """ Stop screening.
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
            self.logger.trace(f"screener status: {status}")
            if not self.is_running:
                self.logger.warning(f"screener is not running.")
            # Sleep before reporting status again
            time.sleep(self._status_interval)

    def _async_watch_table(self):
        """ Watch the data table for unscreened files
        and add all valid files to the screening queue. """
        self.logger.debug("Starting watch thread.")
        while True:
            if self._stop:
                self.logger.debug("Stopping watch thread.")
                break
            # update list of new files that have not been ingested
            self._get_filenames_to_ingest()
            # add files to ingest queue
            for filename in self._entries_to_ingest:
                self._screen_queue.put([current_time(), filename])

            # update list of filenames to screen
            self._get_filenames_to_screen()
            # Loop over filenames and add them to the queue
            # Duplicates are taken care of later on
            for filename in self._entries_to_screen['filename']:
                self._screen_queue.put([current_time(), filename])
            # Sleep before checking again
            time.sleep(self.sleep_interval.to_value(u.second))

    def _async_ingest_files(self, sleep=10):
        """ screen files that have been in the queue longer than self.delay_interval.
        Args:
            sleep (float, optional): Sleep for this long while waiting for self.delay_interval to
                expire. Default: 10s.
        """
        while True:
            if self._stop and self._ingest_queue.empty():
                self.logger.debug("Stopping ingest thread.")
                break
            # Get the oldest file from the queue
            try:
                track_time, filename = self._ingest_queue.get(
                    block=True, timeout=sleep)
            except queue.Empty:
                continue
            with suppress(FileNotFoundError):
                self._ingest_file(filename)
                self._n_ingest += 1
            # Tell the queue we are done with this file
            self._ingest_queue.task_done()

    def _async_screen_files(self, sleep=10):
        """ screen files that have been in the queue longer than self.delay_interval.
        Args:
            sleep (float, optional): Sleep for this long while waiting for self.delay_interval to
                expire. Default: 10s.
        """
        while True:
            if self._stop and self._screen_queue.empty():
                self.logger.debug("Stopping screen thread.")
                break
            # Get the oldest file from the queue
            try:
                track_time, filename = self._screen_queue.get(
                    block=True, timeout=sleep)
            except queue.Empty:
                continue
            with suppress(FileNotFoundError):
                self._screen_file(filename)
                self._n_screened += 1
            # Tell the queue we are done with this file
            self._screen_queue.task_done()

    def _get_filenames_to_ingest(self, monitored_directory='/data/nifi/huntsman_priv/images'):
        """ Watch top level directory for new files to process/ingest into database.
        TODO: monitored_directory should be loaded from a config or somthing
        Returns:
            list: The list of filenames to process.
        """
        # create a list of fits files within the directory of interest
        files_in_directory = list_fits_files_in_directory(monitored_directory)
        # list of all entries in data base
        files_in_table = [item['filename'] for item in self._table.query()]
        files_to_ingest = set(files_in_directory) - set(files_in_table)
        self._files_to_ingest = files_to_ingest

    def _get_filenames_to_screen(self):
        """ Get valid filenames in the data table to screen.
        Returns:
            list: The list of filenames to screen.
        """
        # Find any entries in database that haven't been screened
        for index, row in self._table.query().iterrows():
            # if the file represented by this entry hasn't been
            # screened add it to queue
            if not self._screen_file(row):
                # extract fname from entry and append that instead
                files_to_screen.append(row['filename'])
        self._files_to_screen = files_to_screen

    def _ingest_file(self, filename):
        """Private method that calls the various screening metrics and collates the results
        """
        # extract metadata from file
        metadata = metadata_from_fits(filename, logger=self.logger)
        # Update metadata in table
        logger.info(f"Adding quality metadata to database.")
        self._table.update(metadata, upsert=True)

    def _screen_file(self, filename):
        """Private method that calls the various screening metrics and collates the results
        """
        metrics = get_raw_metrics(filename)

        # Make the document and update the DB
        # TODO: safe to assume there won't be duplicate entries in the datatable?
        metadata = self._table.find_one({'filename': filename})
        document = {k: metadata[k] for k in required_keys}
        to_update = {"quality": {"rawexp": metrics}}
        self._exposure_table.update_one(document, to_update=to_update)
