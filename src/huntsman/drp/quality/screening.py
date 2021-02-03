""" Code for automated metadata processing of new files """
import os
import time
import queue
import atexit
import shutil
from contextlib import suppress
from threading import Thread
from astropy import units as u

from panoptes.utils import get_quantity_value
from panoptes.utils.time import current_time
from panoptes.pocs.base import PanBase

from huntsman.drp.base import HuntsmanBase
from huntsman.drp.datatable import ExposureTable


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
        self._stop = False
        self._screen_queue = queue.Queue()

        self._status_thread = Thread(target=self._async_monitor_status)
        self._watch_thread = Thread(target=self._async_watch_table)
        self._screen_thread = Thread(target=self._async_screen_files)
        self._threads = [self._status_thread,
                         self._watch_thread, self._screen_thread]

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
                  "screen_thread": self._status_thread.is_alive(),
                  "queued": self._screen_queue.qsize(),
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
            # update list of filenames to screen
            self._get_filenames_to_screen()
            # Loop over filenames and add them to the queue
            # Duplicates are taken care of later on
            for filename in self._entries_to_screen['filename']:
                self._screen_queue.put([current_time(), filename])
            # Sleep before checking again
            time.sleep(self.sleep_interval.to_value(u.second))

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

    def _get_filenames_to_screen(self):
        """ Get valid filenames in the data table to screen.
        Returns:
            list: The list of filenames to screen.
        """
        files_to_screen = []
        dtq = self.data_table.query()
        for index, row in dtq.iterrows():
            # if the file represented by this entry hasn't been
            # screened add it to queue
            if not self._screen_file(row):
                # extract fname from entry and append that instead
                files_to_screen.append(row['filename'])
        self._files_to_screen = files_to_screen

    def _screen_file(self):
        """Private method that calls the various screening metrics and collates the results
        """
        pass
