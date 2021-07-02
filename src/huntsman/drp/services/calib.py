import time
import datetime
from copy import copy
from threading import Thread, Event

from huntsman.drp.base import HuntsmanBase
from huntsman.drp.utils.date import current_date
from huntsman.drp.lsst.butler import TemporaryButlerRepository
from huntsman.drp.collection import RawExposureCollection, MasterCalibCollection


class MasterCalibMaker(HuntsmanBase):

    def __init__(self, date_begin=None, validity=1, min_docs_per_calib=1, max_docs_per_calib=None,
                 nproc=1, **kwargs):
        """
        Args:
            date_begin (datetime.datetime, optional): Make calibs for this date and after. If None
                (default), use current date - validity.
            validity (int, optional): Make calibs on this interval in days. Default: 1.
            min_docs_per_calib (int, optional): Calibs must match with at least this many raw docs
                to be created. Default: 1.
            max_docs_per_calib (int, optional): No more than this many raw docs will contribute to
                a single calib. If None (default), no upper-limit is applied.
            nproc (int, optional): The number of processes to use. Default: 1.
        """
        super().__init__(**kwargs)

        self.validity = datetime.timedelta(days=validity)
        self.date_begin = current_date() - self.validity if date_begin is None else date_begin

        self._min_docs_per_calib = int(min_docs_per_calib)
        self._max_docs_per_calib = int(max_docs_per_calib)

        self._nproc = int(nproc)

        # Create collection client objects
        self.exposure_collection = RawExposureCollection(**kwargs)
        self.calib_collection = MasterCalibCollection(**kwargs)

        # Create threads
        self._stop_threads = Event()
        self._calib_thread = Thread(target=self._run)

    # Properties

    @property
    def is_running(self):
        """ Check if the asynchronous calib processing loop is running.
        Returns:
            bool: True if running, else False.
        """
        return self._calib_thread.is_alive()

    @property
    def threads_stopping(self):
        """ Return True if threads should stop, else False. """
        return self._stop_thread.is_set()

    # Public methods

    def start(self):
        """ Start the asynchronous calib processing loop. """
        self.logger.info(f"Starting {self}.")
        self._stop_threads.clear()
        self._calib_thread.start()

    def stop(self):
        """ Stop the asynchronous calib processing loop.
        Note that this will block until any ongoing processing has finished.
        """
        self.logger.info(f"Stopping {self}.")
        self._stop_threads.set()
        try:
            self._calib_thread.join()
            self.logger.info("Calib maker stopped.")
        except RuntimeError:
            pass

    def process_date(self, calib_date):
        """ Create all master calibs for a given calib date.
        Args:
            calib_date (object): The calib date.
        """
        # Get valid raw docs and their corresponding set of partial calib docs
        raw_docs, calib_docs = self.exposure_collection.get_calib_docs(
            calib_date=calib_date,
            validity=self._validity,
            min_docs_per_calib=self._min_docs_per_calib,
            max_docs_per_calib=self._max_docs_per_calib)

        # Process data in a temporary butler repo
        with TemporaryButlerRepository() as br:

            # Ingest raw exposures
            br.ingest_raw_data([_["filename"] for _ in raw_docs])

            # Make master calibs
            calib_docs = br.make_master_calibs(calib_docs=calib_docs,
                                               validity=self._validity.days,
                                               procs=self._nproc)
            # Archive the master calibs
            for calib_doc in calib_docs:
                self.calib_collection.archive_master_calib(filename=calib_doc["filename"],
                                                           metadata=calib_doc)

    def sleep_until_date_is_valid(self, date, interval=5):
        """ Sleep until current_date >= date + validity.
        Args:
            date (datetime.datetime): The date.
            interval (float, optional): The sleep interval in seconds. Default: 5.
        """
        valid_date = date + self.validity
        if valid_date > date:

            self.logger.info(f"Waiting for date: {valid_date}")
            self._sleep_event.set()

            while current_date() < valid_date:
                if self.threads_stopping:
                    return
                time.sleep(interval)

            self.logger.info(f"Finished waiting for date: {valid_date}")

    # Private methods

    def _run(self):
        """ Continually process calibs and increment date. """
        date = copy(self.date_begin)

        while True:
            # Wait for the date to become vald e.g. if the date is in the future
            self.sleep_until_date_is_valid(date)

            if self.threads_stopping:
                return

            # Attempt to make the calibs for the date
            try:
                self.process_date(date)
            except Exception as err:
                self.logger.error(f"Error making master calibs for date={date}: {err!r}")
            # Finally, increment the date
            finally:
                date += self.validity
