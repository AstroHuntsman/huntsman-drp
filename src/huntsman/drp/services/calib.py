import os
import time
import datetime
from copy import copy
from threading import Thread, Event

from huntsman.drp.base import HuntsmanBase
from huntsman.drp.utils.date import current_date, parse_date, date_to_ymd
from huntsman.drp.lsst.butler import TemporaryButlerRepository
from huntsman.drp.collection import ExposureCollection, CalibCollection


__all__ = ("CalibService",)


class CalibService(HuntsmanBase):

    def __init__(self, date_begin=None, validity=1, min_exps_per_calib=1,
                 max_exps_per_calib=None, nproc=1, remake_existing=False, **kwargs):
        """
        Args:
            date_begin (datetime.datetime, optional): Make calibs for this date and after. If None
                (default), use current date - validity.
            validity (int, optional): Make calibs on this interval in days. Will get from config
                if not provided, with default of 1.
            min_exps_per_calib (int, optional): Calibs must match with at least this many raw docs
                to be created. Default: 1.
            max_exps_per_calib (int, optional): No more than this many raw docs will contribute to
                a single calib. If None (default), no upper-limit is applied.
            nproc (int, optional): The number of processes to use. Default: 1.
            remake_existing (bool, optional): If True, remake existing calibs. Default: False.
        """
        super().__init__(**kwargs)

        self.nproc = nproc

        # If this is true then existing calibs will be remade
        self.remake_existing = bool(remake_existing)

        # Set the validity, which determines the frquency that calibs are made
        self.validity = datetime.timedelta(days=validity)

        # Set the start date for the calibs
        date_begin = current_date() - self.validity if date_begin is None else date_begin
        self.date_begin = parse_date(date_to_ymd(date_begin))

        # Private attributes
        self._ordered_calib_types = self.config["calibs"]["types"]
        self._min_exps_per_calib = min_exps_per_calib
        self._max_exps_per_calib = max_exps_per_calib

        # Create collection client objects
        self.exposure_collection = ExposureCollection(config=self.config, logger=self.logger)
        self.calib_collection = CalibCollection(config=self.config, logger=self.logger)

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
        return self._stop_threads.is_set()

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

    def process_date(self, date, **kwargs):
        """ Create all master calibs for a given calib date.
        Args:
            date (object): The calib date.
        """
        # Calculate date range from date and validity
        date = parse_date(date)
        date_min = date - self.validity
        date_max = date + self.validity

        # Specify common find kwargs
        find_kwargs = {"date_min": date_min,
                       "date_max": date_max}
        find_kwargs.update(kwargs)

        # Get set of calib docs we can make from the set of exposure docs
        calib_docs = self.exposure_collection.get_calib_docs(date=date, **find_kwargs)

        if len(calib_docs) == 0:
            self.logger.warning(f"No calib documents found in {self.exposure_collection} for"
                                f" {date}. Skipping.")
            return

        # Figure out which calibs we can ingest / skip processing
        calib_docs_ingest = []
        if self.remake_existing:
            calib_docs_process = calib_docs
        else:
            calib_docs_process = []
            for calib_doc in calib_docs:
                # Get the archived filename. This may not actually exist yet.
                filename = self.calib_collection.get_calib_filename(calib_doc)
                if os.path.isfile(filename):
                    calib_doc["filename"] = filename
                    calib_docs_ingest.append(calib_doc)
                else:
                    calib_docs_process.append(calib_doc)
            self.logger.debug(f"Skipping {len(calib_docs_ingest)} existing calibs.")

        # Get documents matching the calib docs
        exp_docs = []
        for calib_doc in calib_docs_process:

            docs = self.exposure_collection.get_matching_raw_calibs(
                calib_doc, sort_date=date, **find_kwargs)

            # Limit the number of documents per calib
            if self._max_exps_per_calib is not None:
                if len(docs) > self._max_exps_per_calib:
                    self.logger.warning(
                        f"Number of matching exposures for calib {calib_doc} ({len(docs)})"
                        f" exceeds maximum. Limiting to first {self._max_exps_per_calib}.")
                    docs = docs[:self._max_exps_per_calib]

            # Make sure there are enough exposures to make the calib
            if self._min_exps_per_calib is not None:
                if len(docs) < self._min_exps_per_calib:
                    self.logger.warning(
                        f"Number of matching exposures for calib {calib_doc} ({len(docs)})"
                        f" lower than minimum ({self._min_exps_per_calib}). Skipping.")
                    docs = None

            exp_docs.append(docs)

        # Construct the calibs and archive them
        self._process_documents(calib_docs_process, exp_docs, calib_docs_ingest=calib_docs_ingest,
                                begin_date=date_min, end_date=date_max)

    def _process_documents(self, calib_docs, exp_docs, calib_docs_ingest=None, **kwargs):
        """ Create calibs from raw exposures using the LSST stack.
        Args:
            calib_docs (list of CalibDocument): The calibs to process.
            exp_docs (list of ExposureDocument): THe exposures to process. Must be the same length
                as calib_docs.
            **kwargs: Parsed to ButlerRepository.construct_calibs.
        """
        with TemporaryButlerRepository(config=self.config) as br:

            # Ingest master calibs that we are not processing
            if calib_docs_ingest:
                br.ingest_calib_docs(calib_docs_ingest)

            # Loop over calib types in order
            for calib_type in self._ordered_calib_types:

                # Loop over calib docs of the correct type
                for calib_doc, docs in zip(calib_docs, exp_docs):
                    if calib_doc["datasetType"] != calib_type:
                        continue
                    elif not docs:
                        continue

                    # Ingest the raw docs
                    br.ingest_raw_files([d["filename"] for d in docs])

                    self.logger.debug("Converting documents into LSST dataIds.")
                    calibId = br.document_to_dataId(calib_doc, datasetType=calib_type)
                    dataIds = [br.document_to_dataId(d) for d in docs]

                    # Process calibs one by one
                    # This does not make full use of LSST quantum graph but gives us more control
                    self.logger.info(f"Constructing {calib_type} for {calib_doc}.")
                    try:
                        br.construct_calibs(calib_type, dataIds=dataIds, nproc=self.nproc, **kwargs)

                    # Log error and continue making the other calibs
                    # This may lead to further errors down the line but it is the best we can do
                    except Exception as err:
                        self.logger.error(
                            f"Error while constructing {calib_type} for {calib_doc}: {err!r}")
                        continue

                    # Use butler to get the calib filename
                    filename = br.get_filenames(calib_type, dataId=calibId)[0]

                    # Archive the calib in the calib collection
                    self.logger.info(f"Archiving {calib_type} for {calib_doc}.")
                    self.calib_collection.archive_master_calib(filename=filename,
                                                               metadata=calib_doc)

    def sleep_until_date_is_valid(self, date, interval=5):
        """ Sleep until current_date >= date + validity.
        This ensures required files will be present in the exposure collection before creating
        the calibs.
        Args:
            date (datetime.datetime): The date.
            interval (float, optional): The sleep interval in seconds. Default: 5.
        """
        valid_date = date + self.validity
        if valid_date > date:

            self.logger.info(f"Waiting for date: {valid_date}")

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

            # Increment the date to the next validity period
            finally:
                date += self.validity
