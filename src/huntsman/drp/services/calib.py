""" Continually produce, update and archive master calibs. """
import time
import datetime
from threading import Thread

from panoptes.utils.time import CountdownTimer

from huntsman.drp.base import HuntsmanBase
from huntsman.drp.utils.date import date_to_ymd, parse_date
from huntsman.drp.collection import RawExposureCollection, MasterCalibCollection
from huntsman.drp.lsst.butler import TemporaryButlerRepository
from huntsman.drp.document import CalibDocument


class MasterCalibMaker(HuntsmanBase):

    _date_key = "dateObs"

    def __init__(self, exposure_collection=None, calib_collection=None, nproc=None, **kwargs):
        """
        Args:
            nproc (int): The number of processes to use. If None (default), will check the config
                item `calib-maker.nproc` with a default value of 1.
        """
        super().__init__(**kwargs)

        self._calib_types = self.config["calibs"]["types"]

        calib_maker_config = self.config.get("calib-maker", {})

        # Set the number of processes
        if nproc is None:
            nproc = calib_maker_config.get("nproc", 1)
        self._nproc = int(nproc)
        self.logger.debug(f"Master calib maker using {nproc} processes.")

        validity = self.config["calibs"]["validity"]
        self._validity = datetime.timedelta(days=validity)  # TODO: Validity based on calib type

        # Create datatable objects
        if exposure_collection is None:
            exposure_collection = RawExposureCollection(config=self.config, logger=self.logger)
        self._exposure_collection = exposure_collection

        if calib_collection is None:
            calib_collection = MasterCalibCollection(config=self.config, logger=self.logger)
        self._calib_collection = calib_collection

        # Create threads
        self._stop_threads = False
        self._calib_thread = Thread(target=self._run)

    # Properties

    @property
    def is_running(self):
        """ Check if the asynchronous calib processing loop is running.
        Returns:
            bool: True if running, else False.
        """
        return self._calib_thread.is_alive()

    # Public methods

    def start(self):
        """ Start the asynchronous calib processing loop. """
        self.logger.info("Starting master calib maker.")
        self._stop_threads = False
        self._calib_thread.start()

    def stop(self):
        """ Stop the asynchronous calib processing loop.
        Note that this will block until any ongoing processing has finished.
        """
        self.logger.info("Stopping master calib maker.")
        self._stop_threads = True
        try:
            self._calib_thread.join()
            self.logger.info("Calib maker stopped.")
        except RuntimeError:
            pass

    def process_date(self, calib_date):
        """ Update all master calibs for a given calib date.
        Args:
            calib_date (object): The calib date.
        """
        # Get metadata for all raw calibs that are valid for this date
        raw_docs = self._find_raw_calibs(calib_date=calib_date)

        # Get a list of all unique calib IDs from the raw calibs
        calib_ids_all = self._get_unique_calib_docs(calib_date=calib_date, documents=raw_docs)
        self.logger.info(f"Found {len(calib_ids_all)} unique calib IDs for"
                         f" calib_date={calib_date}.")

        # Figure out which calib IDs need processing and which ones we can ingest
        calibs_to_process = set()
        raw_docs_to_process = set()
        calibs_to_ingest = set()

        for datasetType in ("bias", "dark", "flat"):  # Order is important

            calib_ids_type = [c for c in calib_ids_all if c["datasetType"] == datasetType]

            for calib_id in calib_ids_type:

                # Check if this document needs to be processed
                calib_doc, should_process, matching_raw_docs = self._should_process(
                    calib_id, calib_date)

            if should_process:
                calibs_to_process.add(calib_doc)
            else:
                calibs_to_ingest.add(calib_doc)

            # Update the list of docs used to make the master calibs
            if matching_raw_docs:
                raw_docs_to_process.update(matching_raw_docs)

        self.logger.info(f"{len(calibs_to_process)} calib IDs require processing for"
                         f" calib_date={calib_date}.")

        self.logger.info(f"Found {len(calibs_to_ingest)} master calibs to ingest for"
                         f" calib_date={calib_date}.")

        self.logger.info(f"Found {len(raw_docs_to_process)} raw calibs that require processing"
                         f" for calib_date={calib_date}.")

        if not calibs_to_process:
            self.logger.warning(f"No calibIds require processing for calibDate={calib_date}.")
            return

        # Process data in a temporary butler repo
        with TemporaryButlerRepository(calib_collection=self._calib_collection) as br:

            # Ingest raw exposures
            br.ingest_raw_data([_["filename"] for _ in raw_docs_to_process])

            # Ingest existing master calibs
            for calib_type in self._calib_types:
                fns = [c["filename"] for c in calibs_to_ingest if c["datasetType"] == calib_type]
                if fns:
                    br.ingest_master_calibs(calib_type, filenames=fns, validity=self._validity.days)

            # Make master calibs
            # NOTE: Implicit error handling
            br.make_master_calibs(calib_date=calib_date, datasetTypes_to_skip=datasetTypes_to_skip,
                                  validity=self._validity.days, procs=self._nproc)

            # Archive the master calibs
            try:
                self.logger.info(f"Archiving master calibs for calib_date={calib_date}.")
                br.archive_master_calibs()

            except Exception as err:
                self.logger.warning(f"Unable to archive master calibs for calib_date={calib_date}:"
                                    f" {err!r}")

    # Private methods

    def _run(self, sleep=300):
        """ Continually call self.process_date for each unique calib date.
        Args:
            sleep (float, optional): Sleep for this long between restarts.
        """
        while True:

            calib_dates = self._get_unique_dates()
            self.logger.info(f"Found {len(calib_dates)} unique calib dates.")

            for calib_date in calib_dates:

                if self._stop_threads:
                    return

                self.logger.info(f"Processing calibs for calib_date={calib_date}.")
                self.process_date(calib_date)

            self.logger.info(f"Finished processing calib dates. Sleeping for {sleep} seconds.")
            timer = CountdownTimer(duration=sleep)
            while not timer.expired():
                if self._stop_threads:
                    return
                time.sleep(1)

    def _should_process(self, calib_id, calib_date, calib_ids_to_process):
        """ Check if the given calib_id should be processed based on existing raw data.
        Args:
            calib_id (CalibDocument): The calib ID.
        Returns:
            CalibDocument (CalibDocument): The calib document in the DB if it exists, else None.
            bool: True if the calib ID requires processing, else False.
            list of RawExposureDocument: The list of raw docs required to make the calib, or None
                if the calib does not require processing.
        """
        # Get the calib doc from the DB if it exists
        calib_doc = self._calib_collection.find_one(document_filter=calib_id)

        # Find matching raw data IDs for this particular calib
        matching_raw_docs = self.exposure_collection.get_matching_raw_calibs(calib_doc,
                                                                             calib_date=calib_date)
        if not matching_raw_docs:
            self.logger.warning(f"No raw calibs found for {calib_id}.")

        # If the calib does not already exist, we need to make it
        if not calib_doc:
            return calib_id, True, matching_raw_docs

        # If there are new files for this calib, we need to make it again
        elif any([r["date_modified"] >= calib_doc["date_modified"] for r in matching_raw_docs]):
            return calib_id, True, matching_raw_docs

        # If the lower-level calibs are being modified, then we need to make it again
        # The order in which datasetTypes are processed in the calling function is important here
        calib_docs_all = self._raw_doc_to_calib_docs(calib_doc, calib_date)
        del calib_docs_all[calib_doc["datasetType"]]
        if any([c in calib_ids_to_process for c in calib_docs_all]):
            return calib_id, True, matching_raw_docs

        # If there are no new files contributing to this existing calib, we can skip it
        # We need to return the calib doc so we know the filename to ingest
        else:
            return calib_doc, False, None

    def _find_raw_calibs(self, calib_date):
        """ Find all valid raw calibs in the raw exposure collection given a calib date.
        Args:
            calib_date (object): The calib date.
        Returns:
            list of RawExposureDocument: The documents.
        """
        parsed_date = parse_date(calib_date)
        date_start = parsed_date - self._validity
        date_end = parsed_date + self._validity

        docs = []
        for calib_type in self._calib_types:

            docs_of_type = self._exposure_collection.find(
                {"dataType": calib_type}, date_start=date_start, date_end=date_end, screen=True,
                quality_filter=True)

            self.logger.info(f"Found {len(docs_of_type)} raw {calib_type} calibs for"
                             f" calib_date={calib_date}.")

            docs.extend(docs_of_type)

        return docs

    def _get_unique_calib_docs(self, calib_date, documents):
        """ Get all possible CalibDocuments from a set of RawExposureDocuments.
        Args:
            calib_date (object): The calib date.
            documents (iterable of RawExposureDocument): The raw exposure documents.
        Returns:
            list of CalibDocument: The calb documents.
        """
        calib_date = date_to_ymd(calib_date)
        calib_types = self.config["calibs"]["types"]

        unique_calib_ids = []

        for document in documents:

            calib_type = document["dataType"]
            if calib_type not in calib_types:
                continue

            calib_id = self._raw_doc_to_calib_docs(document, calib_date)

            if calib_id not in unique_calib_ids:
                unique_calib_ids.append(calib_id)

        return unique_calib_ids

    def _get_unique_dates(self):
        """ Get all calib dates specified by files in the raw data table.
        Returns:
            list of datetime: The list of dates.
        """
        dates = set(
            [date_to_ymd(d) for d in self._exposure_collection.find(key="date", screen=True,
             quality_filter=True)])
        return list(dates)

    def _raw_doc_to_calib_docs(self, document, calib_date):
        """ Get calib docs for each datasetType that match a RawExposureDocument.
        Args:
            document (RawExposureDocument): The document.
            calib_date (object): The date object.
        Returns:
            dict: A dict of datasetType: CalibDocument pairs.
        """
        calib_docs = {}

        for calib_type in self._calib_types:

            keys = self.config["calibs"]["matching_columns"][calib_type]
            calib_dict = {k: document[k] for k in keys}
            calib_dict["calibDate"] = date_to_ymd(calib_date)
            calib_dict["datasetType"] = calib_type

            calib_docs[calib_type] = CalibDocument(calib_dict)

        return calib_docs
