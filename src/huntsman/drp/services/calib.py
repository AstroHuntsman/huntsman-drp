""" Continually produce, update and archive master calibs. """
import os
import time
import datetime
from threading import Thread

from panoptes.utils.time import CountdownTimer

from lsst.obs.huntsman import HuntsmanMapper

from huntsman.drp.base import HuntsmanBase
from huntsman.drp.utils.date import date_to_ymd, parse_date
from huntsman.drp.collection import RawExposureCollection, MasterCalibCollection
from huntsman.drp.document import CalibDocument
from huntsman.drp.lsst.butler import TemporaryButlerRepository
from huntsman.drp.lsst.utils.calib import get_calib_filename


class MasterCalibMaker(HuntsmanBase):

    _date_key = "dateObs"

    def __init__(self, exposure_collection=None, calib_collection=None, nproc=None, **kwargs):
        """
        Args:
            nproc (int): The number of processes to use. If None (default), will check the config
                item `calib-maker.nproc` with a default value of 1.
        """
        super().__init__(**kwargs)

        self._ordered_calib_types = self.config["calibs"]["types"]

        calib_maker_config = self.config.get("calib-maker", {})

        # Set the number of processes
        if nproc is None:
            nproc = calib_maker_config.get("nproc", 1)
        self._nproc = int(nproc)
        self.logger.debug(f"Master calib maker using {nproc} processes.")

        validity = self.config["calibs"]["validity"]
        self._validity = datetime.timedelta(days=validity)  # TODO: Validity based on calib type

        # Create mapper used to get calib filenames
        self._mapper = HuntsmanMapper()

        # Create collection client objects
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
        # Get set of all unique calib IDs from the raw calibs
        calib_docs = self._get_unique_calib_docs(calib_date=calib_date)

        # Figure out which calib IDs need processing and which ones we can ingest
        calibs_to_process, calibs_to_ingest = self._get_calib_sets(calib_docs)
        if not calibs_to_process:
            self.logger.warning(f"No calibIds require processing for calibDate={calib_date}.")
            return

        # Get set of raw calibs that need processing
        raw_docs_to_process = set()
        for doc in calibs_to_process:
            raw_docs = self._exposure_collection.get_matching_raw_calibs(doc, calib_date=calib_date)
            raw_docs_to_process.update(raw_docs)
        self.logger.info(f"Raw documents to process: {len(raw_docs_to_process)}.")

        # Process data in a temporary butler repo
        with TemporaryButlerRepository(calib_collection=self._calib_collection) as br:

            # Ingest raw exposures
            br.ingest_raw_data([_["filename"] for _ in raw_docs_to_process])

            # Ingest existing master calibs
            for calib_type in self._ordered_calib_types:
                fns = [c["filename"] for c in calibs_to_ingest if c["datasetType"] == calib_type]
                br.ingest_master_calibs(calib_type, filenames=fns, validity=self._validity.days)

            # Make master calibs
            # NOTE: Implicit error handling
            br.make_master_calibs(calib_date=calib_date, validity=self._validity.days,
                                  procs=self._nproc)

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

    def _should_process(self, calib_doc):
        """ Check if the given calib_doc should be processed based on existing raw data.
        Args:
            calib_doc (CalibDocument): The calib ID.
            matching_raw_docs (list of RawExposureDocument): The RawExposureDocuments matching
                with this calib.
        Returns:
            bool: True if the calib ID requires processing, else False.
        """
        # Get the calib doc from the DB if it exists
        calib_doc = self._calib_collection.find_one(document_filter=calib_doc)

        # If the calib does not already exist, we need to make it
        if not calib_doc:
            return True

        # If the file doesn't exist, we need to make it
        elif not os.path.isfile(calib_doc["filename"]):
            return True

        matching_raw_docs = self._exposure_collection.get_matching_raw_calibs(
                                    calib_doc, calib_date=calib_doc["calibDate"])

        # If there are new files for this calib, we need to make it again
        if any([r["date_modified"] >= calib_doc["date_modified"] for r in matching_raw_docs]):
            return True

        # If there are no new files contributing to this existing calib, we can skip it
        return False

    def _get_calib_sets(self, calib_docs):
        """ Identify which calib IDs need processing and which should be ingested.
        Args:
            calib_docs (list of CalibDocument): The list of calib docs to check.
        Returns:
            Set of CalibDocument: Calib documents that need to be processed.
            Set of CalibDocument: Calib documents that need to be ingested.
        """
        calibs_to_process = set()
        calibs_to_ingest = set()

        for calib_doc in calib_docs:
            if calib_doc in calibs_to_process:
                continue

            # Check if we should process this calib
            if self._should_process(calib_doc):

                # Find all dependent calibs that need to be recreated
                calib_docs_dep = self._get_dependent_calibs(calib_doc)

                # Update the sets
                calibs_to_process.update(calib_docs_dep)
                calibs_to_ingest.difference_update(calib_docs_dep)

            # If we don't need to remake the calib then we should ingest it
            else:
                calibs_to_ingest.add(calib_doc)

        self.logger.info(f"calibs to process: {len(calibs_to_process)},"
                         f" calibs to ingest: {len(calibs_to_ingest)}.")

        return calibs_to_process, calibs_to_ingest

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
        for calib_type in self._ordered_calib_types:

            docs_of_type = self._exposure_collection.find(
                {"dataType": calib_type}, date_start=date_start, date_end=date_end, screen=True,
                quality_filter=True)

            self.logger.info(f"Found {len(docs_of_type)} raw {calib_type} calibs for"
                             f" calib_date={calib_date}.")

            docs.extend(docs_of_type)

        return docs

    def _get_unique_calib_docs(self, calib_date):
        """ Get all possible CalibDocuments from a set of RawExposureDocuments.
        Args:
            calib_date (object): The calib date.
        Returns:
            set of CalibDocument: The calb documents.
        """
        calib_date = date_to_ymd(calib_date)

        # Get metadata for all raw calibs that are valid for this date
        raw_docs = self._find_raw_calibs(calib_date=calib_date)

        unique_calib_docs = set()
        for document in raw_docs:
            unique_calib_docs.add(self._raw_doc_to_calib_doc(document, calib_date))

        self.logger.info(f"Found {len(unique_calib_docs)} calib IDs for calib_date={calib_date}.")

        return unique_calib_docs

    def _get_unique_dates(self):
        """ Get all calib dates specified by files in the raw data table.
        Returns:
            set of datetime: The list of dates.
        """
        dates = self._exposure_collection.find(key="date", screen=True, quality_filter=True)
        return set([date_to_ymd(d) for d in dates])

    def _raw_doc_to_calib_doc(self, document, calib_date):
        """ Get the matching calib document given a raw exposure calib document.
        Args:
            document (RawExposureDocument): The raw calib document.
            calib_date (object): The calib date.
        Returns:
            CalibDocument: The matching calib document.
        """
        calib_type = document["dataType"]

        # Get minimal LSST-style calib dataId
        keys = self.config["calibs"]["matching_columns"][calib_type]
        calib_dict = {k: document[k] for k in keys}
        calib_dict["calibDate"] = date_to_ymd(calib_date)

        # Get archived filename for this calib
        calib_dict["filename"] = get_calib_filename(datasetType=calib_type, dataId=calib_dict,
                                                    config=self.config, mapper=self._mapper)

        # Create the calib document
        calib_dict["datasetType"] = calib_type
        return CalibDocument(calib_dict)

    def _get_dependent_calibs(self, calib_doc):
        """ Get all dependent calibs for a calib doc.
        For example, a flat depends on the dark and bias used to create it.
        Args:
            calib_doc (CalibDocument): The calib document.
        Returns:
            list of CalibDoc: All dependent calibs, including the input calib doc.
        """
        calib_docs = set([calib_doc])

        dataset_type = calib_doc["datasetType"]

        # Identify columns used to identify dependent calibs
        matching_columns = self.config["calibs"]["matching_columns"][dataset_type].copy()
        if "calibDate" not in matching_columns:
            matching_columns.append("calibDate")

        # Make the matching dict
        matching_dict = {k: calib_doc[k] for k in matching_columns}

        if dataset_type == "flat":  # Nothing to be done here
            pass

        # If dataset type is dark, need to add all dependent flats
        elif dataset_type in ("bias", "dark"):
            query = matching_dict.copy()
            query["datasetType"] = "flat"
            new_docs = self._calib_collection.find(query)
            if new_docs is not None:
                calib_docs.update(new_docs)

        # If dataset type is dark, need to add all dependent biases and darks
        elif dataset_type == "bias":
            query = matching_dict.copy()
            query["datasetType"] = {"in": ["dark", "flat"]}
            new_docs = self._calib_collection.find(query)
            if new_docs is not None:
                calib_docs.update(new_docs)

        return calib_docs
