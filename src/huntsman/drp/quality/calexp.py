import time
from threading import Thread

from huntsman.drp.base import HuntsmanBase
from huntsman.drp.datatable import ExposureTable
from huntsman.drp.butler import TemporaryButlerRepository

CALEXP_SCREEN_FLAG = "screened_calexp"


class CalexpQualityMonitor(HuntsmanBase):

    def __init__(self, sleep=600, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._sleep = sleep

        self._stop = False
        self._filenames = None
        self._n_processed = 0
        self._table = ExposureTable(config=self.config, logger=self.logger)
        self._monitor_thread = Thread(target=self._async_process_files)

    @property
    def status(self):
        """
        """
        status = {"processed": self._n_processed,
                  "queued": len(self._filenames),
                  "running": self._monitor_thread.is_alive()}
        return status

    def start(self):
        """
        """
        self.logger.info(f"Starting {self}.")
        self._stop = False
        self._monitor_thread.start()

    def stop(self):
        """
        """
        self.logger.info(f"Stopping {self}.")
        self._stop = True
        self._monitor_thread.join()

    def _refresh_file_list(self):
        """
        """
        filenames = []
        for file_info in self._table.query(self._query, criteria={"dataType": "science"},
                                           screen=True):
            if self._requires_processing(file_info):
                filenames.append(file_info["filename"])
        self.logger.info(f"Found {len(filenames)} files that require processing.")
        self._filenames = filenames

    def _async_process_files(self):
        """
        """
        self.logger.debug("Starting processing thread.")
        while True:
            self.logger.info(f"Status: {self.status}")

            if self._stop:
                self.logger.debug("Stopping processing thread.")
                break

            # Identify files that require processing
            self._refresh_file_list()

            # Sleep if no new files
            if len(self._filenames) == 0:
                self.logger.info(f"No new files to process. Sleeping for {self._sleep}s.")
                time.sleep(self._sleep)
                continue

            # Process files
            self._process_files()
            self._n_processed += len(self._filenames)

    def _process_files(self):
        """
        """
        # Get corresponding raw calibs
        filenames_calib = set()
        for filename in self._filenames:
            filenames_calib.update(self._table.find_matching_raw_calibs(filename), key="filename")

        with TemporaryButlerRepository() as br:

            # Ingest raw exposures
            br.ingest_raw_data(self._filenames)
            br.ingest_raw_data(filenames_calib)

            # Make the master calibs
            br.make_master_calibs()

            # Make the calexps
            br.make_calexps()

            # Update the datatable with the metadata
            quality_metadata = br.get_calexp_metadata()
            self._insert_metadata(quality_metadata)

    def _requires_processing(self, file_info):
        """
        """
        return CALEXP_SCREEN_FLAG not in file_info.keys()

    def _insert_metadata(self):
        pass
