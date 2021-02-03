import time
from threading import Thread

from huntsman.drp.base import HuntsmanBase
from huntsman.drp.utils.library import load_module
from huntsman.drp.datatable import ExposureTable
from huntsman.drp.butler import TemporaryButlerRepository
from huntsman.drp.quality.metrics.calexp import METRICS


def get_quality_metrics(calexp):
    """ Evaluate metrics for a single calexp. This could probably be improved in future.
    TODO: Implement version control here.
    """
    result = {}
    for metric in METRICS:
        func = load_module(f"huntsman.drp.quality.metrics.calexp.{metric}")
        result[metric] = func(calexp)
    return result


class CalexpQualityMonitor(HuntsmanBase):

    _rerun = "default"

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
        """ Return the status of the quality monitor.
        Returns:
            dict: The status dict.
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
        # TODO: Screen raw data before ingesting it here
        for file_info in self._table.query(self._query, criteria={"dataType": "science"}):
            if self._requires_processing(file_info):
                filenames.append(file_info["filename"])
        self.logger.info(f"Found {len(filenames)} files that require processing.")
        self._filenames = filenames

    def _async_process_files(self):
        """ Continually check for and process files that require processing. """
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
        """ Get calexp quality metadata for each file and store in exposure data table. """

        # Get filenames of corresponding raw calibs
        filenames_calib = set()
        for filename in self._filenames:
            filenames_calib.update(self._table.find_matching_raw_calibs(filename), key="filename")

        with TemporaryButlerRepository() as br:

            # Ingest raw exposures
            br.ingest_raw_data(self._filenames)
            br.ingest_raw_data(filenames_calib)

            # Make the master calibs
            br.make_master_calibs()

            # Make the calexps, also getting the dataIds to match with their raw frames
            br.make_calexps()
            required_keys = br.get_keys("raw")
            calexps, data_ids = br.get_calexps(extra_keys=required_keys)

            # Evaluate metrics and insert into the database
            # TODO: Use multiprocessing?
            for calexp, data_id in zip(calexps, data_ids):

                metrics = get_quality_metrics(calexp)

                # Make the document to insert into the DB
                to_insert = {k: data_id[k] for k in required_keys}
                to_insert.update({"quality": {"calexp": metrics}})  # Use nested dictionary
                self._table.update(to_insert)

    def _requires_processing(self, file_info):
        """ Check if a file requires processing.
        Args:
            file_info (dict): The file document from the exposure data table.
        Returns:
            bool: True if processing required, else False.
        """
        try:
            return "calexp" in file_info["quality"]
        except KeyError:
            return False
