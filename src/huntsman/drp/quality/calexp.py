from huntsman.drp.base import HuntsmanBase
from huntsman.drp.datatable import DataTable
from huntsman.drp.quality.utils import screen_success
from huntsman.drp.butler import TemporaryButlerRepository


class CalexpMonitor(HuntsmanBase):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._stop = False
        self._filenames = None
        self._table = DataTable(config=self.config, logger=self.logger)

    def start(self):
        """
        """
        self.logger.info(f"Starting {self}.")
        self._stop = False

        while True:
            if self._stop:
                break

            # Identify files that require processing
            self._refresh_file_list()

            # Process files
            self._process_files()

    def stop(self):
        """
        """
        self.logger.info(f"Stopping {self}.")
        self._stop = True

    def _refresh_file_list(self):
        """
        """
        filenames = []
        for file_info in self._table.query(self._query, criteria={"dataType": "science"}):
            if self._requires_processing(file_info):
                filenames.append(file_info["filename"])
        self._filenames = filenames

    def _requires_processing(self, file_info):
        """
        """
        # TODO: Also need to check if it has already been processed
        # TODO: Handle the screen implicitly in the table query
        return screen_success(file_info)

    def _process_files(self):
        """
        """
        # Get corresponding raw calibs
        filenames_calib = []
        for filename in self._filenames:
            filenames_calib.extend(self._table.find_matching_raw_calibs(filename), key="filename")

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

    def _insert_metadata(self):
        pass
