from huntsman.drp.base import HuntsmanBase
from huntsman.drp.datatable import DataTable
from huntsman.drp.quality.utils import screen_success
from huntsman.drp.butler import TemporaryButlerRepository


class DataQualityMonitor(HuntsmanBase):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._stop = False
        self._file_info_list = None
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
        file_info_list = []
        for file_info in self._table.query(self._query):
            if self._requires_processing(file_info):
                file_info_list.append(file_info)
        self._file_info_list = file_info_list

    def _requires_processing(self, file_info):
        """
        """
        # TODO: Also need to check if it has already been processed
        # TODO: Handle the screen implicitly in the table query
        return screen_success(file_info)

    def _process_files(self):
        """
        """
        with TemporaryButlerRepository() as br:
            br.ingest_raw_data()
            br.ingest_master_calibs()
            br.make_calexps()
            quality_metadata = br.get_calexp_metadata()
            self._insert_metadata(quality_metadata)

    def _insert_metadata(self):
        pass
