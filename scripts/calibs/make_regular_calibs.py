"""
Script to create master calibs in regular intervals. In each time interval, produce master calibs
for today's date and send them to the archive. Only the most recent raw calib data will be used.
Existing calibs for today's date will be overwritten.
"""
import time
import argparse
from multiprocessing import Pool
import numpy as np

from huntsman.drp.core import get_logger, get_config
from huntsman.drp.datatable import RawDataTable


class RegularCalibMaker():
    _calib_types = "flat", "bias"

    def __init__(self, sleep_interval=86400, day_range=1000, nproc=1):
        self.logger = get_logger()
        self.config = get_config()
        self.sleep_interval = sleep_interval
        self.day_range = day_range
        self.datatable = RawDataTable(config=self.config, logger=self.logger)
        self._nproc = nproc

    def run(self):
        with Pool(self._nproc) as pool:
            while True:
                pool.apply_async(self._enqueue_next)
                time.sleep(self.sleep_interval)

    def _enqueue_next(self):

        # Get latest files
        df = self.datatable.query_latest(days=self.day_range)
        is_calib = np.zeros(df.shape[0], dtype="bool")
        for calib_type in self._calib_types:
            is_calib = np.logical_or(is_calib, df["dataType"].values == calib_type)
        filenames = df["filenames"].values[is_calib]

        # Ingest into repo
        self.butler_repository.ingest_raw_data(filenames)

        # Make master calibs and archive them
        self.butler_repository.make_master_calibs()
        self.butler_repository.archive_master_calibs()


if __name__ == "__main__":

    RegularCalibMaker().run()
