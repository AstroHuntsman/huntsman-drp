"""Script to periodically query and process new data."""
import os
import time
from queue import Queue
from threading import Thread
from datetime import datetime, timedelta

from huntsman.drp.utils import get_current_date_ymd
from huntsman.drp.datatable import RawDataTable
from huntsman.drp import lsst_tasks as lsst
from huntsman.drp.bulter import TemporaryButlerRepository


def query_latest_files(datatable, interval):
    """
    Get latest filenames specified by a time interval.

    Args:
        datatable (`huntsman.drp.datatable.RawDataTable`): The raw data table.
        interval (float): The time interval in seconds.

    Returns:
        list of filenames.
    """
    time_now = datetime.utcnow()
    time_start = time_now - timedelta(seconds=interval)
    filenames = datatable.query_column("filename", date_start=time_start, date_end=time_now,
                                       dataType="science")
    return filenames


def process_exposures(filenames, butler_directory, rerun="dwfrerun", validity=1000,
                      filter="g_band", make_coadd=True, ingest_raw=True, ingest_calibs=True, calib_date=None):
    """
    Function that takes list of science exposures and processes them to
    produce a coadd. Master calibs are assumed to have already been produced
    amd ingested into butler repo. Skymapper catalogue is also assumed to have
    been ingested.

    Args:
        files (list): List of filepaths for processing.

    TODO:
        -Find way to handle exposures with different filters
    """
    if calib_date is None:
        calib_date = get_current_date_ymd()

    # Ingest raw exposures
    if ingest_raw:
        butler_repository.ingest_sci_images(filenames)

    # Ingest the master calibs
    if ingest_calibs:
        butler_repository.ingest_master_biases(calib_date, rereun)
        butler_repository.ingest_master_flates(calib_date, rereun)

    # Create calibrated exposures
    butler_repository.processCcd(dataType='science', rerun=rerun)

    # Make the coadd
    if make_coadd:
        lsst.makeDiscreteSkyMap(butler_directory=butler_directory, rerun=f'{rerun}:coadd')
        lsst.makeCoaddTempExp(filter, butler_directory=butler_directory, calibdir=calibdir,
                              rerun=f'{rerun}:coadd')
        lsst.assembleCoadd(filter, butler_directory=butler_directory, calibdir=calibdir,
                           rerun=f'{rerun}:coadd')


def process_data_async(queue, make_coadd=False, rerun="dwfrerun"):
    """Get queued filename list and start processing it."""
    while True:
        # Get the next set of filenames
        filenames = queue.get()

        try:
            # Create temp butler repo
            butler_repository = TemporaryButlerRepository()
            with butler_repository:

                # Ingest raw data
                butler_repository.ingest_raw_data(filenames)

                # Make calexps
                butler_repository.processCcd(dataType="science", rerun=rerun)

                # Assemble coadd
                if make_coadd:
                    butler_repository.make_coadd(rerun=rerun)

        except Exception as err:
            print(f"Error processing files: {err}.")
        finally:
            queue.task_done()


if __name__ == "__main__":

    # Factor these out as command line args
    interval = 60

    datatable = RawDataTable()
    queue = Queue()

    # Start the queue's worker thread
    thread = Thread(target=process_data_async, daemon=False, args=(queue))

    while True:

        # Get the latest filenames
        filenames = query_latest_files(datatable, interval)

        # Queue the filenames for processing
        print(f"Queuing {len(filenames)} files.")
        queue.put(filenames)

        # Wait for next batch
        time.sleep(interval)
