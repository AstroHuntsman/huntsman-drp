"""Script to periodically query and process new data."""
import time
from queue import Queue
from threading import Thread
from datetime import datetime, timedelta

from huntsman.drp.datatable import RawDataTable
from reduction import process_concurrent_exposures


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
    filenames = datatable.query_column("filename", date_start=time_start, date_end=time_now)
    return filenames


def process_data(queue):
    """Get queued filename list and start processing it."""
    while True:
        date = datetime.utcnow().strftime('%Y-%m-%d')
        filenames = queue.get()
        process_concurrent_exposures(filenames, master_bias_date=date, master_flat_date=date)
        queue.task_done()


if __name__ == "__main__":

    # Factor these out as command line args
    interval = 60

    datatable = RawDataTable()
    queue = Queue()

    # Start the queue's worker thread
    thread = Thread(target=process_data, daemon=False, args=(queue))

    while True:

        # Get the latest filenames
        filenames = query_latest_files(datatable, interval)

        # Queue the filenames for processing
        print(f"Queuing {len(filenames)} files.")
        queue.put(filenames)

        # Wait for next batch
        time.sleep(interval)
