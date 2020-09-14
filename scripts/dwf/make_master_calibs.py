"""Code to make a master calibs from most recent files for each camera."""
from datetime import datetime, timedelta

from huntsman.drp.datatable import RawDataTable
from huntsman.drp.butler import TemporaryButlerRepository


def get_recent_calibs(interval):
    """Get the most recent calibration images."""
    datatable = RawDataTable()
    date_end = datetime.utcnow()
    date_start = datetime.utcnow() - timedelta(days=interval)

    # Get bias filenames
    filenames_bias = datatable.query_column("filename", date_start=date_start, date_end=date_end,
                                            dataType="bias")
    # Get flat filenames
    # This is a hack to cope with the non-standard field naming
    metalist = datatable.query(date_start=date_start, date_end=date_end)
    filenames_flat = [m["filename"] for m in metalist if m["FIELD"].startswith("Day_Flats")]

    return [*filenames_bias, *filenames_flat]


if __name__ == "__main__":

    interval = 7  # Days

    # Get filenames
    filenames = get_recent_calibs(interval)

    with TemporaryButlerRepository() as butler_repo:

        # Ingest raw data
        butler_repo.ingest_raw_data(filenames)

        # Make master calibs
        butler_repo.make_master_calibs()
