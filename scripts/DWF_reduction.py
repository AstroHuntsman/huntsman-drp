"""Script to monitor for new DWF run exposures and calibrate/coadd them.

-assume cal data taken at some other time

-monitor mongodb raw metadata database etc monitor it for new files

-recheck for new files every "exposure time length" query by timestamp
(look datatable)

-get files, ingest and do the processing (processCcd then follow tutorial
for doing coadds.... also need skymapper catalogue for region of interest
(look in refcat))

-will have a mounted output directory in the docker container, make this a
parameter output_directory
"""


from dateutil.parser import parse as parse_date
import datetime.datetime as datetime
from huntsman.drp.datatable import RawDataTable
import argparse


def main(date=None, exposure_time=300):
    """Main function for DWF reduction script, which monitors for incoming
    concurrent exposures and calibrates/coadds them.

    Args:
        date (str, optional): Date to search for files.
        date_range (int, optional): Number of days to search before date arg.
    """
    #add default option/exception handling in case config isnt set
    rdb = RawDataTable()

    if date is not None:
        date = datetime.today()
    date_parsed = parse_date(date)
    date_range = datetime.timedelta(seconds=exposure_time)




if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('raw-db', type=str, help='raw database file location.')
    args = parser.parse_args()

    main()
