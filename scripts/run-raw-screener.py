""" Run the initial data quality screener. The task of the initial data quality screener is to
extract information like whether a file has wcs/if the file is corrupt and other basic
metadata. This metadata is then stored in the database quality table.
"""
import os
from huntsman.drp.screener import Screener

if __name__ == "__main__":

    screener = Screener()

    # Set niceness level
    niceness = screener.config.get("niceness", None)
    if niceness:
        os.nice(niceness - os.nice(0))

    screener.start()
