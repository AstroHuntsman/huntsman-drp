"""Python script to output metadata as json given an input filename."""
import json
import argparse
from huntsman.drp.fitsutil import read_fits_header, FitsHeaderTranslator


def main(filename, print_stdout=True):
    # Read the header
    header = read_fits_header(filename)

    # Parse the header
    meta = FitsHeaderTranslator().parse_header(header)

    # Add the filename to the metadata
    meta["filename"] = filename

    # Print as json
    meta_json = json.dumps(meta)
    if print_stdout:
        print(meta_json)


if __name__ == "__main__":

    # Parse the filename
    parser = argparse.ArgumentParser()
    parser.add_argument('filename', type=str)
    filename = parser.parse_args().filename

    main(filename)
