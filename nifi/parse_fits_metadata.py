"""Python script to output metadata as json given an input filename."""
import json
import glob
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


def test_main(glob_strings_list):
    count = 0
    for glob_string in glob_strings_list:
        for filename in glob.glob(glob_string):
            count += 1
            try:
                main(filename, print_stdout=False)
            except KeyError as err:
                print(filename, err)

    print(f"\n\n\nTested {count} fits files using the following glob strings:")
    for glob_string in glob_strings_list:
        print(glob_string)


if __name__ == "__main__":

    # Parse the filename
    parser = argparse.ArgumentParser()
    parser.add_argument('filename', type=str)
    parser.add_argument('--test-mode', dest='test_mode', action='store_true')

    filename = parser.parse_args().filename
    test_mode = parser.parse_args().test_mode

    if test_mode:
        glob_strings_list = ["/var/huntsman/images/fields/*/*/*/*.fits*",
                             "/var/huntsman/images/flats/*/*/*.fits*",
                             "/var/huntsman/images/darks/*/*/*.fits*"]
        test_main(glob_strings_list)
    else:
        main(filename)
