import json
import argparse
from huntsman.drp.fitsutil import read_header, FitsHeaderTranslator

if __name__ == "__main__":

    # Parse the filename
    parser = argparse.ArgumentParser()
    parser.add_argument('filename', type=str)
    filename = parser.parse_args().filename

    # Read the header
    header = read_header(filename)

    # Parse the header
    meta = FitsHeaderTranslator().parse_header(header)

    # Print as json
    meta_json = json.dumps(meta)
    print(meta_json)
