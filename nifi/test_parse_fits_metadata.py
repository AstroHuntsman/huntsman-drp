import glob

import parse_fits_metadata


def main():
    basedir = "/var/huntsman/images/fields/*/*/*/*.fits*"
    for filename in glob.glob(basedir):
        try:
            parse_fits_metadata.main(filename, print_stdout=False)
        except KeyError as err:
            print(filename, err)


if __name__ == '__main__':
    main()
