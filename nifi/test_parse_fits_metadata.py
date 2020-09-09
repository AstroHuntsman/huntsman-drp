import glob

import parse_fits_metadata


def main():
    glob_strings = ["/var/huntsman/images/fields/*/*/*/*.fits*",
                    "/var/huntsman/images/flats/*/*/*.fits*",
                    "/var/huntsman/images/darks/*/*/*.fits*"]

    for glob_string in glob_strings:
        for filename in glob.glob(glob_string):
            try:
                parse_fits_metadata.main(filename, print_stdout=False)
            except KeyError as err:
                print(filename, err)


if __name__ == '__main__':
    main()
