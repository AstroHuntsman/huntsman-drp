import glob


import importlib.util
spec = importlib.util.spec_from_file_location("parse_fits_metadata",
                                              "/home/huntsman/dan_scripts/test_nifi_script/huntsman-drp/nifi/parse_fits_metadata.py")
parse_fits_metadata = importlib.util.module_from_spec(spec)
spec.loader.exec_module(parse_fits_metadata)


def main():
    basedir = "/var/huntsman/images/fields/*/*/*/*.fits*"
    for filename in glob.glob(basedir):
        try:
            parse_fits_metadata.main(filename, print_stdout=False)
        except KeyError:
            print(filename)


if __name__ == '__main__':
    main()
