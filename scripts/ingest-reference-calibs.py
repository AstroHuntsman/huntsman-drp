"""
Script to ingest reference calibs from config. Should be run before starting the CalibService.
"""
from huntsman.drp.core import get_config, get_logger
from huntsman.drp.collection import ReferenceCalibCollection

CONFIG = get_config()
LOGGER = get_logger()

CONFIG_KEY = "reference_calib_filenames"  # Reference calibs should be listed under this key


if __name__ == "__main__":

    filenames = CONFIG.get(CONFIG_KEY, None)
    if not filenames:
        LOGGER.warning("No reference calibs found")
        exit()

    LOGGER.info(f"Found {len(filenames)} reference calibs to ingest.")

    # Ingest the files
    collection = ReferenceCalibCollection.from_config(CONFIG)
    for filename in filenames:
        try:
            collection.ingest_file(filename)
        except Exception as err:
            LOGGER.error(f"Error ingesting {filename}: {err!r}. Continuing with other files.")
