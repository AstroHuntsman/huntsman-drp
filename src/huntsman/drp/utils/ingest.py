import os

METRICS = "clipped_stats", "flipped_asymmetry"  # TODO: Refactor!
METRIC_SUCCESS_FLAG = "screen_success"


def screen_success(document):
    """ Test if the file has passed screening.
    Args:
        document (dict): The document for the file.
    Returns:
        bool: True if success, else False.
    """
    return bool(document.get(f"metrics.{METRIC_SUCCESS_FLAG}", False))


def list_fits_files_recursive(directory):
    """Returns list of all files contained within a top level directory, including files
    within subdirectories.
    Args:
        directory (str): Directory to examine.
    """
    # Create a list of fits files within the directory of interest
    files_in_directory = []

    for dirpath, dirnames, filenames in os.walk(directory):
        for file in filenames:

            # Append the filepath to fpaths if file is a fits or fits.fz file
            if file.endswith('.fits') or file.endswith('.fits.fz'):
                files_in_directory.append(os.path.join(dirpath, file))

    return files_in_directory


def ingest_exposure(filename, collection=None, **kwargs):
    """ Convenience function to easily ingest one file.
    Args:
        filename (str): The name of the file to ingest.
        collection (ExposureCollection, optional): The exposure collection. If not provided,
            will make one from config.
        **kwargs: Used to create the exposure collection if it is not provided.
    """
    # Do import here to prevent circular import
    from huntsman.drp.services.ingestor import ingest_file

    if collection is None:
        # Do import here to prevent circular import
        from huntsman.drp.collection import ExposureCollection
        collection = ExposureCollection(**kwargs)

    return ingest_file(filename, collection=collection)
