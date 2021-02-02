import os
from huntsman.drp.core import get_config
from huntsman.drp.butler import ButlerRepository


def create_test_bulter_repository(directory, config=None, **kwargs):
    """ Create a butler repository and ingest the testing dataset.
    Args:
        **kwargs: Parsed to ButlerRepository.
    """
    if config is None:
        config = get_config()
    br = ButlerRepository(directory=directory, config=config, **kwargs)

    datadir = os.path.join(config["directories"]["root"], "tests", "data")

    # Get test data filenames
    filenames = []
    for filename in os.listdir(datadir):
        if filename.endswith(".fits"):
            filenames.append(os.path.join(datadir, filename))

    # Ingest test data into butler repository
    br.ingest_raw_data(filenames)

    # Ingest the refcat
    filename_refcat = os.path.join(datadir, "refcat.csv")
    br.ingest_reference_catalogue([filename_refcat])

    return br
