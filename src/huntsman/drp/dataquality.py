import os
from contextlib import suppress
from tempfile import TemporaryDirectory

from huntsman.drp.metadatabase import MetaDatabase
from huntsman.drp.calibs import make_recent_calibs
from huntsman.drp.lsst import ingest_raw_data


class TemporaryButler():
    _mapper = "lsst.obs.huntsman.HuntsmanMapper"

    def __init__(self):
        self._tempdir = None

    def __enter__(self):
        """Create temporary directory and initialise as a Bulter repository."""
        self._tempdir = TemporaryDirectory()
        self._initialise_directory()

    def __exit__(self, *args, **kwargs):
        """Close temporary directory."""
        self._tempdir.close()
        self._tempdir = None

    def ingest_raw_data(self, filenames):
        """Ingest raw data into the repository."""
        ingest_raw_data(filenames, bulter_directory=self._tempdir.name)

    def make_master_calibs(self):
        """Make master calibs from ingested raw calibs."""
        make_recent_calibs(bulter_directory=self._tempdir.name)

    def make_calexps(self):
        """Make calexps from ingested raw data."""
        pass

    def get_calexp_metadata(self):
        """Get calexp metadata"""
        pass

    def _initialise_directory(self):
        """Initialise a new butler repository."""
        # Add the mapper file to each subdirectory, making directory if necessary
        for subdir in ["", "CALIB"]:
            dir = os.path.join(self._tempdir.name, subdir)
            with suppress(FileExistsError):
                os.mkdir(dir)
            filename_mapper = os.path.join(dir, "__mapper")
            with open(filename_mapper, "w") as f:
                f.write(self._mapper)


def generate_science_data_quality(meta_database=None, table="calexp_qc"):
    """
    Generate metadata for science data.

    Args:
        meta_database (huntsman.drp.MetaDatabase, optional): The meta database object.
        table (str, optional): The table in which to insert the resulting metadata.
    """
    if meta_database is None:
        meta_database = MetaDatabase()

    # Get filenames of science data to process
    filenames = meta_database.query_recent_files()

    # Create a new butler repo in temp directory
    with TemporaryButler() as butler_repo:

        # Ingest raw data
        butler_repo.ingest_raw_data(filenames)

        # Make master calibs for today (discarded after use)
        butler_repo.make_master_calibs()

        # Make the calexps
        butler_repo.make_calexps()

        # Get calexp metadata and insert into database
        calexp_metadata = butler_repo.get_calexp_metadata()
        for metadata in calexp_metadata:
            meta_database.insert(metadata, table=table)
