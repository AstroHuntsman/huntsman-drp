import os
from contextlib import suppress
from tempfile import TemporaryDirectory

import lsst.daf.persistence as dafPersist
from huntsman.drp.lsst_tasks import ingest_raw_data
from huntsman.drp.calibs import make_recent_calibs


class TemporaryButlerRepository():
    _mapper = "lsst.obs.huntsman.HuntsmanMapper"

    def __init__(self):
        self.directory = None
        self.butler = None

    def __enter__(self):
        """Create temporary directory and initialise as a Bulter repository."""
        self.directory = TemporaryDirectory()
        self._initialise_directory()
        self.butler = dafPersist.Butler(inputs=self.directory.name)

    def __exit__(self, *args, **kwargs):
        """Close temporary directory."""
        self.butler = None
        self.directory.cleanup()
        self.directory = None

    def ingest_raw_data(self, filenames):
        """Ingest raw data into the repository."""
        ingest_raw_data(filenames, butler_directory=self.directory.name)

    def make_master_calibs(self):
        """Make master calibs from ingested raw calibs."""
        make_recent_calibs(butler_directory=self.directory.name)

    def make_calexps(self):
        """Make calibrated science exposures (calexps) from ingested raw data."""
        pass

    def get_calexp_metadata(self):
        """Get calibrated science exposure (calexp) metadata"""
        pass

    def _initialise_directory(self):
        """Initialise a new butler repository."""
        # Add the mapper file to each subdirectory, making directory if necessary
        for subdir in ["", "CALIB"]:
            dir = os.path.join(self.directory.name, subdir)
            with suppress(FileExistsError):
                os.mkdir(dir)
            filename_mapper = os.path.join(dir, "_mapper")
            with open(filename_mapper, "w") as f:
                f.write(self._mapper)
