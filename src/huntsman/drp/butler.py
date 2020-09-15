import os
from contextlib import suppress
from collections import defaultdict
from tempfile import TemporaryDirectory

import lsst.daf.persistence as dafPersist

import huntsman.drp.lsst_tasks as lsst
from huntsman.drp.base import HuntsmanBase
from huntsman.drp.utils import date_to_ymd


class ButlerRepository(HuntsmanBase):
    _mapper = "lsst.obs.huntsman.HuntsmanMapper"

    def __init__(self, directory, calibdir=None, initialise=True, **kwargs):
        super().__init__(**kwargs)
        self.butlerdir = directory
        if (calibdir is None) and (directory is not None):
            calibdir = os.path.join(directory, "CALIB")
        self._calibdir = calibdir
        self.butler = None
        if initialise:
            self._initialise()

    @property
    def calibdir(self):
        return self._calibdir

    def ingest_raw_data(self, filenames):
        """Ingest raw data into the repository."""
        self.logger.debug(f"Ingesting {len(filenames)} files.")
        lsst.ingest_raw_data(filenames, butler_directory=self.butlerdir)

    def make_master_calibs(self, calib_date, rerun, **kwargs):
        """Make master calibs from ingested raw calibs."""
        # self.make_master_biases(calib_date, rerun, **kwargs)
        self.make_master_flats(calib_date, rerun, **kwargs)

    def make_master_biases(self, calib_date, rerun, nodes=1, procs=1):
        """

        """
        metalist = self.butler.queryMetadata('raw', ['ccd', 'expTime', 'dateObs', 'visit'],
                                             dataId={'dataType': 'bias'})
        # Select the exposures we are interested in
        exposures = defaultdict(dict)
        for (ccd, exptime, dateobs, visit) in metalist:
            if exptime not in exposures[ccd].keys():
                exposures[ccd][exptime] = []
            exposures[ccd][exptime].append(visit)

        # Parse the calib date
        calib_date = date_to_ymd(calib_date)

        # Construct the calib for this ccd/exptime combination (do we need this split?)
        for ccd, exptimes in exposures.items():
            for exptime, data_ids in exptimes.items():
                self.logger.debug(f'Making master biases for ccd {ccd} using {len(data_ids)}'
                                  f' exposures of {exptime}s.')
                lsst.constructBias(butlerdir=self.butlerdir, rerun=rerun, calibdir=self.calibdir,
                                   data_ids=data_ids, exptime=exptime, ccd=ccd, nodes=nodes,
                                   procs=procs, calib_date=calib_date)

    def make_master_flats(self, calib_date, rerun, nodes=1, procs=1):
        """

        """
        metalist = self.butler.queryMetadata('raw', ['ccd', 'filter', 'dateObs', 'visit'],
                                             dataId={'dataType': 'flat'})

        print(len(metalist))

        # Select the exposures we are interested in
        exposures = defaultdict(dict)
        for (ccd, filter_name, dateobs, visit) in metalist:
            if filter_name not in exposures[ccd].keys():
                exposures[ccd][filter_name] = []
            exposures[ccd][filter_name].append(visit)
            print("hello")

        # Parse the calib date
        calib_date = date_to_ymd(calib_date)

        # Construct the calib for this ccd/filter combination (do we need this split?)
        for ccd, filter_names in exposures.items():
            for filter_name, data_ids in filter_names.items():
                self.logger.debug(f'Making master flats for ccd {ccd} using {len(data_ids)}'
                                  f' exposures in {filter_name} filter.')
                lsst.constructFlat(butlerdir=self.butlerdir, rerun=rerun, calibdir=self.calibdir,
                                   data_ids=data_ids, filter_name=filter_name, ccd=ccd, nodes=nodes,
                                   procs=procs, calib_date=calib_date)

    def make_calexps(self):
        """Make calibrated science exposures (calexps) from ingested raw data."""
        pass

    def get_calexp_metadata(self):
        """Get calibrated science exposure (calexp) metadata"""
        pass

    def _initialise(self):
        """Initialise a new butler repository."""
        # Add the mapper file to each subdirectory, making directory if necessary
        for subdir in ["", "CALIB"]:
            dir = os.path.join(self.butlerdir, subdir)
            with suppress(FileExistsError):
                os.mkdir(dir)
            filename_mapper = os.path.join(dir, "_mapper")
            with open(filename_mapper, "w") as f:
                f.write(self._mapper)
        self.butler = dafPersist.Butler(inputs=self.butlerdir)


class TemporaryButlerRepository(ButlerRepository):
    """ Create a new Butler repository in a temporary directory."""

    def __init__(self, **kwargs):
        super().__init__(directory=None, initialise=False, **kwargs)

    def __enter__(self):
        """Create temporary directory and initialise as a Bulter repository."""
        self._tempdir = TemporaryDirectory()
        self.butlerdir = self._tempdir.name
        self._initialise()

    def __exit__(self, *args, **kwargs):
        """Close temporary directory."""
        self.butler = None
        self._tempdir.cleanup()
        self.butlerdir = None

    @property
    def calibdir(self):
        if self.butlerdir is None:
            return None
        return os.path.join(self.butlerdir, "CALIB")
