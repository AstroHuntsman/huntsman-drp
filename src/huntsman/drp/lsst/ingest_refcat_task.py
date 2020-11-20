""" Minimal overrides to remove unnecessary and time consuming file lock creation in current
implementation of ingestIndexReferenceTask."""

from ctypes import c_int
import multiprocessing

import astropy.units as u
import numpy as np

import lsst.pipe.base as pipeBase
from .indexerRegistry import IndexerRegistry

from lsst.meas.algorithms.ingestIndexManager import IngestIndexManager
from lsst.meas.algorithms.ingestIndexReferenceTask import (IngestIndexedReferenceConfig,
                                                           IngestReferenceRunner)

# global shared counter to keep track of source ids
# (multiprocess sharing is most easily done with a global)
COUNTER = multiprocessing.Value(c_int, 0)
# global shared counter to keep track of number of files processed.
FILE_PROGRESS = multiprocessing.Value(c_int, 0)


class singleProccessIngestIndexManager(IngestIndexManager):
    """
    Ingest a reference catalog from external files into a butler repository,
    using a multiprocessing Pool to speed up the work.
    Parameters
    ----------
    filenames : `dict` [`int`, `str`]
        The HTM pixel id and filenames to ingest the catalog into.
    config : `lsst.meas.algorithms.IngestIndexedReferenceConfig`
        The Task configuration holding the field names.
    file_reader : `lsst.pipe.base.Task`
        The file reader to use to load the files.
    indexer : `lsst.meas.algorithms.HtmIndexer`
        The class used to compute the HTM pixel per coordinate.
    schema : `lsst.afw.table.Schema`
        The schema of the output catalog.
    key_map : `dict` [`str`, `lsst.afw.table.Key`]
        The mapping from output field names to keys in the Schema.
    htmRange : `tuple` [`int`]
        The start and end HTM pixel ids.
    addRefCatMetadata : callable
        A function called to add extra metadata to each output Catalog.
    log : `lsst.log.Log`
        The log to send messages to.
    """
    _flags = ['photometric', 'resolved', 'variable']

    def __init__(self, filenames, config, file_reader, indexer,
                 schema, key_map, htmRange, addRefCatMetadata, log):
        self.filenames = filenames
        self.config = config
        self.file_reader = file_reader
        self.indexer = indexer
        self.schema = schema
        self.key_map = key_map
        self.htmRange = htmRange
        self.addRefCatMetadata = addRefCatMetadata
        self.log = log
        if self.config.coord_err_unit is not None:
            # cache this to speed up coordinate conversions
            self.coord_err_unit = u.Unit(self.config.coord_err_unit)

    def run(self, inputFiles):
        """Index a set of input files from a reference catalog, and write the
        output to the appropriate filenames, in parallel.
        Parameters
        ----------
        inputFiles : `list`
            A list of file paths to read data from.
        """
        global COUNTER, FILE_PROGRESS
        self.nInputFiles = len(inputFiles)
        COUNTER.value = 0
        FILE_PROGRESS.value = 0
        for filename in inputFiles:
            self._ingestOneFile(filename)

    def _ingestOneFile(self, filename):
        """Read and process one file, and write its records to the correct
        indexed files.
        Parameters
        ----------
        filename : `str`
            The file to process.
        """
        global FILE_PROGRESS
        inputData = self.file_reader.run(filename)
        fluxes = self._getFluxes(inputData)
        coordErr = self._getCoordErr(inputData)
        matchedPixels = self.indexer.indexPoints(inputData[self.config.ra_name],
                                                 inputData[self.config.dec_name])
        pixel_ids = set(matchedPixels)
        for pixelId in pixel_ids:
            self._doOnePixel(inputData, matchedPixels, pixelId, fluxes, coordErr)
        with FILE_PROGRESS.get_lock():
            oldPercent = 100 * FILE_PROGRESS.value / self.nInputFiles
            FILE_PROGRESS.value += 1
            percent = 100 * FILE_PROGRESS.value / self.nInputFiles
            # only log each "new percent"
            if np.floor(percent) - np.floor(oldPercent) >= 1:
                self.log.info("Completed %d / %d files: %d %% complete ",
                              FILE_PROGRESS.value,
                              self.nInputFiles,
                              percent)


class HuntsmanIngestIndexedReferenceTask(pipeBase.CmdLineTask):
    """Class for producing and loading indexed reference catalogs.
    This implements an indexing scheme based on hierarchical triangular
    mesh (HTM). The term index really means breaking the catalog into
    localized chunks called shards.  In this case each shard contains
    the entries from the catalog in a single HTM trixel
    For producing catalogs this task makes the following assumptions
    about the input catalogs:
    - RA, Dec are in decimal degrees.
    - Epoch is available in a column, in a format supported by astropy.time.Time.
    - There are no off-diagonal covariance terms, such as covariance
      between RA and Dec, or between PM RA and PM Dec. Support for such
     covariance would have to be added to to the config, including consideration
     of the units in the input catalog.
    Parameters
    ----------
    butler : `lsst.daf.persistence.Butler`
        Data butler for reading and writing catalogs
    """
    canMultiprocess = False
    ConfigClass = IngestIndexedReferenceConfig
    RunnerClass = IngestReferenceRunner
    _DefaultName = 'IngestIndexedReferenceTask'

    @classmethod
    def _makeArgumentParser(cls):
        """Create an argument parser.
        This returns a standard parser with an extra "files" argument.
        """
        parser = pipeBase.InputOnlyArgumentParser(name=cls._DefaultName)
        parser.add_argument("files", nargs="+", help="Names of files to index")
        return parser

    def __init__(self, *args, butler=None, **kwargs):
        self.butler = butler
        super().__init__(*args, **kwargs)
        self.indexer = IndexerRegistry[self.config.dataset_config.indexer.name](
            self.config.dataset_config.indexer.active)
        self.makeSubtask('file_reader')
        self.IngestManager = singleProccessIngestIndexManager
