""" Minimal overrides to remove unnecessary and time consuming file lock creation in current
implementation of ingestIndexReferenceTask."""
import os
import multiprocessing

import astropy.units as u
import numpy as np

import lsst.sphgeom
import lsst.afw.table as afwTable
from lsst.meas.algorithms import IngestIndexedReferenceTask
from lsst.meas.algorithms.ingestIndexReferenceTask import addRefCatMetadata
from lsst.meas.algorithms.ingestIndexManager import IngestIndexManager


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
        self._couter = 0

    def run(self, inputFiles):
        """Index a set of input files from a reference catalog, and write the
        output to the appropriate filenames, in parallel.
        Parameters
        ----------
        inputFiles : `list`
            A list of file paths to read data from.
        """
        self.nInputFiles = len(inputFiles)

        with multiprocessing.Manager():
            self._counter = 0
            self._file_progress = 0
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
        inputData = self.file_reader.run(filename)
        fluxes = self._getFluxes(inputData)
        coordErr = self._getCoordErr(inputData)
        matchedPixels = self.indexer.indexPoints(inputData[self.config.ra_name],
                                                 inputData[self.config.dec_name])
        pixel_ids = set(matchedPixels)
        for pixelId in pixel_ids:
            self._doOnePixel(inputData, matchedPixels, pixelId, fluxes, coordErr)

        oldPercent = 100 * self._file_progress / self.nInputFiles
        self._file_progress += 1
        percent = 100 * self._file_progress / self.nInputFiles
        # only log each "new percent"
        if np.floor(percent) - np.floor(oldPercent) >= 1:
            self.log.info("Completed %d / %d files: %d %% complete ",
                          self._file_progress,
                          self.nInputFiles,
                          percent)

    def _doOnePixel(self, inputData, matchedPixels, pixelId, fluxes, coordErr):
        """Process one HTM pixel, appending to an existing catalog or creating
        a new catalog, as needed.
        Parameters
        ----------
        inputData : `numpy.ndarray`
            The data from one input file.
        matchedPixels : `numpy.ndarray`
            The row-matched pixel indexes corresponding to ``inputData``.
        pixelId : `int`
            The pixel index we are currently processing.
        fluxes : `dict` [`str`, `numpy.ndarray`]
            The values that will go into the flux and fluxErr fields in the
            output catalog.
        coordErr : `dict` [`str`, `numpy.ndarray`]
            The values that will go into the coord_raErr, coord_decErr, and
            coord_ra_dec_Cov fields in the output catalog (in radians).
        """
        idx = np.where(matchedPixels == pixelId)[0]
        catalog = self.getCatalog(pixelId, self.schema, len(idx))
        for outputRow, inputRow in zip(catalog[-len(idx):], inputData[idx]):
            self._fillRecord(outputRow, inputRow)

        self._setIds(inputData[idx], catalog)

        # set fluxes from the pre-computed array
        for name, array in fluxes.items():
            catalog[self.key_map[name]][-len(idx):] = array[idx]

        # set coordinate errors from the pre-computed array
        for name, array in coordErr.items():
            catalog[name][-len(idx):] = array[idx]

        catalog.writeFits(self.filenames[pixelId])

    def _setIds(self, inputData, catalog):
        """ Fill the `id` field of catalog with a running index.
        last values up to the length of ``inputData``.
        Fill with `self.config.id_name` if specified, otherwise use the
        global running counter value.
        Parameters
        ----------
        inputData : `numpy.ndarray`
            The input data that is being processed.
        catalog : `lsst.afw.table.SimpleCatalog`
            The output catalog to fill the ids.
        """
        size = len(inputData)
        if self.config.id_name:
            catalog['id'][-size:] = inputData[self.config.id_name]
        else:
            idEnd = self._counter + size
            catalog['id'][-size:] = np.arange(self._counter, idEnd)
            self._counter = idEnd


class HuntsmanIngestIndexedReferenceTask(IngestIndexedReferenceTask):
    """ Overrides to make reference catalogues using Gen3 butler.
    These overrides should be considered as hacks while we wait for the official implementation.
    """
    _template = "ref_cats/%(name)s/%(pixel_id)s.fits"  # Template normally specified in gen2 policy

    def __init__(self, output_directory, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.IngestManager = singleProccessIngestIndexManager
        self.output_directory = output_directory

    def createIndexedCatalog(self, inputFiles):
        """ Override method to remove Gen2 Butler put. """
        schema, key_map = self._saveMasterSchema(inputFiles[0])

        # create an HTM we can interrogate about pixel ids
        htm = lsst.sphgeom.HtmPixelization(self.indexer.htm.get_depth())
        filename_dict = self._getButlerFilenames(htm)
        worker = self.IngestManager(filename_dict,
                                    self.config,
                                    self.file_reader,
                                    self.indexer,
                                    schema,
                                    key_map,
                                    htm.universe()[0],
                                    addRefCatMetadata,
                                    self.log)
        worker.run(inputFiles)

        # write the config that was used to generate the refcat
        # dataId = self.indexer.makeDataId(None, self.config.dataset_config.ref_dataset_name)
        # self.butler.put(self.config.dataset_config, 'ref_cat_config', dataId=dataId)

        # Return the filenames so it is easy to ingest them with Gen3 Butler
        return {htmId: filename for htmId, filename in filename_dict.items() if os.path.isfile(
            filename)}

    def _saveMasterSchema(self, filename):
        """ Override method to remove Gen2 Butler put. """
        arr = self.file_reader.run(filename)
        schema, key_map = self.makeSchema(arr.dtype)

        # dataId = self.indexer.makeDataId('master_schema',
        #                                  self.config.dataset_config.ref_dataset_name)

        catalog = afwTable.SimpleCatalog(schema)
        addRefCatMetadata(catalog)
        # self.butler.put(catalog, 'ref_cat', dataId=dataId)

        return schema, key_map

    def _getButlerFilenames(self, htm):
        """ Override to get filenames for each output pixel without using Gen2 butler. """

        filenames = {}
        start, end = htm.universe()[0]

        # Get path to first index
        dataId = self.indexer.makeDataId(start, self.config.dataset_config.ref_dataset_name)
        path = os.path.join(self.output_directory, self._template % dataId)

        os.makedirs(os.path.dirname(path), exist_ok=True)

        # Hack path for the other indices
        base = os.path.join(os.path.dirname(path), "%d"+os.path.splitext(path)[1])

        for pixelId in range(start, end):
            filenames[pixelId] = base % pixelId

        return filenames
