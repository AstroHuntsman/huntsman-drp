import os
from functools import lru_cache

import lsst.daf.butler as dafButler
from lsst.obs.base.utils import getInstrument
from lsst.obs.base import RawIngestTask, RawIngestConfig

from huntsman.drp.base import HuntsmanBase
from huntsman.drp.lsst.utils import pipeline
from huntsman.drp.lsst.utils.refcat import RefcatIngestor


class ButlerRepository(HuntsmanBase):

    _instrument_name = "Huntsman"  # TODO: Move to config
    _instrument_class_str = "lsst.obs.huntsman.HuntsmanCamera"  # TODO: Move to config

    _raw_collection = f"{_instrument_name}/raw/all"

    def __init__(self, directory, calib_collection=None, **kwargs):
        """
        Args:
            directory (str): The path of the butler reposity.
        """
        super().__init__(**kwargs)

        if directory is not None:
            directory = os.path.abspath(directory)
        self.root_directory = directory

        if calib_collection is None:
            calib_collection = f"{self._instrument_name}/calib"
        self._calib_collection = calib_collection

        # These are the collections to be used by default
        self.collections = set([self._raw_collection, self._calib_collection])

        self._initialise_repository()

    # Methods

    def document_to_dataId(self, document, datasetType="raw"):
        """ Extract an LSST dataId from a Document.
        Args:
            document (Document): The document to convert.
        Returns:
            dict: The corresponding dataId.
        """
        try:
            return {k: document[k] for k in self.get_dimension_names(datasetType)}
        except KeyError as err:
            raise KeyError(f"Unable to determine dataId from {document}: {err!r}")

    def document_to_calibId(self, document):
        """ Extract an LSST dataId from a CalibDocument.
        Args:
            document (CalibDocument): The calib document.
        Returns:
            dict: The calibId.
        """
        datasetType = document["datasetType"]
        return self.document_to_dataId(document, datasetType=datasetType)

    @lru_cache()  # Use caching so we don't have to keep reinitialising Butler objects
    def get_butler(self, collections=None, *args, **kwargs):
        """ Get a butler object for a given rerun.
        We cache created butlers to avoid the overhead of having to re-create them each time.
        Args:
            collections (list of str):
            *args, **kwargs:
        Returns:
            butler: The butler object.
        """
        collections = self.collections if collections is None else collections
        return dafButler.Butler(self.root_directory, collections=collections, *args, **kwargs)

    def get_dimension_names(self, datasetType, **kwargs):
        """ Get dimension names in a dataset type.
        Args:
            datasetType (str): The dataset type (raw, flat, bias etc.).
        Returns:
            list of str: A list of keys.
        """
        butler = self.get_butler(**kwargs)
        datasetTypeInstance = butler.registry.getDatasetType(datasetType)
        return [d.name for d in datasetTypeInstance.dimensions]

    def get_filenames(self, datasetType, dataId, **kwargs):
        """ Get filenames matching a datasetType and dataId.
        Args:
            datasetType (str): The dataset type (raw, flat, bias etc.).
            dataId (dict): The dataId.
        Returns:
            list of str: The filenames.
        """
        butler = self.get_butler(**kwargs)
        datasetRefs = butler.registry.queryDatasets(datasetType=datasetType,
                                                    collections=butler.collections,
                                                    dataId=dataId)
        return [butler.getURI(ref).path for ref in datasetRefs]

    def get_dataIds(self, datasetType, **kwargs):
        """ Get ingested dataIds for a given datasetType.
        Args:
            datasetType (str): The datasetType (raw, bias, flat etc.).
            dataId (dict, optional): A complete or partial dataId to match with.
        Returns:
            list of dict: A list of dataIds.
        """
        datasetRefs = self._get_datasetRefs(datasetType, **kwargs)
        return [d.dataId for d in datasetRefs]

    def ingest_raw_files(self, filenames, **kwargs):
        """ Ingest raw files into the Butler repository.
        Args:
            filenames (iterable of str): The list of raw data filenames.
            **kwargs: Parsed to self.get_butler.
        """
        filenames = set([os.path.abspath(os.path.realpath(_)) for _ in filenames])
        self.logger.debug(f"Ingesting {len(filenames)} file(s).")

        kwargs.update({"writeable": True})
        butler = self.get_butler(**kwargs)

        task_config = RawIngestConfig()  # TODO: Check if this gets overriden automatically
        # Symlink files rather than copying them
        # TODO: Remove in favour of config override
        task_config.transfer = "symlink"

        task = RawIngestTask(config=task_config, butler=butler)
        task.run(filenames)

    def ingest_reference_catalogue(self, filenames, **kwargs):
        """ Ingest the reference catalogue into the repository.
        Args:
            filenames (iterable of str): A list of filenames containing reference catalogue.
            **kwargs: Parsed to self.get_butler.
        """
        butler = self.get_butler(writeable=True, **kwargs)
        ingestor = RefcatIngestor(butler=butler)

        self.logger.debug(f"Ingesting reference catalogue from {len(filenames)} file(s).")
        ingestor.run(filenames)

    def make_master_calibs(self, datasetType, dataIds=None, begin_date=None, end_date=None,
                           output_collection=None, **kwargs):
        """ Make a master calib from ingested raw exposures.
        Args:
            calib_doc (CalibDocument): The calib document of the calib to make.
        Returns:
            str: The filename of the newly created master calib.
        """
        butler = self.get_butler()

        # If dataIds not provided, make calib using all ingested dataIds of the correct type
        if dataIds is None:
            dataIds = self.get_dataIds(datasetType,
                                       where=f"exposure.observation_type='{datasetType}'")

        self.logger.info(f"Making master {datasetType}s for from {len(dataIds)} dataIds.")

        # Make the calibs in their own collection / run
        if output_collection is None:
            output_collection = os.path.join(self._calib_collection, f"{datasetType}")

        # Ensure the output collection is setup
        butler.registry.registerCollection(output_collection, type=dafButler.CollectionType.RUN)
        butler.registry.registerDatasetType(datasetType)

        # Make the master calibs
        calib_type = datasetType.title()  # Capitalise first letter
        pipeline.pipetask_run(f"construct{calib_type}", dataIds=dataIds,
                              output_collection=output_collection,
                              input_collection=self.collections, **kwargs)

        # Certify the calibs
        # This associates them with the calib collection and a validity range
        dafButler.script.certifyCalibrations(repo=self.root_directory,
                                             input_collection=output_collection,
                                             output_collection=self._calib_collection,
                                             dataset_type_name=datasetType,
                                             begin_date=begin_date,
                                             end_date=end_date)

    # Private methods

    def _initialise_repository(self):
        """ Initialise a new butler repository. """
        try:
            dafButler.Butler.makeRepo(self.root_directory)
        except FileExistsError:
            return
        butler = self.get_butler(writeable=True)  # Creates empty butler repo

        # Register the Huntsman instrument config with the repo
        instrInstance = getInstrument(self._instrument_class_str, butler.registry)
        instrInstance.register(butler.registry)

        # Setup camera and calibrations
        instr = getInstrument(self._instrument_name, butler.registry)
        instr.writeCuratedCalibrations(butler, collection=None, labels=())

    def _get_datasetRefs(self, datasetType, dataId=None, **kwargs):
        """ Return datasetRefs for a given datasetType matching dataId.
        Args:
            datasetType (str): The datasetType.
            dataId (dict, optional): If given, returned datasetRefs match with this dataId.
            **kwargs: Parsed to self.get_butler.
        Returns:
            lsst.daf.butler.registry.queries.ChainedDatasetQueryResults: The query results.
        """
        butler = self.get_butler(**kwargs)
        return butler.registry.queryDatasets(datasetType=datasetType,
                                             collections=butler.collections,
                                             dataId=dataId)
