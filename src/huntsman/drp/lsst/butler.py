import os
from functools import lru_cache
from contextlib import suppress

import lsst.daf.butler as dafButler
from lsst.obs.base.utils import getInstrument
from lsst.obs.base import RawIngestTask, RawIngestConfig
from lsst.daf.butler.script.certifyCalibrations import certifyCalibrations

from huntsman.drp.base import HuntsmanBase
from huntsman.drp.lsst.utils import pipeline
from huntsman.drp.lsst.utils.refcat import RefcatIngestor


class ButlerRepository(HuntsmanBase):

    _instrument_name = "Huntsman"  # TODO: Move to config
    _instrument_class_str = "lsst.obs.huntsman.HuntsmanCamera"  # TODO: Move to config

    _raw_collection = f"{_instrument_name}/raw/all"
    _refcat_collection = "refCat"

    def __init__(self, directory, calib_collection=None, **kwargs):
        """
        Args:
            directory (str): The path of the butler reposity.
        """
        super().__init__(**kwargs)

        if directory is not None:
            directory = os.path.abspath(directory)
        self.root_directory = directory

        # Calib directory relative to root
        self._calib_directory = os.path.join("Huntsman", "calib")

        if calib_collection is None:
            calib_collection = f"{self._instrument_name}/calib/CALIB"
        self._calib_collection = calib_collection

        # These are the collections to be searched by default
        self.collections = set([self._raw_collection,
                                self._refcat_collection])

        self._initialise_repository()

    # Properties

    @property
    def search_collections(self):
        """ Get default search collections. """
        butler = self.get_butler()

        collections = set(butler.registry.queryCollections())

        with suppress(KeyError):
            collections.remove(self._calib_collection)
            collections.remove("Huntsman/calib/unbounded")

        return collections

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

    def get_butler(self, *args, **kwargs):
        """ Get a butler object for a given rerun.
        We cache created butlers to avoid the overhead of having to re-create them each time.
        Args:
            *args, **kwargs: Parsed to dafButler.Butler.
        Returns:
            butler: The butler object.
        """
        return dafButler.Butler(self.root_directory, *args, **kwargs)

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
        ingestor = RefcatIngestor(butler=butler, collection=self._refcat_collection)

        self.logger.debug(f"Ingesting reference catalogue from {len(filenames)} file(s).")
        ingestor.run(filenames)

    def construct_calibs(self, datasetType, dataIds=None, begin_date=None, end_date=None,
                         output_collection=None, **kwargs):
        """ Make a master calib from ingested raw exposures.
        Args:
            calib_doc (CalibDocument): The calib document of the calib to make.
        Returns:
            str: The filename of the newly created master calib.
        """
        # If dataIds not provided, make calib using all ingested dataIds of the correct type
        if dataIds is None:
            dataIds = self.get_dataIds("raw", where=f"exposure.observation_type='{datasetType}'")

        self.logger.info(f"Making master {datasetType}(s) from {len(dataIds)} dataIds.")

        # Specify the input collections we need to make the calibs
        input_collections = (self._raw_collection, self._calib_collection)

        # Make the calibs in their own collection
        if output_collection is None:
            output_collection = os.path.join(self._calib_directory, f"{datasetType}")

        # Make the master calibs
        calib_type = datasetType.title()  # Capitalise first letter
        pipeline.pipetask_run(f"construct{calib_type}", self.root_directory, dataIds=dataIds,
                              output_collection=output_collection,
                              input_collections=input_collections, **kwargs)

        # Certify the calibs
        # This associates them with the calib collection and a validity range
        certifyCalibrations(repo=self.root_directory,
                            input_collection=output_collection,
                            output_collection=self._calib_collection,
                            search_all_inputs=False,
                            dataset_type_name=datasetType,
                            begin_date=begin_date,
                            end_date=end_date)

        # Add the output collection to the default search collections
        self.collections.add(output_collection)

    def construct_calexps(self):
        """
        """

        # Define visits
        # TODO: Figure out why this is necessary
        # butler define-visits ./repo lsst.obs.decam.DarkEnergyCamera --collections DECam/raw/object

        # Get dataIds

        # Run task

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
        instr.writeCuratedCalibrations(butler, collection=self._calib_collection, labels=())

    def _get_datasetRefs(self, datasetType, collections=None, **kwargs):
        """ Return datasetRefs for a given datasetType matching dataId.
        Args:
            datasetType (str): The datasetType.
            dataId (dict, optional): If given, returned datasetRefs match with this dataId.
            **kwargs: Parsed to self.get_butler.
        Returns:
            lsst.daf.butler.registry.queries.ChainedDatasetQueryResults: The query results.
        """
        butler = self.get_butler(collections=self.search_collections)
        return butler.registry.queryDatasets(datasetType=datasetType,
                                             collections=self.search_collections,
                                             **kwargs)
