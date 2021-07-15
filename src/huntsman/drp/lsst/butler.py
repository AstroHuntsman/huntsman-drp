import os
import random
from tempfile import TemporaryDirectory

import lsst.daf.butler as dafButler
from lsst.daf.butler import DatasetType
from lsst.daf.butler.script.certifyCalibrations import certifyCalibrations

from lsst.obs.base.utils import getInstrument
from lsst.obs.base import RawIngestTask, RawIngestConfig
from lsst.obs.base.script.defineVisits import defineVisits

from huntsman.drp.base import HuntsmanBase
from huntsman.drp.lsst.utils import pipeline
from huntsman.drp.lsst.utils.refcat import RefcatIngestor
from huntsman.drp.lsst.utils import butler as utils


class ButlerRepository(HuntsmanBase):
    """ This class provides a convenient interface to LSST Butler functionality.
    The goal of ButlerRepository is to facilitate simple calls to LSST code, including running
    pipelines to construct calibs and coadds.
    """
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
        # This is where new calibs will be created
        self._calib_directory = os.path.join("Huntsman", "calib")

        # Calib collection relative to root
        # This is where calibs are registered
        if calib_collection is None:
            calib_collection = os.path.join(self._calib_directory, "CALIB")
        self._calib_collection = calib_collection

        self._initialise_repository()

    # Properties

    @property
    def search_collections(self):
        """ Get default search collections. """
        butler = self.get_butler()
        collections = set()

        # Temporary workaround because we cannot query CALIBRATION collections yet
        # TODO: Remove when implemented in LSST code
        for collection in butler.registry.queryCollections():

            collection_type = butler.registry.getCollectionType(collection).name

            if collection_type not in ("CALIBRATION", "CHAINED"):
                collections.add(collection)

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
            return {k: document[k] for k in self.get_dimension_names(datasetType, required=True)}
        except KeyError as err:
            raise KeyError(f"Unable to determine dataId from {document}: {err!r}")

    def get_butler(self, *args, **kwargs):
        """ Get a butler object for this repository.
        Args:
            *args, **kwargs: Parsed to dafButler.Butler.
        Returns:
            butler: The butler object.
        """
        return dafButler.Butler(self.root_directory, *args, **kwargs)

    def get(self, datasetType, collections=None, **kwargs):
        """ Get an item from the Butler repository.
        Args:
            collections (iterable of str, optional): Search for item in these collections. If not
                provided, will use default search collections.
            *args, **kwargs: Parsed to butler.get.
        Returns:
            object: The retrieved object.
        """
        if collections is None:
            collections = self.search_collections
        butler = self.get_butler(collections=collections)
        return butler.get(datasetType, **kwargs)

    def get_dimension_names(self, datasetType, required=False, **kwargs):
        """ Get dimension names in a dataset type.
        Args:
            datasetType (str): The dataset type (raw, flat, bias etc.).
            required (bool, optional): If True, only return dimensions that are required in the
                dataId. Default: False.
        Returns:
            list of str: A list of keys.
        """
        butler = self.get_butler(**kwargs)
        datasetTypeInstance = butler.registry.getDatasetType(datasetType)
        dimensions = datasetTypeInstance.dimensions
        if required:
            dimensions = dimensions.required
        return [d.name for d in dimensions]

    def get_filenames(self, datasetType, **kwargs):
        """ Get filenames matching a datasetType and dataId.
        Args:
            datasetType (str): The dataset type (raw, flat, bias etc.).
        Returns:
            list of str: The filenames.
        """
        datasetRefs, butler = self._get_datasetRefs(datasetType, get_butler=True, **kwargs)
        return [butler.getURI(ref).path for ref in datasetRefs]

    def get_dataIds(self, datasetType, as_dict=True, **kwargs):
        """ Get ingested dataIds for a given datasetType.
        Args:
            datasetType (str): The datasetType (raw, bias, flat etc.).
            as_dict (bool, optional): If True, return as a dictionary rather than dataId object.
                Default: True.
            **kwargs: Parsed to self._get_datasetRefs.
        Returns:
            list of dict: A list of dataIds.
        """
        datasetRefs = self._get_datasetRefs(datasetType, **kwargs)
        dataIds = []
        for datasetRef in datasetRefs:
            dataId = datasetRef.dataId
            if as_dict:
                dataId = dataId.to_simple().dict()["dataId"]
            dataIds.append(dataId)
        return dataIds

    def ingest_raw_files(self, filenames, transfer="symlink", **kwargs):
        """ Ingest raw files into the Butler repository.
        Args:
            filenames (iterable of str): The list of raw data filenames.
            **kwargs: Parsed to self.get_butler.
        """
        filenames = set([os.path.abspath(os.path.realpath(_)) for _ in filenames])
        self.logger.debug(f"Ingesting {len(filenames)} files into {self}.")

        kwargs.update({"writeable": True})
        butler = self.get_butler(run=self._raw_collection, **kwargs)

        task_config = RawIngestConfig()
        task_config.transfer = transfer

        task = RawIngestTask(config=task_config, butler=butler)
        task.run(filenames)

    def ingest_calibs(self, datasetType, filenames, collection=None, begin_date=None,
                      end_date=None, **kwargs):
        """ Ingest pre-prepared master calibs into the Butler repository.
        Calibs ingested by a single call to this function are assumed to be valid over the same
        date range. Calibs that differ by date should be stored in separate collections for now.
        Args:
            datasetType (str): The dataset type (e.g. bias, flat).
            filenames (list of str): The files to ingest.
            collection (str, optional): The collection to ingest into.
            **kwargs: Parsed to utils.ingest_calibs.
        """
        butler = self.get_butler(writeable=True)

        # Define the collection that the calibs will be ingested into
        # NOTE: There is nothing in the path to distinguish between dates, so use random for now
        if collection is None:
            randstr = f"{random.randint(0, 1E+6):06d}"
            collection = os.path.join(self._calib_directory, "calib", "ingest", randstr,
                                      datasetType)

        self.logger.info(f"Ingesting {len(filenames)} {datasetType} calibs in"
                         f" collection: {collection}")

        dimension_names = self.get_dimension_names(datasetType, required=True)

        utils.ingest_calibs(butler, datasetType, filenames=filenames, collection=collection,
                            dimension_names=dimension_names, **kwargs)

        # Certify the calibs
        self._certify_calibrations(datasetType, collection, begin_date, end_date)

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
        self._certify_calibrations(datasetType, output_collection, begin_date, end_date)

    def construct_calexps(self, dataIds=None, output_collection="calexp", **kwargs):
        """ Create calibrated exposures (calexps) from raw exposures.
        Args:
            dataIds (list of dict, optional): List of dataIds to process. If None (default),
                will use all appropriate ingested raw files.
            output_collection (str, optional): The name of the output collection. If None (default),
                will determine automatically.
            **kwargs: Parsed to pipeline.pipetask_run.
        """
        # If dataIds not provided, make calexp using all ingested dataIds of the correct type
        if dataIds is None:
            dataIds = self.get_dataIds("raw", where="exposure.observation_type='science'")

        # Specify the input collections we need to make the calexps
        input_collections = (self._raw_collection, self._calib_collection, self._refcat_collection)

        # Define visits
        # TODO: Figure out what this actually does
        defineVisits(self.root_directory, config_file=None, collections=self._raw_collection,
                     instrument=self._instrument_name)

        # Run task
        pipeline.pipetask_run("processCcd", self.root_directory, dataIds=dataIds,
                              output_collection=output_collection,
                              input_collections=input_collections, **kwargs)

    # Private methods

    def _initialise_repository(self):
        """ Initialise a new butler repository. """
        try:
            dafButler.Butler.makeRepo(self.root_directory)
        except FileExistsError:
            self.logger.info(f"Found existing butler repository: {self.root_directory}")
            return
        self.logger.info(f"Creating new butler repository: {self.root_directory}")

        butler = self.get_butler(writeable=True)  # Creates empty butler repo

        # Register the Huntsman instrument config with the repo
        instrInstance = getInstrument(self._instrument_class_str, butler.registry)
        instrInstance.register(butler.registry)

        # Setup instrument and calibrations collection
        instr = getInstrument(self._instrument_name, butler.registry)
        instr.writeCuratedCalibrations(butler, collection=self._calib_collection, labels=())

        # Register calib dataset types
        self._register_calib_datasetTypes(butler)

    def _register_calib_datasetTypes(self, butler):
        """ Register calib dataset types with the repository. """
        universe = butler.registry.dimensions

        for dataset_type, dimension_names in self.config["calibs"]["required_fields"].items():
            datasetType = DatasetType(dataset_type, dimensions=dimension_names, universe=universe,
                                      storageClass="ExposureF", isCalibration=True)
            butler.registry.registerDatasetType(datasetType)

    def _get_datasetRefs(self, datasetType, collections=None, get_butler=False, **kwargs):
        """ Return datasetRefs for a given datasetType matching dataId.
        Args:
            datasetType (str): The datasetType.
            dataId (dict, optional): If given, returned datasetRefs match with this dataId.
            get_butler (bool, optional): If True, also return the Butler object. Default: False.
            **kwargs: Parsed to self.get_butler.
        Returns:
            lsst.daf.butler.registry.queries.ChainedDatasetQueryResults: The query results.
        """
        if collections is None:
            collections = self.search_collections
        butler = self.get_butler(collections=collections)

        datasetRefs = butler.registry.queryDatasets(
            datasetType=datasetType, collections=collections, **kwargs)

        if get_butler:
            return datasetRefs, butler
        return datasetRefs

    def _certify_calibrations(self, datasetType, collection, begin_date, end_date):
        """ Certify the calibs in a collection.
        This associates them with the repository's calib collection and a date validity range.
        """
        certifyCalibrations(repo=self.root_directory,
                            input_collection=collection,
                            output_collection=self._calib_collection,
                            search_all_inputs=False,
                            dataset_type_name=datasetType,
                            begin_date=begin_date,
                            end_date=end_date)


class TemporaryButlerRepository():
    """ Class to return a ButlerRepository in a temporary directory.
    Used as a context manager.
    """

    def __init__(self, prefix=None,  *args, **kwargs):
        self._args = args
        self._kwargs = kwargs
        self._prefix = prefix
        self._tempdir = None

    def __enter__(self):
        self._tempdir = TemporaryDirectory(prefix=self._prefix)
        return ButlerRepository(self._tempdir.name, *self._args, **self._kwargs)

    def __exit__(self, *args, **kwargs):
        self._tempdir.cleanup()
        self._tempdir = None
