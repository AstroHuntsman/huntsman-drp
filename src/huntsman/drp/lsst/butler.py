import os
import random
from tempfile import TemporaryDirectory

import lsst.daf.butler as dafButler
from lsst.daf.butler import DatasetType
from lsst.daf.butler.script.certifyCalibrations import certifyCalibrations

from lsst.obs.base.utils import getInstrument
from lsst.obs.base.script.defineVisits import defineVisits
from lsst.obs.base import RawIngestTask

from lsst.pipe.tasks.makeDiscreteSkyMap import MakeDiscreteSkyMapTask

from huntsman.drp.base import HuntsmanBase
from huntsman.drp.lsst.utils import pipeline
from huntsman.drp.lsst.utils.refcat import RefcatIngestor
from huntsman.drp.lsst.utils import butler as utils


class ButlerRepository(HuntsmanBase):
    """ This class provides a convenient interface to LSST Butler functionality.
    The goal of ButlerRepository is to facilitate simple calls to LSST code, including running
    pipelines to construct calibs and run pipelines.
    """
    _instrument_name = "Huntsman"  # TODO: Move to config
    _instrument_class_name = "lsst.obs.huntsman.HuntsmanCamera"  # TODO: Move to config

    _raw_collection = f"{_instrument_name}/raw/all"
    _refcat_collection = "refCat"
    _skymap_collection = "skymaps"

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

        self._instrument = None
        self._initialise_repository()

    def __str__(self):
        return f"{self.__class__.__name__} ({self.root_directory})"

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
                dataId = utils.dataId_to_dict(dataId)
            dataIds.append(dataId)
        return dataIds

    def ingest_raw_files(self, filenames, transfer="symlink", define_visits=False,
                         skip_existing=True, **kwargs):
        """ Ingest raw files into the Butler repository.
        Args:
            filenames (iterable of str): The list of raw data filenames.
            skip_existing (bool, optional): If True (default), do not attempt to ingest exposures
                that are already present. Else, an error is raised.
            **kwargs: Parsed to self.get_butler.
        Returns:
            list of dict: List of failed dataIds.
        """
        filenames = set([os.path.abspath(os.path.realpath(_)) for _ in filenames])
        self.logger.debug(f"Ingesting {len(filenames)} files into {self}.")

        kwargs.update({"writeable": True})
        butler = self.get_butler(run=self._raw_collection, **kwargs)

        failed_dataIds = []

        # Define callback for failed ingestion
        def on_ingest_failure(rawExposureData, exception):
            dataId = utils.dataId_to_dict(rawExposureData.dataId)
            self.logger.warning(f"Failure during butler ingestion: {dataId}: {exception!r}")
            failed_dataIds.append(dataId)

        # Create the configured task instance
        task = self._make_task(RawIngestTask,
                               butler=butler,
                               config_overrides={"transfer": transfer},
                               on_ingest_failure=on_ingest_failure)

        # Ingest the files. If there is an error on one file, other files will still be ingested
        try:
            task.run(filenames, skip_existing_exposures=skip_existing)
        except RuntimeError as err:
            self.logger.error(f"{err}. Proceeding anyway.")

        if define_visits:
            self.define_visits()

        return failed_dataIds

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
                            dimension_names=dimension_names, logger=self.logger, **kwargs)

        # Certify the calibs
        self._certify_calibrations(datasetType, collection, begin_date, end_date)

    def ingest_calib_docs(self, calib_docs, **kwargs):
        """ Convenience function to ingest master calibs from calib documents.
        Args:
            calib_docs (list): The list of CalibDocuments.
            **kwargs: Parsed to self.ingest_calibs.
        """
        datasetTypes = set([d["datasetType"] for d in calib_docs])
        for datasetType in datasetTypes:
            filenames = [d["filename"] for d in calib_docs if d["datasetType"] == datasetType]
            self.ingest_calibs(datasetType, filenames, **kwargs)

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

    def run_pipeline(self, pipeline_name, output_collection, input_collections=None, **kwargs):
        """ Run a LSST pipeline.
        Args:
            pipeline_name (str): The pipeline file name. If not an absolute path, assumed relative
                to the $OBS_HUNTSMAN pipeline directory. Should not include the file extension.
            output_collection (str): The name of the butler output collection.
            input_collections (iterable of str, optional): The input collections to use. If not
                provided, will attempt to determine automatically.
            **kwargs: Parsed to pipeline.pipetask_run.
        Returns:
            object: The result of the call to pipeline.pipetask_run.
        """
        if input_collections is None:
            input_collections = [self._raw_collection,
                                 self._refcat_collection,
                                 self._calib_collection]
            if os.path.isdir(os.path.join(self.root_directory, self._skymap_collection)):
                input_collections.append(self._skymap_collection)

        return pipeline.pipetask_run(pipeline_name,
                                     self.root_directory,
                                     instrument=self._instrument_class_name,
                                     output_collection=output_collection,
                                     input_collections=input_collections, **kwargs)

    def construct_calibs(self, datasetType, dataIds=None, begin_date=None, end_date=None,
                         output_collection=None, **kwargs):
        """ Make a master calib from ingested raw exposures.
        Args:
            datasetType (str): The name of the datasetType (i.e. calib type) to create.
            dataIds (iterable of dict, optional): The list of dataIds to process. If not provided,
                will use all ingested raw exposures of the appropriate observation type.
            begin_date (datetime.datetime, optional): If provided, signifies the date from which
                the produced calibs will be valid.
            end_date (datetime.datetime): If provided, signifies the date up until which the
                produced catlobs will be valid.
            output_collection (str, optional): The butler output collection. If not provided,
                will assume from the datasetType.
            **kwargs: Parsed to self.run_pipeline.
        Returns:
            str: The filename of the newly created master calib.
        """
        # If dataIds not provided, make calib using all ingested dataIds of the correct type
        if dataIds is None:
            # Defects are made from darks
            if datasetType == "defects":
                dataIds = self.get_dataIds("raw", where="exposure.observation_type='dark'")
            else:
                dataIds = self.get_dataIds(
                    "raw", where=f"exposure.observation_type='{datasetType}'")

        self.logger.info(f"Making master {datasetType}(s) from {len(dataIds)} dataIds.")

        # Specify collections we will use as inputs
        input_collections = (self._raw_collection, self._calib_collection)

        # Make the calibs in their own collection
        if output_collection is None:
            output_collection = os.path.join(self._calib_directory, f"{datasetType}")

        # Make the master calibs
        pipeline_name = f"construct{datasetType.title()}"
        self.run_pipeline(pipeline_name, output_collection=output_collection,
                          dataIds=dataIds, input_collections=input_collections, **kwargs)

        # Certify the calibs
        self._certify_calibrations(datasetType, output_collection, begin_date, end_date)

    def construct_calexps(self, dataIds=None, output_collection="calexp",
                          pipeline_name="processCcd", **kwargs):
        """ Create calibrated exposures (calexps) from raw exposures.
        Args:
            dataIds (list of dict, optional): List of dataIds to process. If None (default),
                will use all appropriate ingested raw files.
            output_collection (str, optional): The name of the output collection. If None (default),
                will determine automatically.
            pipeline_name (str, optional): The name of the pipeline to run. Default: "processCcd".
            **kwargs: Parsed to pipeline.pipetask_run.
        """
        # If dataIds not provided, make calexp using all ingested dataIds of the correct type
        if dataIds is None:
            dataIds = self.get_dataIds("raw", where="exposure.observation_type='science'")

        # Define visits
        # TODO: Figure out what this actually does
        self.define_visits()

        # Run pipeline
        self.run_pipeline(pipeline_name, output_collection=output_collection,
                          dataIds=dataIds, **kwargs)

    def construct_skymap(self, datasetType="raw", skymap_id="discrete", **kwargs):
        """ Construct a skyMap from ingested science exposures.
        NOTE: This currently cannot be done inside a LSST pipeline.
        Args:
            datasetType (str, optional): The dataset type name. Default: "raw".
            skymap_id (str, optional): The name of the skymap. Default: "discrete".
            **kwargs: Parsed to self._make_task.
        """
        collections = [self._raw_collection]
        butler = self.get_butler(collections=collections, writeable=True)

        # Get datasets from which to extract bounding boxes and WCS
        datasets = self._get_datasetRefs(datasetType, where="exposure.observation_type='science'",
                                         **kwargs)

        self.logger.info(f"Constructing skyMap from {datasetType} exposures.")

        wcs_bbox_tuple_list = []
        for ref in datasets:
            exp = butler.getDirect(ref)
            wcs_bbox_tuple_list.append((exp.getWcs(), exp.getBBox()))

        task = self._make_task(MakeDiscreteSkyMapTask, **kwargs)
        result = task.run(wcs_bbox_tuple_list, oldSkyMap=None)
        result.skyMap.register(skymap_id, butler)

    def define_visits(self):
        """ Define visits from raw exposures.
        This must be called before constructing calexps.
        """
        self.logger.debug(f"Defining visits in {self}.")
        defineVisits(self.root_directory, config_file=None, collections=self._raw_collection,
                     instrument=self._instrument_name)

    # Private methods

    def _initialise_repository(self):
        """ Initialise a new butler repository. """
        try:
            dafButler.Butler.makeRepo(self.root_directory)

        except FileExistsError:
            self.logger.info(f"Found existing butler repository: {self.root_directory}")
            butler = self.get_butler(writeable=True)
            self._instrument = getInstrument(self._instrument_name, butler.registry)
            return

        self.logger.info(f"Creating new butler repository: {self.root_directory}")

        butler = self.get_butler(writeable=True)  # Creates empty butler repo

        # Register the Huntsman instrument config with the repo
        instrInstance = getInstrument(self._instrument_class_name, butler.registry)
        instrInstance.register(butler.registry)

        # Setup instrument and calibrations collection
        instr = getInstrument(self._instrument_name, butler.registry)
        instr.writeCuratedCalibrations(butler, collection=self._calib_collection, labels=())
        self._instrument = instr

        # Register calib dataset types
        self._register_calib_datasetTypes(butler)

    def _register_calib_datasetTypes(self, butler):
        """ Register calib dataset types with the repository.
        This is necessary because we want to ingest master calibs into new butler repositories,
        where datasetTypes may not already be defined.
        """
        universe = butler.registry.dimensions

        for dataset_type, dimension_names in self.config["calibs"]["required_fields"].items():
            storageClass = "Defects" if dataset_type == "defects" else "ExposureF"
            # Create DatasetType instance
            datasetType = DatasetType(dataset_type, dimensions=dimension_names, universe=universe,
                                      storageClass=storageClass, isCalibration=True)
            # Register datasetType with Butler registry
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

    def _make_task(self, taskClass, config_overrides=None, **kwargs):
        """ Make the task and apply instrument overrides to the config.
        Args:
            taskClass (Class): The task class.
            **kwargs: Parsed to task initialiser.
        Returns:
            Task: The initialised Task class.
        """
        # Load the default instrument-specific config
        config = taskClass.ConfigClass()
        self._instrument.applyConfigOverrides(taskClass._DefaultName, config)

        # Apply additional config overrides
        if config_overrides:
            for k, v in config_overrides.items():
                setattr(config, k, v)

        # Return the task instance
        return taskClass(config=config, **kwargs)


class TemporaryButlerRepository(HuntsmanBase):
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
