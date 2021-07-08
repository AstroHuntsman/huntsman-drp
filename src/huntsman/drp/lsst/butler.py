import os
from copy import deepcopy
from functools import lru_cache
from tempfile import TemporaryDirectory

import lsst.daf.butler as dafButler
from lsst.obs.base.utils import getInstrument
from lsst.obs.base import RawIngestTask, RawIngestConfig

from huntsman.drp.base import HuntsmanBase
from huntsman.drp.lsst import tasks, pipeline
from huntsman.drp.lsst.utils.refcat import RefcatIngestor
from huntsman.drp.lsst.utils.coadd import get_skymap_ids
from huntsman.drp.lsst.utils.calib import get_calib_filename, make_defects_from_dark

# https://jira.lsstcorp.org/browse/DM-27922


class ButlerRepository(HuntsmanBase):

    _instrument_name = "Huntsman"
    _instrument_class_str = "lsst.obs.huntsman.HuntsmanCamera"

    _raw_collection = f"{_instrument_name}/raw/all"
    _calib_collection = f"{_instrument_name}/calib"
    _default_collections = set([_raw_collection, _calib_collection])

    def __init__(self, directory, initialise=True, calib_validity=1000, **kwargs):
        """
        Args:
            directory (str): The path of the butler reposity.
            initialise (bool, optional): If True (default), initialise the butler reposity
                with required files.
        """
        super().__init__(**kwargs)

        self._ordered_calib_types = self.config["calibs"]["types"]
        self._hot_pixel_threshold = self.config["calibs"].get("hot_pixel_threshold", 0.05)

        if directory is not None:
            directory = os.path.abspath(directory)
        self.root_directory = directory

        self._calib_validity = calib_validity

        if self.root_directory is None:
            self._refcat_filename = None
        else:
            self._refcat_filename = os.path.join(self.root_directory, "refcat_raw",
                                                 "refcat_raw.csv")

        if initialise:
            self._initialise()

    def __enter__(self):
        return self

    def __exit__(self, *args, **kwargs):
        pass

    def document_to_dataId(self, document, datasetType="raw"):
        """ Extract an LSST dataId from a Document.
        Args:
            document (Document): The document to convert.
        Returns:
            dict: The corresponding dataId.
        """
        return {k: document[k] for k in self.get_dimension_names(datasetType)}

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
        if collections is None:
            collections = self._default_collections
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

    def ingest_raw_files(self, filenames, *args, **kwargs):
        """ Ingest raw files into the Butler repository.
        Args:
            filenames (iterable of str): The list of raw data filenames.
        """
        filenames = set([os.path.abspath(os.path.realpath(_)) for _ in filenames])
        self.logger.debug(f"Ingesting {len(filenames)} file(s).")

        kwargs.update({"writeable": True})
        butler = self.get_butler(*args, **kwargs)

        task_config = RawIngestConfig()  # TODO: Check if this gets overriden automatically
        # Symlink files rather than copying them
        # TODO: Remove in favour of config override
        task_config.transfer = "symlink"

        task = RawIngestTask(config=task_config, butler=butler)
        task.run(filenames)

    def ingest_reference_catalogue(self, filenames, **kwargs):
        """ Ingest the reference catalogue into the repository.
        Args:
            filenames (iterable of str): The list of filenames containing reference data.
        """
        butler = self.get_butler(writeable=True, **kwargs)
        ingestor = RefcatIngestor(butler=butler)

        self.logger.debug(f"Ingesting reference catalogue from {len(filenames)} file(s).")
        ingestor.run(filenames)

    def ingest_master_calibs(self):

        # Create and register collection

        # Create and register datasetType

        # Create dataset objects

        # Ingest dataset objects

        pass

    def make_master_calib(self, calib_doc, begin_date=None, end_date=None, **kwargs):
        """ Make a master calib from ingested raw exposures.
        NOTES:
            - Certification: Associate one or more datasets with a calibration collection and a
                             validity range within it.
        Args:
            calib_doc (CalibDocument): The calib document of the calib to make.
        Returns:
            str: The filename of the newly created master calib.
        """
        butler = self.get_butler()
        datasetType = calib_doc["datasetType"]

        # Get the dataId for the calib
        calibId = self.document_to_calibId(calib_doc)

        # Get dataIds applicable to this calibId
        dataIds = self._calibId_to_dataIds(datasetType, calibId)

        self.logger.info(f"Making master {datasetType} for calibId={calibId} from"
                         f" {len(dataIds)} dataIds.")

        # Make the calibs in their own collection / run
        collection_name = f"{self._instrument_name}/{datasetType}"
        butler.registry.registerCollection(collection_name, type=dafButler.CollectionType.RUN)
        butler.registry.registerDatasetType(datasetType)

        # Make the master calib
        calib_type = datasetType.title()  # Capitalise first letter
        pipeline.pipetask_run(f"construct{calib_type}", dataIds=dataIds)

        # Certify the calibs (see docstring notes)
        dafButler.script.certifyCalibrations(repo=self.root_directory,
                                             input_collection=collection_name,
                                             output_collection=self._calib_collection,
                                             dataset_type_name=datasetType,
                                             begin_date=begin_date,
                                             end_date=end_date)


        """
        directory = os.path.join(self.root_directory, "rerun", rerun)
        filename = get_calib_filename(calib_doc, directory=directory, config=self.config)

        # Check the calib exists
        if not os.path.isfile(filename):
            raise FileNotFoundError(f"Master {datasetType} not found: {calibId},"
                                    f" filename={filename}")

        # Ingest the calib
        self.ingest_master_calibs(datasetType, [filename], validity=validity)

        return filename
        """

    # Private methods

    def _initialise(self):
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

    def _calibId_to_dataIds(self, datasetType, calibId):
        """ Get ingested dataIds for a specific calibId.
        TODO: Use Butler to do this directly?
        Args:
            calibId (dict): The calibId.
        Returns:
            list of dict: Matching dataIds from ingested raw files.
        """
        dataIds = self.get_dataIds("raw", where=f"exposure.observation_type = '{datasetType}'")

        dimensions = self.get_dimension_names("raw")
        calib_dimensions = self.get_dimension_names(datasetType)

        common_dimensions = [d for d in dimensions if d in calib_dimensions]

        matching_dataIds = []
        for dataId in dataIds:
            if all([dataId[k] == calibId[k]] for k in common_dimensions):
                matching_dataIds.append(dataId)

        return matching_dataIds

    def make_master_calibs(self, calib_docs, **kwargs):
        """ Make master calibs for a list of calib documents.
        Args:
            calib_docs (list of CalibDocument): The list of calib documents to make.
            **kwargs: Parsed to tasks.make_master_calib.
        Returns:
            dict: Dictionay containing lists of filename for each datasetType.
        """
        docs = []
        for datasetType in self._ordered_calib_types:  # Order is important

            for calib_doc in [c for c in calib_docs if c["datasetType"] == datasetType]:
                try:
                    filename = self.make_master_calib(calib_doc, **kwargs)

                    # Update the filename
                    doc = calib_doc.copy()
                    doc["filename"] = filename
                    docs.append(doc)

                except Exception as err:
                    self.logger.error(f"Problem making calib for calibId={calib_doc}: {err!r}")
        return docs

    def make_calexp(self, dataId, rerun="default", **kwargs):
        """ Make calibrated exposure using the LSST stack.
        Args:
            rerun (str, optional): The name of the rerun. Default is "default".
        """
        self.logger.info(f"Making calexp for {dataId}.")

        return tasks.make_calexp(dataId, rerun=rerun, butler_dir=self.root_directory,
                                 calib_directory=self.calib_directory, **kwargs)

    def make_calexps(self, dataIds=None, rerun="default", remake_existing=True, **kwargs):
        """ Make calibrated exposures (calexps) using the LSST stack.
        Args:
            dataIds (list of dict): List of dataIds to process. If None (default), will process
                all ingested science exposures.
            rerun (str, optional): The name of the rerun. Default is "default".
            remake_existing (bool, optional): If True (default), remake calexps that already exist.
            **kwargs: Parsed to `tasks.make_calexps`.
        """
        # Get dataIds for the raw science frames
        # TODO: Remove extra keys as this should be taken care of by policy now
        if dataIds is None:
            dataIds = self.get_dataIds(datasetType="raw", dataId={'dataType': "science"},
                                       extra_keys=["filter"])
        if not remake_existing:

            dataIds_to_skip = []
            for dataId in dataIds:
                try:
                    calexp = self.get("calexp", dataId=dataId, rerun=rerun)
                    if calexp:
                        dataIds_to_skip.append(dataId)
                except Exception:
                    pass

            dataIds = [d for d in dataIds if d not in dataIds_to_skip]

        self.logger.info(f"Making calexp(s) from {len(dataIds)} dataId(s).")

        # Process the science frames in parallel using LSST taskRunner
        tasks.make_calexps(dataIds, rerun=rerun, butler_dir=self.root_directory,
                           calib_directory=self.calib_directory, doReturnResults=False, **kwargs)

        # Check if we have the right number of calexps
        if not len(self.get_calexps(rerun=rerun, dataIds=dataIds)[0]) == len(dataIds):
            raise RuntimeError("Number of calexps does not match the number of dataIds.")

        self.logger.debug("Finished making calexps.")

    def make_coadd(self, dataIds=None, filter_names=None, rerun="default:coadd", **kwargs):
        """ Make a coadd from all the calexps in this repository.
        See: https://pipelines.lsst.io/getting-started/coaddition.html
        Args:
            filter_names (list, optional): The list of filter names to process. If not given,
                all filters will be independently processed.
            rerun (str, optional): The rerun name. Default is "default:coadd".
            dataIds (list, optional): The list of dataIds to process. If None (default), all files
                will be processed.
        """
        if dataIds is None:
            dataIds = self.get_dataIds("raw")

        # Make the skymap in a chained rerun
        # The skymap is a discretisation of the sky and defines the shapes and sizes of coadd tiles
        self.logger.info(f"Creating sky map with rerun: {rerun}.")
        tasks.make_discrete_sky_map(self.root_directory, calib_directory=self.calib_directory, rerun=rerun,
                                    dataIds=dataIds)

        # Get the output rerun
        rerun_out = rerun.split(":")[-1]

        # Get the tract / patch indices from the skymap
        # A skymap ID consists of a tractId and associated patchIds
        skymapIds = self._get_skymap_ids(rerun=rerun_out)

        # Process all filters if filter_names is not provided
        if filter_names is None:
            md = self.get_metadata("calexp", keys=["filter"], dataId={"dataType": "science"})
            filter_names = list(set([_["filter"] for _ in md]))

        self.logger.info(f"Creating coadd in {len(filter_names)} filter(s).")

        for filter_name in filter_names:

            self.logger.info(f"Creating coadd in {filter_name} filter from"
                             f" {len(skymapIds)} tracts.")

            dataIds_filter = [d for d in dataIds if d["filter"] == filter_name]

            task_kwargs = dict(butler_dir=self.root_directory, calib_directory=self.calib_directory,
                               rerun=rerun_out, skymapIds=skymapIds, dataIds=dataIds_filter,
                               filter_name=filter_name)

            # Warp the calexps onto skymap
            tasks.make_coadd_temp_exp(**task_kwargs)

            # Combine the warped calexps
            tasks.assemble_coadd(**task_kwargs)

        # Check all tracts and patches exist in each filter
        self._verify_coadd(rerun=rerun_out, filter_names=filter_names, skymapIds=skymapIds)

        self.logger.info("Successfully created coadd.")

    # Private methods

    def _get_skymap_ids(self, rerun):
        """ Get the sky map IDs, which consist of a tract ID and associated patch IDs.
        Args:
            rerun (str): The rerun name.
        Returns:
            dict: A dict of tractId: [patchIds].
        """
        skymap = self.get("deepCoadd_skyMap", rerun=rerun)
        return get_skymap_ids(skymap)

    def _verify_coadd(self, skymapIds, filter_names, rerun):
        """ Verify all the coadd patches exist and can be found by the Butler.
        Args:
            rerun (str): The rerun name.
            filter_names (list of str): The list of filter names to check.
        Raises:
            Exception: An unspecified exception is raised if there is a problem with the coadd.
        """
        self.logger.info("Verifying coadd.")

        butler = self.get_butler(rerun=rerun)

        for filter_name in filter_names:
            for skymapId in skymapIds:

                tractId = skymapId["tractId"]
                patchIds = skymapId["patchIds"]

                for patchId in patchIds:
                    dataId = {"tract": tractId, "patch": patchId, "filter": filter_name}
                    try:
                        butler.get("deepCoadd", dataId=dataId)
                    except Exception as err:
                        self.logger.error(f"Error encountered while verifying coadd: {err!r}")
                        raise err

    def _make_defects(self, doc, **kwargs):
        """ Make defects file from a corresponding dark document.
        Args:
            doc (Document): The calib document corresponding to the defects file.
            **kwargs: Parsed to self.get_butler.
        """
        # Defects are based on master darks, so need to retrieve the appropriate master dark
        dark_doc = deepcopy(doc)
        dark_doc["datasetType"] = "dark"
        dark_dataId = self.document_to_calibId(dark_doc)

        self.logger.info(f"Making defects file from dark: {dark_dataId}")

        # Create the master defects file and store in butler repo
        make_defects_from_dark(butler=self.get_butler(**kwargs), dataId=dark_dataId,
                               hot_pixel_threshold=self._hot_pixel_threshold)


class TemporaryButlerRepository(ButlerRepository):
    """ Create a new Butler repository in a temporary directory."""

    def __init__(self, directory_prefix=None, **kwargs):
        """
        Args:
            directory_prefix (str): String to prefix the name of the temporary directory.
                Default: None.
            **kwargs: Parsed to ButlerRepository init function.
        """
        self._directory_prefix = directory_prefix
        super().__init__(directory=None, initialise=False, **kwargs)

    def __enter__(self):
        """Create temporary directory and initialise as a butler repository."""
        self._tempdir = TemporaryDirectory(prefix=self._directory_prefix)
        self.root_directory = self._tempdir.name
        self._refcat_filename = os.path.join(self.root_directory, "refcat_raw", "refcat_raw.csv")
        self._initialise()
        return self

    def __exit__(self, *args, **kwargs):
        """Close temporary directory."""
        self._butlers = {}
        self._tempdir.cleanup()
        self.root_directory = None
        self._refcat_filename = None

    @property
    def calib_directory(self):
        if self.root_directory is None:
            return None
        return os.path.join(self.root_directory, "CALIB")
