import os
from copy import deepcopy
from functools import lru_cache
from tempfile import TemporaryDirectory

import lsst.daf.butler as dafButler
from lsst.obs.base.utils import getInstrument
from lsst.obs.base import RawIngestConfig
from lsst.obs.base import RawIngestTask

from huntsman.drp.base import HuntsmanBase
from huntsman.drp.lsst import tasks
from huntsman.drp.lsst.utils import butler as utils
from huntsman.drp.lsst.utils.coadd import get_skymap_ids
from huntsman.drp.lsst.utils.calib import get_calib_filename, make_defects_from_dark


class ButlerRepository(HuntsmanBase):

    _instrument_class_str = "lsst.obs.huntsman.HuntsmanCamera"

    def __init__(self, directory, calib_dir=None, initialise=True, calib_validity=1000, **kwargs):
        """
        Args:
            directory (str): The path of the butler reposity.
            calib_dir (str, optional): The path of the butler calib repository. If None (default),
                will create a new CALIB directory under the butler repository root.
            initialise (bool, optional): If True (default), initialise the butler reposity
                with required files.
        """
        super().__init__(**kwargs)

        self._ordered_calib_types = self.config["calibs"]["types"]
        self._hot_pixel_threshold = self.config["calibs"].get("hot_pixel_threshold", 0.05)

        if directory is not None:
            directory = os.path.abspath(directory)
        self.root_directory = directory

        if (calib_dir is None) and (directory is not None):
            calib_dir = os.path.join(self.root_directory, "CALIB")
        self._calib_dir = calib_dir

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

    @property
    def calib_dir(self):
        return self._calib_dir

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

    @lru_cache  # Use caching so we don't have to keep reinitialising Butler objects
    def get_butler(self, collections=None, *args, **kwargs):
        """ Get a butler object for a given rerun.
        We cache created butlers to avoid the overhead of having to re-create them each time.
        Args:
            rerun (str, optional): The rerun name. If None, the butler is created for the root
                butler directory.
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

    # Private methods

    def _initialise(self):
        """ Initialise a new butler repository. """
        butler = self.get_butler()  # Automatically creates empty butler repo

        # Register the Huntsman instrument config with the repo
        instrInstance = getInstrument(self._instrument_class_str, butler.registry)
        instrInstance.register(butler.registry)

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








    def ingest_reference_catalogue(self, filenames):
        """ Ingest the reference catalogue into the repository.
        Args:
            filenames (iterable of str): The list of filenames containing reference data.
        """
        self.logger.debug(f"Ingesting reference catalogue from {len(filenames)} file(s).")
        tasks.ingest_reference_catalogue(self.root_directory, filenames)

    def ingest_master_calibs(self, datasetType, filenames, validity=None):
        """ Ingest the master calibs into the butler repository.
        Args:
            datasetType (str): The calib dataset type (e.g. bias, flat).
            filenames (list of str): The files to ingest.
            validity (int, optional): How many days the calibs remain valid for. Default 1000.
        """
        filenames = set([os.path.abspath(os.path.realpath(_)) for _ in filenames])

        if not filenames:
            self.logger.warning(f"No master {datasetType} files to ingest.")
            return

        if validity is None:
            validity = self._calib_validity

        self.logger.info(f"Ingesting {len(filenames)} master {datasetType} calib(s) with validity="
                         f"{validity}.")
        tasks.ingest_master_calibs(datasetType, filenames, butler_dir=self.root_directory,
                                   calib_dir=self.calib_dir, validity=validity)

    def make_master_calib(self, calib_doc, rerun="default", validity=None, **kwargs):
        """ Make a master calib from ingested raw exposures.
        Args:
            datasetType (str): The calib datasetType (e.g. bias, dark, flat).
            calib_doc (CalibDocument): The calib document of the calib to make.
            rerun (str, optional): The name of the rerun. Default is "default".
            validity (int, optional): The calib validity in days.
            **kwargs: Parsed to tasks.make_master_calib.
        Returns:
            str: The filename of the newly created master calib.
        """
        datasetType = calib_doc["datasetType"]

        calibId = self.document_to_calibId(calib_doc)

        # Defects is treated separately from other calibs as there is no official makeDefectsTask
        if datasetType == "defects":
            self._make_defects(calib_doc, rerun=rerun)

        else:
            # Get dataIds applicable to this calibId
            dataIds = self.calibId_to_dataIds(datasetType, calibId, with_calib_date=True)

            self.logger.info(f"Making master {datasetType} for calibId={calibId} from"
                             f" {len(dataIds)} dataIds.")

            # Make the master calib
            tasks.make_master_calib(datasetType, calibId, dataIds, butler_dir=self.root_directory,
                                    calib_dir=self.calib_dir, rerun=rerun, **kwargs)

        directory = os.path.join(self.root_directory, "rerun", rerun)
        filename = get_calib_filename(calib_doc, directory=directory, config=self.config)

        # Check the calib exists
        if not os.path.isfile(filename):
            raise FileNotFoundError(f"Master {datasetType} not found: {calibId},"
                                    f" filename={filename}")

        # Ingest the calib
        self.ingest_master_calibs(datasetType, [filename], validity=validity)

        return filename

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
                                 calib_dir=self.calib_dir, **kwargs)

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
                           calib_dir=self.calib_dir, doReturnResults=False, **kwargs)

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
        tasks.make_discrete_sky_map(self.root_directory, calib_dir=self.calib_dir, rerun=rerun,
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

            task_kwargs = dict(butler_dir=self.root_directory, calib_dir=self.calib_dir,
                               rerun=rerun_out, skymapIds=skymapIds, dataIds=dataIds_filter,
                               filter_name=filter_name)

            # Warp the calexps onto skymap
            tasks.make_coadd_temp_exp(**task_kwargs)

            # Combine the warped calexps
            tasks.assemble_coadd(**task_kwargs)

        # Check all tracts and patches exist in each filter
        self._verify_coadd(rerun=rerun_out, filter_names=filter_names, skymapIds=skymapIds)

        self.logger.info("Successfully created coadd.")

    def calibId_to_dataIds(self, datasetType, calibId, limit=False, with_calib_date=False):
        """ Find all matching dataIds given a calibId.
        Args:
            calibId (dict): The calibId.
            limit (bool): If True, limit the number of returned dataIds to a maximum value
                indicated by self._max_dataIds_per_calib. This avoids long processing times and
                apparently also segfaults. Default: False.
        Returns:
            list of dict: All matching dataIds.
        """
        dataIds = utils.calibId_to_dataIds(datasetType, calibId, butler=self.get_butler())

        if with_calib_date:
            for dataId in dataIds:
                dataId["calibDate"] = calibId["calibDate"]

        return dataIds

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
    def calib_dir(self):
        if self.root_directory is None:
            return None
        return os.path.join(self.root_directory, "CALIB")
