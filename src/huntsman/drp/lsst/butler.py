import os
import shutil
from contextlib import suppress
from tempfile import TemporaryDirectory
import sqlite3

import lsst.daf.persistence as dafPersist
from lsst.daf.persistence.policy import Policy

from huntsman.drp.base import HuntsmanBase
from huntsman.drp.lsst import tasks
from huntsman.drp.collection import MasterCalibCollection
from huntsman.drp.refcat import TapReferenceCatalogue
from huntsman.drp.utils.date import date_to_ymd, current_date_ymd
import huntsman.drp.lsst.utils.butler as utils
from huntsman.drp.fitsutil import read_fits_header
from huntsman.drp.lsst.utils.coadd import get_skymap_ids
from huntsman.drp.utils.calib import get_calib_filename


class ButlerRepository(HuntsmanBase):
    _mapper = "lsst.obs.huntsman.HuntsmanMapper"
    _policy_filename = Policy.defaultPolicyFile("obs_huntsman", "HuntsmanMapper.yaml",
                                                relativePath="policy")
    _ra_key = "RA-MNT"
    _dec_key = "DEC-MNT"  # TODO: Move to config

    def __init__(self, directory, calib_dir=None, initialise=True, calib_table=None,
                 max_dataIds_per_calib=50, **kwargs):
        super().__init__(**kwargs)

        if directory is not None:
            directory = os.path.abspath(directory)
        self.butler_dir = directory

        if (calib_dir is None) and (directory is not None):
            calib_dir = os.path.join(self.butler_dir, "CALIB")
        self._calib_dir = calib_dir

        self._calib_validity = self.config["calibs"]["validity"]
        self._max_dataIds_per_calib = int(max_dataIds_per_calib)

        if self.butler_dir is None:
            self._refcat_filename = None
        else:
            self._refcat_filename = os.path.join(self.butler_dir, "refcat_raw", "refcat_raw.csv")

        if calib_table is None:
            calib_table = MasterCalibCollection(config=self.config, logger=self.logger)
        self._calib_table = calib_table

        # Load the policy file
        self._policy = Policy(self._policy_filename)

        # Initialise the butler repository
        self._butlers = {}  # One butler for each rerun
        if initialise:
            self._initialise()

    def __enter__(self):
        return self

    def __exit__(self, *args, **kwargs):
        pass

    @property
    def calib_dir(self):
        return self._calib_dir

    @property
    def status(self):
        # TODO: Information here about number of ingested files etc
        raise NotImplementedError

    # Getters

    def get_butler(self, rerun=None):
        """ Get a butler object for a given rerun.
        Args:
            rerun (str, optional): The rerun name. If None, the butler is created for the root
                butler directory.
        Returns:
            butler: The butler object.
        """
        try:
            return self._butlers[rerun]
        except KeyError:
            self.logger.debug(f"Creating new butler object for rerun={rerun}.")
            if rerun is None:
                butler_dir = self.butler_dir
            else:
                butler_dir = os.path.join(self.butler_dir, "rerun", rerun)
            os.makedirs(butler_dir, exist_ok=True)
            self._butlers[rerun] = dafPersist.Butler(inputs=butler_dir)
        return self._butlers[rerun]

    def get(self, datasetType, data_id=None, rerun=None, **kwargs):
        """ Get a dataset from the butler repository.
        Args:
            datasetType (str): The dataset type (raw, flat, bias etc.).
            data_id (dict): The data ID that uniquely specifies a file.
            rerun (str, optional): The rerun name. If None (default), will use the root butler
                directory.
        Returns:
            object: The dataset.
        """
        butler = self.get_butler(rerun=rerun)
        return butler.get(datasetType, dataId=data_id, **kwargs)

    def get_keys(self, datasetType, **kwargs):
        """ Get set of keys required to uniquely identify ingested data.
        Args:
            datasetType (str): The dataset type (raw, flat, bias etc.).
        Returns:
            list of str: A list of keys.
        """
        butler = self.get_butler(**kwargs)
        return list(butler.getKeys(datasetType))

    def get_filename(self, datasetType, data_id, **kwargs):
        """ Get the filename for a data ID of data type.
        Args:
            datasetType (str): The dataset type (raw, flat, bias etc.).
            data_id (dict): The data ID that uniquely specifies a file.
        Returns:
            str: The filename.
        """
        return self.get(datasetType + "_filename", data_id=data_id, **kwargs)

    def get_metadata(self, datasetType, keys, data_id=None, **kwargs):
        """ Get metadata for a dataset.
        Args:
            datasetType (str): The dataset type (e.g. raw, flat, calexp).
            keys (list of str): The keys contained in the metadata.
            data_id (optional): A list of dataIds to query on.
        """
        butler = self.get_butler(**kwargs)
        md = butler.queryMetadata(datasetType, format=keys, dataId=data_id)

        if len(keys) == 1:  # Butler doesn't return a consistent data structure if len(keys)=1
            return [{keys[0]: _} for _ in md]

        return [{k: v for k, v in zip(keys, _)} for _ in md]

    def get_calib_metadata(self, datasetType, keys_ignore=None):
        """ Query the ingested calibs. TODO: Replace with the "official" Butler version.
        Args:
            datasetType (str): The dataset type (e.g. bias, dark, flat).
            keys_ignore (list of str, optional): If provided, drop these keys from result.
        Returns:
            list of dict: The query result in column: value.
        """
        # Access the sqlite DB
        conn = sqlite3.connect(os.path.join(self.calib_dir, "calibRegistry.sqlite3"))
        c = conn.cursor()

        # Query the calibs
        result = c.execute(f"SELECT * from {datasetType}")
        metadata_list = []

        for row in result:
            d = {}
            for idx, col in enumerate(c.description):
                d[col[0]] = row[idx]
            metadata_list.append(d)
        c.close()

        if keys_ignore is not None:
            keys_keep = [k for k in metadata_list[0].keys() if k not in keys_ignore]
            metadata_list = [{k: _[k] for k in keys_keep} for _ in metadata_list]

        return metadata_list

    def get_dataIds(self, datasetType, data_id=None, extra_keys=None, **kwargs):
        """ Get ingested data_ids for a given datasetType.
        Args:
            datasetType (str): The datasetType (raw, bias, flat etc.).
            data_id (dict, optional): A complete or partial data_id to match with.
            extra_keys (list, optional): List of additional keys to be included in the data_ids.
        Returns:
            list of dict: A list of data_ids.
        """
        butler = self.get_butler(**kwargs)

        keys = list(butler.getKeys(datasetType).keys())
        if extra_keys is not None:
            keys.extend(extra_keys)

        return self.get_metadata(datasetType, keys=keys, data_id=data_id)

    def get_calexp_data_ids(self, rerun="default", filter_name=None, **kwargs):
        """ Convenience function to get data_ids for calexps.
        Args:
            rerun (str, optional): The rerun name. Default: "default".
            filter_name (str, optional): If given, only return data Ids for this filter.
            **kwargs: Parsed to self.get_dataIds.
        Returns:
            list of dict: The list of dataIds.
        """
        data_id = {"dataType": "science"}
        if filter_name is not None:
            data_id["filter"] = filter_name

        return self.get_dataIds("calexp", data_id=data_id, rerun=rerun, **kwargs)

    def get_calexps(self, rerun="default", **kwargs):
        """ Convenience function to get the calexps produced in a given rerun.
        Args:
            rerun (str, optional): The rerun name. Default: "default".
            **kwargs: Parsed to self.get_calexp_data_ids.
        Returns:
            list of lsst.afw.image.exposure: The list of calexp objects.
        """
        data_ids = self.get_calexp_data_ids(rerun=rerun, **kwargs)

        calexps = [self.get("calexp", data_id=d, rerun=rerun) for d in data_ids]
        if len(calexps) != len(data_ids):
            raise RuntimeError("Number of data_ids does not match the number of calexps.")

        return calexps, data_ids

    # Ingesting

    def ingest_raw_data(self, filenames, **kwargs):
        """ Ingest raw data into the repository.
        Args:
            filenames (iterable of str): The list of raw data filenames.
        """
        self.logger.debug(f"Ingesting {len(filenames)} file(s).")
        tasks.ingest_raw_data(filenames, butler_dir=self.butler_dir, **kwargs)

    def ingest_reference_catalogue(self, filenames):
        """ Ingest the reference catalogue into the repository.
        Args:
            filenames (iterable of str): The list of filenames containing reference data.
        """
        self.logger.debug(f"Ingesting reference catalogue from {len(filenames)} file(s).")
        tasks.ingest_reference_catalogue(self.butler_dir, filenames)

    def ingest_master_calibs(self, datasetType, filenames, validity=None):
        """ Ingest the master calibs into the butler repository.
        Args:
            datasetType (str): The calib dataset type (e.g. bias, flat).
            filenames (list of str): The files to ingest.
            validity (int, optional): How many days the calibs remain valid for. Default 1000.
        """
        if len(filenames) == 0:
            self.logger.warning(f"No master {datasetType} files to ingest.")
            return

        if validity is None:
            validity = self._calib_validity

        self.logger.info(f"Ingesting {len(filenames)} master {datasetType} calib(s) with validity="
                         f"{validity}.")
        tasks.ingest_master_calibs(datasetType, filenames, butler_dir=self.butler_dir,
                                   calib_dir=self.calib_dir, validity=validity)

    # Making

    def make_master_calibs(self, calib_date=None, rerun="default", datasetTypes_to_skip=None,
                           validity=None, **kwargs):
        """ Make master calibs from ingested raw calib data for a given calibDate.
        Args:
            calib_date (object, optional): The calib date to assign to the master calibs. If None
                (default), will use the current date.
            rerun (str, optional): The name of the rerun. If None (default), use default rerun.
            skip_bias (bool, optional): Skip creation of master biases? Default False.
            skip_dark (bool, optional): Skip creation of master darks? Default False.
            datasetTypes_to_skip (list, optional):
        """
        butler_kwargs = dict(butler_dir=self.butler_dir, calib_dir=self.calib_dir, rerun=rerun)
        butler_kwargs.update(kwargs)

        if datasetTypes_to_skip is None:
            datasetTypes_to_skip = []

        if calib_date is None:
            calibDate = current_date_ymd()
        else:
            calibDate = date_to_ymd(calib_date)
        self.logger.info(f"Making master calibs for calib_date={calibDate}.")

        for datasetType in ("bias", "dark", "flat"):  # Order is important

            if datasetType in datasetTypes_to_skip:
                self.logger.debug(f"Skipping {datasetType} frames for calibDate={calibDate}.")
                continue

            # Get the unique set of calibIds defined by the set of all ingested dataIds
            calibIds = self._get_all_calibIds(datasetType, calibDate)
            self.logger.debug(f"Found {len(calibIds)} calibId(s) for datasetType={datasetType},"
                              f" calibDate={calibDate}.")

            self.logger.info(f"Making master {datasetType} frame(s) for calibDate={calibDate}.")

            for calibId in calibIds:  # Process each calibId separately

                # Get dataIds that correspond to this calibId
                dataIds = self._calibId_to_dataIds(datasetType, calibId, limit=True)

                self.logger.info(f"Making master {datasetType} frame for calibId={calibId} using"
                                 f" {len(dataIds)} dataIds.")

                # For some reason the dataIds also need to contain the calibDate
                # TODO: Figure out why and remove
                for dataId in dataIds:
                    dataId["calibDate"] = calibDate

                # Run the LSST command
                try:
                    tasks.make_master_calib(datasetType, calibId, dataIds, **butler_kwargs)
                except Exception as err:
                    self.logger.error(f"Problem making master {datasetType} frame for"
                                      f" calibId={calibId}: {err!r}")

            # Ingest the master calibs for this datasetType
            calib_dir = os.path.join(self.butler_dir, "rerun", rerun)
            filenames = utils.get_files_of_type(
                f"calibrations.{datasetType}", directory=calib_dir, policy=self._policy)[1]

            self.ingest_master_calibs(datasetType, filenames, validity=validity)

    def make_reference_catalogue(self, ingest=True, **kwargs):
        """ Make the reference catalogue for the ingested science frames.
        Args:
            ingest (bool, optional): If True (default), ingest refcat into butler repo.
        """
        butler = self.get_butler(**kwargs)

        # Get the filenames of ingested images
        data_ids, filenames = utils.get_files_of_type("exposures.raw", self.butler_dir,
                                                      policy=self._policy)
        # Use the FITS header sto retrieve the RA/Dec info
        ra_list = []
        dec_list = []
        for data_id, filename in zip(data_ids, filenames):

            data_type = butler.queryMetadata("raw", ["dataType"], dataId=data_id)[0]

            if data_type == "science":  # Only select science files
                header = read_fits_header(filename, ext="all")  # Use all as .fz extension is lost
                ra_list.append(header[self._ra_key])
                dec_list.append(header[self._dec_key])

        self.logger.debug(f"Creating reference catalogue for {len(ra_list)} science frames.")

        # Make the reference catalogue
        tap = TapReferenceCatalogue(config=self.config, logger=self.logger)
        tap.make_reference_catalogue(ra_list, dec_list, filename=self._refcat_filename)

        # Ingest into the repository
        if ingest:
            self.ingest_reference_catalogue(filenames=(self._refcat_filename,))

    def make_calexps(self, rerun="default", **kwargs):
        """ Make calibrated exposures (calexps) using the LSST stack.
        Args:
            rerun (str, optional): The name of the rerun. Default is "default".
            procs (int, optional): Run on this many processes (default 1).
        """
        # Get data_ids for the raw science frames
        data_ids = self.get_dataIds(datasetType="raw", data_id={'dataType': "science"},
                                    extra_keys=["filter"])

        self.logger.info(f"Making calexp(s) from {len(data_ids)} data_id(s).")

        # Process the science frames
        tasks.make_calexps(data_ids, rerun=rerun, butler_dir=self.butler_dir,
                           calib_dir=self.calib_dir, **kwargs)

        # Check if we have the right number of calexps
        if not len(self.get_calexps(rerun=rerun)[0]) == len(data_ids):
            raise RuntimeError("Number of calexps does not match the number of data_ids.")

    def make_coadd(self, filter_names=None, rerun="default:coadd", **kwargs):
        """ Make a coadd from all the calexps in this repository.
        See: https://pipelines.lsst.io/getting-started/coaddition.html
        Args:
            filter_names (list, optional): The list of filter names to process. If not given,
                all filters will be processed.
            rerun (str, optional): The rerun name. Default is "default:coadd".
        """
        # Make the skymap in a chained rerun
        self.logger.info(f"Creating sky map with rerun: {rerun}.")
        tasks.make_discrete_sky_map(self.butler_dir, calib_dir=self.calib_dir, rerun=rerun)

        # Get the output rerun
        rerun_out = rerun.split(":")[-1]

        # Get the tract / patch indices from the skymap
        skymap_ids = self._get_skymap_ids(rerun=rerun_out)

        # Process all filters if filter_names is not provided
        if filter_names is None:
            md = self.get_metadata("calexp", keys=["filter"], data_id={"dataType": "science"})
            filter_names = list(set([_["filter"] for _ in md]))

        self.logger.info(f"Creating coadd in {len(filter_names)} filter(s).")

        for filter_name in filter_names:
            for tract_id, patch_ids in skymap_ids.items():  # TODO: Use multiprocessing

                self.logger.debug(f"Warping calexps for tract {tract_id} in {filter_name} filter.")

                task_kwargs = dict(butler_dir=self.butler_dir, calib_dir=self.calib_dir,
                                   rerun=rerun_out, tract_id=tract_id,
                                   patch_ids=patch_ids, filter_name=filter_name)

                # Warp the calexps onto skymap
                tasks.make_coadd_temp_exp(**task_kwargs)

                # Combine the warped calexps
                tasks.assemble_coadd(**task_kwargs)

        # Check all tracts and patches exist in each filter
        self._verify_coadd(rerun=rerun_out, filter_names=filter_names)

        self.logger.info("Successfully created coadd.")

    # Archiving

    def archive_master_calibs(self):
        """ Copy the master calibs from this Butler repository into the calib archive directory
        and insert the metadata into the master calib metadatabase.
        """
        for datasetType in self.config["calibs"]["types"]:

            # Retrieve filenames and data_ids for all files of this type
            data_ids, filenames = utils.get_files_of_type(f"calibrations.{datasetType}",
                                                          directory=self.calib_dir,
                                                          policy=self._policy)
            self.logger.info(f"Archiving {len(filenames)} master {datasetType} files.")

            for metadata, filename in zip(data_ids, filenames):

                metadata["datasetType"] = datasetType

                # Create the filename for the archived copy
                archived_filename = get_calib_filename(config=self.config, **metadata)
                metadata["filename"] = archived_filename

                # Copy the file into the calib archive, overwriting if necessary
                self.logger.debug(f"Copying {filename} to {archived_filename}.")
                os.makedirs(os.path.dirname(archived_filename), exist_ok=True)
                shutil.copy(filename, archived_filename)

                # Insert the metadata into the calib database
                self._calib_table.insert_one(metadata, overwrite=True)

    # Private methods

    def _initialise(self):
        """Initialise a new butler repository."""
        # Add the mapper file to each subdirectory, making directory if necessary
        for subdir in ["", "CALIB"]:
            dir = os.path.join(self.butler_dir, subdir)
            with suppress(FileExistsError):
                os.mkdir(dir)
            filename_mapper = os.path.join(dir, "_mapper")
            with open(filename_mapper, "w") as f:
                f.write(self._mapper)

    def _get_all_calibIds(self, datasetType, calibDate):
        """ Get the full set of calibIds from all the ingested dataIds.
        Args:
            datasetType (str): The datasetType (e.g. bias).
            calibDate (str): The calibDate.
        Returns:
            list of dict: All possible calibIds.
        """
        dataIds = self.get_dataIds(datasetType="raw")
        return utils.get_all_calibIds(datasetType, dataIds, calibDate, butler=self.get_butler())

    def _calibId_to_dataIds(self, datasetType, calibId, limit=False):
        """ Find all matching dataIds given a calibId.
        Args:
            datasetType (str): The datasetType (e.g. bias).
            calibId (dict): The calibId.
            limit (bool): If True, limit the number of returned dataIds to a maximum value
                indicated by self._max_dataIds_per_calib. This avoids long processing times and
                apparently also segfaults. Default: False.
        Returns:
            list of dict: All matching dataIds.
        """
        dataIds = utils.calibId_to_dataIds(datasetType, calibId, butler=self.get_butler())

        # Limit the number of dataIds per calib
        if limit:
            if len(dataIds) >= self._max_dataIds_per_calib:
                self.logger.warning(
                    f"Number of {datasetType} dataIds for calibId={calibId} ({len(dataIds)})"
                    f" exceeds allowed maximum ({self._max_dataIds_per_calib}). Using first"
                    f" {self._max_dataIds_per_calib} matches.")
                dataIds = dataIds[:self._max_dataIds_per_calib]

        return dataIds

    def _get_skymap_ids(self, rerun):
        """ Get the sky map IDs, which consist of a tract ID and associated patch IDs.
        Args:
            rerun (str): The rerun name.
        Returns:
            dict: A dict of tract_id: [patch_ids].
        """
        skymap = self.get("deepCoadd_skyMap", rerun=rerun)
        return get_skymap_ids(skymap)

    def _verify_coadd(self, filter_names, rerun):
        """ Verify all the coadd patches exist and can be found by the Butler.
        Args:
            rerun (str): The rerun name.
            filter_names (list of str): The list of filter names to check.
        Raises:
            Exception: An unspecified exception is raised if there is a problem with the coadd.
        """
        self.logger.info("Verifying coadd.")

        butler = self.get_butler(rerun=rerun)
        skymap_ids = self._get_skymap_ids(rerun=rerun)

        for filter_name in filter_names:
            for tract_id, patch_ids in skymap_ids.items():
                for patch_id in patch_ids:

                    data_id = {"tract": tract_id, "patch": patch_id, "filter": filter_name}
                    try:
                        butler.get("deepCoadd", dataId=data_id)
                    except Exception as err:
                        self.logger.error(f"Error encountered while verifying coadd: {err!r}")
                        raise err


class TemporaryButlerRepository(ButlerRepository):
    """ Create a new Butler repository in a temporary directory."""

    def __init__(self, **kwargs):
        super().__init__(directory=None, initialise=False, **kwargs)

    def __enter__(self):
        """Create temporary directory and initialise as a butler repository."""
        self._tempdir = TemporaryDirectory()
        self.butler_dir = self._tempdir.name
        self._refcat_filename = os.path.join(self.butler_dir, "refcat_raw", "refcat_raw.csv")
        self._initialise()
        return self

    def __exit__(self, *args, **kwargs):
        """Close temporary directory."""
        self._butlers = {}
        self._tempdir.cleanup()
        self.butler_dir = None
        self._refcat_filename = None

    @property
    def calib_dir(self):
        if self.butler_dir is None:
            return None
        return os.path.join(self.butler_dir, "CALIB")
