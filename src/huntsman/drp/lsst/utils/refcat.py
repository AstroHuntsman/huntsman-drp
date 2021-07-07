""" Temporary hack solution to creating a reference catalogue in Butler Gen 3.
NOTES:
- Each refcat (e.g. skymapper) is treated as it's own datasetType.
- Old gen2 ingestIndexReferenceTask has been overriden so as not to use a Gen2 butler.
- This code is inspired by the convertRepo code in lsst.obs.base.
"""
import os
import tempfile

from lsst.utils import getPackageDir
from lsst.daf.butler import DatasetType, DatasetRef, FileDataset, CollectionType

from huntsman.drp.lsst.tasks.ingestRefcat import HuntsmanIngestIndexedReferenceTask

PACKAGE_NAME = "obs_huntsman"


class RefcatIngestor():

    _config_filename = os.path.join(getPackageDir(PACKAGE_NAME), "config",
                                    "ingestSkyMapperReference.py")

    def __init__(self, butler, run="refCat"):
        """
        Args:
            butler (lsst.daf.butler.Butler): The butler object.
            run (str, optional): The run in which to store the refcat. Default: "refCat".
        """
        self.butler = butler
        self.run_name = run

        package_dir = getPackageDir(PACKAGE_NAME)
        config_filename = os.path.join(package_dir, "config", "ingestSkyMapperReference.py")

        self.task_config = HuntsmanIngestIndexedReferenceTask.ConfigClass()
        self.task_config.load(config_filename)

        self._refcat_name = self.task_config.dataset_config.ref_dataset_name
        self._htm_depth = self.task_config.dataset_config.indexer['HTM'].depth

    # Methods

    def run(self, filenames):
        """ Create and ingest a refcat from a list of raw (e.g. csv) refcat files.
        Args:
            filenames (list of str): The raw refcat filenames.
        """
        registry = self.butler.registry

        # Use a temporary directory to make the HTM refcat
        with tempfile.TemporaryDirectory() as tempdir:

            filename_dict = self._make_htm_refcat(tempdir, filenames)

            datasetType, datasets = self._make_datasets_from_htm_refcat(filename_dict, registry)

            # Copy the files into their proper location
            self._ingest_files(datasetType, datasets)

    # Private methods

    def _make_htm_refcat(self, output_directory, raw_refcat_filenames):
        """ Use override / hack class to make HTM refcat without using Gen2 Butler.
        Args:
            output_directory (str): The directory to store the HTM refcat files.
            raw_refcat_filenames (list of str): List of raw (e.g. csv) refcat filenames.
        Returns:
            dict: Dictionary of HTM index: filename pairs.
        """
        task = HuntsmanIngestIndexedReferenceTask(output_directory, config=self.task_config)

        filename_dict = task.createIndexedCatalog(raw_refcat_filenames)

        return filename_dict

    def _make_datasets_from_htm_refcat(self, filename_dict, registry):
        """ Make FileDataset objects from refcat files for Gen3 ingest.
        Args:
            filename_dict (dict): Dictionary of HTM index: filename pairs.
            registry (lsst.daf.butler.Registry): The butler registry.
        Returns:
            lsst.daf.butler.DatasetType: The refcat datasetType.
            list of lsst.daf.butler.FileDataset: The refcat datasets.
        """
        universe = self.butler.registry.dimensions
        dimensions = universe[f"htm{self._htm_depth}"]

        datasetType = DatasetType(self._refcat_name, dimensions=[dimensions], universe=universe,
                                  storageClass="SimpleCatalog")

        datasets = []
        for htmId, filename in filename_dict.items():

            dataId = registry.expandDataId({dimensions: htmId})
            datasetRef = DatasetRef(datasetType, dataId)

            datasets.append(FileDataset(path=filename, refs=datasetRef))

        return datasetType, datasets

    def _ingest_files(self, datasetType, datasets, transfer="copy"):
        """ Ingest refcat FileDatasets into Gen3 butler repository.
        Not sure how much of this is necessary.
        Args:
            datasetType (lsst.daf.butler.DatasetType): The refcat datasetType.
            datasets (list of lsst.daf.butler.FileDataset): The refcat datasets.
            transfer (str): The transfer mode. Default: "copy".
        """
        self.butler.registry.registerCollection(self.run_name, type=CollectionType.RUN)

        self.butler.registry.registerRun(self.run_name)

        self.butler.registry.registerDatasetType(datasetType)

        self.butler.ingest(*datasets, transfer=transfer, run=self.run_name)
