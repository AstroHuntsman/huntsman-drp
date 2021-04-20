import os
from abc import ABC, abstractmethod
from collections import defaultdict

from huntsman.drp.base import HuntsmanBase
from huntsman.drp.document import Document
from huntsman.drp.collection import RawExposureCollection, MasterCalibCollection
from huntsman.drp.refcat import TapReferenceCatalogue

from huntsman.drp.lsst.butler import ButlerRepository


def make_reduction_from_file(filename):
    """ Make a DataReduction object using a yaml configuration file.
    """


class AbstractDataReduction(HuntsmanBase, ABC):

    _ra_key = "RA-MNT"  # TODO: Move to config
    _dec_key = "DEC-MNT"

    def __init__(self, name, document_filter, directory=None, **kwargs):
        """
        """
        super().__init__(**kwargs)

        self.document_filter = Document(document_filter)

        if directory is None:
            directory = self.config["directories"]["archive"]
        self.directory = os.path.join(directory, name)
        self.logger.info(f"Data reduction directory: {self.directory}")

        if os.path.exists(directory):
            self.logger.warning(f"Directory {directory} already exists.")
        os.makedirs(directory, exist_ok=True)

        self.refcat_filename = os.path.join(self.directory, "refcat.csv")

        self._raw_collection = RawExposureCollection(config=self.config, logger=self.logger)
        self._calib_collection = MasterCalibCollection(config=self.config, logger=self.logger)

    # Properties

    # Methods

    def run(self):
        """
        """
        # Get docs from database
        raw_docs, calib_docs = self.get_documents()

        # Make the photometry / astrometry reference catalogue
        self.make_reference_catalogue()

        # Run the data reduction
        self._run(raw_docs, calib_docs)

        # Archive the result
        self._archive_products()

    def get_documents(self):
        """
        """
        raw_docs = self._raw_collection.find(self.document_filter)

        calib_docs = defaultdict(set)
        for raw_doc in raw_docs:
            for datasetType, calib_doc in self._calib_collection.get_matching_calibs(raw_doc):
                calib_docs[datasetType].update([calib_doc])

        return raw_docs, calib_docs

    def make_reference_catalogue(self):
        """
        """
        self.logger.info(f"Making reference catalogue for {self}.")
        ra_list = []
        dec_list = []
        for doc in self.raw_documents:
            ra_list.append(doc[self._ra_key])
            dec_list.append(doc[self._dec_key])

        tap = TapReferenceCatalogue(config=self.config, logger=self.logger)
        tap.make_reference_catalogue(ra_list, dec_list, filename=self._refcat_filename)

    # Private methods

    @abstractmethod
    def _run(self):
        pass

    @abstractmethod
    def _make_missing_calibs(self):
        pass

    @abstractmethod
    def _archive_products(self):
        pass


class LsstDataReduction(AbstractDataReduction):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.butler_directory = os.path.join(self.directory, "butler_repo")
        self.butler_repo = ButlerRepository(self.butler_directory)

    def _run(self, raw_docs, calib_docs):
        """
        """
        # Ingest raw files
        raw_filenames = [r["filename"] for r in raw_docs]
        self.butler_repo.ingest_raw_data(raw_filenames)

        # Ingest master calibs
        for datasetType in set([c["datasetType"] for c in calib_docs]):
            filenames = [c["filename"] for c in calib_docs if c["datasetType"] == datasetType]
            self.butler_repo.ingest_master_calibs(datasetType, filenames)

        # Ingest reference catalogue
        self.butler_repo.ingest_reference_catalogue([self.refcat_filename])

        # Make calexps
        self.butler_repo.make_calexps()

        # Make coadd
        self.butler_repo.make_coadd()

    def _archive_products(self):
        """
        """
        pass
