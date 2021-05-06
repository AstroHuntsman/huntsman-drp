import os
from collections import defaultdict

from huntsman.drp.utils import normalise_path
from huntsman.drp.base import HuntsmanBase
from huntsman.drp.collection import RawExposureCollection, MasterCalibCollection
from huntsman.drp.document import Document
from huntsman.drp.refcat import RefcatClient


class DataReductionBase(HuntsmanBase):
    """ Generic class for data reductions """

    def __init__(self, directory, document_filter, exposure_collection=None, calib_collection=None,
                 initialise=True, **kwargs):

        super().__init__(**kwargs)

        self.directory = normalise_path(directory)
        self._refcat_filename = os.path.join(self.directory, "refcat.csv")

        self._document_filter = Document(document_filter)

        if not exposure_collection:
            exposure_collection = RawExposureCollection(config=self.config)
        self._exposure_collection = exposure_collection

        if not calib_collection:
            calib_collection = MasterCalibCollection(config=self.config)
        self._calib_collection = calib_collection

        self.science_docs = None
        self.calib_docs = None

        if initialise:
            self._initialise()

    # Properties

    # Methods

    def prepare(self):
        """ Prepare the data to reduce.
        This method is responsible for querying the database, ingesting the files and producing
        the reference catalogue.
        """
        dataType = self._document_filter.get("dataType", None)
        if dataType != "science":
            self.logger.warning("dataType=science not specified in document filter.")

        # Identify science docs
        self.science_docs = self._exposure_collection.find(self._document_filter, screen=True,
                                                           quality_filter=True)

        # Get matching master calibs
        self.calib_docs = self._get_calibs(self.science_docs)

        # Make reference catalogue
        self._make_reference_catalogue()

    def reduce(self):
        """ Make coadds and store results in the reduction directory.
        """
        raise NotImplementedError

    # Private methods

    def _initialise(self):
        """ Abstract instance method responsible for initialising the data reduction.
        """
        os.makedirs(self.directory, exist_ok=True)

    def _get_calibs(self, science_docs):
        """ Get matching calib docs for a set of science docs.
        Args:
            science_docs (list of RawExposureDocument): The list of science docs to match with.
        Returns:
            dict of set: Dictionary of calib type: set of matching calib documents.
        """
        calib_docs = defaultdict(list)

        for doc in science_docs:
            calibs = self._calib_collection.get_matching_calibs(doc)

            for k, v in calibs.items():
                calib_docs[k].append(v)

        return {k: set(v) for k, v in calib_docs.items()}

    def _make_reference_catalogue(self):
        """ Make reference catalogue and write it to file in reduction directory. """

        refcat = RefcatClient(config=self.config)

        refcat.make_from_documents(self.science_docs, filename=self._refcat_filename)
