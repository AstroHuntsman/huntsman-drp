import numpy as np

from pymongo.errors import DuplicateKeyError

from huntsman.drp.utils import mongo
from huntsman.drp.utils.ingest import METRIC_SUCCESS_FLAG
from huntsman.drp.utils.date import parse_date
from huntsman.drp.utils.fits import read_fits_data, read_fits_header, parse_fits_header
from huntsman.drp.collection.collection import Collection
from huntsman.drp.collection.calib import ReferenceCalibCollection
from huntsman.drp.document import ExposureDocument, CalibDocument
from huntsman.drp.metrics.raw import metric_evaluator
from huntsman.drp.document import CalibDocument, ExposureDocument

__all__ = ("ExposureCollection",)


class ExposureCollection(Collection):
    """ Table to store metadata for Huntsman exposures. """

    # Document type associated with this collection
    _DocumentClass = ExposureDocument

    # Flag to specify if the raw metrics were calculated successfully during ingestion
    _metric_success_flag = METRIC_SUCCESS_FLAG

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # Ignore certain metrics if required
        metrics_ignore = self.config.get("raw_metrics_ignore", ())
        for metric_name in metrics_ignore:
            metric_evaluator.remove_function(metric_name)

        self.ref_calib_collection = ReferenceCalibCollection.from_config(self.config)

    # Public methods

    def insert_one(self, document, *args, **kwargs):
        """ Override to make sure the document does not clash with an fpacked version.
        Args:
            document (ExposureDocument): The document to insert.
            *args, **kwargs: Parsed to super().insert_one
        Raises:
            DuplicateKeyError: If a .fz / .fits duplicate already exists.
        """
        doc = self._DocumentClass(document, copy=True, config=self.config)
        filename = doc["filename"]

        if filename.endswith(".fits"):
            if self.find({"filename": filename + ".fz"}):
                raise DuplicateKeyError(f"Tried to insert {filename} but a .fz version exists.")

        elif filename.endswith(".fits.fz"):
            if self.find({"filename": filename.strip(".fz")}):
                raise DuplicateKeyError(f"Tried to insert {filename} but a .fits version exists.")

        return super().insert_one(document, *args, **kwargs)

    def ingest_file(self, filename, **kwargs):
        """ Calculate metrics and insert / update document in collection.
        Args:
            filename (str): The filename to ingest.
        """
        self.logger.debug(f"Ingesting file into {self}: {filename}.")

        document = {"filename": filename}
        try:
            document["metrics"], success = self._calculate_metrics(filename, **kwargs)
            document[self._metric_success_flag] = success
        except Exception as err:
            self.logger.error(f"Error calculating metrics for {filename}: {err!r}")
            document[self._metric_success_flag] = False

        # Try and update the document with the parsed header
        # NOTE: We read the header again because it may have been modified
        try:
            parsed_header = parse_fits_header(read_fits_header(filename))
            document.update(parsed_header)
        # Log error and insert document into DB anyway
        except Exception as err:
            self.logger.error(f"Error parsing header for {filename}: {err!r}")

        # Use filename query as metrics etc can change
        self.replace_one({"filename": filename}, document, upsert=True)

        # Raise an exception if not success
        if not document[self._metric_success_flag]:
            raise RuntimeError(f"Metric evaluation unsuccessful for {filename}.")

    def get_matching_raw_calibs(self, calib_document, sort_date=None, **kwargs):
        """ Return matching set of calib IDs for a given calib document.
        Args:
            calib_document (CalibDocument): The calib document to match with.
            sort_date (object, optional)
            **kwargs
        Returns:
            list of ExposureDocument: The matching raw calibs ordered by increasing time diff.
        """
        self.logger.debug(f"Finding raw calibs for {calib_document}.")

        dataset_type = calib_document["datasetType"]

        # Make the document filter
        matching_keys = self.config["calibs"]["required_fields"][dataset_type]
        doc_filter = {k: calib_document[k] for k in matching_keys}

        # Add observation_type to doc filter
        # NOTE: Defects are made from dark exposures
        doc_filter["observation_type"] = "dark" if dataset_type == "defects" else dataset_type

        # Do the query
        documents = self.find(doc_filter, **kwargs)
        self.logger.debug(f"Found {len(documents)} calib exposures matching {calib_document}.")

        # Sort by time difference in increasing order
        # This makes it easy to select only the nearest matches using indexing
        if sort_date is not None:
            date = parse_date(sort_date)
            timedeltas = [abs(d["date"] - date) for d in documents]
            indices = np.argsort(timedeltas)
            documents = [documents[i] for i in indices]

        return documents

    def get_calib_docs(self, date, quality_filter=True, **kwargs):
        """ Get all possible CalibDocuments from a set of ExposureDocuments.
        Args:
            date (object): The calib date.
            documents (list of ExposureDocument, optional): The list of documents to process.
                If not provided, will lookup the appropriate documents from the collection.
            validity (datetime.timedelta): The validity of the calibs.
        Returns:
            set of CalibDocument: The calb documents.
        """
        self.logger.debug(f"Finding calib docs from exposure documents for {date}.")

        data_types = self.config["calibs"]["types"]

        # Get metadata for all raw calibs that are valid for this date
        documents = self.find({"observation_type": {"$in": data_types}},
                              quality_filter=quality_filter, **kwargs)

        # Extract the calib docs from the set of exposure docs
        calib_docs = set([self.raw_doc_to_calib_doc(d, date=date) for d in documents])
        self.logger.debug(f"Found {len(calib_docs)} possible calib documents.")

        # Get defects docs by copying darks
        # NOTE: This assumes a one-to-one correspondence between darks and defects
        defects_docs = []
        for doc in calib_docs:
            if doc["datasetType"] == "dark":
                doc = doc.copy()
                doc["datasetType"] = "defects"
                defects_docs.append(doc)
        calib_docs.update(defects_docs)

        return calib_docs

    def raw_doc_to_calib_doc(self, document, date):
        """ Convert a ExposureDocument into its corresponding CalibDocument.
        Args:
            document (ExposureDocument): The raw calib document.
            date (object): The calib date.
        Returns:
            CalibDocument: The matching calib document.
        """
        datasetType = document["observation_type"]

        # Get minimal calib metadata
        keys = self.config["calibs"]["required_fields"][datasetType]
        calib_dict = {k: document[k] for k in keys}

        # Add extra required metadata
        calib_dict["date"] = date
        calib_dict["datasetType"] = datasetType

        return CalibDocument(calib_dict)

    def clear_calexp_metrics(self):
        """ Clear all calexp metrics from the collection.
        This is useful e.g. to trigger them for reprocessing.
        """
        self.logger.info(f"Clearing all calexp metrics from {self}.")

        self._collection.update_many({}, {"$unset": {"metrics.calexp": ""}})

    # Private methods

    def _get_quality_filter(self):
        """ Return the Query object corresponding to quality cuts.
        Returns:
            huntsman.drp.utils.query.Query: The Query object.
        """
        quality_config = self.config["quality"]["raw"].copy()

        filters = []
        for data_type, document_filter in quality_config.items():

            if document_filter is not None:
                # Create a new document filter for this data type
                document_filter["observation_type"] = data_type
                filters.append(mongo.encode_mongo_filter(document_filter))

        # Allow data types that do not have any quality requirements in config
        data_types = list(quality_config.keys())
        filters.append({"observation_type": {"$nin": data_types}})

        return mongo.mongo_logical_or(filters)

    def _calculate_metrics(self, filename, **kwargs):
        """ Calculate metrics for a file, typically on ingestion.
        This function will query the calib collection for reference images.
        Args:
            filename (str): The filename.
            **kwargs: Parsed to metric_evaluator.evaluate.
        Returns:
            dict: The dictionary of metrics.
            bool: True if the metric calculation was successful, else False.
        """
        self.logger.debug(f"Calculating metrics for {filename}")

        # Read the file
        data = read_fits_data(filename)
        header = read_fits_header(filename)
        parsed_header = parse_fits_header(header)

        # Get a reference image from the calib collection
        ref_image = None
        if "observation_type" in parsed_header:
            if parsed_header["observation_type"] in self.config["calibs"]["types"]:
                try:
                    ref_doc = self.ref_calib_collection.get_matching_calib(parsed_header)
                    ref_image = read_fits_data(ref_doc["filename"])

                    self.logger.debug(f"Found reference calib for {filename}")

                except Exception as err:
                    self.logger.warning(f"Unable to find reference calib for {filename}: {err!r}")

        # Calculate metrics
        metrics, success = metric_evaluator.evaluate(
            filename, header=header, parsed_header=parsed_header, data=data, ref_image=ref_image,
            **kwargs)

        self.logger.debug(f"Finished calculating metrics for {filename}, success={success}")

        return metrics, success
