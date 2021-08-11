import os
import shutil

import numpy as np

from huntsman.drp.utils.date import parse_date, date_to_ymd
from huntsman.drp.collection.collection import Collection
from huntsman.drp.document import CalibDocument

__all__ = ("CalibCollection",)


class CalibCollection(Collection):
    """ Table to store metadata for master calibs. """

    _DocumentClass = CalibDocument

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # Set required fields with type dependency
        # This is useful for e.g. requiring a filter name for flats but not for biases
        self._required_fields_by_type = self.config["collections"][self.__class__.__name__][
                "required_fields_by_type"]

        # Set the calib archive directory
        self.archive_dir = self.config["directories"]["calib"]

        # Fields used to match raw documents with calib documents
        self._matching_fields_by_type = self.config["calibs"]["required_fields"]

        self._calib_types = self.config["calibs"]["types"]

    def get_reference_calib(self, document, observation_type=None, **kwargs):
        """ Get a reference
        """
        # Make sure document is of a valid raw calib type
        if observation_type is None:
            observation_type = document["observation_type"]
        if observation_type not in self._calib_types:
            raise ValueError(f"observation_type {observation_type} not in {self._calib_types}")

        self.logger.debug(f"Finding best matching {observation_type} for {document}.")

        # Find matching calib docs
        matching_keys = self._matching_fields_by_type[observation_type]
        doc_filter = {k: document[k] for k in matching_keys[observation_type]}
        doc_filter["datasetType"] = observation_type
        calib_docs = self.find(doc_filter, **kwargs)

        # If there are no matches, raise an error
        if len(calib_docs) == 0:
            raise FileNotFoundError(f"No matching {observation_type} for {document}.")

        # Choose the one with the nearest date
        date = parse_date(document["observing_day"])
        dates = [parse_date(_["date"]) for _ in calib_docs]
        timediffs = [abs(date - d) for d in dates]

        return calib_docs[np.argmin(timediffs)]

    def get_matching_calibs(self, document, **kwargs):
        """ Return best matching set of calibs for a given document.
        Args:
            document (ExposureDocument): The document to match with.
            **kwargs: Parsed to self.find.
        Returns:
            dict: A dict of datasetType: CalibDocument.
        Raises:
            FileNotFoundError: If there is no matching calib of any type.
            TODO: Make new MissingCalibError and raise instead.
        """
        self.logger.debug(f"Finding best matching calibs for {document}.")

        # Get best matching calib for each calib type
        best_calibs = {}
        for calib_type in self._calib_types:
            best_calibs[calib_type] = self.get_reference_calib(
                document, observation_type=calib_type, **kwargs)

        return best_calibs

    def get_calib_filename(self, metadata, extension=".fits"):
        """ Get the archived calib filename from metadata.
        Args:
            metadata (dict): The calib metadata.
            extension (str, optional): The file extension. Default: '.fits'.
        Returns:
            str: The archived filename.
        """
        datasetType = metadata["datasetType"]

        # LSST calib filenames do not include calib date, so add as parent directory
        # Also store in subdirs of datasetType
        date_ymd = date_to_ymd(metadata["date"])
        subdir = os.path.join(date_ymd, datasetType)

        # Get ordered fields used to create archived filename
        required_fields = sorted(self.config["calibs"]["required_fields"][datasetType])

        # Create the archive filename
        basename = datasetType + "_"
        basename += "_".join([str(metadata[k]) for k in required_fields])
        basename += "_" + date_ymd + extension

        return os.path.join(self.archive_dir, subdir, basename)

    def archive_master_calib(self, filename, metadata):
        """ Copy the FITS files into the archive directory and update the entry in the DB.
        Args:
            filename (str): The filename of the calib to archive, which is copied into the archive
                dir.
            metadata (abc.Mapping): The calib metadata to be stored in the document.
        """
        extension = os.path.splitext(filename)[-1]
        archive_filename = self.get_calib_filename(metadata, extension=extension)

        # Copy the file into the calib archive, overwriting if necessary
        self.logger.debug(f"Copying {filename} to {archive_filename}.")
        os.makedirs(os.path.dirname(archive_filename), exist_ok=True)
        shutil.copy(filename, archive_filename)

        # Update the document before archiving
        metadata = metadata.copy()
        metadata["filename"] = archive_filename

        # Insert the metadata into the calib database
        # Use replace operation with upsert because old document may already exist
        self.replace_one({"filename": archive_filename}, metadata, upsert=True)

    # Private methods

    def _validate_document(self, document):
        """ Validate a document for insersion.
        Args:
            document (Document): The document to validate.
        Raises:
            ValueError: If the document is invalid.
        """
        super()._validate_document(document)
        required_fields = self._required_fields_by_type.get(document["datasetType"])
        if required_fields:
            super()._validate_document(document, required_fields=required_fields)
