import os
import shutil
from datetime import timedelta

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

    def get_matching_calibs(self, document):
        """ Return best matching set of calibs for a given document.
        Args:
            document (ExposureDocument): The document to match with.
        Returns:
            dict: A dict of datasetType: CalibDocument.
        Raises:
            FileNotFoundError: If there is no matching calib of any type.
            TODO: Make new MissingCalibError and raise instead.
        """
        self.logger.debug(f"Finding best matching calibs for {document}.")

        validity = timedelta(days=self.config["calibs"]["validity"])
        matching_keys = self.config["calibs"]["matching_columns"]

        # Specify valid date range
        date = parse_date(document["observing_day"])
        date_min = date - validity
        date_max = date + validity

        best_calibs = {}
        for calib_type in self.config["calibs"]["types"]:

            doc_filter = {k: document[k] for k in matching_keys[calib_type]}
            doc_filter["datasetType"] = calib_type

            # Find matching docs within valid date range
            calib_docs = self.find(doc_filter, date_min=date_min, date_max=date_max)

            # If none within valid range, log a warning and proceed
            if len(calib_docs) == 0:
                self.logger.warning(f"Best {calib_type} outside valid date range for {document}.")
                calib_docs = self.find(doc_filter)

            # If there are still no matches, raise an error
            if len(calib_docs) == 0:
                raise FileNotFoundError(f"No matching master {calib_type} for {doc_filter}.")

            dates = [parse_date(_["calib_date"]) for _ in calib_docs]
            timediffs = [abs(date - d) for d in dates]

            # Choose the one with the nearest date
            best_calibs[calib_type] = calib_docs[np.argmin(timediffs)]

        return best_calibs

    def archive_master_calib(self, filename, metadata):
        """ Copy the FITS files into the archive directory and update the entry in the DB.
        Args:
            filename (str): The filename of the calib to archive, which is copied into the archive
                dir.
            metadata (abc.Mapping): The calib metadata to be stored in the document.
        """
        # LSST calib filenames do not include calib date, so add as parent directory
        # Also store in subdirs of datasetType
        subdir = os.path.join(date_to_ymd(metadata["calib_date"]), metadata["datasetType"])

        # Create the archive filename
        basename = os.path.basename(filename)
        archive_filename = os.path.join(self.archive_dir, subdir, basename)

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
