""" Class to regularly check and improve the health of the various collections. """
import os
import time

from huntsman.drp.base import HuntsmanBase


# Dict of collection class name: list of required checks
CHECKS = {"RawExposureCollection": ["check_fits_fz_duplicate"]}


class HealthMonitor(HuntsmanBase):

    def __init__(self, sleep_interval=120, **kwargs):
        """
        Args:
            sleep_interval (float): The sleep interval in seconds.
            **kwargs: Parsed to super init function.
        """
        super().__init__(**kwargs)

        self._stop = True
        self._sleep_interval = sleep_interval

    # Methods

    def start(self):
        """ Start the service. """
        self.logger.info(f"Starting {self}.")
        self._stop = False
        for thread in self._threads:
            thread.start()

    def stop(self):
        """ Stop the service. """
        self.logger.info(f"Stopping {self}.")
        self._stop = True
        for thread in self._threads:
            thread.join()

    # Static methods

    @staticmethod
    def check_file_exists(document, **kwargs):
        """ Check if a file exists.
        Args:
            document (huntsman.drp.document.Document): The document to test.
        Returns:
            bool: True if the file exists, else False.
        """
        return os.path.isfile(document["filename"])

    @staticmethod
    def check_fits_fz_duplicate(document, documents, **kwargs):
        """ Check if a duplicate fits/fz file exists.
        Args:
            document (huntsman.drp.document.Document): The document to test.
        Returns:
            bool: True if duplicate exists, else False.
        """
        filename = document["filename"]

        if filename.endswith(".fits"):
            test_filename = filename + ".fz"

        elif filename.endswith(".fits.fz"):
            test_filename = filename[:-3]

        return test_filename not in [d["filename"] for d in documents]

    # Private methods

    def _monitor_collection(self, collection):
        """ Regularly check the status of a collection.
        Args:
            collection (huntsman.drp.collection.Collection): A collection object.
        """
        collection_name = collection.__class__.__name__
        n_deleted = 0

        while True:
            if self._stop:
                return

            documents = collection.find()
            docs_to_delete = set()

            # Do common checks
            self._make_common_checks(documents, docs_to_delete)

            # Do collection-specific checks
            self._make_specific_checks(collection_name, documents, docs_to_delete)

            # Delete documents
            collection.delete_many(docs_to_delete, force=True)
            n_deleted += len(docs_to_delete)

            # Status report
            status = {"size": len(documents) - n_deleted,
                      "deleted": n_deleted}

            self.logger.info(f"Health status for {collection_name}: {status}")

            self.logger.debug(f"{self} sleeping for {self._sleep_interval} seconds.")
            time.sleep(self._sleep_interval)

    def _make_common_checks(self, documents, docs_to_delete):
        """ Perform checks common to all collections.
        Args:
            documents (list of Document): The list of documents in the collection.
            documents_to_delete (set): Set of documents to delete which gets modified by this
                function.
        """
        for document in documents:

            # Make sure the file exists
            if not self._make_check("check_file_exists", document, documents=documents):
                docs_to_delete.add(document)
                continue

    def _make_specific_checks(self, collection_name, documents, docs_to_delete):
        """ Perform checks common to all collections.
        Args:
            collection_name (str): The name of the collection class.
            documents (list of Document): The list of documents in the collection.
            documents_to_delete (set): Set of documents to delete which gets modified by this
                function.
        """
        required_checks = CHECKS.get(collection_name, None)
        if not required_checks:
            return docs_to_delete

        for document in documents:
            for check_name in required_checks:
                if not self._make_check(check_name, document, documents=documents):
                    docs_to_delete.add(document)
                    continue

    def _make_check(self, check_name, document, **kwargs):
        """ Check a specific requirement.
        Args:
            check_name: The name of the class method to check.
            document (huntsman.drp.document.Document): The document to test.
            **kwargs: Parsed to to check method.
        Returns:
            bool: True if check success, else False.
        """
        result = getattr(self, check_name)(document, **kwargs)
        if not result:
            self.logger.warning(f"Document {document} failed {check_name} check.")
        return result
