from huntsman.drp.services.base import ProcessQueue
from huntsman.drp.utils.ingest import list_fits_files_recursive, METRIC_SUCCESS_FLAG

__all__ = ("FileIngestor",)


def ingest_file(filename, exposure_collection, **kwargs):
    """ Ingest a file into the collection.
    Args:
        filename (str): The name of the file to ingest.
        exposure_collection (ExposureCollection): The collection in which to ingest the file.
    """
    exposure_collection.ingest_file(filename)


class FileIngestor(ProcessQueue):
    """ Class to watch for new file entries in database and process their metadata. """

    def __init__(self, directory=None, nproc=None, *args, **kwargs):
        """
        Args:
            directory (str): The top level directory to watch for new files, so they can
                be added to the relevant datatable.
            nproc (int): The number of processes to use. If None (default), will check the config
                item `screener.nproc` with a default value of 1.
            *args, **kwargs: Parsed to ProcessQueue initialiser.
        """
        super().__init__(*args, **kwargs)

        ingestor_config = self.config.get("ingestor", {})

        # Set the number of processes
        if nproc is None:
            nproc = ingestor_config.get("nproc", 1)
        self._nproc = int(nproc)

        # Set the monitored directory
        if directory is None:
            directory = ingestor_config["directory"]
        self._directory = directory
        self.logger.debug(f"Ingesting files in directory: {self._directory}")

        # Create container for failed files
        self.files_failed = set()

    def _async_process_objects(self, *args, **kwargs):
        """ Continually process objects in the queue. """
        return super()._async_process_objects(process_func=ingest_file)

    def _get_objs(self):
        """ Get list of files to process. """
        # Get set of all files in watched directory
        files_in_directory = set(list_fits_files_recursive(self._directory))
        self.logger.debug(f"Found {len(files_in_directory)} FITS files in {self._directory}.")

        # Get set of all files that are ingested and pass screening
        doc_filter = {METRIC_SUCCESS_FLAG: True}
        files_ingested = set(self.exposure_collection.find(doc_filter, key="filename"))

        # Identify files that require processing
        files_to_process = files_in_directory - files_ingested - self.files_failed
        self.logger.debug(f"Found {len(files_to_process)} files requiring processing.")

        return files_to_process

    def _on_failure(self, filename):
        """ Callback function for failed file ingestion. """
        self.logger.debug(f"Adding {filename} to failed files.")
        self.files_failed.add(filename)
