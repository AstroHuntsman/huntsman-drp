import os
import yaml
from datetime import timedelta

from huntsman.drp.reduction.base import ReductionBase
from huntsman.drp.reduction.lsst import LsstReduction
from huntsman.drp.lsst.utils.pipeline import plot_quantum_graph

EXTRA_CONFIG_SCI = {"characterizeImage:detection.reEstimateBackground": False,
                    "calibrate:detection.reEstimateBackground": False}

EXTRA_CONFIG_SKY = {"calibrate:doPhotoCal": False}


class OffsetSkyReduction(LsstReduction):
    """ Data reduction using offset sky frames to estimate background for science images. """

    def __init__(self, sky_query, sky_pipeline, timedelta_minutes, *args, **kwargs):

        super().__init__(initialise=False, *args, **kwargs)

        # Store the sky pipeline
        self.sky_pipeline = sky_pipeline
        self._sky_pipeline_filename = os.path.join(self.directory, "sky_pipeline.yaml")

        self._sky_query = sky_query
        self._timedelta_minutes = timedelta_minutes

        self.sky_docs = {}

        # Setup required task config
        self._sky_pipeline_config = EXTRA_CONFIG_SKY
        self._pipeline_config.update(EXTRA_CONFIG_SCI)

        self._initialise()

    # Methods

    def prepare(self):
        """ Override method to get matching sky offset docs and their associated calibs. """

        # Use base prepare method to set science docs, calibs and make reference catalogue
        ReductionBase.prepare(self)

        for doc in self.science_docs:
            # Get background docs
            self.sky_docs[doc] = self._get_matching_sky_docs(doc)

            all_sky_docs = self._get_all_sky_docs()  # List rather than dict

            # Update set of calibs so we can reduce the background docs
            calib_docs = self._get_calibs(all_sky_docs)
            for datasetType, docs in calib_docs.items():
                self.calib_docs[datasetType].update(docs)

        # Ingest raw data, calibs and refcat
        # Note: Only the science docs need a refcat because we don't need to calibrate the sky ones
        super().prepare(call_super=False)

        # Ingest extra sky docs
        self.butler_repo.ingest_raw_files([d["filename"] for d in all_sky_docs], define_visits=True)

        # Plot the quantum graph for the offset sky observation
        qgfilename = os.path.join(self.image_dir, "pipeline_sky_qg.jpg")
        plot_quantum_graph(self._sky_pipeline_filename, qgfilename)

    def reduce(self):
        """ Override method to measure the offset sky backgrounds before processing. """

        # Get dataIds for sky documents
        dataIds = [self.butler_repo.document_to_dataId(d) for d in self.sky_docs]

        self.logger.info(f"Making offset sky images from {len(dataIds)} dataIds.")

        # Run the pipeline
        self.butler_repo.run_pipeline(self._sky_pipeline_filename,
                                      dataIds=dataIds,
                                      output_collection=self._output_collection,
                                      config=self._sky_pipeline_config)

        # Reduce the science exposures
        super().reduce()

    # Private methods

    def _initialise(self):
        """ Override method to write the sky pipeline. """
        super()._initialise()

        # Write the sky pipeline to yaml file
        with open(self._sky_pipeline_filename, 'w') as f:
            yaml.dump(self.sky_pipeline, f, default_flow_style=False)

    def _get_matching_sky_docs(self, document):
        """ Get list of documents to measure the offset sky background with.
        Args:
            document (ExposureDocument): The raw exposure document to match with.
        Returns:
            list of ExposureDocument: The matching documents.
        """
        td = timedelta(minutes=self._timedelta_minutes)
        date_min = document["date"] - td
        date_max = document["date"] + td

        matches = self._exposure_collection.find(date_min=date_min, date_max=date_max,
                                                 **self._sky_query)

        if not matches:
            raise RuntimeError(f"No matching offset sky images for {document}.")

        self.logger.debug(f"Found {len(matches)} matching offset sky images for {document}.")

        return matches

    def _get_all_sky_docs(self):
        """ Get all sky documents in a set rather than a nested dictionary. """
        all_sky_docs = set()
        for sky_docs in self.sky_docs.values():
            all_sky_docs.update(sky_docs)
        return all_sky_docs
