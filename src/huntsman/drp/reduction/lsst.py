import os
import yaml

from huntsman.drp.reduction.base import ReductionBase
from huntsman.drp.lsst.butler import ButlerRepository
from huntsman.drp.lsst.utils.pipeline import plot_quantum_graph

from huntsman.drp.utils.date import validity_range, get_date_range_from_docs


class LsstReduction(ReductionBase):
    """ Data reduction using LSST stack. """

    def __init__(self, pipeline=None, initialise=True, *args, **kwargs):
        super().__init__(initialise=False, *args, **kwargs)

        self._butler_directory = os.path.join(self.directory, "lsst")
        self.butler_repo = None

        # Store the pipeline
        if pipeline is None:
            raise ValueError(f"pipeline must be specified for {self.__class__.__name__}")
        self.pipeline = pipeline

        self._pipeline_filename = os.path.join(self.directory, "pipeline.yaml")

        # Write pipeline results to this butler subdirectory
        self._output_collection = "pipeline_outputs"

        # Setup task configs
        self._pipeline_config = {}

        if initialise:
            self._initialise()

    # Methods

    def prepare(self, call_super=True):
        """ Override the prepare method to ingest the files into the butler repository.
        Args:
            call_super (bool, optional): If True (default), call super method before other tasks.
        """
        if call_super:
            super().prepare()

        # Ingest raw files into butler repository
        self.butler_repo.ingest_raw_files([d["filename"] for d in self.science_docs],
                                          define_visits=True)

        # Ingest master calibs into butler repository
        for datasetType, docs in self.calib_docs.items():
            dates = get_date_range_from_docs(docs)
            begin_date, end_date = validity_range(
                dates, validity=int(self.config['CalibService']['validity']))
            self.butler_repo.ingest_calibs(
                datasetType, [d["filename"] for d in docs],
                begin_date=begin_date, end_date=end_date)

        # Ingest reference catalogue
        self.butler_repo.ingest_reference_catalogue([self._refcat_filename])

        # Create the skyMap from the ingested science files
        self.butler_repo.construct_skymap()

    def reduce(self):
        """ Use the LSST stack to calibrate and stack exposures. """

        dataIds = [self.butler_repo.document_to_dataId(d) for d in self.science_docs]

        self.logger.info(f"Reducing {len(dataIds)} dataIds.")

        # Run the pipeline
        self.butler_repo.run_pipeline(self._pipeline_filename,
                                      dataIds=dataIds,
                                      output_collection=self._output_collection,
                                      config=self._pipeline_config)

    # Private methods

    def _initialise(self):
        """ Override method to create the butler repository. """
        super()._initialise()

        # Initialise the butler repository
        self.butler_repo = ButlerRepository(self._butler_directory, config=self.config)

        # Write the pipeline to yaml
        with open(self._pipeline_filename, 'w') as f:
            yaml.dump(self.pipeline, f, default_flow_style=False)

        # Plot the pipeline quantum graph
        qgfilename = os.path.join(self.image_dir, "pipeline_qg.jpg")
        plot_quantum_graph(self._pipeline_filename, qgfilename)
