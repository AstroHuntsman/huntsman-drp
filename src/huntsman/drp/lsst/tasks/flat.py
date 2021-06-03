""" Huntsman overrides to the flat field task.
- Adds multiscale source masking to remove potentially out of focus stars.
"""
from lsst.pex.config import ConfigurableField, ListField
from lsst.pipe.drivers.background import MaskObjectsTask
from lsst.pipe.drivers.constructCalibs import FlatConfig, FlatTask


# Override the config to add extra fields
class HuntsmanFlatConfig(FlatConfig):
    multiscaleSigmas = ListField(dtype=float, default=[1, 3, 5, 10],
                                 doc="Gaussian kernal widths to use for multiscale filtering.")
    maskObjects = ConfigurableField(target=MaskObjectsTask,
                                    doc="Configuration for masking objects aggressively")


class HuntsmanFlatTask(FlatTask):

    ConfigClass = HuntsmanFlatConfig

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.makeSubtask("maskObjects")

    def processSingle(self, dataRef):
        """ Override method to apply multiscale filter source masking. """
        exposure = super().processSingle(dataRef)

        for sigma in self.config.multiscaleSigmas:

            # Set the smoothing scale
            self.maskObjects.config.detectSigma = sigma

            # Mask detections
            self.maskObjects.run(exposure)

        return exposure
