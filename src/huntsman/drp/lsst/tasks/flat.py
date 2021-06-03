""" Huntsman overrides to the flat field task.
- Adds multiscale source masking to remove potentially out of focus stars.
"""
from lsst.pex.config import ConfigurableField, ListField
from lsst.pipe.drivers.background import MaskObjectsTask, MaskObjectsConfig
from lsst.pipe.drivers.constructCalibs import FlatConfig, FlatTask


# Annoyingly "frozen" configs can't be modified
# This means that we cannot change the "detectSigma" config item at runtime
# Therefore we have to override the class...
class MaskMultiscaleObjectsConfig(MaskObjectsConfig):
    detectSigmas = ListField(dtype=float, default=[1, 3, 5, 10],
                             doc="Gaussian kernal widths to use for multiscale filtering.")


class MaskMultiscaleObjectsTask(MaskObjectsTask):

    ConfigClass = MaskMultiscaleObjectsConfig

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def findObjects(self, exposure):
        """Iteratively find multi-scale objects on an exposure.
        Objects are masked with the ``DETECTED`` mask plane.
        Args:
            exposure (lsst.afw.image.Exposure): Exposure on which to mask objects.
        """
        mask = exposure.maskedImage.mask.clone()
        mask_frac = (mask.getArray() > 0).mean()
        self.log.info(f"Initial mask fraction: {mask_frac:.2f}.")

        # This loop is added for the multiscale functionality
        self.log.info(f"Running findObjects with {len(self.config.detectSigmas)} sigma values.")
        for detectSigma in self.config.detectSigmas:

            for _ in range(self.config.nIter):

                # Subtract a local background estimate
                bg = self.subtractBackground.run(exposure).background

                # Do source detection and create a new source mask
                self.detection.detectFootprints(exposure, sigma=detectSigma, clearMask=True)

                # Replace the subtracted background
                exposure.maskedImage += bg.getImage()

            mask_frac = (exposure.mask.getArray() > 0).mean()
            self.log.info(f"Mask fraction for detectSigma={detectSigma}: {mask_frac:.2f}.")

            # Add the contribution from this sigma to the total mask
            mask |= exposure.mask

        mask_frac = (mask.getArray() > 0).mean()
        self.log.info(f"Final mask fraction: {mask_frac:.2f}.")

        # Finally, set the exposure mask to the combined mask
        exposure.setMask(mask)


# Override the config to add extra fields
class HuntsmanFlatConfig(FlatConfig):
    maskObjects = ConfigurableField(target=MaskMultiscaleObjectsTask,
                                    doc="Configuration for masking objects aggressively")


class HuntsmanFlatTask(FlatTask):

    ConfigClass = HuntsmanFlatConfig

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.makeSubtask("maskObjects")

    def processSingle(self, dataRef):
        """ Override method to apply multiscale filter source masking. """
        exposure = super().processSingle(dataRef)

        # Mask detections over multiple spatial scales
        self.maskObjects.run(exposure)

        return exposure
