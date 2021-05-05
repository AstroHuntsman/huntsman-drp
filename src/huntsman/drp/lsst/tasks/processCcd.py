""" Class overrides for ProcessCcdTask. """
import lsst.pipe.base as pipeBase
from lsst.pipe.tasks.processCcd import ProcessCcdTask


class HuntsmanProcessCcdTask(ProcessCcdTask):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @pipeBase.timeMethod
    def runDataRef(self, sensorRef):
        """Process one CCD
        The sequence of operations is:
        - remove instrument signature
        - characterize image to estimate PSF and background
        - calibrate astrometry and photometry
        @param sensorRef: butler data reference for raw data
        @return pipe_base Struct containing these fields:
        - charRes: object returned by image characterization task; an lsst.pipe.base.Struct
            that will include "background" and "sourceCat" fields
        - calibRes: object returned by calibration task: an lsst.pipe.base.Struct
            that will include "background" and "sourceCat" fields
        - exposure: final exposure (an lsst.afw.image.ExposureF)
        - background: final background model (an lsst.afw.math.BackgroundList)
        """
        self.log.info("Processing %s" % (sensorRef.dataId))

        # Default return values
        exposure = None
        charRes = None
        calibRes = None

        # Instrument signature removal
        isrSuccess = True
        try:
            exposure = self.isr.runDataRef(sensorRef).exposure
        except Exception:
            isrSuccess = False

        # Characterise image
        charSuccess = True
        if isrSuccess:
            try:
                charRes = self.charImage.runDataRef(dataRef=sensorRef, exposure=exposure,
                                                    doUnpersist=False)
                exposure = charRes.exposure
            except Exception:
                charSuccess = False

        # Do image calibration (astrometry + photometry)
        calibSuccess = False
        if self.config.doCalibrate and charSuccess:
            try:
                calibRes = self.calibrate.runDataRef(
                    dataRef=sensorRef, exposure=charRes.exposure, background=charRes.background,
                    doUnpersist=False, icSourceCat=charRes.sourceCat)
                calibSuccess = True
            except Exception:
                pass

        return pipeBase.Struct(
            charRes=charRes,
            calibRes=calibRes,
            exposure=exposure,
            calibSuccess=calibSuccess,
            charSuccess=charSuccess,
            isrSuccess=isrSuccess,
        )
