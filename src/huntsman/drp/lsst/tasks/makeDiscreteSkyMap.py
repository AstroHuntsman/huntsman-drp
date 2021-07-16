import lsst.pipe.base as pipeBase
import lsst.pipe.base.connectionTypes as cT
from lsst.pipe.tasks import makeDiscreteSkyMap as taskBase


# DIMENSIONS = ("skymap",)
DIMENSIONS = ("skymap",)

TEMPLATES = {"calexpType": ""}


# Create a connections class
# This tells the pipeline the inputs and outputs of the task
class MakeDiscreteSkyMapConnections(pipeBase.PipelineTaskConnections,
                                    dimensions=DIMENSIONS,
                                    defaultTemplates=TEMPLATES):

    # Based on lsst.pipe.tasks.makeCoaddTempExp
    calExpList = cT.Input(
        doc="Input exposures to be covered by the output skyMap.",
        name="{calexpType}calexp",
        storageClass="ExposureF",
        dimensions=("instrument", "visit", "detector"),
        multiple=False,
        deferLoad=True,
    )

    # Based on lsst.skymap.baseSkyMap
    skyMap = cT.Output(
            name="skyMap",
            doc="The sky map divided into tracts and patches.",
            dimensions=["skymap"],
            storageClass="SkyMap"
    )


# Make a config class which uses the connections
class MakeDiscreteSkyMapConfig(pipeBase.PipelineTaskConfig, taskBase.MakeDiscreteSkyMapConfig,
                               pipelineConnections=MakeDiscreteSkyMapConnections):
    pass


class MakeDiscreteSkyMapTask(taskBase.MakeDiscreteSkyMapTask):

    ConfigClass = MakeDiscreteSkyMapConfig

    def runQuantum(self, butlerQC, inputRefs, outputRefs):
        """
        """
        inputs = butlerQC.get(inputRefs)

        # Organise inputs to what the base task needs
        wcs_md_tuple_list = []
        for calexp in inputs["calExpList"]:
            wcs = calexp.getWcs()
            md = calexp.getMetadata()
            wcs_md_tuple_list.append((wcs, md))

        # Run the task
        outputs = self.run(wcs_md_tuple_list)

        # Use butler to store the outputs
        butlerQC.put(outputs, outputRefs)
