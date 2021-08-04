from contextlib import suppress

from huntsman.drp.services.ingestor import *

# Ingestor docker image does not have LSST code installed
with suppress(ModuleNotFoundError):
    from huntsman.drp.services.calib import *
    from huntsman.drp.services.quality import *
    from huntsman.drp.services.plotter import *
