![Python Tests](https://github.com/AstroHuntsman/huntsman-drp/workflows/Python%20Tests/badge.svg?branch=develop)
![Docker CI](https://github.com/AstroHuntsman/huntsman-drp/workflows/Docker%20CI/badge.svg)

# huntsman-drp
The Huntsman data reduction pipeline (`huntsman-drp`) is responsible for creating calibrated science data from raw images taken by the Huntsman telephoto array. The pipeline uses the LSST code stack configured using the [AstroHuntsman/obs_huntsman](https://github.com/AstroHuntsman/obs_huntsman) package.

To set up for running LSST reductions within docker you must have the following environment variables set,
OBS_HUNTSMAN: the location of the obs_huntsman repository
HUNTSMAN_DRP: the location of the huntsman-drp repository
OBS_HUNTSMAN_TESTDATA: the location of test_metah_data repository
DATA: the location of the main DATA directory in which the bulter repository will be established (this makes it easier to view files processed within the docker container)

## Testing
To run tests locally, ensure that the `HUNTSMAN_DRP` and `OBS_HUNTSMAN` environment variables point to the `huntsman-drp` and `obs_huntsman` repositories respectively. Testing is done inside a docker container:
```
cd $HUNTSMAN_DRP/docker/testing
docker-compose up
```
When the tests have finished, you might need to ``ctrl+c`` cancel the test script. 

When finished testing, be sure to type the following to shut down the docker containers:
```
docker-compose down
```

You can view an html coverage report after the tests complete using the following on OSX:
```
open ../../src/huntsman/drp/htmlcov/index.html
```

## Astrometry.net
Plate solving images can be done using Astrometry.net's `solve-field` function. The docker image has astrometry.net installed as well as panoptes-utils, which offers a convenient python wrapper (`from panoptes.utils.images.fits import get_solve_field`). The `Huntsman-drp/scripts/astrometry/` directory contains two scripts. The `download_index_files.sh` script downloads the astrometry.net index files needed to plate-solve images. This should be run outside the docker container. The location of the index files should then be stored in the `ASTROMETRY_INDEX_DATA` environment variable, so that the relevant directory can be mounted into the docker container. The second script, `plate_solve_directory.py`, is a convenience script for processing all the fits files within a specified parent directory.
