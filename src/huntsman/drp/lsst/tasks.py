import os
import copy
import subprocess

from lsst.pipe.tasks.ingest import IngestTask
from lsst.utils import getPackageDir

from huntsman.drp.core import get_logger
from huntsman.drp.utils.date import date_to_ymd
from huntsman.drp.lsst.utils.butler import get_unique_calib_ids, fill_calib_keys
from huntsman.drp.lsst.ingest_refcat_task import HuntsmanIngestIndexedReferenceTask


INGEST_CALIB_CONFIGS = {"bias": "ingestBias.py",
                        "dark": "ingestDark.py",
                        "flat": "ingestFlat.py"}


MASTER_CALIB_SCRIPTS = {"bias": "constructBias.py",
                        "dark": "constructDark.py",
                        "flat": "constructFlat.py"}


def run_command(cmd, logger=None):
    """
    """
    if logger is None:
        logger = get_logger()
    logger.debug(f"Running LSST command in subprocess: {cmd}")
    return subprocess.run(cmd, shell=True, check=True)


def ingest_raw_data(filename_list, butler_directory, mode="link", ignore_ingested=True):
    """ Ingest raw files into a butler repository.

    """
    # Create the ingest task
    task = IngestTask()
    task = task.prepareTask(root=butler_directory, mode=mode, ignoreIngested=ignore_ingested)

    # Ingest the files
    task.ingestFiles(filename_list)


def ingest_reference_catalogue(butler_directory, filenames, output_directory=None):
    """

    """
    if output_directory is None:
        output_directory = butler_directory

    # Load the config file
    pkgdir = getPackageDir("obs_huntsman")
    config_file = os.path.join(pkgdir, "config", "ingestSkyMapperReference.py")
    config = HuntsmanIngestIndexedReferenceTask.ConfigClass()
    config.load(config_file)

    # Convert the files into the correct format and place them into the repository
    args = [butler_directory,
            "--configfile", config_file,
            "--output", output_directory,
            "--clobber-config",
            *filenames]
    HuntsmanIngestIndexedReferenceTask.parseAndRun(args=args)


def ingest_master_calibs(datasetType, filenames, butler_directory, calib_directory, validity):
    """
    Ingest the master bias of a given date.
    """
    cmd = f"ingestCalibs.py {butler_directory}"
    cmd += " " + " ".join(filenames)
    cmd += f" --validity {validity}"
    cmd += f" --calib {calib_directory} --mode=link"

    # We currently have to provide the config explicitly
    config_file = INGEST_CALIB_CONFIGS[datasetType]

    config_file = os.path.join(getPackageDir("obs_huntsman"), "config", config_file)
    cmd += " --config clobber=True"
    cmd += f" --configfile {config_file}"

    # Run the LSST command
    run_command(cmd)


def make_master_calibs(datasetType, data_ids, calib_date, butler, butler_directory, calib_directory,
                       rerun, nodes=1, procs=1):
    """
    Use constructBias.py to construct master bias frames for the data_ids. The master calibs are
    produced for each unique calibId obtainable from the list of dataIds.

    Args:
        datasetType (str): The calib datasetType (e.g. bias, flat).
        data_ids (list of dict): The list of dataIds used to produce the master calibs.
        calib_date (date): The date to associate with the master calibs.
        butler_repository (huntsman.drp.butler.ButlerRepository): The butler repository object.
        rerun (str): The rerun name.
        nodes (int): The number of nodes to run on.
        procs (int): The number of processes to use per node.
    """
    calib_date = date_to_ymd(calib_date)

    # We currently have to provide the config explicitly
    script_name = MASTER_CALIB_SCRIPTS[datasetType]

    # Prepare the dataIds
    data_ids = copy.deepcopy(data_ids)
    for data_id in data_ids:
        # Fill required missing keys
        data_id.update(fill_calib_keys(data_id, datasetType, butler=butler,
                                       keys_ignore=["calibDate"]))
        # Add the calib date to the dataId
        data_id["calibDate"] = calib_date

    # For some reason we have to run each calibId separately
    unique_calib_ids = get_unique_calib_ids(datasetType, data_ids, butler=butler)
    for calib_id in unique_calib_ids:

        # Get data_ids corresponding to this calib_id
        data_id_subset = [d for d in data_ids if calib_id.items() <= d.items()]

        # Construct the command
        cmd = f"{script_name} {butler_directory} --rerun {rerun}"
        cmd += f" --calib {calib_directory}"
        for data_id in data_id_subset:
            cmd += " --id"
            for k, v in data_id.items():
                cmd += f" {k}={v}"
        cmd += f" --nodes {nodes} --procs {procs}"
        cmd += " --calibId " + " ".join([f"{k}={v}" for k, v in calib_id.items()])

        # Run the LSST command
        run_command(cmd)


def make_calexps(data_ids, rerun, butler_directory, calib_directory, no_exit=True, procs=1,
                 clobber_config=False):
    """ Make calibrated exposures (calexps) using the LSST stack. These are astrometrically
    and photometrically calibrated as well as background subtracted. There are several byproducts
    of making calexps including sky background maps and preliminary source catalogues and metadata,
    inclding photometric zeropoints.
    Args:
        data_ids (list of abc.Mapping): The data IDs of the science frames to process.
        rerun (str): The name of the rerun.
        butler_directory (str): The butler repository directory name.
        calib_directory (str): The calib directory used by the butler repository.
        no_exit (bool, optional): If True (default), the program will not exit if an error is
            raised by the stack.
        procs (int, optional): The number of processes to use per node.  Default 1.
    """
    cmd = f"processCcd.py {butler_directory}"
    if no_exit:
        cmd += " --noExit"
    cmd += f" --rerun {rerun}"
    cmd += f" --calib {calib_directory}"
    cmd += f" -j {procs}"
    for data_id in data_ids:
        cmd += " --id"
        for k, v in data_id.items():
            cmd += f" {k}={v}"
    if clobber_config:
        cmd += " --clobber-config"
    run_command(cmd)


def makeDiscreteSkyMap(butler_directory='DATA', rerun='processCcdOutputs:coadd'):
    """Create a sky map that covers processed exposures."""
    cmd = f"makeDiscreteSkyMap.py {butler_directory} --id --rerun {rerun} "
    cmd += "--config skyMap.projection='TAN'"
    subprocess.check_output(cmd, shell=True)


def makeCoaddTempExp(filter, butler_directory='DATA', calib_directory='DATA/CALIB',
                     rerun='coadd'):
    """Warp exposures onto sky map."""
    cmd = f"makeCoaddTempExp.py {butler_directory} --rerun {rerun} "
    cmd += f"--selectId filter={filter} --id filter={filter} tract=0 "
    cmd += "patch=0,0^0,1^0,2^1,0^1,1^1,2^2,0^2,1^2,2"
    cmd += "--config doApplyUberCal=False"
    print(f'The command is: {cmd}')
    subprocess.check_output(cmd, shell=True)


def assembleCoadd(filter, butler_directory='DATA', calib_directory='DATA/CALIB',
                  rerun='coadd'):
    """Assemble the warped exposures into a coadd"""
    cmd = f"assembleCoadd.py {butler_directory} --rerun {rerun} "
    cmd += f"--selectId filter={filter} --id filter={filter} tract=0 "
    cmd += "patch=0,0^0,1^0,2^1,0^1,1^1,2^2,0^2,1^2,2"
    print(f'The command is: {cmd}')
    subprocess.check_output(cmd, shell=True)
