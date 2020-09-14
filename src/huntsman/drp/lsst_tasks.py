import subprocess
from collections import defaultdict
from lsst.pipe.tasks.ingest import IngestTask

from huntsman.drp.utils import date_to_ymd


def ingest_raw_data(filename_list, butler_directory, mode="link"):
    """

    """
    # Create the ingest task
    task = IngestTask()
    task = task.prepareTask(root=butler_directory, mode=mode)

    # Ingest the files
    task.ingestFiles(filename_list)


def make_master_biases(self, calib_date, rerun, nodes=1, procs=1):
    """

    """
    metalist = self.butler.queryMetadata('raw', ['ccd', 'expTime', 'dateObs', 'visit'],
                                         dataId={'dataType': 'bias'})

    # Select the exposures we are interested in
    exposures = defaultdict(dict)
    for (ccd, exptime, dateobs, visit) in metalist:
        if exptime not in exposures[ccd].keys():
            exposures[ccd][exptime] = []
        exposures[ccd][exptime].append(visit)

    # Parse the calib date
    calib_date = date_to_ymd(calib_date)

    for ccd, exptimes in exposures.items():
        for exptime, image_ids in exptimes.items():
            self.logger.debug(f'Making master biases for ccd {ccd} using {len(image_ids)}'
                              f' exposures of {exptime}s.')

            # Construct the calib for this ccd/exptime combination (do we need this split?)
            cmd = f"constructBias.py {self.butlerdir} --rerun {rerun}"
            cmd += f" --calib {self.calibdir}"
            cmd += f" --id visit={'^'.join([f'{id}' for id in image_ids])}"
            cmd += f" expTime={exptime}"
            cmd += f" ccd={ccd}"
            cmd += f" --nodes {nodes} --procs {procs}"
            cmd += f" --calibId expTime={exptime} calibDate={calib_date}"
            self.logger.debug(f'Calling command: {cmd}')
            subprocess.call(cmd, shell=True)






def ingest_master_bias(date, butler_directory='DATA', calibdir='DATA/CALIB',
                       rerun='processCcdOutputs', validity=1000):
    """Ingest the master bias of a given date."""
    print(f"Ingesting master bias frames.")
    cmd = f"ingestCalibs.py {butler_directory}"
    cmd += f" {butler_directory}/rerun/{rerun}/calib/bias/{date}/*/*.fits"
    cmd += f" --validity {validity}"
    cmd += f" --calib {calibdir} --mode=link"
    print(f'The ingest command is: {cmd}')
    subprocess.call(cmd, shell=True)


def ingest_master_flat(date, filter, butler_directory='DATA', calibdir='DATA/CALIB',
                       rerun='processCcdOutputs', validity=1000):
    """Ingest the master flat of a given date."""
    print(f"Ingesting master {filter} filter flats frames.")
    cmd = f"ingestCalibs.py {butler_directory}"
    cmd += f" {butler_directory}/rerun/{rerun}/calib/flat/{date}/*/*.fits"
    cmd += f" --validity {validity}"
    cmd += f" --calib {calibdir} --mode=link"
    print(f'The ingest command is: {cmd}')
    subprocess.call(cmd, shell=True)


def ingest_sci_images(file_list, butler_directory='DATA', calibdir='DATA/CALIB'):
    """Ingest science images to be processed."""
    cmd = f"ingestImages.py {butler_directory}"
    cmd += f" testdata/science/*.fits --mode=link --calib {calibdir}"
    print(f'The command is: {cmd}')
    subprocess.call(cmd, shell=True)


def processCcd(dataType='science', butler_directory='DATA', calibdir='DATA/CALIB',
               rerun='processCcdOutputs'):
    """Process ingested exposures."""
    cmd = f"processCcd.py {butler_directory} --rerun {rerun}"
    cmd += f" --calib {calibdir} --id dataType={dataType}"
    print(f'The command is: {cmd}')
    subprocess.call(cmd, shell=True)


def makeDiscreteSkyMap(butler_directory='DATA', rerun='processCcdOutputs:coadd'):
    """Create a sky map that covers processed exposures."""
    cmd = f"makeDiscreteSkyMap.py {butler_directory} --id --rerun {rerun} "
    cmd += f"--config skyMap.projection='TAN'"
    subprocess.call(cmd, shell=True)


def makeCoaddTempExp(filter, butler_directory='DATA', calibdir='DATA/CALIB',
                     rerun='coadd'):
    """Warp exposures onto sky map."""
    cmd = f"makeCoaddTempExp.py {butler_directory} --rerun {rerun} "
    cmd += f"--selectId filter={filter} --id filter={filter} tract=0 "
    cmd += f"patch=0,0^0,1^0,2^1,0^1,1^1,2^2,0^2,1^2,2 "
    cmd += f"--config doApplyUberCal=False"
    print(f'The command is: {cmd}')
    subprocess.call(cmd, shell=True)


def assembleCoadd(filter, butler_directory='DATA', calibdir='DATA/CALIB',
                  rerun='coadd'):
    """Assemble the warped exposures into a coadd"""
    cmd = f"assembleCoadd.py {butler_directory} --rerun {rerun} "
    cmd += f"--selectId filter={filter} --id filter={filter} tract=0 "
    cmd += f"patch=0,0^0,1^0,2^1,0^1,1^1,2^2,0^2,1^2,2"
    print(f'The command is: {cmd}')
    subprocess.call(cmd, shell=True)
