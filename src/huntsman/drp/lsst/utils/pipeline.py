""" Temporary wrappers around cli commands until we agree on a better approach to running LSST. """
import os
import subprocess

from huntsman.drp.core import get_logger

PIPELINE_DIR = os.path.expandvars("${OBS_HUNTSMAN}/pipelines")


def _run_pipetask_cmd(option_str=None, command_str=None, args_str=None, logger=None):
    """ Run a command using the pipetask cli.
    Args:

    Returns:

    Raises:
        subprocess.CalledProcessError: Raise this error if the command fails.
    """
    logger = get_logger() if logger is None else logger

    cmd = "pipetask"
    if option_str:
        cmd += f" {option_str}"
    if command_str:
        cmd += f" {command_str}"
    if args_str:
        cmd += f" {args_str}"

    logger.debug(f"Running LSST command in subprocess: {cmd}")
    return subprocess.check_output(cmd, shell=True)


def _dataIds_to_query_str(dataIds):
    """ Return a SQL-style query string from a set of dataIds.
    Args:
        dataIds (iterable): An iterable of dataIds.
    """
    queries = []

    for dataId in dataIds:

        # Strings need to have single quotes for the query
        dataIdDict = {}
        for k, v in dataId.items():
            if isinstance(v, str):
                dataIdDict[k] = f"'{v}'"
            else:
                dataIdDict[k] = v

        query = " AND ".join([f"{k}={v}" for k, v in dataIdDict.items()])
        queries.append("(" + query + ")")

    return "-d " + "\"" + " OR ".join(queries) + "\""


def pipetask_run(pipeline_name, root_directory, input_collections=None, output_collection=None,
                 dataIds=None, args_str=None, register_dataset_types=True, **kwargs):
    """ Use the LSST pipetask cli to run a pipeline.

    """
    if os.path.isabs(pipeline_name):
        pipeline_filename = pipeline_name
    else:
        pipeline_filename = os.path.join(PIPELINE_DIR, pipeline_name + '.yaml')

    args_str = "" if args_str is None else args_str + " "
    args_str += f"-p {pipeline_filename}"
    args_str += f" -b {root_directory}"

    if register_dataset_types:
        args_str += " --register-dataset-types"

    if input_collections is not None:
        args_str += f" --input {','.join([i for i in input_collections])}"

    if output_collection is not None:
        args_str += f" --output {output_collection}"

    if dataIds is not None:
        args_str += " " + _dataIds_to_query_str(dataIds)

    return _run_pipetask_cmd(command_str="run", args_str=args_str, **kwargs)
