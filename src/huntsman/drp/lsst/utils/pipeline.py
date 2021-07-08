""" Temporary wrappers around cli commands until we agree on a better approach to running LSST. """
import os
import subprocess

from huntsman.drp.core import get_logger

PIPELINE_DIR = os.path.expandvars("${OBS_HUNTSMAN}/pipelines")


def _run_pipetask_cmd(option_str=None, command_str=None, args_str=None, logger=None):
    """ Run a command using the pipetask cli.
    Args:

    Returns:
    """
    option_str = "" if option_str is None else option_str
    command_str = "" if command_str is None else command_str
    args_str = "" if args_str is None else args_str

    logger = get_logger() if logger is None else logger

    cmd = f"pipetask {option_str} {command_str} {args_str}"
    logger.debug(f"Running LSST command in subprocess: {cmd}")

    return subprocess.check_output(cmd, shell=True)


def _dataIds_to_query_str(dataIds):
    """ Return a SQL-style query string from a set of dataIds.
    Args:
        dataIds (iterable): An iterable of dataIds.
    """
    queries = []
    for dataId in dataIds:
        query = " AND ".join([f"{k}={v}" for k, v in dataId.items()])
        queries.append("(" + query + ")")
    return "-d" + "\"" + " OR ".join(queries) + "\""


def pipetask_run(pipeline_name, inputs=None, output=None, dataIds=None, args_str=None, **kwargs):
    """ Use the LSST pipetask cli to run a pipeline.

    """
    args_str = "" if args_str is None else args_str + " "
    args_str += f"-p {os.path.join(PIPELINE_DIR, pipeline_name + '.yaml')}"

    if inputs is not None:
        args_str += f"--input ({','.join([i for i in inputs])})"

    if output is not None:
        args_str += f" --output {output}"

    if dataIds is not None:
        args_str += " " + _dataIds_to_query_str(dataIds)

    return _run_pipetask_cmd(command_str="run", args_str=args_str, **kwargs)
