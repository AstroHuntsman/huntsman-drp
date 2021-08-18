""" Temporary wrappers around cli commands until we agree on a better approach to running LSST. """
import os
import subprocess
import tempfile

from huntsman.drp.core import get_logger

# Default search directory for pipeline files
PIPELINE_DIR = os.path.expandvars("${OBS_HUNTSMAN}/pipelines")


def _run_pipetask_cmd(option_str=None, command_str=None, args_str=None, logger=None):
    """ Run a command using the pipetask command line interface.
    Args:
        option_str (str): The options for the pipetask CLI.
        command_str (str): The command to run.
        args_str (str): Arguments for the command.
        logger (logger): The logger.
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
    subprocess.check_output(cmd, shell=True)


def _dataIds_to_query_str(dataIds):
    """ Return a SQL-style query string from a set of dataIds.
    Args:
        dataIds (iterable): An iterable of dataIds.
    Returns:
        str: The dataIds string for CLI commands.
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


def parse_pipeline_name(pipeline, extension=".yaml"):
    """ Get the absolute path to the pipeline.
    Args:
        pipeline (str): The pipeline name.
        extension (str, optional): The extension. Default: ".yaml".
    Returns:
        str: The absolute path to the pipeline file.
    """
    if os.path.isabs(pipeline):
        pipeline_name = pipeline
    else:
        pipeline_name = os.path.join(PIPELINE_DIR, pipeline + extension)
    return pipeline_name


def pipetask_run(pipeline_name, root_directory, input_collections=None, output_collection=None,
                 dataIds=None, args_str=None, register_dataset_types=True, config=None,
                 instrument=None, nproc=1, start_method="spawn", **kwargs):
    """ Use the LSST pipetask cli to run a pipeline.
    Args:
        pipeline_name (str): The pipeline name to run.
        root_directory (str): The Butler root directory.
        input_collections (str, optional): The Butler input collections.
        output_collection (str, optional): The output collection.
        dataIds (list of dict, optional): The dataIds to process.
        args_str (str, optional): Extra arguments.
        register_dataset_types (bool, optional). If True (default), register datasetTypes with
            butler.
        config (dict, optional): Config overrides for LSST tasks.
        instrument (str, optional): The full python class name of the instrument. Used for
            applying default config overrides.
        nproc (int, optional): The number of processes to use. Default: 1.
        start_method (str, optional): The multiprocessing start method to use. Default: "spawn".
        **kwargs: Parsed to _run_pipetask_cmd.
    """
    pipeline_filename = parse_pipeline_name(pipeline_name)

    args_str = "" if args_str is None else args_str + " "
    args_str += f"-p {pipeline_filename}"
    args_str += f" -b {root_directory}"
    args_str += f" --processes {int(nproc)}"

    # Add instrument if provided
    # This will be used to apply instrument config overrides
    if instrument is not None:
        args_str += f" --instrument {instrument}"

    # Add config overrides to arg string
    # These will override any existing config values
    if config is not None:
        for k, v in config.items():
            args_str += f" --config {k}={v}"

    # if this flag is provided, will define new datasetTypes in the Butler registry
    if register_dataset_types:
        args_str += " --register-dataset-types"

    # These are the directories containing the input data
    if input_collections is not None:
        args_str += f" --input {','.join([i for i in input_collections])}"

    # These are the directories in which the output goes
    if output_collection is not None:
        args_str += f" --output {output_collection}"

    # If provided, these are the specific dataIds to process
    if dataIds is not None:
        args_str += " " + _dataIds_to_query_str(dataIds)

    # Set the start method
    if start_method is not None:
        args_str += f" --start-method {start_method}"

    return _run_pipetask_cmd(command_str="run", args_str=args_str, **kwargs)


def plot_quantum_graph(pipeline, filename):
    """ Plot the quantum graph associated with the pipeline.
    Args:
        pipeline (str): The pipeline name.
        filename (str): The filename of the output plot.
    """
    if not filename.endswith(".jpg"):
        raise ValueError("Filename should have jpg extension.")

    pipeline = parse_pipeline_name(pipeline)

    with tempfile.NamedTemporaryFile() as tf:

        # Build the pipeline and write to a tempfile
        args_str = f"-p {pipeline} --pipeline-dot {tf.name}"
        _run_pipetask_cmd(command_str="build", args_str=args_str)

        # Create the graph image from the pipeline
        subprocess.check_output(f"dot {tf.name} -Tjpg > {filename}", shell=True)
