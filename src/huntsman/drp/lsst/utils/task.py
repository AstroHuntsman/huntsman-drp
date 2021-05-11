import subprocess

from huntsman.drp.core import get_logger


def run_cmdline_task_subprocess(cmd, logger=None, timeout=None):
    """Run an LSST command line task.
    Args:
        cmd (str): The LSST commandline task to run in a subprocess.
        logger (logger, optinal): The logger.
        timeout (float, optional): The subprocess timeout in seconds. If None (default), no timeout
            is applied.
    Raises:
        subprocess.CalledProcessError: If the subprocess return code is non-zero.
        subprocess.TimeoutExpired: If the subprocess timeout is reached.
    """
    if logger is None:
        logger = get_logger()
    logger.debug(f"Running LSST command in subprocess: {cmd}")

    with subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE,
                          stderr=subprocess.STDOUT) as proc:

        # Log subprocess output in real time
        with proc.stdout as pipe:
            for line in iter(pipe.readline, b''):
                logger.debug(line.decode().strip("\n"))

        # Wait for subprocess to finish
        returncode = proc.wait(timeout=timeout)

    # Raise an error if the command failed
    # This does not always seem to work as some LSST scripts always seem to exit 0
    if returncode != 0:
        raise subprocess.CalledProcessError(cmd=cmd, returncode=returncode)

    return


def run_cmdline_task(Task, args, config=None, log=None, doReturnResults=True, **kwargs):
    """ Run a command line task and return results.
    Args:
        Task (class): The LSST Task to run.
        args (list): List of args for the task. This can be obtained by splitting the command line
            input string.
        config (): ???
        log (): ???
        doReturnResults (bool): If True (default), return task results.
    Returns:
        lsst.pipe.base.struct.Struct: The task results.
    """
    results = Task.parseAndRun(args=args, config=config, log=log, doReturnResults=doReturnResults,
                               **kwargs)

    return results
