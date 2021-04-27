import time
import Pyro5.errors
from Pyro5.core import locate_ns
from Pyro5.nameserver import start_ns_loop

from huntsman.drp.base import HuntsmanBase


def wait_for_nameserver(timeout=30, **kwargs):
    """ Simple function to wait until nameserver is available.
    Args:
        timeout (float): The timeout in seconds.
        **kwargs: Parsed to NameServer init function.
    Raises:
        RuntimeError: If the timeout is reached before the NS is available.
    """
    ns = NameServer(**kwargs)
    ns.logger.info(f"Waiting for {timeout}s for nameserver.")
    i = 0
    while not ns.connect(suppress_error=True):
        if i > timeout:
            raise RuntimeError("Timeout while waiting for pyro nameserver.")
        i += 1
        time.sleep(1)


class NameServer(HuntsmanBase):

    """ Class to start or connect to the Pyro nameserver given a config file. """

    def __init__(self, host=None, port=None, connect=True, *args, **kwargs):
        super().__init__(*args, **kwargs)

        try:
            ns_config = self.config["pyro"]
        except KeyError:
            ns_config = {}

        self.host = host if host is not None else ns_config["host"]
        self.port = port if port is not None else ns_config["port"]

        self.name_server = None
        if connect:
            self.connect()

    def connect(self, broadcast=True, suppress_error=False):
        """ Connect to the name server.
        See documentation for Pyro5.core.locate_ns. """
        try:
            self.logger.info(f'Looking for nameserver on {self.host}:{self.port}')
            self.name_server = locate_ns(host=self.host, port=self.port, broadcast=broadcast)
            self.logger.info(f'Found Pyro name server: {self.name_server}')
            return True
        except Pyro5.errors.NamingError:
            if not suppress_error:
                self.logger.error("Unable to find nameserver.")
            return False

    def serve(self):
        """ Start the nameserver (blocking). """
        if self.connect(suppress_error=True):
            self.logger.warning("Name server is already running.")
        else:
            self.logger.info("Starting pyro name server.")
            start_ns_loop(host=self.host, port=self.port, enableBroadcast=True)
