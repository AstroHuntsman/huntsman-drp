from multiprocessing import Process
from Pyro5.api import Daemon as PyroDaemon

from huntsman.drp.utils.pyro.nameserver import NameServer, wait_for_nameserver
from huntsman.drp.refcat import RefcatServer, PYRO_NAME


def run_nameserver():
    ns = NameServer(connect=False)
    ns.serve()


def run_refcat(host="localhost", port=0):

    # Connect to the nameserver
    ns = NameServer()

    # Create the refcat server instance
    server = RefcatServer()

    # Register the refcat server with pyro
    with PyroDaemon(host=host, port=port) as daemon:
        uri = daemon.register(server)

        server.logger.info(f"Registering {server} with URI: {uri}")
        ns.name_server.register(PYRO_NAME, uri, safe=True)

        server.logger.info(f"Starting request loop for {server}.")
        daemon.requestLoop()


if __name__ == "__main__":

    nameserver_proc = Process(target=run_nameserver)
    refcat_proc = Process(target=run_refcat)

    # Start the pyro nameserver
    nameserver_proc.start()
    wait_for_nameserver()

    # Start the other pyro services
    refcat_proc.start()

    # Run until nameserver process joins
    nameserver_proc.join()
