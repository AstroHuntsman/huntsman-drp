import time
import atexit
import queue
from threading import Thread
from contextlib import suppress
from multiprocessing import Pool
from abc import ABC, abstractmethod

from panoptes.utils.time import CountdownTimer

from huntsman.drp.base import HuntsmanBase
from huntsman.drp.collection import RawExposureCollection, MasterCalibCollection


class ProcessQueue(HuntsmanBase, ABC):
    """ Abstract class to process queued objects in parallel. """

    _pool_class = Pool  # Allow class overrides

    def __init__(self, exposure_collection=None, calib_collection=None, queue_interval=60,
                 status_interval=60, nproc=None, directory=None, *args, **kwargs):
        """
        Args:
            queue_interval (float): The amout of time to sleep in between checking for new
                files to process in seconds. Default 60s.
            status_interval (float, optional): Sleep for this long between status reports. Default
                60s.
            directory (str): The top level directory to watch for new files, so they can
                be added to the relevant datatable.
            nproc (int): The number of processes to use. If None (default), will check the config
                item `screener.nproc` with a default value of 1.
            *args, **kwargs: Parsed to HuntsmanBase initialiser.
        """
        super().__init__(*args, **kwargs)

        self._nproc = 1 if not nproc else int(nproc)

        # Setup the exposure collections
        if exposure_collection is None:
            exposure_collection = RawExposureCollection(config=self.config, logger=self.logger)
        self._exposure_collection = exposure_collection

        # Setup the exposure collection
        if calib_collection is None:
            calib_collection = MasterCalibCollection(config=self.config, logger=self.logger)
        self._calib_collection = calib_collection

        # Sleep intervals
        self._queue_interval = queue_interval
        self._status_interval = status_interval

        # Setup threads
        self._queue = queue.Queue()
        self._status_thread = Thread(target=self._async_monitor_status)
        self._queue_thread = Thread(target=self._async_queue_objects)
        self._process_thread = Thread(target=self._async_process_objects)
        self._threads = [self._status_thread, self._queue_thread, self._process_thread]

        # Starting values
        self._n_processed = 0
        self._n_failed = 0
        self._stop = False
        self._queued_objs = set()

        atexit.register(self.stop)  # This gets called when python is quit

    def __str__(self):
        return str(self.__class__.__name__)

    @property
    def is_running(self):
        """ Check if the screener is running.
        Returns:
            bool: True if running, else False.
        """
        return all([t.is_alive() for t in self._threads])

    @property
    def status(self):
        """ Return a status dictionary.
        Returns:
            dict: The status dictionary.
        """
        status = {"status_thread": self._status_thread.is_alive(),
                  "queue_thread": self._queue_thread.is_alive(),
                  "process_thread": self._process_thread.is_alive(),
                  "processed": self._n_processed,
                  "failed": self._n_failed,
                  "queued": self._queue.qsize()}
        return status

    def start(self):
        """ Start the file ingestor. """
        self.logger.info(f"Starting {self}.")
        self._stop = False
        for thread in self._threads:
            thread.start()

    def stop(self, blocking=True):
        """ Stop the file ingestor.
        Args:
            blocking (bool, optional): If True (default), blocks until all threads have joined.
        """
        self.logger.info(f"Stopping {self}.")
        self._stop = True
        if blocking:
            for thread in self._threads:
                with suppress(RuntimeError):
                    thread.join()
        self.logger.info(f"{self} stopped.")

    @abstractmethod
    def _get_objs(self):
        """ Return a list of objects to process.
        Returned objects do not have to be unique and may already exist in the queue.
        """
        pass

    def _async_monitor_status(self):
        """ Report the status on a regular interval. """
        self.logger.debug("Starting status thread.")

        while True:
            if self._stop:
                self.logger.debug("Stopping status thread.")
                break

            # Get the current status
            status = self.status
            self.logger.info(f"{self} status: {status}")
            if not self.is_running:
                self.logger.warning(f"{self} is not running.")

            # Sleep before reporting status again
            timer = CountdownTimer(duration=self._status_interval)
            while not timer.expired():
                if self._stop:
                    break
                time.sleep(1)

        self.logger.debug("Status thread stopped.")

    def _async_queue_objects(self):
        """ Add new objs to the queue. """
        self.logger.debug("Starting queue thread.")

        while True:
            if self._stop:
                self.logger.debug("Stopping queue thread.")
                break

            objs_to_process = self._get_objs()

            # Update files to process
            for obj in objs_to_process:

                if obj not in self._queued_objs:  # Make sure queue objs are unique
                    self._queued_objs.add(obj)
                    self._queue.put(obj)

            timer = CountdownTimer(duration=self._queue_interval)
            while not timer.expired():
                if self._stop:
                    break
                time.sleep(1)

        self.logger.debug("Queue thread stopped.")

    def _async_process_objects(self, process_func, pool_init=None, pool_init_args=None,
                               process_func_kwargs=None):
        """ Continually process objects in the queue.
        This method is indended to be overridden with all arguments provided by the subclass.
        Args:
            process_func (Function): The function to parallelise.
            pool_init (Function or None): The function used to initialise the process pool.
            pool_init_args (tuple): Args parsed to the pool initialiser.
            process_func_kwargs (dict): Kwargs parsed to the process function.
        """
        self.logger.debug(f"Starting process with {self._nproc} processes.")

        # Avoid Pool context manager to make multiprocessing coverage work
        pool = self._pool_class(self._nproc, initializer=pool_init, initargs=pool_init_args)
        try:
            while True:
                if self._stop:
                    self.logger.debug("Stopping process thread.")
                    break

                try:
                    obj = self._queue.get(timeout=5)
                except queue.Empty:
                    continue
                self.logger.debug(f"Got object {obj} from queue.")

                # Process the file in the pool asyncronously
                pool.apply_async(process_func, (obj,), process_func_kwargs,
                                 callback=self._process_callback,
                                 error_callback=self._error_callback)
        finally:
            pool.close()
            pool.join()

        self.logger.debug("Process thread stopped.")

    def _process_callback(self, result):
        """ Function that is called after an object is successfully processed.
        Args:
            result (tuple): A tupe of (obj, success).
        """
        obj, success = result
        self.logger.debug(f"successfully processed {obj}.")

        self._n_processed += 1
        if not success:
            self._n_failed += 1

        self._queue.task_done()
        self._queued_objs.remove(obj)

    def _error_callback(self, exception):
        """ This function is called if there is an uncaught exception in a process.
        Args:
            exception (Exception): The raised exception.
        """
        self.logger.error(f"{exception!r}")
        raise exception
