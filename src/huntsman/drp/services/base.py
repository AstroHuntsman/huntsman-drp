"""
Code to provide a base class for processing objects in a queue in parallel.

Features:
- Can run using either a process pool or thread pool.
- Handles and logs uncaught exceptions.
- Minimal CPU downtime.
"""
import time
import atexit
import queue
from functools import partial
from threading import Thread
from contextlib import suppress
from multiprocessing import Pool
from multiprocessing import JoinableQueue as Queue
from abc import ABC, abstractmethod

from panoptes.utils.time import CountdownTimer

from huntsman.drp.base import HuntsmanBase
from huntsman.drp.collection import RawExposureCollection, MasterCalibCollection


def _wrap_process_func(i, func):
    """ Get objects from the input queue and process them, putting results in output queue.
    Args:
        i (int): Dummy variable used to start the pool.
        func (Function): Function used to process the object.
    """
    global exposure_collection
    global input_queue
    global output_queue
    global stop_queue

    while True:

        # Check if we should break out of the loop
        with suppress(queue.Empty):
            stop_queue.get_nowait()
            break

        # Get an object from the queue
        try:
            obj = input_queue.get(timeout=1)
        except queue.Empty:
            continue

        # Process the object
        result = {"obj": obj}
        try:
            func(obj, calib_collection=calib_collection, exposure_collection=exposure_collection)
        except Exception as err:
            result["exception"] = err

        # Put the result in the output queue
        output_queue.put(result)


def _init_pool(function, config, logger, in_queue, out_queue, stp_queue, exp_coll_name,
               calib_coll_name):
    """ Initialise the process pool.

    """
    global exposure_collection
    global calib_collection
    global input_queue
    global output_queue
    global stop_queue

    input_queue = in_queue
    output_queue = out_queue
    stop_queue = stp_queue

    exposure_collection = RawExposureCollection(collection_name=exp_coll_name,
                                                config=config, logger=logger)
    calib_collection = MasterCalibCollection(collection_name=calib_coll_name,
                                             config=config, logger=logger)


class ProcessQueue(HuntsmanBase, ABC):
    """ Abstract class to process queued objects in parallel. """

    _pool_class = Pool  # Allow class overrides

    def __init__(self, exposure_collection=None, calib_collection=None, queue_interval=300,
                 status_interval=60, nproc=None, directory=None, *args, **kwargs):
        """
        Args:
            queue_interval (float): The amout of time to sleep in between checking for new
                files to process in seconds. Default 300s.
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

        # Make queues
        self._input_queue = Queue()
        self._output_queue = Queue()
        self._stop_queue = Queue()

        # Setup threads
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
                  "queued": self._input_queue.qsize()}
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

        for _ in range(self._nproc):
            self._stop_queue.put("stop")

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
                    self._input_queue.put(obj)

            timer = CountdownTimer(duration=self._queue_interval)
            while not timer.expired():
                if self._stop:
                    break
                time.sleep(1)

        self.logger.debug("Queue thread stopped.")

    def _async_process_objects(self, process_func):
        """ Continually process objects in the queue.
        This method is indended to be overridden with all arguments provided by the subclass.
        Args:
            process_func (Function): Univariate function to parallelise.
        """
        self.logger.debug(f"Starting processing with {self._nproc} processes.")

        wrapped_func = partial(_wrap_process_func, func=process_func)

        pool_init_args = (wrapped_func, self.config, self.logger,
                          self._input_queue, self._output_queue, self._stop_queue,
                          self._exposure_collection.collection_name,
                          self._calib_collection.collection_name)

        # Avoid Pool context manager to make multiprocessing coverage work
        pool = self._pool_class(self._nproc, initializer=_init_pool, initargs=pool_init_args)

        try:
            pool.map_async(wrapped_func, range(self._nproc))

            while not (self._stop and self._output_queue.empty()):
                self._process_results()
        finally:
            pool.close()
            pool.join()

        self.logger.debug("Process thread stopped.")

    def _process_results(self):
        """ Process the results in the output queue. """
        try:
            result = self._output_queue.get(timeout=1)
        except queue.Empty:
            return

        obj = result["obj"]
        success = True

        err = result.get("exception", None)
        if err:
            self.logger.error(f"Unhandled exception while processing {obj}: {err!r}")
            success = False

        success_or_fail = "success" if success else "fail"
        self.logger.debug(f"Finished processing {obj} ({success_or_fail}).")

        self._n_processed += 1
        if not success:
            self._n_failed += 1

        self._input_queue.task_done()
        self._output_queue.task_done()
        self._queued_objs.remove(obj)
