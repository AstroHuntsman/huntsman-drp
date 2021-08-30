Contributing
============

GitHub
------

Testing
-------

In order to test local changes to |project|, set the environment variable ``HUNTSMAN_DRP`` to
point to your local repository. If you also want to test local changes to ``obs_huntsman``, do the
same for that repository using the ``OBS_HUNTSMAN`` variable.

To run all the unit tests, do:

.. code-block:: console

  $ bash ${HUNTSMAN_DRP}/scripts/testing/run-local-tests.sh

If instead you want to enter the docker container and run some other commands, you can do:

.. code-block:: console

  $ docker-compose -f ./docker/testing/docker-compose.yml run --rm python-tests /bin/bash

Code style guide
----------------
