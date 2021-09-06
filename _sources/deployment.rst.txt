==========
Deployment
==========

|project| is currently deployed on AAO's ``dccompute3``, which is accessed via VPN or web browser.
AAO hosts the ``mongodb`` server required by |project| on ``dccompute3``,
which is not otherwise included in the package.

If you want to deploy on a different machine, you will need to set up the ``mongodb`` server yourself.
This can be accomplished by following the instructions on their `website <https://www.mongodb.com/>`_.

Config
======

Deployment-specific config override files are located in `huntsman-config <https://github.com/AstroHuntsman/huntsman-config>`_, which can be obtained via:

.. code-block:: console

   $ git pull https://github.com/AstroHuntsman/huntsman-config

There are three important config files used by |project|:

1. The local config override ``.yaml`` file: This is used to override config items in the default config file.

2. The ``docker-compose.yaml`` file: This contains configuration for the docker services and volumes.

3. The ``.env`` file: This is used by ``docker-compose`` to setup environment variables between the docker host and containers.

Running the services
====================

Once the config files have been downloaded, pull the latest docker images via:

.. code-block:: console

   $ docker-compose --env_file <env_file> pull

Then, start the services:

.. code-block:: console

   $ docker-compose --env_file <env_file> up

Once the services are running, connect to the docker control container like this:

.. code-block:: console

  $ docker exec -it hunts-drp-control /bin/bash

.. _archive-directory:

The archive directory
=====================

Since |project| is deployed using docker, we need to be able to share data between the containers
and the host machine. This is accomplished by mounting the archive directory into the containers.
The archive directory is intended to contain all data products produced by the DRP, including master
calibration files and reduction outputs. The location of the archive directory on the host machine
is specified in the ``.env`` file.

Logs
====

The log directory is mapped into the docker containers using the environment variable specified in
the ``.env`` file. However, it is often more convenient to work with the docker logs directly. For example,
to follow the logs from the running `zhunts-calib-maker`` service, do:

.. code-block:: console

   $ docker logs --follow hunts-calib-maker

You can also search the docker logs for something specific, like:

.. code-block:: console

   $ docker logs hunts-calib-maker | grep "something specific"
