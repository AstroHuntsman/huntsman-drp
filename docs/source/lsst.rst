=====================
Integration with LSST
=====================

|project| uses the `LSST science pipelines <https://pipelines.lsst.io/>`_ to do the bulk of its data processing. It is
highly recommended to read through the `official tutorial <https://pipelines.lsst.io/getting-started/index.html#>`_
before proceeding.

The LSST science pipelines are designed to use ``Butler`` repositories, which are essentially
directories containing configuration files, a variety of datasets and associated metadata. |project|
currently uses the "Generation 3" ``Butler`` version.

In order to allow the LSST code stack to work with Huntsman data, a Huntsman-specific "obs package"
is required. This package contains configuration information about the instrument, as well as
custom overrides for LSST tasks. The Huntsman obs package, ``obs_huntsman``, is developed `here <https://github.com/AstroHuntsman/obs_huntsman>`_.
The current approach is to develop pure LSST code in ``obs_huntsman``, such as task overrides and custom pipelines.

File ingestion
==============

Standardised file ingestion is performed using the `astro_metadata_translator <https://github.com/lsst/astro_metadata_translator>`_ package.
This is the standard approach for ingestion files into a Butler Repository. All metadata is derived
from the FITS header rather than the filename. A description of the standard metadata items can
be found `here <https://astro-metadata-translator.lsst.io/py-api/astro_metadata_translator.ObservationInfo.html#astro_metadata_translator.ObservationInfo>`_.

Butler Repositories
===================

The |project| package provides a ``ButlerRepository`` class, a wrapper encapsulating the functionality
of a Butler Repository, which otherwise requires command-line interface (CLI) to setup and use. The
``ButlerRepository`` class makes it easy to do this without using the command-line interface,
and provides additional convenience features used throughout the package. Please see the tutorial for
how to use this class.

Task overrides
==============

One of the main advantages of using the LSST stack is that we can use pipeline tasks. These are highly configurable pieces
of code that are designed to perform specific objectives, such as detecting sources, measuring background
or matching a photometric reference catalogue to a detected source catalogue. Most tasks are usable with their default settings
and do not need to be overridden. However, tasks that require modifications are overridden in the
`obs_huntsman <https://github.com/AstroHuntsman/obs_huntsman/tree/develop/python/lsst/obs/huntsman/tasks>`_ package.

Pipelines
=========

LSST pipelines are a series of tasks that linked together by their respective inputs and outputs, often
written in ``yaml`` files. They are used for combining individual tasks into larger pipelines, for example
to create master calibration files or to calibrate and / or coadd science exposures starting from the raw
exposures as inputs. Custom overrides to LSST pipelines are located in the ``obs_huntsman`` `pipelines directory <https://github.com/AstroHuntsman/obs_huntsman/tree/develop/pipelines>`_.
