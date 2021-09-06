========
Overview
========

Scope
=====

When data is moved moved from the Huntsman observatory control computer, it gets stored in a
directory on ``dccompute3`` (the raw data directory) with the same filename and base directory
structure as the original file. It is the responsibility of the observatory control software to
ensure filenames are unique, for example by including the time of observation in the filename.

|project| aims to ingest incoming files as they arrive on the filesystem, placing their metadata
inside the databse. Master calibration files are produced automatically on regular intervals. Additional
data quality metrics for calibrated science files (e.g. PSF FWHM and zeropoint) are automatically
gathered as soon as a full set of appropriate calibration files becomes available. These tasks are
accomplished asyncronously through several long-running services.

Services
========

FileIngestor
~~~~~~~~~~~~

It is the job of the DRP to monitor the raw data directory for new files, to extract metadata from
their FITS headers and store it in the database. This process is referred to as file ingestion.
Standardised file ingestion is performed using the `astro_metadata_translator <https://github.com/lsst/astro_metadata_translator>`_ package.
A description of the standard metadata items can be found `here <https://astro-metadata-translator.lsst.io/py-api/astro_metadata_translator.ObservationInfo.html#astro_metadata_translator.ObservationInfo>`_.
Note that the standard metadata contains the ``observation_type`` key, which identifies the type of
the exposure e.g.bias, dark, flat or science.

The ``FileIngestor`` service continually monitors the raw data directory for new files and inserts
their metadata into the ``ExposureCollection``. In addition to the standard metadata, the ``FileIngestor``
will also attempt to measure a set of metrics for each file. These are stored in the nested ``metrics``
component of ``ExposureDocument`` instances.

The service is designed to be as robust as possible. If a file fails ingestion, the error will
be logged and the offending file will not be queued for reprocessing until the service is restarted.
Even in the case of failure, the ``FileIngestor`` will attempt to record as much metadata as possible;
in the worst case scenario, e.g. if a FITS header cannot be read, the inserted document will only
contain the filename.

NOTE: Currently, the astrometric calibration (WCS) is also performed during ingestion. This happens
at the metric evaluation stage. Unlike all other tasks in |project|, this directly modifies the FITS
headers of the raw files.

CalibService
~~~~~~~~~~~~

QualityMonitor
~~~~~~~~~~~~~~
