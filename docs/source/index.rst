============
huntsman-drp
============

|project| is an automated data reduction pipeline for the Huntsman telescope. Its primary
goal is to archive and monitor incoming raw files, produce regular master calibration files, and to
facilitate flexible data reduction using the `LSST science pipelines <https://pipelines.lsst.io/>`_.

|project| uses a ``mongodb`` database to store all metadata for raw files and master calibration files.
In order to preserve the integrity of the database, it should not be used directly with the
``mongodb`` client, but instead through the ``Collection`` API.

The pipeline contains several concurrent and interlinked services, which perform the following tasks
automatically:

- **File Ingestion**: The processes of identifying new raw files and adding their metadata to the ``mongoDB`` database. Currently, the astrometric calibration (WCS) is also performed during ingestion.

- **Production of master calibration files**: The process of creating master calibration frames (biases, darks and flats) when new raw calibs become available and inserting their metadata into the database.

- **Data quality monitoring**: The gathering and storing of data quality metrics from calibrated images, like the PSF FWHM and magnitude zero point.

Contents
========

.. toctree::
   :maxdepth: 2

   overview
   lsst
   tutorial/index
   deployment
   contributing
   license
   module

Indices and tables
==================

* :ref:`genindex`
