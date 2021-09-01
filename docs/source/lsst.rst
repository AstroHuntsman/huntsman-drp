=====================
Integration with LSST
=====================

|project| uses the `LSST science pipelines<https://pipelines.lsst.io/>`_ to do the bulk of its data processing. It is
highly recommended to read through the `official tutorial <https://pipelines.lsst.io/getting-started/index.html#>`_
before proceeding.

The LSST science pipelines are designed to use ``Butler`` repositories, which are essentially
directories containing configuration files, a variety of datasets and associated metadata. |project|
currently uses the "Generation 3" ``Butler`` version.

In order to allow the LSST code stack to work with Huntsman data, a Huntsman-specific "obs package"
is required. This package contains configuration information about the instrument, as well as
custom overrides for LSST tasks. The Huntsman obs package, ``obs_huntsman``, is developed `here <https://github.com/AstroHuntsman/obs_huntsman>`_.

Butler Repositories
===================

Task overrides
==============
