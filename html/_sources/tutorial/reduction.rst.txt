==============
Data reduction
==============

The LSST stack can be used to perform data reduction. It is not within the scope of this documentation to provide a tutorial for the LSST stack because one already exists `here <https://pipelines.lsst.io/getting-started/data-setup.html>`_.
However, the DRP can be used to setup a new Butler Repository and automatically ingest the science and master calibration files required by the user.

The automated approach
======================

A ready-made Butler repository can be constructed using :code:`huntsman.drp.reduction.lsst.LsstDataReduction` and subclasses.
This includes ingestion of the science files, master calibration files as well as the catalogue of reference objects for photometry / astrometry.

The basic usage looks like this:

.. code-block:: python

  from huntsman.drp.reduction.lsst import LsstDataReduction as Reduction

  query = {"observation_type": "science", "field": {"in": ["CenA, CentaurusA"]}}

  reduction = Reduction(name="test_reduction",
                        query=query)

  reduction.prepare()

This will create the Butler Repository in the "test_reduction" reduction of the "reductions" directory specified in the config.

Alternatively, we can create reduction instances from a yaml config file:

.. code-block:: python

   from huntsman.drp.reduction import create_from_file

   reduction = create_from_file(reduction_filename)

Please see the :code:`Reduction` API for further details.


The manual approach
===================

First, create a Butler Repository instance:

.. code-block:: python

  from huntsman.drp.lsst.bulter import ButlerRepository

  repo = ButlerRepository(directory_name)

The directory name can be any valid directory name. Typically this should be within the pre-existing "reductions" directory, which is mounted into the Docker containers.

One can then query for the science files they want to process and ingest them into the repo:

.. code-block:: python

  from huntsman.drp.collection import ExposureCollection

  collection = ExposureCollection.from_config()

  docs = collection.find({"observation_type": "science", "field": {"in": ["CenA, CentaurusA"]}})

  repo.ingest_raw_files([d["filename"] for d in docs])

We can automatically find the best set of calibs for these science files and ingest them:

.. code-block:: python

  from huntsman.drp.collection import CalibCollection

  calib_collection = CalibCollection.from_config()

  calib_docs = defaultdict(set)
  for doc in docs:
      cdocs = calib_collection.get_matching_calibs(doc)
      calib_docs.update(cdocs.values())

  repo.ingest_calib_docs(calib_docs)

Now use the reference catalogue client to download reference sources and ingest them into the repo:

.. code-block:: python

  from huntsman.drp.refcat import RefcatClient

  refcat_client = RefcatClient.from_config()

  refcat_client.make_from_documents(docs, filename=refcat_filename)

  self.butler_repo.ingest_reference_catalogue([self._refcat_filename])

Once the files are ingested into the repository, one can do the remainder of the processing using
the LSST stack directly. There are also several methods in the `ButlerRepository`
class that may be used to process the data, e.g. :code:`construct_skymap` and :code:`construct_calexps`.
