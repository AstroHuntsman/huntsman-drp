==============
Data reduction
==============

The LSST stack can be used to perform data reduction. It is not within the scope of this documentation
to provide a tutorial for the LSST stack because one already exists: <https://pipelines.lsst.io/getting-started/data-setup.html>.
However, the DRP can be used to setup a new Butler Repository and automatically ingest the science and master calibration files required by the user.

The manual approach
===================

First, create a Butler Repository instance:

.. code-block:: python

  from huntsman.drp.lsst.bulter import ButlerRepository

  repo = ButlerRepository(directory_name)

the directory name can be any valid directory name. Typically this should be within the pre-existing
"reductions" directory, which is mounted into the Docker containers.

One can then query for the science files they want to process and ingest them into the repo:

.. code-block:: python

  from huntsman.drp.collection import ExposureCollection

  collection = ExposureCollection.from_config()

  docs = collection.find({"observation_type": "science", "field": {"in": ["CenA, CentaurusA"]})

  repo.ingest_raw_files([d["filename"] for d in docs])

We can automatically find the best set of calibs for these science files and ingest them:

.. code-block:: python

  calib_docs = defaultdict(set)
  for doc in docs:
      cdocs = calib_collection.get_matching_calibs(doc))
      calib_docs.update(cdocs.values())

  repo.ingest_calib_docs(calib_docs)

The automated approach
======================

Experimental!
