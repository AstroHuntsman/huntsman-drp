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

Once the files are ingested into the repository, one can do the remainder of the processing using
the LSST stack directly. Additionally, there are several convenience functions in the `ButlerRepository`
class that may be used to process the data, e.g. `construct_skymap` and `construct_calexps`. However,
it is recommended to use the former approach because it is more flexible.

The automated approach
======================

Please see the class `huntsman.drp.recuction.lsst.LsstDataReduction`, which attempts to automate the above process. This should be considered experimental.
