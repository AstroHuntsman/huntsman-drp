Quick Start
===========

|project| uses a `mongodb <https://www.mongodb.com/>`_ database to store file metadata. ``mongodb``
databases store nested metadata similar to python dictionaries. It is recommended to familiarise
yourself with the basics ``mongodb`` before proceeding.

The relevant terminology is as follows:

**Document**: A single item in the database. This is in some sense equivalent to a "row" in a standard database table, but can contain nested / hierarchical data.

**Collection**: A collection is similar to a table in a standard database, but contains a set of documents rather than rows. Documents in a collection do not have to share the same data structure.

Collections
-----------

|project| implements the ``Collection`` class as a wrapper around ``mongodb`` collections to ensure
standardisation of the database. ``Collection`` instances are used to perform all database operations,
including inserting, deleting, modifying and searching for documents.

The following ``Collection`` subclasses are currently implemented:

``ExposureCollection``: This is used to store metadata for all exposures, with each document corresponding to a single exposure.

``CalibCollection``: This stores metadata for master calibration files. Each document corresponds to a single calibration file.

Collection instances should be created like this:

.. code-block:: python

  from huntsman.drp.collection import ExposureCollection
  collection = ExposureCollection.from_config()

Using ``from_config`` ensures that the class instances will be correctly initialised from the config file.
Once a ``Collection`` instance is created, it automatically connects to the ``mongodb`` client and is ready for use.

Querying for files
^^^^^^^^^^^^^^^^^^

File queries are performed using **document filters**, which are simple python dictionaries. For example
to find all one-second exposures taken using the ``g_band`` filter, one can do:

.. code-block:: python

  document_filter = {"physical_filter": "g_band", "exposure_time": 1}
  docs = collection.find(document_filter)

Dot notation can be used to query for nested items, e.g.:

.. code-block:: python

  document_filter = {"metrics.has_wcs": True}

For more advanced queries, we can use `mongodb query operators <https://docs.mongodb.com/manual/reference/operator/query/>`_, which are specified as part of the
document filter. For example, to query for files with exposure times greater than one second, we can do:

.. code-block:: python

  document_filter = {"exposure_time": {"$gt": 1}}

There are also various key word arguments that can be used with ``collection.find``. For example, to
query in a date range, we can do:

.. code-block:: python

  docs = collection.find(date_min="2021-01-01", date_max="2021-02-01")

Please see the ``Collection`` API for more details.

The return value from ``Collection.find`` is a list of ``Document`` objects. These behave very similarly
to python dictionaries, but they are hashable (can be contained in sets) and facilitate ``get`` calls with
"dot" notation for nested items.

Getting calibration files
-------------------------
