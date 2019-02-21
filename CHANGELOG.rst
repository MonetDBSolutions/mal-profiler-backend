=========
Changelog
=========

All notable changes to this project will be documented in this file.

The format is based on `Keep a Changelog <https://keepachangelog.com/en/1.0.0/>`_,
and this project adheres to `Semantic Versioning <https://semver.org/spec/v2.0.0.html>`_.

[Unreleased]
============
v0.3.0 (2019-02-21)
===================
Added
*****
* Wrappers for ``Connection::transaction``, ``Connection::commit`` and
  ``Connection::rollback``.
* An ``AnalyticsException`` that is the at the root of the exception
  hierarchy for this package.

Changed
*******
* Only the query text is now inserted in the db, instead of the string
  ``define("<query>", "<optimizer>", <number>)``
* ``DatabaseManager::execute`` returns now the results using the
  ``fetchnumpy`` call of MonetDBLite
* Refactored the constructor of ``ProfilerObjectParser`` in order to
  pass one dictionary with the limits.
* ``DatabaseManager::execute_query`` now accepts an argument that
  collects parameters for the query. The semantics are identical to
  ``MonetdbLite::Cursor::execute``.
* Generalized the ``supervises_execution`` relation: It now expresses
  the call graph between executions, not only the remote calls. The
  relevant SQL table was renamed from ``supervises_execution`` to
  ``initiates_executions``. A ``remote`` boolean field was added to
  this table to indicate if parent and child execution run on
  different MonetDB sessions.


v0.2.0 (2019-01-14)
===================
Added
*****
* Some documentation
* More tests
* A user defined mnemonic label for queries
* Limits for the new tables
* New tables:

  - query
  - supervises_executions

* Support for distributed queries (Remote table)

v0.1.0 (2018-12-13)
===================
Added
*****
* DatabaseManager class. Instantiates a *singleton* object that
  handles communication with the database.
* ProfilerObjectParser class. Code for parsing trace JSON objects.
* SQL script to create tables in the database.
* SQL scripts for dropping and adding constraints in order to
  accelerate data loading.
* Functions to facilitate opening of compressed files
  (currently supports .gz and .bz2 files).
* Test suite.
