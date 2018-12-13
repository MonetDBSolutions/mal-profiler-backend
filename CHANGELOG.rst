=========
Changelog
=========

All notable changes to this project will be documented in this file.

The format is based on `Keep a Changelog <https://keepachangelog.com/en/1.0.0/>`_,
and this project adheres to `Semantic Versioning <https://semver.org/spec/v2.0.0.html>`_.

[Unreleased]
============

v0.1.0
======
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
