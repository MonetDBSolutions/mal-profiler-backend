=========
Changelog
=========

All notable changes to this project will be documented in this file.

The format is based on `Keep a Changelog <https://keepachangelog.com/en/1.0.0/>`_,
and this project adheres to `Semantic Versioning <https://semver.org/spec/v2.0.0.html>`_.

[Unreleased]
============
Added
*****
* Schema for the trace/heartbeat database
* Parser for traces
* Parser for heartbeats
* An abstraction layer to facilitate opening of compressed files
  (currently supports .gz and .bz2 files)
* SQL scripts for dropping and adding constraints in order to
  accelerate data loading.
