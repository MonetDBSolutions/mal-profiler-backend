.. image:: https://travis-ci.org/MonetDBSolutions/mal-profiler-backend.svg?branch=master
    :target: https://travis-ci.org/MonetDBSolutions/mal-profiler-backend

.. image:: https://coveralls.io/repos/github/MonetDBSolutions/mal-profiler-backend/badge.svg?branch=master&service=github
    :target: https://coveralls.io/github/MonetDBSolutions/mal-profiler-backend?branch=master

####################
Python MAL analytics
####################

Introduction
============

This project is a library that parses MonetDB JSON profiling events
and stores them in a `MonetDBLite-Python
<https://github.com/hannesmuehleisen/MonetDBLite-Python>`_
database. The user can then use SQL queries to compile information
about the queries that produced those profiling events.

This is intended to be used as a back end of other programs like
Marvin, or Malcom.

