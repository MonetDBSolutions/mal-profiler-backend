====================
Python MAL analytics
====================

.. image:: https://travis-ci.org/MonetDBSolutions/mal_analytics.svg?branch=master
    :target: https://travis-ci.org/MonetDBSolutions/mal_analytics

.. image:: https://coveralls.io/repos/github/MonetDBSolutions/mal_analytics/badge.svg?branch=master&service=github
    :target: https://coveralls.io/github/MonetDBSolutions/mal_analytics?branch=master

Introduction
============

This project is a library that parses MonetDB JSON profiling events
and stores them in a `MonetDBLite-Python
<https://github.com/hannesmuehleisen/MonetDBLite-Python>`_
database. The user can then use SQL queries to compile information
about the queries that produced those profiling events.

This is intended to be used as a back end of other programs like
Marvin, or Malcom.

