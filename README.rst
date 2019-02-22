====================
Python MAL analytics
====================

.. image:: https://travis-ci.org/MonetDBSolutions/mal_analytics.svg?branch=master
    :target: https://travis-ci.org/MonetDBSolutions/mal_analytics

.. image:: https://codecov.io/gh/MonetDBSolutions/mal_analytics/branch/master/graph/badge.svg
    :target: https://codecov.io/gh/MonetDBSolutions/mal_analytics

.. image:: https://www.codefactor.io/repository/github/monetdbsolutions/mal_analytics/badge/master
   :target: https://www.codefactor.io/repository/github/monetdbsolutions/mal_analytics/overview/master
   :alt: CodeFactor


Introduction
============

This project is a library that parses MonetDB JSON profiling events
and stores them in a `MonetDBLite-Python
<https://github.com/hannesmuehleisen/MonetDBLite-Python>`_
database. The user can then use SQL queries to compile information
about the queries that produced those profiling events.

This is intended to be used as a back end of other programs like
Marvin, or Malcom.
