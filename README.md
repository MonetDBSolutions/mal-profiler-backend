# Python MAL analytics
[![Build Status](https://travis-ci.org/MonetDBSolutions/mal-profiler-backend.svg?branch=master)](https://travis-ci.org/MonetDBSolutions/mal-profiler-backend)
[![Coverage Status](https://coveralls.io/repos/github/MonetDBSolutions/mal-profiler-backend/badge.svg)](https://coveralls.io/github/MonetDBSolutions/mal-profiler-backend)

## Introduction

This project is a library that parses MonetDB JSON profiling events
and stores them in a
[MonetDBLite-Python](https://github.com/hannesmuehleisen/MonetDBLite-Python)
database. The user can then use SQL queries to compile information
about the queries that produced those profiling events.

This is intended to be used as a back end of other programs like
Marvin, or Malcom.

