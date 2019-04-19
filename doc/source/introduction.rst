.. _introduction:

Introduction
============

The purpose of the *MAL Analytics* library is to parse and store
profiling information emitted by the MonetDB server. It stores this
information using `MonetDBLite-Python
<https://github.com/MonetDB/MonetDBLite-Python>`_. This allows us to
use the expressive power of SQL and Python combined with MonetDB's
computational efficiency for analytic workloads.

This library was developed as part of `Marvin
<https://github.com/MonetDBSolutions/marvin_backend>`_, the MonetDB
profiler, but should be useful for any application that needs to
analyze performance characteristics of queries executed on MonetDB.

The MonetDB server translates SQL queries into MAL (MonetDB Assembly
Language) programs, and executes them. It emits profiling events at
the beginning and the end of the execution of each MAL instruction. By
capturing and parsing these events we are able to answer questions
like: "What is the most expensive operation in this query?", "Are the
allocated resources (RAM, CPU cores, etc), used efficiently by the
server?" etc.
