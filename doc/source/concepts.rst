.. _concepts:

Concepts
========

This section describes briefly the most important concepts of the MAL Analytics
package, how they relate to each other and the most important ideas about their
storage in the database.

.. _mal_event:

MAL Event
^^^^^^^^^

In the context of MAL analytics a *MAL event* is the start or the end of the
execution of a MAL instruction. This maps one-to-one to JSON objects emitted by
the MonetDB Server, i.e. each JSON object defines a distinct event. The
combination of the attributes ``server_session``, ``tag``, ``pc`` and
``execution_state`` identify uniquely one MAL event in the database.

.. _mal_execution:

MAL Execution
^^^^^^^^^^^^^

A *MAL execution* is a group of MAL events that have been executed together as a
block. All these instructions have the same ``server_session`` and ``tag``
values (see :ref:`data_structures`). In other words, ``server_session`` and
``tag`` uniquely identify a MAL execution in the database. The combination of
``server_session`` and ``tag`` maps one-to-one to ``execution_id``.

Normally, each execution should have an instruction with ``pc == 0`` but this
assumption is violated for distributed queries (merge tables). This is probably
a bug of the MonetDB Server.

.. _mal_variables:

MAL Variables
^^^^^^^^^^^^^

MAL variables are scoped inside MAL executions. This means that different MAL
executions may define and use variables with the same name. The combination of
``execution_id``, and ``variable_name``, uniquely identify a variable in the
database.
