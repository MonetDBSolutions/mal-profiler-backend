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

.. _initiates_relation:

Initiates relation for MAL Executions
-------------------------------------

MAL executions may initiate other MAL executions. This happens in three
different ways:

.. _local_calls:

Local calls
###########

*Local calls*, are calls of MAL functions defined in the ``user`` module.

.. _remote_calls:

Remote calls
############

*Remote calls*, i.e. executions in different server sessions, initiated through
the remote table feature. The mechanism for associating executions in two
different MonetDB server sessions is the ``remote.register_supervisor`` MAL
instruction. This is a no-op instruction that takes two arguments. The plan
generation code of the MonetDB server, inserts a call of the
``remote.register_supervisor`` both in the local and the remote session, with
the same arguments. The first argument is the session id of the local session
and the second argument is a UUID generated just before the call.

When the parser sees a call to the ``remote.register_supervisor``, it searches
in an association table for the arguments. If it finds an entry, the association
is resolved and an entry in the ``initiates_executions`` SQL table is
(eventually) created. If it does not find an entry in the association table,
then it creates one. In other words the resolution of the call graph of remote
executions needs both the local and the remote plans to be processed by the
parser.

.. _execution_tree:

Execution tree
##############

Each query has one special execution, the root of the execution call tree. It
is the execution that runs the MAL instruction ``querylog.define``. We adopt the
convention that this execution initiates itself.

.. _mal_variables:

MAL Variables
^^^^^^^^^^^^^

MAL variables are scoped inside MAL executions. This means that different MAL
executions may define and use variables with the same name. The combination of
``execution_id``, and ``variable_name``, uniquely identify a variable in the
database.
