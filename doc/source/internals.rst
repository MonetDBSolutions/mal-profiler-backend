.. _internals:

Internals
=========

.. _data_structures:

Data Structures
---------------


The internal representation of the profiler information is expressed
using a number of Python data structures that map more or less directly
to the tables in the database. These are:

- ``event`` This is a Python dictionary that holds metadata about
  individual instructions in a MAL plan. In fact the MonetDB server
  emits two such events for each instruction: one at the beginning of
  execution and one at the end. The dictionary has the following keys:

    ``session``
      The MonetDB server UUID.

      This field is *always* present.

    ``tag``
      An increasing number, different for each query in a
      specific server. The combination of `session` and `tag`,
      uniquely identifies a query.

      This field is *always* present.

    ``pc (program counter)``
      An increasing number, different for each instruction in the plan
      of a query. This field is *always* present.

    ``execution_state``
      Shows the execution state of the instruction. It takes the
      following values:

      - 0: start
      - 1: done
      - 2: pause

      This field is *always* present.

    ``clk``
      Number of microseconds since server startup.

      This field is *always* present.

    ``ctime``
      Number of microseconds since server UNIX epoch.

      This field is *always* present.

    ``thread``
      Which thread executed this instruction.

      This field is *always* present.

    ``mal_function``
      The name and the module of the function the current MAL
      instruction belongs to.

    ``usec``
      Estimated Time of Completion for the current
      instruction. (*Note*: This is mostly unused, at least by
      Marvin). The value is valid only for ``execution_state == 1``.

    ``rss``
      An estimation of the Resident Set Size (in Megabytes) at the
      current moment. (*Note*: This is mostly unused, and maybe should
      be removed from the traces?).  The value is valid only for
      ``execution_state == 1``.

    ``size``
      An estimation for the size (in Megabytes) this instruction
      created. The value is valid only for ``execution_state == 1``.

    ``long_statement``
      The full text of the executed instruction.

    ``short_statement``
      A short version of the text of the executed instruction.

    ``instruction``
      The name of the instruction. For example ``thetaselect``,
      ``append``, etc.

    ``mal_module``
      The name of the MAL module this instruction belongs to. For
      example ``algebra``, ``bat``, etc.

    ``version`` **(after Jan2019 version)**
      The version of the server that produced the profiling trace.

- ``variable``
    ``type_id``
        The database identifier of the type of the variable (see ...).

    ``name``
        The name of the variable.

    ``alias``
        (???)

    ``is_persistent``
        If the variable is persistent ``True`` or intermediate ``False``.

    ``bid``
        BAT ID(???).

    ``count``
        If the variable refers to a BAT, how many elements are in the BAT.

    ``size``
        The size of the type (???).

    ``seqbase``
        (???)

    ``hghbase``
        (???)
	
    ``eol``
        If `True` then the variable can be garbage collected.

    ``mal_value``
        If the variable is scalar, this is its value.

.. _error_codes:

Error codes
-----------

============= ====================================
Error code    Meaning
------------- ------------------------------------
W001          Ignoring object that contains errors
============= ====================================
