Internals
=========

The internal representation of the profiler information is expressed
using a number of Python data structures that map more or less directly
to the tables in the database. These are:

- ``event``
  This is a Python dictionary that holds metadata about individual
  instructions in a MAL plan. In fact the MonetDB server emits two
  such events for each instruction: one at the beginning of execution
  and one at the end. The dictionary has the following keys:

    ``session``
      The MonetDB server UUID.

    ``tag``
      An increasing number, different for each query in a
      specific server. The combination of `session` and `tag`,
      uniquely identifies a query.

    ``pc (program counter)``
      An increasing number, different for each instruction in the plan
      of a query.

    ``execution_state``
      Shows the execution state of the instruction. It takes the
      following values:

      - 0: start
      - 1: done
      - 2: pause

    ``clk``
      Number of microseconds since server startup.

    ``ctime``
      Number of microseconds since server UNIX epoch.

    ``thread``
      Which thread executed this instruction.

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

- ``variable``
